// FlightShuffleServer: Arrow Flight server that multiplexes shuffle data
// from multiple executor handles on a single port per worker.
//
// Maps executor handles to their actual shared producer registration,
// enabling all shuffle handles to share one Flight server instance.
// Configures SO_REUSEPORT on the server socket to allow kernel-level
// load balancing across threads.
//
// Placement: shuffle/ — Native shuffle components.
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <arrow/flight/server.h>

#include "engine/shuffle/SerializedShuffleQueue.h"
#include "engine/shuffle/ShuffleBufferStore.h"
#include "engine/shuffle/ShuffleControlGate.h"
#include "engine/shuffle/ShuffleProducerRegistration.h"
#include "engine/shuffle/TicketVerifier.h"

namespace pangrova::shuffle {

// ---------------------------------------------------------------------------
// detail:: — extracted helpers exposed for unit-testing.
// ---------------------------------------------------------------------------
namespace detail {

// Validates that a ticket's claimed identity (query_id, stage_id) matches the
// identity recorded when the shuffle handle was registered. Prevents cross-query
// handle reuse (F-CHAIN-SHUFFLE-06).
bool ticketOwnershipValid(
    const std::string& ticket_query_id,
    int32_t ticket_stage_id,
    const std::string& registered_query_id,
    int32_t registered_stage_id);

}  // namespace detail

// Configuration for the Flight shuffle server.
struct FlightShuffleServerConfig {
  // Port to bind the Flight server (0 = auto-assign).
  int port = 0;
  // Number of server threads for handling DoGet requests.
  int num_threads = 4;
  // Maximum batch size in bytes for a single Flight response.
  int64_t max_batch_bytes = static_cast<int64_t>(kCanonicalShuffleBatchBytes);
  // Shared production demux budgets; max_payload_bytes is set from
  // max_batch_bytes so each indivisible serialized batch fits one response.
  ShuffleBufferLimits buffer_limits;
  // Enable SO_REUSEPORT on the server socket.
  bool reuse_port = true;
  // TLS certificate chain file path (PEM). Empty = no TLS.
  std::string tls_cert_chain_path;
  // TLS private key file path (PEM). Required when tls_cert_chain_path is set.
  std::string tls_private_key_path;
  // TLS root CA certificate path (PEM) for client verification (mTLS).
  // Empty = no client certificate verification.
  std::string tls_root_ca_path;
  // HMAC key for ticket verification (raw bytes). Empty = verification disabled.
  std::string hmac_current_key;
  // Previous HMAC key for rotation window. Empty = no rotation in progress.
  std::string hmac_previous_key;
  // TTL for lingering demuxers (milliseconds). Demuxers that linger beyond
  // this deadline are evicted regardless of consume state by the background
  // evictor thread (F-SHUF-LINGER). A default of 0 disabled TTL eviction and,
  // combined with eviction only firing on registerHandle(), let a client that
  // disconnected without draining DoGet leak demuxers unboundedly (OOM). The
  // default is now 5 min so slow/abandoned consumers are proactively reaped.
  int64_t lingering_ttl_ms = 300'000;
};

// Demuxes batches from a single SerializedShuffleQueue to per-partition
// buffers. A background thread drains the source queue and dispatches
// each ShuffleBatch to the buffer matching its partition_id.
//
// @ThreadSafe — all public methods are thread-safe.
// A shared state mutex protects metadata only; no lock is held during spill I/O.
class PartitionDemuxer {
 public:
  explicit PartitionDemuxer(
      SerializedShuffleQueue* source,
      std::shared_ptr<ShuffleBufferStore> store =
          std::make_shared<ShuffleBufferStore>(),
      bool disk_only = false);
  ~PartitionDemuxer();

  // Non-copyable, non-movable.
  PartitionDemuxer(const PartitionDemuxer&) = delete;
  PartitionDemuxer& operator=(const PartitionDemuxer&) = delete;

  // Starts the background drain thread.
  void start();

  // Cancels unconsumed data, wakes waiters, joins the drain thread and releases
  // storage. The borrowed source must live until quiesce()/stop() returns;
  // after quiesce observes completion it is never accessed again.
  void stop();

  // First terminal error, including forced teardown. Never converted to EOF.
  arrow::Status status() const;
  bool validPartition(int32_t partition_id) const;

  // Pops a batch for the given partition. Blocks up to timeout.
  // Returns true if a batch was obtained, false on timeout or closed.
  bool pop(int32_t partition_id, ShuffleBatch& batch,
           std::chrono::milliseconds timeout,
           const std::function<bool()>& reader_cancelled = {});

  // Returns true if the source queue is closed and the partition buffer
  // for the given partition_id is empty.
  bool isPartitionDrained(int32_t partition_id);

  // --- F-CHAIN-SHUFFLE-01: Consume-then-release barrier ---

  // Bounded wait for the drain thread to observe a clean producer close
  // (source queue returns CLOSED). Returns true if clean close was observed;
  // returns false and force-stops if timeout expires without clean close.
  bool quiesce(std::chrono::milliseconds timeout);

  // Returns true if source queue was cleanly closed by the producer.
  bool isComplete() const;

  // Returns true if all dispatched batches have been consumed by pop().
  bool isFullyConsumed() const;

  // Records a clean EOF delivered to a reader for one partition. An empty
  // partition has no batches to consume, but its late reader must still be
  // able to open the producer's ticket after the producer has exited.
  bool markPartitionReadComplete(int32_t partition_id);

  // True only after every advertised partition has delivered a clean EOF.
  bool allPartitionsReadComplete() const;

  // Returns true if a partition has been drained AND the source was NOT
  // cleanly completed (indicates truncation / forced teardown).
  bool isPartitionTruncated(int32_t partition_id);

  // Returns the current number of buffered batches for a partition.
  size_t bufferedBatchCount(int32_t partition_id);

  // Returns the total number of batches committed (dispatched) to a partition.
  int64_t committedBatchCount(int32_t partition_id);

  // --- F-CHAIN-SHUFFLE-09: Partition bound ---

  // Sets the maximum allowed partition count. partition_id must be in
  // [0, max_partitions). Must be positive and within the store's limit.
  // Configure before start().
  void setMaxPartitions(int32_t max_partitions);

 private:
  void drainLoop();

  void fail(arrow::Status error);
  void finishDrain(bool complete);

  // Protected by mutex_. Disk reads/writes and entry destruction happen only
  // after releasing it. A reading flag serializes same-partition replay without
  // holding a mutex during I/O; other partitions remain independent.
  struct PartitionBuffer {
    std::deque<std::unique_ptr<ShuffleBufferStore::Entry>> batches;
    int64_t committed_count = 0;
    bool reading = false;
  };

  SerializedShuffleQueue* source_;
  const std::shared_ptr<ShuffleBufferStore> store_;
  // Spill segments belong to this handle so its own reader can release them.
  ShuffleBufferStore::HandleScope spill_scope_;
  std::shared_ptr<ShuffleBufferStore::Reservation> handle_reservation_;
  std::atomic<bool> running_{false};
  CancellationSource cancellation_;
  std::thread drain_thread_;
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  std::unordered_map<int32_t, PartitionBuffer> partitions_;
  arrow::Status status_;
  bool source_closed_ = false;
  bool source_complete_ = false;
  bool drain_exited_ = false;
  int32_t max_partitions_;
  std::vector<bool> completed_partitions_;
  // Serialize concurrent stop calls, without holding a mutex across join/I/O.
  std::mutex stop_mutex_;
  std::condition_variable stop_cv_;
  bool stopping_ = false;
  bool stopped_ = false;
};

// Multiplexed Arrow Flight server that serves shuffle data from multiple
// executor handles on a single port per worker.
//
// Lifecycle:
//   1. Construct with config
//   2. start() — binds port with SO_REUSEPORT, begins accepting DoGet
//   3. registerHandle() — register executor handle with its queue
//   4. Downstream workers call DoGet with ticket containing handle routing
//   5. unregisterHandle() — remove handle when task completes
//   6. stop() — graceful shutdown, drains in-flight requests
//
// @ThreadSafe — registerHandle/unregisterHandle/DoGet are all thread-safe.
//
// Lock order: routing maps -> demux state. The store accounting lock is never
// held during I/O; demux entries and routing owners are destroyed outside locks.
// Demuxers never acquire routing locks. No lock is held across drain-thread join.
class FlightShuffleServer : public arrow::flight::FlightServerBase {
 public:
  explicit FlightShuffleServer(FlightShuffleServerConfig config);
  ~FlightShuffleServer() override;

  // Non-copyable, non-movable.
  FlightShuffleServer(const FlightShuffleServer&) = delete;
  FlightShuffleServer& operator=(const FlightShuffleServer&) = delete;

  int start();
  void stop();
  ShuffleControlGate::Lease acquireControl() { return control_gate_.acquire(); }

  // Register the exact fenced producer identity and partition count.
  std::shared_ptr<ShuffleProducerRegistration> registerHandle(
                      int64_t handle, SerializedShuffleQueue* queue,
                      const std::string& query_id, int32_t stage_id,
                      int32_t num_partitions,
                      uint64_t plan_epoch,
                      uint32_t attempt_id,
                      uint64_t fencing_token, bool disk_only = false);

  // Unregister an executor handle. Quiesces/stops its demuxer.
  // Clean producers linger; un-clean producers are force-dropped.
  void unregisterHandle(int64_t handle);

  // Returns the number of currently registered (active) handles.
  size_t handleCount() const;

  // Returns the number of lingering handles (producer done, data unconsumed).
  size_t lingeringHandleCount() const;

  // Evicts fully-consumed lingering demuxers, returning the count removed.
  // Should be called periodically to prevent resource leaks from client
  // disconnections where DoGet never completes.
  size_t evictFullyConsumedLingering(
      std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now());

  // Pop a batch from a (possibly lingering) handle's demuxer. The real Flight
  // reader uses the demuxer directly; this path models its clean EOF in tests.
  bool popForHandle(int64_t handle, int32_t partition_id,
                    ShuffleBatch& batch, std::chrono::milliseconds timeout);

  // Resource accounting includes active and lingering handles.
  ShuffleBufferUsage bufferUsage() const { return buffer_store_->usage(); }

  int boundPort() const { return bound_port_; }
  bool isRunning() const { return running_.load(); }

  // Atomically updates the verifier key window without restarting the server.
  void setHmacKeys(std::string current_key, std::string previous_key);
  std::string signingKey() const;

  arrow::Status DoGet(
      const arrow::flight::ServerCallContext& context,
      const arrow::flight::Ticket& request,
      std::unique_ptr<arrow::flight::FlightDataStream>* stream) override;

 private:
  void stopInternal();
  FlightShuffleServerConfig config_;
  std::shared_ptr<ShuffleBufferStore> buffer_store_;
  int bound_port_ = 0;
  std::atomic<bool> running_{false};
  std::thread server_thread_;

  // Ticket HMAC verification (null when hmac_current_key is empty).
  mutable std::mutex ticket_verifier_mutex_;
  std::shared_ptr<TicketVerifier> ticket_verifier_;
  std::string ticket_signing_key_;

  // Multiplexed handle routing. Shared ownership is required because an
  // in-flight Flight reader may outlive producer unregister.
  mutable std::mutex handles_mutex_;
  std::unordered_map<int64_t, std::shared_ptr<ShuffleProducerRegistration>> handle_routes_;
  ShuffleControlGate control_gate_;
  std::mutex stop_mutex_;
  std::condition_variable stop_cv_;
  bool stopping_ = false;
  bool stopped_ = false;

  // Eviction snapshots route owners, closes outside routing locks and removes
  // only that same closed owner. Readers retain their actual demuxer credits.
  std::thread lingering_evictor_thread_;
  std::mutex evictor_mutex_;
  std::condition_variable evictor_cv_;
};

}  // namespace pangrova::shuffle
