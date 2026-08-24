// FlightShuffleServer: Arrow Flight server that multiplexes shuffle data
// from multiple executor handles on a single port per worker.
//
// Uses a HandleRouter to map executor handles to their PartitionDemuxer,
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
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include <arrow/flight/server.h>

#include "shuffle/SerializedShuffleQueue.h"
#include "shuffle/TicketVerifier.h"

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
  int64_t max_batch_bytes = 4 * 1024 * 1024; // 4MB
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
// Uses per-partition mutexes to reduce contention when multiple DoGet
// streams read different partitions concurrently.
class PartitionDemuxer {
 public:
  explicit PartitionDemuxer(SerializedShuffleQueue* source);
  ~PartitionDemuxer();

  // Non-copyable, non-movable.
  PartitionDemuxer(const PartitionDemuxer&) = delete;
  PartitionDemuxer& operator=(const PartitionDemuxer&) = delete;

  // Starts the background drain thread.
  void start();

  // Stops the drain thread and marks all partition buffers as closed.
  void stop();

  // Pops a batch for the given partition. Blocks up to timeout.
  // Returns true if a batch was obtained, false on timeout or closed.
  bool pop(int32_t partition_id, ShuffleBatch& batch,
           std::chrono::milliseconds timeout);

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

  // Returns true if a partition has been drained AND the source was NOT
  // cleanly completed (indicates truncation / forced teardown).
  bool isPartitionTruncated(int32_t partition_id);

  // --- F-CHAIN-SHUFFLE-04: Backpressure ---

  // Sets the maximum number of buffered batches per partition.
  // 0 (default) means unbounded (legacy behavior).
  void setMaxBufferedBatchesPerPartition(size_t cap);

  // Returns the current number of buffered batches for a partition.
  size_t bufferedBatchCount(int32_t partition_id);

  // Returns the total number of batches committed (dispatched) to a partition.
  int64_t committedBatchCount(int32_t partition_id);

  // --- F-CHAIN-SHUFFLE-09: Partition bound ---

  // Sets the maximum allowed partition count. partition_id must be in
  // [0, max_partitions). 0 means unbounded (legacy).
  void setMaxPartitions(int32_t max_partitions);

 private:
  void drainLoop();

  // Per-partition state with its own mutex for reduced contention.
  struct PartitionBuffer {
    std::mutex mutex;
    std::condition_variable cv;
    std::deque<ShuffleBatch> batches;
    int64_t committed_count = 0;
    int64_t popped_count = 0;
    // Approximate queue depth readable without holding mutex, used by
    // the backpressure predicate to avoid buf.mutex → backpressure_mutex_
    // lock-order inversion (F-NATIVE-02).
    std::atomic<size_t> approx_size{0};
  };

  PartitionBuffer& getOrCreatePartition(int32_t partition_id);

  SerializedShuffleQueue* source_;
  std::atomic<bool> running_{false};
  std::atomic<bool> source_closed_{false};
  std::atomic<bool> source_complete_{false};  // clean producer close
  std::thread drain_thread_;

  // Partition registry protected by registry_mutex_ (only for creation).
  std::mutex registry_mutex_;
  std::unordered_map<int32_t, std::unique_ptr<PartitionBuffer>> partitions_;

  // Backpressure: cap per partition (0 = unbounded).
  std::atomic<size_t> max_buffered_per_partition_{0};
  // Condition variable for drain thread to wait when a partition is full.
  std::condition_variable backpressure_cv_;
  std::mutex backpressure_mutex_;

  // Partition bound (0 = unbounded).
  std::atomic<int32_t> max_partitions_{0};

  // Quiesce synchronization.
  std::mutex quiesce_mutex_;
  std::condition_variable quiesce_cv_;
  std::atomic<bool> drain_exited_{false};
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
// ---------------------------------------------------------------------------
// LOCK ACQUISITION ORDER (Task 27.5, updated F-NATIVE-02).
// ---------------------------------------------------------------------------
// Three mutex families are used across this file. To avoid deadlocks, locks
// MUST be acquired in the order listed below; any code path that needs an
// inner lock while already holding an outer one is fine, but never the
// reverse.
//
//   1. FlightShuffleServer::handles_mutex_                  (outermost)
//      Protects handle_demuxers_. Acquired by registerHandle(),
//      unregisterHandle(), handleCount(), and DoGet() during routing.
//
//   2. PartitionDemuxer::registry_mutex_                    (per-demuxer)
//      Protects partitions_ map of a single PartitionDemuxer. Acquired by
//      getOrCreatePartition(), stop(), drainLoop() teardown, and
//      isPartitionDrained().
//
//   3. PartitionDemuxer::PartitionBuffer::mutex             (innermost)
//      Protects an individual partition's deque + cv. May be taken nested
//      under registry_mutex_ when notifying all partitions on shutdown.
//
//   backpressure_mutex_ is INDEPENDENT — never nested under buf.mutex or
//   vice versa. The backpressure predicate reads PartitionBuffer::approx_size
//   (atomic) instead of locking buf.mutex, preventing the lock-order inversion
//   that existed before F-NATIVE-02 was fixed.
//
// Invariants:
//   - DoGet() never holds handles_mutex_ while calling into a demuxer; it
//     copies the demuxer pointer under the lock and drops it before reading.
//   - unregisterHandle() stops a demuxer OUTSIDE handles_mutex_ to avoid
//     holding the server-wide lock during a thread join.
//   - PartitionDemuxer never acquires handles_mutex_ — flow is one way only.
//   - pop() releases buf.mutex before notifying backpressure_cv_.
//   - drainLoop() backpressure predicate reads approx_size (atomic), never
//     acquires buf.mutex while holding backpressure_mutex_.
//
// Adding a new mutex requires extending this ordering and updating call
// sites; do not introduce cross-family locking outside this hierarchy.
class FlightShuffleServer : public arrow::flight::FlightServerBase {
 public:
  explicit FlightShuffleServer(FlightShuffleServerConfig config);
  ~FlightShuffleServer() override;

  // Non-copyable, non-movable.
  FlightShuffleServer(const FlightShuffleServer&) = delete;
  FlightShuffleServer& operator=(const FlightShuffleServer&) = delete;

  int start();
  void stop();

  // Register an executor handle with its shuffle queue for multiplexing.
  // Creates a PartitionDemuxer for the queue and routes DoGet requests
  // to the correct demuxer based on the handle in the ticket.
  void registerHandle(int64_t handle, SerializedShuffleQueue* queue);

  // Extended registration with query/stage identity and partition count.
  void registerHandle(int64_t handle, SerializedShuffleQueue* queue,
                      const std::string& query_id, int32_t stage_id,
                      int32_t num_partitions);

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
  size_t evictFullyConsumedLingering();

  // Pop a batch from a (possibly lingering) handle's demuxer.
  // Used by tests and internally by DoGet readers.
  bool popForHandle(int64_t handle, int32_t partition_id,
                    ShuffleBatch& batch, std::chrono::milliseconds timeout);

  int boundPort() const { return bound_port_; }
  bool isRunning() const { return running_.load(); }

  arrow::Status DoGet(
      const arrow::flight::ServerCallContext& context,
      const arrow::flight::Ticket& request,
      std::unique_ptr<arrow::flight::FlightDataStream>* stream) override;

 private:
  FlightShuffleServerConfig config_;
  int bound_port_ = 0;
  std::atomic<bool> running_{false};
  std::thread server_thread_;

  // Ticket HMAC verification (null when hmac_current_key is empty).
  std::unique_ptr<TicketVerifier> ticket_verifier_;

  // Multiplexed handle routing: handle -> PartitionDemuxer.
  mutable std::mutex handles_mutex_;
  std::unordered_map<int64_t, std::unique_ptr<PartitionDemuxer>> handle_demuxers_;
  // Ticket ownership: handle -> query_id for cross-query data leak prevention.
  std::unordered_map<int64_t, std::string> handle_query_ids_;

  // Lingering handles: producer done but data not fully consumed.
  struct LingeringEntry {
    std::unique_ptr<PartitionDemuxer> demuxer;
    std::chrono::steady_clock::time_point entered_at;
  };
  mutable std::mutex lingering_mutex_;
  std::unordered_map<int64_t, LingeringEntry> lingering_demuxers_;

  // Background evictor: periodically reaps fully-consumed / TTL-expired
  // lingering demuxers so a client that disconnects without draining DoGet
  // cannot leak demuxers unboundedly (F-SHUF-LINGER). Only touches
  // lingering_mutex_ via evictFullyConsumedLingering(), so it stays outside
  // the handles_mutex_ / registry_mutex_ / buf.mutex lock hierarchy.
  std::thread lingering_evictor_thread_;
  std::mutex evictor_mutex_;
  std::condition_variable evictor_cv_;
};

}  // namespace pangrova::shuffle
