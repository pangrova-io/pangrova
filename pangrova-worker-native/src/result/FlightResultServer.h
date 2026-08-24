// FlightResultServer: Arrow Flight server that pulls pre-serialized
// SerializedPage from Velox's OutputBufferManager and serves them
// to the coordinator via Arrow Flight DoGet.
//
// No custom queue needed — Velox's PartitionedOutputNode serializes
// output via PrestoVectorSerde and routes it through OutputBufferManager.
// This server pulls from OutputBufferManager::getData() directly.
//
// Placement: result/ — Native result output path components.
// Aligned with Prestissimo architecture (same Velox components, Flight transport).
#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

#include <arrow/flight/server.h>
#include <arrow/result.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include <velox/type/Type.h>

namespace pangrova::result {

// ---------------------------------------------------------------------------
// detail:: — extracted helpers exposed for unit-testing (F-NATIVE-SHUF-01).
// ---------------------------------------------------------------------------
namespace detail {

// Fetch-wait decision policy: determines whether to keep waiting, time out, or
// report a dead producer.
enum class FetchWaitDecision {
  kKeepWaiting,
  kTimedOut,
  kProducerDead,
};

// Evaluates the fetch-wait policy.
//   alive: whether the producing task is still alive.
//   elapsed_seconds: seconds since the fetch started.
//   timeout_seconds: configured deadline (<=0 means unbounded).
FetchWaitDecision decideFetchWait(
    bool alive,
    int64_t elapsed_seconds,
    int64_t timeout_seconds);

// Parses a page buffer of [4-byte length | Arrow IPC RecordBatch] frames into
// OWNING RecordBatches. Each batch copies its frame data so it remains valid
// after the source buffer is freed — this is the fix for F-NATIVE-SHUF-01.
arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>>
parseArrowBatchFrames(
    const std::shared_ptr<arrow::Schema>& schema,
    const uint8_t* data,
    int64_t length,
    const std::string& task_id);

}  // namespace detail

// Configuration for the Flight result server.
struct FlightResultServerConfig {
  // Port to bind the Flight server (0 = auto-assign).
  int port = 0;
  // Number of server threads for handling DoGet requests.
  int num_threads = 4;
  // Maximum bytes per getData() call to OutputBufferManager.
  int64_t max_bytes_per_response = 4 * 1024 * 1024; // 4MB
  // TLS certificate chain file path (PEM). Empty = no TLS.
  std::string tls_cert_chain_path;
  // TLS private key file path (PEM). Required when tls_cert_chain_path is set.
  std::string tls_private_key_path;
  // TLS root CA certificate path (PEM) for client verification (mTLS).
  // Empty = no client certificate verification.
  std::string tls_root_ca_path;
  // F-NATIVE-SHUF-05: Configurable timeout for result fetch (seconds).
  // Default 300s (5 min) to accommodate long-running sparse-output queries.
  // Set <=0 for unbounded (rely on task liveness check only).
  int64_t result_fetch_timeout_seconds = 300;
};

// Arrow Flight server that serves query result data from Velox OutputBufferManager.
//
// Lifecycle:
//   1. Construct with config
//   2. start() — binds port, begins accepting DoGet requests
//   3. Coordinator calls DoGet with ResultTicket (taskId string)
//   4. stop() — graceful shutdown, drains in-flight requests
//
// Per-worker singleton, started once at bootstrap via JNI command.
// No task registry needed — OutputBufferManager is Velox's global singleton
// that tracks all tasks by taskId automatically.
//
// Thread safety: start/stop are serialized via NativeCommandDispatcher
// (single command channel per executor handle). DoGet handlers are
// thread-safe (Arrow Flight server manages concurrency).
class FlightResultServer : public arrow::flight::FlightServerBase {
 public:
  explicit FlightResultServer(FlightResultServerConfig config);

  ~FlightResultServer() override;

  // Non-copyable, non-movable.
  FlightResultServer(const FlightResultServer&) = delete;
  FlightResultServer& operator=(const FlightResultServer&) = delete;

  // Starts the Flight server. Returns the actual bound port.
  // Throws on bind failure.
  int start();

  // Stops the server gracefully, waiting for in-flight requests to complete.
  void stop();

  // Returns the port the server is listening on (valid after start()).
  int boundPort() const { return bound_port_; }

  // Returns true if the server is running.
  bool isRunning() const { return running_.load(); }

  // Registers the output RowType for a task so DoGet can deserialize results
  // even after the Velox task has completed and left getRunningTasks().
  static void registerTaskOutputType(
      const std::string& task_id,
      facebook::velox::RowTypePtr output_type);

  // Removes the cached output type for a task (called during task cleanup).
  static void unregisterTaskOutputType(const std::string& task_id);

  // Look up the output RowType for a task — first from instance cache, then
  // from running tasks. Used by DoGet and DirectBatchStreamReader.
  // F-NATIVE-SHUF-08: moved from anonymous namespace to allow access to
  // instance members.
  static facebook::velox::RowTypePtr lookupTaskOutputType(
      const std::string& task_id);

  // Singleton instance accessor. Set during start(), cleared during stop().
  static FlightResultServer* instance();

  // Registers the serde mode used by a result task's PartitionedOutputNode.
  // DoGet uses this to determine whether pages are Arrow IPC (direct stream)
  // or PrestoVectorSerde (deserialize + re-export).
  static void registerTaskSerdeMode(
      const std::string& task_id,
      const std::string& serde_mode);

  // Removes the cached serde mode for a task.
  static void unregisterTaskSerdeMode(const std::string& task_id);

  // Returns the serde mode for a task ("presto" or "arrow_batch").
  // Returns "presto" if not registered (safe default).
  static std::string lookupTaskSerdeMode(const std::string& task_id);

  // Arrow Flight DoGet override.
  // Ticket format: taskId string (Velox OutputBufferManager key).
  //
  // DoGet implementation:
  //   1. Parse Ticket → taskId
  //   2. Look up serde mode for the task (Arrow IPC or PrestoVectorSerde)
  //   3a. Arrow IPC mode: stream pre-serialized RecordBatch frames directly
  //   3b. Legacy mode: deserialize PrestoVectorSerde → Arrow C Data Interface export
  //   4. Acknowledge consumed pages via OutputBufferManager::acknowledge()
  //   5. Signal end-of-stream when OutputBuffer has no more data
  arrow::Status DoGet(
      const arrow::flight::ServerCallContext& context,
      const arrow::flight::Ticket& request,
      std::unique_ptr<arrow::flight::FlightDataStream>* stream) override;

 private:
  FlightResultServerConfig config_;
  int bound_port_ = 0;
  std::atomic<bool> running_{false};
  std::thread server_thread_;

  // F-NATIVE-SHUF-08: Per-instance caches instead of process-global statics.
  // Guarded by their respective mutexes; accessed via instance().
  mutable std::mutex output_type_mutex_;
  std::unordered_map<std::string, facebook::velox::RowTypePtr> output_type_cache_;
  mutable std::mutex serde_mode_mutex_;
  std::unordered_map<std::string, std::string> serde_mode_cache_;
  static std::atomic<FlightResultServer*> instance_;
};

}  // namespace pangrova::result
