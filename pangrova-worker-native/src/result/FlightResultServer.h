// FlightResultServer: Arrow Flight server that pulls pre-serialized
// SerializedPage from Velox's OutputBufferManager and serves them
// to the authorized client-facing data-plane consumer via Arrow Flight DoGet.
//
// Velox's PartitionedOutputNode serializes one canonical Arrow stream per
// payload and routes it through OutputBufferManager.
// This server pulls from OutputBufferManager::getData() directly.
//
// Placement: result/ — Native result output path components.
#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <arrow/flight/server.h>
#include <arrow/result.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include <velox/type/Type.h>
#include "memory/ColumnarMemoryPool.h"
#include "pangrova/protocol/v2/result.pb.h"

#include "result/ResultColumnType.h"
#include "result/ResultOwnershipState.h"

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

// Copies a borrowed source into the actual task allocator, then decodes every
// batch through the sole canonical stream codec and validates its exact schema.
arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>>
parseArrowBatchFrames(
    const std::shared_ptr<arrow::Schema>& schema,
    const uint8_t* data,
    int64_t length,
    const std::shared_ptr<memory::ColumnarMemoryPool>& pool);

// Adds exact logical type metadata to fields whose physical storage matches
// the validated result contract. Leaves ordinary lists untouched.
arrow::Result<std::shared_ptr<arrow::Schema>> withResultColumnTypes(
    const std::shared_ptr<arrow::Schema>& schema,
    const std::vector<ResultColumnVectorDimension>& result_column_types);

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
  // Set <=0 to use the finite admitted result-authority expiry only.
  int64_t result_fetch_timeout_seconds = 300;
};

// Arrow Flight server that serves query result data from Velox OutputBufferManager.
//
// Lifecycle:
//   1. Construct with config
//   2. start() — binds port, begins accepting DoGet requests
//   3. Authorized client calls DoGet with producer-issued scoped authority
//   4. stop() — graceful shutdown, drains in-flight requests
//
// Per-worker singleton, started once at bootstrap via JNI command.
// Live registrations bind exact attempt/security/schema/allocator ownership.
//
// Thread safety: start/stop are serialized via NativeCommandDispatcher
// (single command channel per executor handle). DoGet handlers are
// thread-safe (Arrow Flight server manages concurrency).
class FlightResultServer : public arrow::flight::FlightServerBase {
 public:
  struct TaskOutputInfo {
    facebook::velox::RowTypePtr output_type;
    // Derived once from the admitted canonical task schema, never from labels
    // or the SDK's nullable physical-schema defaults.
    std::shared_ptr<arrow::Schema> wire_schema;
    std::vector<ResultColumnVectorDimension> result_column_types;
    std::vector<std::string> column_labels;
    std::shared_ptr<memory::ColumnarMemoryPool> pool;
    std::shared_ptr<ResultOwnershipState> ownership;
    protocol::v2::ExecutionCallbackContext authority;
    bool claimed = false;
  };

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
      TaskOutputInfo info);

  // Removes the cached output type for a task (called during task cleanup).
  static void unregisterTaskOutputType(const std::string& task_id);

  // Returns physical and logical result types in one atomic cache lookup.
  static std::optional<TaskOutputInfo> lookupTaskOutputInfo(
      const std::string& task_id);

  static arrow::Result<TaskOutputInfo> claimTaskOutputInfo(
      const protocol::v2::ResultReadGrant& grant);

  // Singleton instance accessor. Set during start(), cleared during stop().
  static FlightResultServer* instance();

  // Arrow Flight DoGet override.
  // Authenticate the scoped grant and claim its exact live result registration;
  // consume typed Arrow batches and require explicit producer EOF evidence.
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
  std::unordered_map<std::string, TaskOutputInfo> output_type_cache_;
  static std::atomic<FlightResultServer*> instance_;
};

}  // namespace pangrova::result
