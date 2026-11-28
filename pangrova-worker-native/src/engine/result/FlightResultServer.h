// Authorizes and streams the exact Worker result attempt over Arrow Flight.
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <mutex>
#include <shared_mutex>
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
#include "engine/memory/ColumnarMemoryPool.h"
#include "pangrova/protocol/v2/result.pb.h"

#include "engine/result/ResultColumnType.h"
#include "engine/result/ResultOwnershipState.h"

namespace pangrova::result {

// ---------------------------------------------------------------------------
// detail:: — extracted helpers exposed for unit-testing (F-NATIVE-SHUF-01).
// ---------------------------------------------------------------------------
namespace detail {

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
  // Actual listener interface, independent of the advertised endpoint.
  std::string bind_host = "0.0.0.0";
  // Number of server threads for handling DoGet requests.
  int num_threads = 4;
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

// Arrow Flight server for the task-owned result payload queue.
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
class ResultPayloadQueue;

class FlightResultServer : public arrow::flight::FlightServerBase {
 public:
  struct TaskOutputInfo {
    facebook::velox::RowTypePtr output_type;
    // Derived once from the admitted canonical task schema, never from labels
    // or the SDK's nullable physical-schema defaults.
    std::shared_ptr<arrow::Schema> wire_schema;
    std::shared_ptr<arrow::Schema> result_schema;
    std::vector<ResultColumnVectorDimension> result_column_types;
    std::vector<std::string> column_labels;
    std::shared_ptr<memory::ColumnarMemoryPool> pool;
    std::shared_ptr<ResultOwnershipState> ownership;
    std::shared_ptr<ResultPayloadQueue> queue;
    protocol::v2::ExecutionCallbackContext authority;
    // A streaming failure is terminal for the exact Native task before the
    // reader releases its credits and unblocks suspended Drivers.
    std::function<void(const std::string&)> report_stream_failure;
    std::function<void(const protocol::v2::NativeResultDeliveryStats&)> report_stream_finished;
    bool ready = false;
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

  // Registers the exact result schema and owned delivery queue before Driver start.
  static void registerTaskOutputType(
      const std::string& task_id,
      TaskOutputInfo info);

  // Activates the registration after Velox has successfully started its Drivers.
  static bool markTaskOutputReady(const std::string& task_id);

  // Removes the cached output type for a task (called during task cleanup).
  static void unregisterTaskOutputType(const std::string& task_id,
      const std::shared_ptr<ResultOwnershipState>& owner);

  // Returns physical and logical result types in one atomic cache lookup.
  static std::optional<TaskOutputInfo> lookupTaskOutputInfo(
      const std::string& task_id);

  // Checks the complete immutable grant before claiming the result reader.
  arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> openTaskOutput(
      const protocol::v2::ResultReadGrant& grant,
      const arrow::flight::ServerCallContext* context = nullptr);

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
  std::mutex lifecycle_mutex_;
  bool initialized_ = false;
  bool transport_stopped_ = false;

  // F-NATIVE-SHUF-08: Per-instance caches instead of process-global statics.
  // Guarded by their respective mutexes; accessed via instance().
  mutable std::mutex output_type_mutex_;
  std::unordered_map<std::string, TaskOutputInfo> output_type_cache_;
  static std::atomic<FlightResultServer*> instance_;
  // Pins the actual service during short registration operations. Never held
  // over network shutdown, joins or output-buffer callbacks.
  static std::shared_mutex instance_mutex_;
};

}  // namespace pangrova::result
