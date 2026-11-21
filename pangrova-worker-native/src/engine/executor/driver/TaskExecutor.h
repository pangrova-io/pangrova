// TaskExecutor: Orchestrator composing TaskLifecycle + PlanExecutionDriver +
// TaskOutputRouter. Public API unchanged — JniEntryPoints is unaware of the split.
#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include "engine/shuffle/ShuffleControlGate.h"
#include "engine/shuffle/ShuffleProducerRegistration.h"

#include "engine/executor/driver/TaskFailureReporter.h"
#include "engine/split/SplitConfig.h"
#include "engine/executor/driver/PlanExecutionDriver.h"
#include "engine/executor/driver/ReturnCodes.h"
#include "engine/executor/driver/TaskLifecycle.h"
#include "engine/executor/driver/TaskOutputRouter.h"
#include "engine/executor/queue/OutputQueue.h"
#include "engine/shuffle/SerializedShuffleQueue.h"
#include "engine/result/ResultColumnType.h"

#include "engine/observability/NativeMetricsCollector.h"
#include "capabilities/artifact/ArtifactWorkReportState.h"
#include "pangrova/protocol/v2/task.pb.h"
#include "velox/vector/ComplexVector.h"

namespace pangrova::velox {
class VeloxTaskRunner;
}  // namespace pangrova::velox

namespace facebook::velox::exec {
class FunctionRegistrySnapshot;
}

namespace pangrova::shuffle {
class ShuffleEgressExecutor;
}  // namespace pangrova::shuffle

namespace pangrova::executor {

// Resource limits extracted from v2 TaskRequest.StageResourceGrant.
struct ResourceLimitConfig {
  int64_t max_memory_bytes = 0;
  // Hand-built Native tests use this bounded value; canonical TaskRequests
  // must provide an explicit grant before a shuffle executor is admitted.
  int64_t max_shuffle_buffer_bytes = 16 * 1024 * 1024;
  int32_t max_threads = 0;
  int64_t max_spill_bytes = 0;
  int64_t max_candidate_rows = 0;
  int64_t max_candidate_bytes = 0;
  int64_t max_object_store_read_bytes = 0;
  int64_t max_object_store_write_bytes = 0;
  int32_t max_provider_concurrency = 0;
  int64_t max_provider_items = 0;
  int64_t max_provider_attempts = 0;
  int64_t max_provider_request_bytes = 0;
  int64_t max_provider_response_bytes = 0;
  int64_t max_media_bytes = 0;
};

// Write configuration forwarded via v2 TaskExecutionOptions.WriteTaskOptions.
struct WriteConfigInfo {
  struct LanceOutputBinding {
    std::string dataset_uri;
    pangrova::protocol::v2::LanceWriteOperation operation =
        pangrova::protocol::v2::LANCE_WRITE_OPERATION_UNSPECIFIED;
    uint64_t expected_read_version = 0;
    std::string accepted_base_snapshot;
    std::array<uint8_t, 32> schema_fence_sha256{};
    std::string write_id;
    std::string transaction_staging_prefix;
    std::string generation;
    std::vector<std::string> write_column_names;
    // Deterministically serialized canonical TaskAttemptId.
    std::vector<uint8_t> task_attempt;
    std::string storage_binding;
  };

  struct PhysicalOutputBinding {
    pangrova::protocol::v2::PhysicalFileWriteBinding binding;
    // Deterministically serialized complete canonical TaskAttemptId.
    std::vector<uint8_t> task_attempt;
    std::string storage_binding;
  };

  std::string target_table;
  std::string write_mode;
  std::vector<std::string> partition_spec;
  std::string transaction_id;
  split::FileFormatType file_format = split::FileFormatType::PARQUET;
  int64_t target_file_size = 0;
  std::optional<LanceOutputBinding> lance_output;
  std::optional<PhysicalOutputBinding> physical_output;
};

// A canonical v2 shuffle service endpoint after task admission.
struct ShuffleServiceEndpointConfig {
  std::string host;
  int32_t port = 0;
  int32_t partition_start = 0;
  int32_t partition_end = 0;
  // Empty selects a plaintext endpoint; otherwise this is the expected TLS
  // peer identity and must be passed unchanged to the data-plane client.
  std::string tls_identity;
};

// Coordinator-allocated remote shuffle identity bound to one task attempt.
struct ShuffleExecutionConfig {
  std::string shuffle_id;
  std::vector<ShuffleServiceEndpointConfig> endpoints;
  uint64_t plan_epoch = 0;
  std::string owner_id;
  std::string owner_incarnation;
  uint64_t owner_fencing_token = 0;
  std::vector<uint8_t> write_access_token;
  uint64_t write_access_expires_at_epoch_ms = 0;
};

// Configuration extracted from TaskRequest protobuf for executor construction.
struct TaskExecutorConfig {
  // Admitted process-local factory owner; never re-read from a live registry.
  std::shared_ptr<const facebook::velox::exec::FunctionRegistrySnapshot> function_registry;
  // Serialized once after canonical admission, reused by every callback batch.
  std::string callback_context;
  // Process-local identity assigned by HandleRegistry before start(). It is
  // never serialized into a plan or exposed as a network routing credential.
  int64_t executor_handle = 0;
  const pangrova::executor::TaskFailureReporter* failure_reporter = nullptr;
  std::string task_id;
  std::string query_id;
  int32_t stage_id = 0;
  int32_t partition_id = 0;
  uint64_t plan_epoch = 0;
  uint32_t attempt_id = 0;
  uint64_t fencing_token = 0;
  uint64_t control_epoch = 0;
  std::string owner_id;
  std::string owner_incarnation;
  uint64_t owner_fencing_token = 0;
  std::vector<uint8_t> substrait_plan;
  std::vector<pangrova::result::ResultColumnVectorDimension> result_column_types;
  std::unordered_map<std::string, pangrova::protocol::v2::OperatorBinding>
      operator_bindings;
  // Runtime filters present at submission time are part of the canonical task
  // envelope. Native applies them before any Velox driver starts; they are not
  // replayed through a second JNI command path.
  std::vector<pangrova::protocol::v2::RuntimeFilterBinding>
      initial_runtime_filters;
  std::vector<split::SplitConfig> splits;
  std::unordered_map<std::string, std::string> session_properties;
  ResourceLimitConfig resource_limit;
  WriteConfigInfo write_config;
  int32_t task_type = 0;  // Maps to canonical v2 TaskKind.
  int32_t num_shuffle_partitions = 0;
  pangrova::protocol::v2::BatchSerde shuffle_batch_serde =
      pangrova::protocol::v2::BATCH_SERDE_ARROW_BATCH;
  std::string shuffle_compression_codec = "none";
  int32_t shuffle_batch_size = 10000;
  // Non-null only for a coordinator-allocated external shuffle binding.
  std::shared_ptr<pangrova::shuffle::ShuffleEgressExecutor> shuffle_egress;
  pangrova::protocol::v2::ShufflePersistenceMode shuffle_persistence_mode =
      pangrova::protocol::v2::SHUFFLE_PERSISTENCE_MODE_UNSPECIFIED;
  std::optional<ShuffleExecutionConfig> shuffle_execution;
  // Indices of columns used for shuffle partition hash computation.
  // When empty, the first column is used as the default partition key.
  std::vector<int32_t> shuffle_partition_key_indices;
  size_t output_queue_capacity = 64;
  size_t shuffle_queue_capacity = 64;
  std::chrono::milliseconds queue_poll_timeout{100};
  // True for final-stage result tasks. When set, VeloxPlanConverter wraps the
  // plan root with a PartitionedOutputNode so Velox serializes output via
  // the canonical batch serde and routes it through OutputBufferManager.
  bool is_result_task = false;
  std::string trace_id;  // Propagated from Java via JNI for tracing.
  std::string span_id;   // Parent span ID from Java for trace chain continuity.
  // Trusted coordinator identity propagated by the internally authenticated
  // task submission channel. Used by governed JVM-backed AI functions.
  std::string tenant_id;
  std::string principal;
  std::string policy_context_hash;
  std::string dependency_scope_hash;
  std::string dependency_version_hash;
  // Absolute query deadline in Unix epoch milliseconds. Zero disables it.
  int64_t deadline_epoch_ms = 0;
  // Task-local metadata bridge used by a Native background artifact sink.
  // No artifact bytes cross this bridge.
  std::shared_ptr<artifact::ArtifactWorkReportState> artifact_report_state =
      std::make_shared<artifact::ArtifactWorkReportState>();
  // Arrow Flight shuffle server port. 0 = Flight disabled (Java shuffle mode).
  // When > 0, FlightShuffleServer binds to this port and serves shuffle data
  // directly from SerializedShuffleQueue, bypassing JNI pull path.
  // Set by Java via START_FLIGHT_SERVER command; reported back to coordinator.
  int flight_shuffle_port = 0;
};

// Orchestrator: composes lifecycle, execution driver, and output routing.
class TaskExecutor {
 public:
  explicit TaskExecutor(const TaskExecutorConfig& config);
  ~TaskExecutor();

  // Non-copyable, non-movable.
  TaskExecutor(const TaskExecutor&) = delete;
  TaskExecutor& operator=(const TaskExecutor&) = delete;

  // Lifecycle — delegates to TaskLifecycle + PlanExecutionDriver.
  void start();
  void cancel();
  void release();
  shuffle::ShuffleControlGate::Lease acquireShuffleStart();
  void installShuffleRegistration(const shuffle::ShuffleControlGate::Lease& lease,
      std::shared_ptr<shuffle::ShuffleProducerRegistration> registration);
  void closeShuffleRegistration();
  bool isShuffleStartSealed() const { return shuffle_start_gate_.isSealed(); }
  bool isExecutionReleased() const { return execution_released_.load(std::memory_order_acquire); }

  // Binds the process-local handle exactly once while the task is still NEW.
  // HandleRegistry calls this before publishing the executor to JNI callers.
  void bindHandle(int64_t executor_handle, const TaskFailureReporter& failure_reporter);

  // Pull model — delegates to TaskOutputRouter.
  // Output path returns RowVectorPtr directly; Arrow C Data Interface export
  // happens at the JNI boundary (nativeGetNextBatch).
  int getNextBatch(facebook::velox::RowVectorPtr& row_vector);
  // Status — delegates to TaskLifecycle.
  ExecutorState state() const;
  std::string taskId() const;
  std::string queryId() const;
  int32_t stageId() const;
  int32_t partitionId() const;
  uint64_t planEpoch() const;
  uint32_t attemptId() const;
  uint64_t fencingToken() const;
  uint64_t controlEpoch() const;
  const std::string& ownerId() const;
  const std::string& ownerIncarnation() const;
  uint64_t ownerFencingToken() const;
  const std::vector<split::SplitConfig>& splits() const;
  const std::unordered_map<std::string, pangrova::protocol::v2::OperatorBinding>&
  operatorBindings() const;
  int64_t deadlineEpochMs() const;
  int64_t nativeMemoryLimitBytes() const;
  const std::string& callbackContext() const;
  bool isResultTask() const;
  int32_t shufflePartitionCount() const;
  bool usesExternalShuffle() const;
  bool usesLocalDiskShuffle() const;
  int64_t executorHandle() const;
  std::string errorMessage() const;
  TaskLifecycle::Snapshot lifecycleSnapshot() const;
  void recordExecutionError(const protocol::v2::ExecutionError& error);
  void recordResultStreamFailure(const std::string& message);
  std::string nativeStackTrace() const;
  std::optional<protocol::v2::ArtifactWorkReport> artifactWorkReport() const;

  // Dynamic filter injection.
  void injectDynamicFilter(const std::string& plan_node_id,
                           const std::string& filter_proto);

  // Memory limit update from Java side — delegates to VeloxMemoryGovernor.
  void updateMemoryLimit(int64_t new_limit_bytes);

  // Set trace ID for observability propagation (called before start).
  void setTraceId(const std::string& trace_id);

  // Transition to FAILED with error message.
  void notifyTaskFailed(const std::string& message);

  // Access queues for producer side (Velox callback / test).
  OutputQueue* outputQueue();
  pangrova::shuffle::SerializedShuffleQueue* serializedShuffleQueue();

  // F-NATIVE-EXEC-05: Drain all unconsumed output from queues.
  // Closes queues and discards remaining items so RowVector memory pools
  // are released before destruction.
  void drainUnconsumedOutput();

  // Metrics collector — accumulates rows/bytes/cpu/memory during execution.
  observability::NativeMetricsCollector& metricsCollector();

  // Collect task stats from Velox TaskStats via VeloxTaskRunner.
  // Returns serialized proto bytes; empty if VeloxTaskRunner is unavailable.
  std::vector<uint8_t> collectTaskStats() const;
  bool hasFinalTaskStats() const;
  uint64_t requestedObjectReadBytes() const;

  // Access the active VeloxTaskRunner (may be null if not yet started).
  // Used by StatusSerializer to collect Velox TaskStats.
  velox::VeloxTaskRunner* veloxTaskRunner() const;

 private:
  void startDeadlineWatchdog();
  void stopDeadlineWatchdog();
  void failDeadline();
  void releaseInternal();
  void sealShuffleStarts();
  shuffle::ShuffleControlGate::Lease acquireExecutionStart();

  TaskExecutorConfig config_;

  // Sub-components: lifecycle, routing, execution.
  TaskLifecycle lifecycle_;
  TaskOutputRouter router_;
  std::unique_ptr<PlanExecutionDriver> execution_driver_;

  observability::NativeMetricsCollector metrics_collector_;
  std::atomic<bool> execution_released_{false};
  shuffle::ShuffleControlGate shuffle_start_gate_;
  std::mutex shuffle_registration_mutex_;
  std::shared_ptr<shuffle::ShuffleProducerRegistration> shuffle_registration_;
  std::mutex release_mutex_;
  std::condition_variable release_cv_;
  bool release_in_progress_ = false;

  // Trace ID for observability — set via constructor config or setTraceId().
  std::string trace_id_;

  // Deadline watchdog starts at executor construction so queueing time counts
  // against the query deadline. The condition variable makes shutdown prompt.
  std::thread deadline_thread_;
  std::mutex deadline_mutex_;
  std::condition_variable deadline_cv_;
  bool deadline_stop_{false};
  bool deadline_join_in_progress_ = false;
  std::thread::id deadline_thread_id_;
};

}  // namespace pangrova::executor
