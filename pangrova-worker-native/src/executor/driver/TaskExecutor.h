// TaskExecutor: Orchestrator composing TaskLifecycle + PlanExecutionDriver +
// TaskOutputRouter. Public API unchanged — JniBridge is unaware of the split.
#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "split/SplitConfig.h"
#include "executor/driver/PlanExecutionDriver.h"
#include "executor/driver/ReturnCodes.h"
#include "executor/driver/TaskLifecycle.h"
#include "executor/driver/TaskOutputRouter.h"
#include "executor/queue/ExchangeBuffer.h"
#include "executor/queue/OutputQueue.h"
#include "shuffle/SerializedShuffleQueue.h"

#include "observability/NativeMetricsCollector.h"
#include "velox/vector/ComplexVector.h"

namespace pangrova::velox {
class VeloxTaskRunner;
}  // namespace pangrova::velox

namespace pangrova::executor {

// Resource limits extracted from TaskRequest.ResourceLimit.
struct ResourceLimitConfig {
  int64_t max_memory_bytes = 0;
  int32_t max_threads = 0;
  int64_t max_spill_bytes = 0;
  std::string spill_directory;
};

// Write configuration forwarded from Java coordinator via TaskRequest.WriteConfig.
struct WriteConfigInfo {
  std::string target_table;
  std::string write_mode;
  std::vector<std::string> partition_spec;
  std::string transaction_id;
  split::FileFormatType file_format = split::FileFormatType::PARQUET;
  int64_t target_file_size = 0;
};

// Configuration extracted from TaskRequest protobuf for executor construction.
struct TaskExecutorConfig {
  std::string task_id;
  std::string query_id;
  std::vector<uint8_t> substrait_plan;
  std::vector<split::SplitConfig> splits;
  std::unordered_map<std::string, std::string> session_properties;
  ResourceLimitConfig resource_limit;
  WriteConfigInfo write_config;
  int32_t task_type = 0;  // Maps to proto TaskType enum value.
  int32_t num_shuffle_partitions = 0;
  // Indices of columns used for shuffle partition hash computation.
  // When empty, the first column is used as the default partition key.
  std::vector<int32_t> shuffle_partition_key_indices;
  size_t output_queue_capacity = 64;
  size_t shuffle_queue_capacity = 64;
  std::chrono::milliseconds queue_poll_timeout{100};
  // True for final-stage result tasks. When set, VeloxPlanConverter wraps the
  // plan root with a PartitionedOutputNode so Velox serializes output via
  // PrestoVectorSerde and routes it through OutputBufferManager.
  bool is_result_task = false;
  std::string trace_id;  // Propagated from Java via JNI for tracing.
  std::string span_id;   // Parent span ID from Java for trace chain continuity.
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

  // Pull model — delegates to TaskOutputRouter.
  // Output path returns RowVectorPtr directly; Arrow C Data Interface export
  // happens at the JNI boundary (nativeGetNextBatch).
  int getNextBatch(facebook::velox::RowVectorPtr& row_vector);
  int getSerializedShuffleBatch(pangrova::shuffle::ShuffleBatch& batch);
  // Status — delegates to TaskLifecycle.
  ExecutorState state() const;
  std::string taskId() const;
  std::string errorMessage() const;
  std::string nativeStackTrace() const;

  // Exchange input — delegates to TaskOutputRouter.
  void addExchangeBatch(const std::string& source_id,
                        facebook::velox::RowVectorPtr row_vector);
  void noMoreExchangeData(const std::string& source_id);
  void noMoreExchangeSources();

  // Dynamic filter injection.
  void injectDynamicFilter(const std::string& plan_node_id,
                           const std::string& filter_proto);

  // Memory limit update from Java side — delegates to VeloxMemoryGovernor.
  void updateMemoryLimit(int64_t new_limit_bytes);

  // Set trace ID for observability propagation (called before start).
  void setTraceId(const std::string& trace_id);

  // Transition to FLUSHING — called when Velox task completes.
  void notifyTaskComplete();

  // Transition to FAILED with error message.
  void notifyTaskFailed(const std::string& message);

  // Mark all queues drained → FINISHED.
  void notifyQueuesDrained();

  // Access queues for producer side (Velox callback / test).
  OutputQueue* outputQueue();
  pangrova::shuffle::SerializedShuffleQueue* serializedShuffleQueue();
  ExchangeBuffer* exchangeBuffer();

  // F-NATIVE-EXEC-05: Drain all unconsumed output from queues.
  // Closes queues and discards remaining items so RowVector memory pools
  // are released before destruction.
  void drainUnconsumedOutput();

  // Metrics collector — accumulates rows/bytes/cpu/memory during execution.
  observability::NativeMetricsCollector& metricsCollector();

  // Collect task stats from Velox TaskStats via VeloxTaskRunner.
  // Returns serialized proto bytes; empty if VeloxTaskRunner is unavailable.
  std::vector<uint8_t> collectTaskStats() const;

  // Access the active VeloxTaskRunner (may be null if not yet started).
  // Used by StatusSerializer to collect Velox TaskStats.
  velox::VeloxTaskRunner* veloxTaskRunner() const;

 private:
  const TaskExecutorConfig config_;

  // Sub-components: lifecycle, routing, execution.
  TaskLifecycle lifecycle_;
  TaskOutputRouter router_;
  std::unique_ptr<PlanExecutionDriver> execution_driver_;

  observability::NativeMetricsCollector metrics_collector_;

  // Trace ID for observability — set via constructor config or setTraceId().
  std::string trace_id_;
};

}  // namespace pangrova::executor
