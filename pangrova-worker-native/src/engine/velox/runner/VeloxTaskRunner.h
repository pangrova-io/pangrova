// VeloxTaskRunner: Wraps Velox exec::Task + exec::Driver to execute a
// Velox PlanNode. Feeds output batches to an OutputQueue for JNI pull.
//
// Placement: velox/ — Velox integration adapters.
#pragma once

#include "engine/result/ResultOwnershipState.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <folly/futures/Future.h>

#include "engine/executor/driver/TaskFailureReporter.h"
#include "engine/split/SplitConfig.h"
#include "engine/executor/queue/OutputQueue.h"
#include "engine/shuffle/SerializedShuffleQueue.h"
#include "engine/shuffle/ShuffleBatchSerializer.h"
#include "engine/shuffle/ShufflePartitionAccumulator.h"
#include "engine/velox/plan/ShuffleOutputSpec.h"
#include "engine/velox/runner/BuildKeyRanges.h"
#include "engine/result/ResultColumnType.h"
#include "pangrova/protocol/v2/result.pb.h"
#include "velox/common/memory/Memory.h"
#include "velox/core/PlanNode.h"
#include "velox/type/Filter.h"

namespace facebook::velox::exec {
class Task;
class OutputBufferManager;
class FunctionRegistrySnapshot;
struct TaskStats;
}

namespace pangrova::observability { class NativeMetricsCollector; }

namespace pangrova::velox {

// Configuration for VeloxTaskRunner.
struct VeloxTaskRunnerConfig {
  std::shared_ptr<const facebook::velox::exec::FunctionRegistrySnapshot> function_registry;
  std::shared_ptr<const substrait::NamedStruct> canonical_output_schema;
  std::shared_ptr<const substrait::NamedStruct> result_output_schema;
  std::unordered_map<std::string, substrait::NamedStruct> serde_schema_bindings;
  int64_t executor_handle = 0;
  const pangrova::executor::TaskFailureReporter* failure_reporter = nullptr;
  std::string callback_context;
  std::string task_id;
  std::string query_id;
  std::string tenant_id;
  std::string principal;
  std::string policy_context_hash;
  std::string dependency_scope_hash;
  std::string dependency_version_hash;
  std::string trace_id;
  int64_t deadline_epoch_ms = 0;
  int32_t max_provider_concurrency = 0;
  int64_t max_provider_items = 0;
  int64_t max_provider_attempts = 0;
  int64_t max_provider_request_bytes = 0;
  int64_t max_provider_response_bytes = 0;
  int64_t max_media_bytes = 0;
  // Minimum 2: Velox serial mode (max_drivers=1) uses Task::next() which
  // deadlocks on multi-pipeline plans (JOIN/agg) because the single driver
  // thread blocks waiting for the build side. Parallel mode (Task::start()
  // + taskCompletionFuture()) delegates scheduling to Velox's internal
  // executor, handling this correctly.
  int32_t max_drivers = 2;

  // Spill configuration.
  bool spill_enabled = false;
  std::string spill_directory;
  int64_t max_spill_bytes = 0;  // 0 = unlimited

  // Advanced execution features.
  int32_t preferred_output_batch_rows = 10000;
  int64_t max_output_batch_bytes = 10L * 1024 * 1024;  // 10MB
  int32_t max_spill_level = 4;
  std::string spill_compression_codec = "zstd";
  bool lazy_materialization = true;

  // Shuffle configuration.
  bool is_shuffle_task = false;
  int32_t num_shuffle_partitions = 0;
  std::optional<ShuffleOutputSpec> shuffle_output_spec;
  std::optional<uint32_t> build_key_ordinal;
  std::string shuffle_compression_codec = "lz4";
  // Indices of columns used for shuffle partition hash computation.
  // When empty, the first column is used as the default partition key.
  std::vector<int32_t> shuffle_partition_key_indices;

  // F-NATIVE-SHUF-09: Configurable shuffle push timeout and accumulator
  // flush threshold. Previously hardcoded as kShufflePushTimeout = 30000ms
  // and kMinPartitionFlushRows = 128.
  int64_t shuffle_push_timeout_ms = 30000;
  int32_t shuffle_min_partition_flush_rows = 128;

  // When true, the plan contains a PartitionedOutputNode and Velox routes
  // output through OutputBufferManager. No consumer callback is set, and
  // OutputQueue is not used for the result path.
  bool is_result_task = false;
  // Exact admitted serialized-output backpressure threshold. This is not a
  // total-memory hard cap: Velox can enqueue a final page before blocking.
  int64_t serialized_output_buffer_threshold_bytes = 0;
  std::vector<pangrova::result::ResultColumnVectorDimension> result_column_types;
  std::vector<std::string> result_column_labels;

  // P0-VELOX-2: Per-task memory limit from Java-side ResourceLimit.
  // When > 0, used as createTaskPool max_bytes instead of totalMemory().
  int64_t max_memory_bytes = 0;

  // Session properties from TaskRequest, forwarded to Velox connector configs.
  std::unordered_map<std::string, std::string> session_properties;

  // Performance tuning knobs (Req 6: QueryConfig tuning).
  // All boolean knobs default to true (enabled); overridable via session
  // properties with prefix "velox." (e.g., velox.hash_probe_finish_early_on_empty_build=false).
  bool hash_probe_finish_early_on_empty_build = true;
  bool hash_probe_string_dynamic_filter_pushdown = true;
  bool adaptive_filter_reordering = true;
  bool parallel_output_join_build_rows = true;
  bool table_scan_scaled_processing = true;
  int32_t max_split_preload_per_driver = 2;

  // L1-1/L1-2: Partial aggregation tuning.
  // abandon_partial_agg_min_rows: after processing this many rows, check
  // reduction ratio. abandon_partial_agg_min_pct: if output/input ratio
  // exceeds this pct, abandon partial aggregation (pass-through mode).
  int32_t abandon_partial_agg_min_rows = 100000;
  int32_t abandon_partial_agg_min_pct = 80;
  int64_t max_partial_agg_memory = 16L * 1024 * 1024;       // 16MB
  int64_t max_extended_partial_agg_memory = 64L * 1024 * 1024; // 64MB

  // L1-3: Spill write buffer size. Larger buffer reduces write syscalls.
  int64_t spill_write_buffer_size = 4L * 1024 * 1024;  // 4MB

  // L1-4: Spill partition bits. More partitions reduce recursive spill.
  int32_t spill_num_partition_bits = 3;  // 8 partitions

  // L1-5: Driver CPU time slice limit (ms). 0 = unlimited.
  int32_t driver_cpu_time_slice_limit_ms = 100;

  // L1-6: Spillable reservation percentages.
  int32_t min_spillable_reservation_pct = 10;
  int32_t spillable_reservation_growth_pct = 25;

  // L2-3: Preserve Arrow Dictionary encoding in result paths (Flight + Arrow
  // IPC serde). When true, dictionary-encoded vectors are exported without
  // flattening — reduces transfer size for low-cardinality columns by 50-80%.
  // Safe for Arrow Flight Java client which handles dictionary batches natively.
  // Write paths (JNI → Java ArrowStreamReader) always flatten regardless.
  bool preserve_dictionary_encoding = true;

  // L3-4: Per-query max drivers override. 0 = use default (max(2, max_threads)).
  // Java planner can set this based on query characteristics (IO-heavy = more,
  // CPU-heavy = fewer). Overridable via session property "velox.max_drivers".
  int32_t max_drivers_override = 0;

  // L4-1B: Minimum partition size (bytes) to apply shuffle compression.
  // Partitions smaller than this threshold skip compression to avoid
  // CPU waste on data that won't benefit (tiny buffers compress poorly).
  int64_t shuffle_compression_min_bytes = 4096;  // 4KB
};

// Executes a Velox PlanNode using Velox exec::Task.
// Output batches are pushed to the provided OutputQueue.
//
// Lifecycle: create → start → (running) → waitForCompletion
// Thread-safe: start() spawns internal Velox drivers.
class VeloxTaskRunner
    : public std::enable_shared_from_this<VeloxTaskRunner> {
 public:
  // Construct with output queue and optional shuffle queue.
  // shuffle_queue is nullable — when non-null and config.is_shuffle_task,
  // the consumer callback serializes RowVectors via ShuffleBatchSerializer
  // and pushes ShuffleBatch to the shuffle queue instead of the output queue.
  VeloxTaskRunner(
      const VeloxTaskRunnerConfig& config,
      std::shared_ptr<const facebook::velox::core::PlanNode> plan,
      const std::vector<split::SplitConfig>& splits,
      pangrova::executor::OutputQueue* output_queue,
      pangrova::shuffle::SerializedShuffleQueue* shuffle_queue = nullptr,
      std::unordered_map<std::string, std::vector<split::SplitConfig>>
          scan_split_overrides = {});

  ~VeloxTaskRunner();

  // Non-copyable, non-movable.
  VeloxTaskRunner(const VeloxTaskRunner&) = delete;
  VeloxTaskRunner& operator=(const VeloxTaskRunner&) = delete;

  // Start Velox task execution.
  void start();

  // Cancel the running Velox task.
  void cancel();

  // Wait for the Velox task to complete. Returns true on success.
  bool waitForCompletion();

  // Non-blocking alternative to waitForCompletion(). Calls
  // velox_task_->start(effective_drivers) and returns a SemiFuture that resolves
  // to true on success, false on failure, when the task reaches a terminal
  // state. The calling thread returns immediately; resolution is driven by
  // Velox's internal executor. Use .via(executor).thenValue(cb) to schedule
  // post-completion work on a specific executor without blocking a pool thread.
  //
  // Must be called after start() has set up the Velox task.
  folly::SemiFuture<bool> startAsync();

  // Returns true if the task has finished (success or failure).
  bool isFinished() const;

  pangrova::result::ResultOwnershipState::Evidence resultDrainEvidence() const;
  void watchResultDrain(std::function<void()> listener) const;
  bool hasResultOutput() const { return static_cast<bool>(result_ownership_); }

  // Returns error message if task failed, empty otherwise.
  std::string errorMessage() const;

  // Inject a dynamic filter for external injection (Java → JNI path).
  // Pushes the filter to the process-global PendingFilterRegistry keyed by
  // task_id + exact scan plan_node_id. Only the matching DataSource drains it.
  // call and applies accumulated filters as row-level predicates.
  // If column_index is out of bounds for the scan node's output schema,
  // logs WARN and ignores the filter without aborting the query.
  void injectDynamicFilter(
      const std::string& plan_node_id,
      facebook::velox::column_index_t column_index,
      std::unique_ptr<facebook::velox::common::Filter> filter);

  // Collect Velox TaskStats after completion.
  // Uses one Velox snapshot for per-operator counters and columnar telemetry.
  std::optional<protocol::v2::NativeTaskStats> collectTaskStats(
      observability::NativeMetricsCollector* metrics = nullptr) const;

  // Access the underlying Velox Task (for direct dynamic filter injection).
  facebook::velox::exec::Task* veloxTask() const;

 private:
  void revokeResultOutput();
  // Idempotent cleanup: unregister profiler + close output queue.
  // Public: PlanExecutionDriver calls cleanup() after drainOutputToWriter
  // to ensure RowVector memory pools are still alive during the drain.
 public:
  // Commit every buffered shuffle row before the producer can publish a
  // successful terminal state. A failed flush must leave the stream abortable.
  void finalizeShuffleOutput();

  void cleanup();

  // Releases the underlying Velox Task and waits until its destructor has
  // finished. Task completion alone is insufficient: the final Driver may
  // still hold a Task reference while unwinding on Velox's executor. Callers
  // that own plan-conversion memory pools must keep them alive through this
  // barrier because Task::~Task() destroys the plan and its vectors.
  void releaseTaskAndWaitForDeletion();

 private:
  // Completion handler invoked from startAsync() continuation.
  // Checks task state and sets finished_/error_message_ accordingly.
  // Called on Velox's internal executor thread — must be thread-safe.
  bool handleTaskCompletion();
  void collectBuildKeyRanges(const facebook::velox::RowVector& rows);

  const VeloxTaskRunnerConfig config_;
  std::shared_ptr<const facebook::velox::core::PlanNode> plan_;
  std::vector<split::SplitConfig> splits_;
  std::atomic<uint64_t> index_splits_evaluated_{0};
  std::atomic<uint64_t> index_splits_skipped_{0};
  std::unordered_map<std::string, std::vector<split::SplitConfig>>
      scan_split_overrides_;
  // Declared before Task so the SDK buffer owner outlives Task destruction.
  std::shared_ptr<facebook::velox::exec::OutputBufferManager> result_buffers_;
  std::shared_ptr<facebook::velox::exec::Task> velox_task_;
  pangrova::executor::OutputQueue* output_queue_;
  pangrova::shuffle::SerializedShuffleQueue* shuffle_queue_{nullptr};
  std::unique_ptr<pangrova::shuffle::ShuffleBatchSerializer> shuffle_writer_;
  std::unique_ptr<pangrova::shuffle::ShufflePartitionAccumulator>
      shuffle_accumulator_;
  // Cancellation cleanup may race the successful completion continuation.
  std::mutex shuffle_accumulator_mutex_;
  // Pre-computed shuffle partition key channel indices. Populated in start()
  // from config_.shuffle_partition_key_indices to avoid recomputing per batch.
  std::vector<facebook::velox::column_index_t> shuffle_key_channels_;
  std::atomic<uint64_t> shuffle_next_row_{0};
  mutable std::mutex build_key_ranges_mutex_;
  BuildKeyRanges build_key_ranges_;
  std::atomic<bool> finished_{false};
  std::shared_ptr<pangrova::result::ResultOwnershipState> result_ownership_;
  std::atomic<bool> cancelled_{false};
  mutable std::mutex error_mutex_;
  std::string error_message_;
  std::once_flag cleanup_once_;
};

}  // namespace pangrova::velox
