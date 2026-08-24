// VeloxTaskRunner: Wraps Velox exec::Task + exec::Driver to execute a
// Velox PlanNode. Feeds output batches to an OutputQueue for JNI pull.
//
// Placement: velox/ — Velox integration adapters.
#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <folly/futures/Future.h>

#include "split/SplitConfig.h"
#include "executor/queue/OutputQueue.h"
#include "shuffle/SerializedShuffleQueue.h"
#include "shuffle/ShuffleBatchSerializer.h"
#include "shuffle/ShufflePartitionAccumulator.h"
#include "velox/common/memory/Memory.h"
#include "velox/core/PlanNode.h"
#include "velox/type/Filter.h"

namespace facebook::velox::exec {
class Task;
class HashPartitionFunction;
struct TaskStats;
}

namespace pangrova::velox {

// Configuration for VeloxTaskRunner.
struct VeloxTaskRunnerConfig {
  std::string task_id;
  std::string query_id;
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

  // P0-VELOX-2: Per-task memory limit from Java-side ResourceLimit.
  // When > 0, used as createTaskPool max_bytes instead of totalMemory().
  int64_t max_memory_bytes = 0;

  // Session properties from TaskRequest, forwarded to Velox connector configs.
  std::unordered_map<std::string, std::string> session_properties;

  // Result serialization mode: "presto" (legacy PrestoVectorSerde) or
  // "arrow_batch" (Arrow IPC direct serving via ArrowBatchVectorSerde).
  // Overridable via session property "velox.result_serde_mode".
  std::string result_serde_mode = "presto";

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
      pangrova::shuffle::SerializedShuffleQueue* shuffle_queue = nullptr);

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
  // velox_task_->start(max_drivers) and returns a SemiFuture that resolves
  // to true on success, false on failure, when the task reaches a terminal
  // state. The calling thread returns immediately; resolution is driven by
  // Velox's internal executor. Use .via(executor).thenValue(cb) to schedule
  // post-completion work on a specific executor without blocking a pool thread.
  //
  // Must be called after start() has set up the Velox task.
  folly::SemiFuture<bool> startAsync();

  // Returns true if the task has finished (success or failure).
  bool isFinished() const;

  // Returns error message if task failed, empty otherwise.
  std::string errorMessage() const;

  // Inject a dynamic filter for external injection (Java → JNI path).
  // Pushes the filter to the process-global PendingFilterRegistry keyed
  // by task_id. PangrovaVeloxDataSource drains the registry on each next()
  // call and applies accumulated filters as row-level predicates.
  // If column_index is out of bounds for the scan node's output schema,
  // logs WARN and ignores the filter without aborting the query.
  void injectDynamicFilter(
      const std::string& plan_node_id,
      facebook::velox::column_index_t column_index,
      std::unique_ptr<facebook::velox::common::Filter> filter);

  // Returns true if the task failed due to an unsupported type in the
  // result serde (ArrowBatch mode). Used by PlanExecutionDriver to detect
  // when serde fallback to PrestoVectorSerde is needed.
  bool isResultSerdeUnsupportedTypeError() const;

  // Collect Velox TaskStats after completion.
  // Returns serialized proto (TaskStats message) for Java-side consumption.
  // Iterates pipelineStats → operatorStats to extract per-operator metrics.
  std::vector<uint8_t> collectTaskStats() const;

  // Access the underlying Velox Task (for direct dynamic filter injection).
  facebook::velox::exec::Task* veloxTask() const;

  // Attach memory pool holders from plan conversion. Must be called before
  // start() to ensure pools outlive the PlanNode tree during async Task
  // destruction on Velox's thread pool.
  void attachPoolHolders(
      std::vector<std::shared_ptr<facebook::velox::memory::MemoryPool>>
          holders);

 private:
  // Idempotent cleanup: unregister profiler + close output queue.
  // Public: PlanExecutionDriver calls cleanup() after drainOutputToWriter
  // to ensure RowVector memory pools are still alive during the drain.
 public:
  void cleanup();

 private:
  // Completion handler invoked from startAsync() continuation.
  // Checks task state and sets finished_/error_message_ accordingly.
  // Called on Velox's internal executor thread — must be thread-safe.
  bool handleTaskCompletion();

  const VeloxTaskRunnerConfig config_;
  // Memory pools created during plan conversion (e.g., for ArrayVector in
  // list literals). Must outlive the PlanNode tree because ArrayVector buffers
  // reference these pools for deallocation. Stored here so that even if the
  // Velox Task is destroyed asynchronously on a thread pool thread (after
  // PlanExecutionDriver is already destroyed), the pools remain alive.
  // DECLARATION ORDER IS LOAD-BEARING: members destruct in reverse declaration
  // order, so this MUST be declared before plan_ (and velox_task_) so the
  // pools are destroyed AFTER the plan tree. Declaring it after plan_ caused
  // a SIGSEGV in NonPODAlignedBuffer::freeToPool() when ~VeloxTaskRunner
  // destroyed the pools first, then the plan's ConstantTypedExpr array
  // literals tried to free their buffers into the dead pools.
  std::vector<std::shared_ptr<facebook::velox::memory::MemoryPool>>
      plan_pool_holders_;
  std::shared_ptr<const facebook::velox::core::PlanNode> plan_;
  std::vector<split::SplitConfig> splits_;
  std::shared_ptr<facebook::velox::exec::Task> velox_task_;
  pangrova::executor::OutputQueue* output_queue_;
  pangrova::shuffle::SerializedShuffleQueue* shuffle_queue_{nullptr};
  std::unique_ptr<pangrova::shuffle::ShuffleBatchSerializer> shuffle_writer_;
  std::unique_ptr<pangrova::shuffle::ShufflePartitionAccumulator>
      shuffle_accumulator_;
  // Pre-computed shuffle partition key channel indices. Populated in start()
  // from config_.shuffle_partition_key_indices to avoid recomputing per batch.
  std::vector<facebook::velox::column_index_t> shuffle_key_channels_;
  // Cached HashPartitionFunction for multi-partition shuffle. Initialized
  // on the first consumer callback via atomic flag + double-check pattern.
  // After construction the partitioner is stateless (partition() is a pure
  // function), so concurrent reads from multiple Driver threads are safe.
  std::atomic<bool> shuffle_partitioner_ready_{false};
  std::mutex shuffle_partitioner_mutex_;
  std::shared_ptr<facebook::velox::exec::HashPartitionFunction>
      shuffle_partitioner_;
  std::atomic<bool> finished_{false};
  std::atomic<bool> cancelled_{false};
  mutable std::mutex error_mutex_;
  std::string error_message_;
  std::once_flag cleanup_once_;
};

}  // namespace pangrova::velox
