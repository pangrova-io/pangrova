// VeloxTaskRunner: Drives Velox exec::Task and feeds output to OutputQueue.
#include "velox/runner/VeloxTaskRunner.h"

#include <chrono>
#include <filesystem>
#include <set>
#include <stdexcept>
#include <vector>

#include "jni/RuntimeInitializer.h"
#include "observability/PangrovaLog.h"
#include "observability/NativeProfiler.h"
#include "shuffle/SerializedShuffleQueue.h"
#include "shuffle/ShuffleBatchSerializer.h"

#include "jni/command/StatusSerializer.h"
#if PANGROVA_HAS_FLIGHT
#include "result/FlightResultServer.h"
#endif

// Logging for dynamic filter injection warnings.
#if __has_include(<glog/logging.h>)
#include <glog/logging.h>
#endif
#include "velox/connector/PangrovaVeloxConnector.h"
#include "velox/filter/PendingFilterRegistry.h"
#include "executor/NativeUnsupportedFeatureException.h"
#include "velox/memory/VeloxMemoryGovernor.h"
#include "velox/core/PlanFragment.h"
#include "velox/core/QueryConfig.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/Task.h"
#include "velox/exec/Driver.h"
#include "velox/exec/TaskStats.h"
#include "velox/common/future/VeloxPromise.h"
#include "velox/exec/HashPartitionFunction.h"

namespace pangrova::velox {

namespace {

// Collect leaf node IDs that require splits, with their table names
// for precise split routing.
struct SplitLeafNode {
  facebook::velox::core::PlanNodeId nodeId;
  std::string tableName;  // from TableScanNode's table handle
};

/**
 * Checks if 'full' ends with '.short_name' or equals 'short_name'.
 * Used for defensive split routing when one side carries a qualified name
 * (e.g. "schema.table") and the other carries only the simple name ("table").
 */
inline bool matchesSuffix(const std::string& full, const std::string& short_name) {
  if (full.size() <= short_name.size()) {
    return false;
  }
  if (full.size() > short_name.size() + 1 &&
      full[full.size() - short_name.size() - 1] == '.') {
    return full.compare(full.size() - short_name.size(), short_name.size(), short_name) == 0;
  }
  return false;
}

void collectSplitLeafNodes(
    const std::shared_ptr<const facebook::velox::core::PlanNode>& node,
    std::vector<SplitLeafNode>& leafNodes) {
  if (!node) {
    return;
  }
  if (node->sources().empty()) {
    if (node->requiresSplits()) {
      std::string tableName;
      // Try to extract table name from TableScanNode.
      auto scanNode = std::dynamic_pointer_cast<
          const facebook::velox::core::TableScanNode>(node);
      if (scanNode && scanNode->tableHandle()) {
        tableName = scanNode->tableHandle()->name();
      }
      leafNodes.push_back({node->id(), std::move(tableName)});
    }
    return;
  }
  for (const auto& source : node->sources()) {
    collectSplitLeafNodes(source, leafNodes);
  }
}

}  // namespace

VeloxTaskRunner::VeloxTaskRunner(
    const VeloxTaskRunnerConfig& config,
    std::shared_ptr<const facebook::velox::core::PlanNode> plan,
    const std::vector<split::SplitConfig>& splits,
    pangrova::executor::OutputQueue* output_queue,
    pangrova::shuffle::SerializedShuffleQueue* shuffle_queue)
    : config_(config),
      plan_(std::move(plan)),
      splits_(splits),
      output_queue_(output_queue),
      shuffle_queue_(shuffle_queue) {
  if (!plan_) {
    throw std::invalid_argument("VeloxTaskRunner: plan must not be null");
  }
  // Result tasks use Velox OutputBufferManager instead of OutputQueue,
  // so output_queue may be nullptr for result tasks.
  if (!output_queue && !config_.is_result_task) {
    throw std::invalid_argument(
        "VeloxTaskRunner: output_queue must not be null for non-result tasks");
  }
  if (config_.is_shuffle_task && !shuffle_queue_) {
    throw std::invalid_argument(
        "VeloxTaskRunner: shuffle_queue must not be null for shuffle tasks");
  }
}

VeloxTaskRunner::~VeloxTaskRunner() {
  cancel();
}

void VeloxTaskRunner::start() {
  // Precondition: this object must be managed by a shared_ptr before start()
  // is called, because the consumer callback captures weak_from_this().
  // PlanExecutionDriver::executeVeloxPlan() creates the runner via
  // make_shared before calling start(), satisfying this requirement.
  if (weak_from_this().expired()) {
    throw std::logic_error(
        "VeloxTaskRunner::start() called before object is managed by shared_ptr");
  }
  cancelled_.store(false);
  {
    std::lock_guard<std::mutex> lock(error_mutex_);
    error_message_.clear();
  }
  finished_.store(false);

  facebook::velox::core::PlanFragment fragment(plan_);

  // Build QueryConfig map with spill settings and advanced features.
  std::unordered_map<std::string, std::string> query_config_values;
  if (config_.spill_enabled) {
    query_config_values[facebook::velox::core::QueryConfig::kSpillEnabled] = "true";
    query_config_values[facebook::velox::core::QueryConfig::kAggregationSpillEnabled] = "true";
    query_config_values[facebook::velox::core::QueryConfig::kJoinSpillEnabled] = "true";
    query_config_values[facebook::velox::core::QueryConfig::kOrderBySpillEnabled] = "true";
    query_config_values[facebook::velox::core::QueryConfig::kWindowSpillEnabled] = "true";
    query_config_values[facebook::velox::core::QueryConfig::kMaxSpillLevel] =
        std::to_string(config_.max_spill_level);
    query_config_values[facebook::velox::core::QueryConfig::kSpillCompressionKind] =
        config_.spill_compression_codec;
    if (config_.max_spill_bytes > 0) {
      query_config_values[facebook::velox::core::QueryConfig::kMaxSpillBytes] =
          std::to_string(config_.max_spill_bytes);
    }
  }

  // P1-VELOX-6: Runtime filter pushdown — removed in current Velox version.
  // Velox enables runtime filters by default; no explicit config needed.

  // Output batch sizing: controls how many rows/bytes per operator output.
  query_config_values[facebook::velox::core::QueryConfig::kPreferredOutputBatchRows] =
      std::to_string(config_.preferred_output_batch_rows);
  query_config_values[facebook::velox::core::QueryConfig::kPreferredOutputBatchBytes] =
      std::to_string(config_.max_output_batch_bytes);

  // Lazy materialization: Velox enables lazy vector loading by default.
  // Ensure it is not explicitly disabled.
  if (config_.lazy_materialization) {
    query_config_values["debug_disable_expression_with_lazy_inputs"] = "false";
  }

  // Performance tuning knobs (Req 6): enable high-value Velox QueryConfig
  // settings that improve hash join, table scan, and filter performance.
  if (config_.hash_probe_finish_early_on_empty_build) {
    query_config_values[facebook::velox::core::QueryConfig::kHashProbeFinishEarlyOnEmptyBuild] = "true";
  }
  if (config_.hash_probe_string_dynamic_filter_pushdown) {
    query_config_values[facebook::velox::core::QueryConfig::kHashProbeStringDynamicFilterPushdownEnabled] = "true";
  }
  if (config_.adaptive_filter_reordering) {
    query_config_values[facebook::velox::core::QueryConfig::kAdaptiveFilterReorderingEnabled] = "true";
  }
  if (config_.parallel_output_join_build_rows) {
    query_config_values[facebook::velox::core::QueryConfig::kParallelOutputJoinBuildRowsEnabled] = "true";
  }
  if (config_.table_scan_scaled_processing) {
    query_config_values[facebook::velox::core::QueryConfig::kTableScanScaledProcessingEnabled] = "true";
  }
  if (config_.max_split_preload_per_driver > 0) {
    query_config_values[facebook::velox::core::QueryConfig::kMaxSplitPreloadPerDriver] =
        std::to_string(config_.max_split_preload_per_driver);
  }

  // L1-1: Partial aggregation abandonment — stop building hash tables
  // when reduction ratio is poor (high-cardinality GROUP BY).
  query_config_values[facebook::velox::core::QueryConfig::kAbandonPartialAggregationMinRows] =
      std::to_string(config_.abandon_partial_agg_min_rows);
  query_config_values[facebook::velox::core::QueryConfig::kAbandonPartialAggregationMinPct] =
      std::to_string(config_.abandon_partial_agg_min_pct);

  // L1-2: Partial aggregation memory cap — prevent hash table from growing
  // unboundedly and triggering premature spill.
  query_config_values[facebook::velox::core::QueryConfig::kMaxPartialAggregationMemory] =
      std::to_string(config_.max_partial_agg_memory);
  query_config_values[facebook::velox::core::QueryConfig::kMaxExtendedPartialAggregationMemory] =
      std::to_string(config_.max_extended_partial_agg_memory);

  // L1-3: Spill write buffer — larger buffer reduces write syscalls during spill.
  query_config_values[facebook::velox::core::QueryConfig::kSpillWriteBufferSize] =
      std::to_string(config_.spill_write_buffer_size);

  // L1-4: Spill partition bits — more partitions reduce recursive spill probability.
  query_config_values[facebook::velox::core::QueryConfig::kSpillNumPartitionBits] =
      std::to_string(config_.spill_num_partition_bits);

  // L1-5: Driver CPU time slice — prevent long-running drivers from
  // monopolizing threads in multi-tenant scenarios.
  if (config_.driver_cpu_time_slice_limit_ms > 0) {
    query_config_values[facebook::velox::core::QueryConfig::kDriverCpuTimeSliceLimitMs] =
        std::to_string(config_.driver_cpu_time_slice_limit_ms);
  }

  // L1-6: Spillable reservation — ensure minimum reservation before triggering
  // micro-spills, and grow reservation proactively.
  query_config_values[facebook::velox::core::QueryConfig::kMinSpillableReservationPct] =
      std::to_string(config_.min_spillable_reservation_pct);
  query_config_values[facebook::velox::core::QueryConfig::kSpillableReservationGrowthPct] =
      std::to_string(config_.spillable_reservation_growth_pct);

  // Create QueryCtx with a managed memory pool if VeloxMemoryGovernor is available.
  std::shared_ptr<facebook::velox::core::QueryCtx> queryCtx;
  auto executor = pangrova::jni::globalExecutor();
  // P1-VELOX-4: Pass AsyncDataCache to QueryCtx::create() for data caching.
  auto* asyncCache = pangrova::jni::globalAsyncDataCache();
  auto* memGovernor = pangrova::jni::globalVeloxMemoryGovernor();
  if (memGovernor) {
    // Create a dedicated pool for this task under Pangrova's memory governance.
    // P0-VELOX-2: Use per-task max_memory_bytes from ResourceLimit when set,
    // instead of totalMemory() which gives every task the entire budget.
    int64_t taskMaxBytes = (config_.max_memory_bytes > 0)
        ? config_.max_memory_bytes
        : memGovernor->totalMemory();
    auto taskPool = memGovernor->createTaskPool(
        config_.query_id, config_.task_id, taskMaxBytes);
    // Build connector configs from session properties for Velox connector.
    std::unordered_map<std::string,
        std::shared_ptr<facebook::velox::config::ConfigBase>> connectorConfigs;
    if (!config_.session_properties.empty()) {
      std::unordered_map<std::string, std::string> connectorProps;
      for (const auto& [key, value] : config_.session_properties) {
        // Forward session properties with connector-relevant prefixes.
        if (key.find("connector.") == 0 || key.find("s3.") == 0 ||
            key.find("hive.") == 0 || key.find("file.") == 0) {
          connectorProps[key] = value;
        }
      }
      if (!connectorProps.empty()) {
        connectorConfigs[pangrova::velox::PangrovaVeloxConnector::kConnectorId] =
            std::make_shared<facebook::velox::config::ConfigBase>(
                std::move(connectorProps));
      }
    }

    queryCtx = facebook::velox::core::QueryCtx::create(
        executor.get(),
        facebook::velox::core::QueryConfig{std::move(query_config_values)},
        std::move(connectorConfigs), asyncCache, std::move(taskPool));
  } else {
    queryCtx = facebook::velox::core::QueryCtx::create(
        executor.get(),
        facebook::velox::core::QueryConfig{std::move(query_config_values)});
  }

  // Execution mode: always parallel. PlanExecutionDriver enforces
  // max_drivers >= 2, and parallel mode (Task::start() +
  // taskCompletionFuture()) delegates scheduling to Velox's internal
  // executor, correctly handling multi-pipeline plans (JOIN/agg).
  auto execMode = facebook::velox::exec::Task::ExecutionMode::kParallel;

  // Create ShuffleBatchSerializer for shuffle tasks.
  if (config_.is_shuffle_task && shuffle_queue_) {
    shuffle::ShuffleBatchSerializer::Config writerConfig;
    writerConfig.compression_codec = config_.shuffle_compression_codec;
    writerConfig.batch_size = config_.preferred_output_batch_rows;
    // L4-1B: Propagate compression minimum threshold.
    writerConfig.compression_min_bytes = config_.shuffle_compression_min_bytes;
    shuffle_writer_ = std::make_unique<shuffle::ShuffleBatchSerializer>(writerConfig);
    // Create partition accumulator for multi-partition shuffle to coalesce
    // small per-partition row sets before serialization.
    if (config_.num_shuffle_partitions > 1) {
      // F-NATIVE-SHUF-09: Use configurable timeout from VeloxTaskRunnerConfig.
      auto shuffle_push_timeout =
          std::chrono::milliseconds(config_.shuffle_push_timeout_ms);
      shuffle_accumulator_ =
          std::make_unique<shuffle::ShufflePartitionAccumulator>(
              config_.num_shuffle_partitions, shuffle_writer_.get(),
              shuffle_queue_, shuffle_push_timeout);
      // Pre-compute shuffle key channels from config to avoid per-batch
      // recomputation in the consumer callback.
      if (!config_.shuffle_partition_key_indices.empty()) {
        for (auto colIdx : config_.shuffle_partition_key_indices) {
          shuffle_key_channels_.push_back(
              static_cast<facebook::velox::column_index_t>(colIdx));
        }
      }
    }
  }

  // For parallel mode, set up a consumer callback that pushes results to
  // the output queue (or shuffle queue for shuffle tasks).
  // Velox calls this from Driver threads with output batches.
  //
  // RESULT TASK EXCEPTION: When is_result_task is true, the plan contains a
  // PartitionedOutputNode that serializes output via PrestoVectorSerde and
  // routes it through Velox's OutputBufferManager automatically. No consumer
  // callback is needed — passing nullptr lets Velox use OutputBufferManager.
  // FlightResultServer (task 1.5) will pull from OutputBufferManager.
  //
  // VELOX VERSION CONSTRAINT: This lambda captures `this` and relies on
  // velox_task_->requestCancel().wait() blocking until all Driver threads
  // exit (guaranteed as of Velox 0.1.x). If upgrading Velox, verify that
  // requestCancel().wait() still provides this guarantee.
  //
  // P0-BUG-2: Safety guarantee for `this` capture: VeloxTaskRunner destructor
  // calls cancel() which invokes requestCancel().wait(), ensuring all Driver
  // threads have stopped before `this` is destroyed. The cancelled_ atomic
  // flag provides an additional early-exit guard.
  facebook::velox::exec::Consumer consumer = nullptr;
  if (execMode == facebook::velox::exec::Task::ExecutionMode::kParallel &&
      !config_.is_result_task) {
    if (config_.is_shuffle_task && shuffle_writer_ && shuffle_queue_) {
      // Shuffle path: serialize RowVector via ShuffleBatchSerializer and push
      // ShuffleBatch to SerializedShuffleQueue.
      //
      // IMPORTANT: Velox CallbackSink::addInput() moves the vector into
      // the consumer callback. After consumer returns kWaitForConsumer,
      // Velox does NOT re-invoke the consumer with the same vector —
      // it fetches a new vector from the upstream operator. Serialized
      // shuffle data cannot be retried after move. Therefore we use
      // blocking push (with timeout) to guarantee delivery instead of
      // tryPush + kWaitForConsumer. The blocking push provides natural
      // backpressure — the Driver thread waits until space is available,
      // which is acceptable because Velox's executor has multiple threads.
      auto weak_self = weak_from_this();
      consumer = [weak_self](
          facebook::velox::RowVectorPtr vector,
          bool /*drained*/,
          facebook::velox::ContinueFuture* /*future*/) -> facebook::velox::exec::BlockingReason {
        auto self = weak_self.lock();
        if (!self || !vector || vector->size() == 0) {
          return facebook::velox::exec::BlockingReason::kNotBlocked;
        }
        if (self->cancelled_.load()) {
          return facebook::velox::exec::BlockingReason::kNotBlocked;
        }

        // F-NATIVE-SHUF-09: Use configurable timeout from VeloxTaskRunnerConfig.
        auto kShufflePushTimeout =
            std::chrono::milliseconds(self->config_.shuffle_push_timeout_ms);

        if (self->config_.num_shuffle_partitions > 1) {
          // Multi-partition: hash-partition rows and accumulate per-partition
          // row sets. The accumulator coalesces small partitions and flushes
          // automatically when a partition exceeds kMinPartitionFlushRows.
          auto numRows = vector->size();
          // Initialize the cached partitioner on first batch via double-check.
          // Fast path: atomic load (no lock) on subsequent batches.
          if (!self->shuffle_partitioner_ready_.load(std::memory_order_acquire)) {
            std::lock_guard<std::mutex> lock(self->shuffle_partitioner_mutex_);
            if (!self->shuffle_partitioner_ready_.load(std::memory_order_relaxed)) {
              auto inputType = std::dynamic_pointer_cast<
                  const facebook::velox::RowType>(vector->type());
              auto keyChannels = self->shuffle_key_channels_;
              if (keyChannels.empty() && vector->childrenSize() > 0) {
                keyChannels.push_back(0);
              }
              self->shuffle_partitioner_ =
                  std::make_shared<facebook::velox::exec::HashPartitionFunction>(
                      false, self->config_.num_shuffle_partitions,
                      inputType, keyChannels);
              self->shuffle_partitioner_ready_.store(true, std::memory_order_release);
            }
          }
          std::vector<uint32_t> partitions(numRows);
          auto singlePartition =
              self->shuffle_partitioner_->partition(*vector, partitions);

          if (singlePartition.has_value()) {
            // All rows in one partition — accumulate as a single chunk.
            std::vector<facebook::velox::vector_size_t> allRows(numRows);
            for (facebook::velox::vector_size_t i = 0; i < numRows; ++i) {
              allRows[i] = i;
            }
            if (!self->shuffle_accumulator_->accumulate(
                    static_cast<int32_t>(singlePartition.value()),
                    vector, std::move(allRows))) {
              self->cancelled_.store(true);
              std::lock_guard<std::mutex> lock(self->error_mutex_);
              self->error_message_ =
                  "Shuffle accumulator flush timed out (query_id=" +
                  self->config_.query_id +
                  ", task_id=" + self->config_.task_id + ")";
            }
          } else {
            // Distribute rows to partitions via accumulator.
            // L2-4: thread_local container reuse — avoids re-allocating the
            // outer vector + inner vectors on every batch. Each Velox Driver
            // thread processes batches sequentially, so thread_local is safe
            // and eliminates heap allocation in the hot path (10K+ batches/sec).
            thread_local std::vector<std::vector<facebook::velox::vector_size_t>>
                partitionRows;
            auto numPartitions = self->config_.num_shuffle_partitions;
            if (static_cast<int32_t>(partitionRows.size()) != numPartitions) {
              partitionRows.resize(numPartitions);
            }
            // Clear sub-vectors (retains capacity from previous batch).
            for (auto& pv : partitionRows) {
              pv.clear();
            }
            // Reserve on first use or when batch size grows.
            auto estimatedRowsPerPartition = static_cast<size_t>(
                numRows / numPartitions) + 1;
            for (auto& pv : partitionRows) {
              if (pv.capacity() < estimatedRowsPerPartition) {
                pv.reserve(estimatedRowsPerPartition);
              }
            }
            for (facebook::velox::vector_size_t r = 0; r < numRows; ++r) {
              partitionRows[partitions[r]].push_back(r);
            }
            for (int32_t pid = 0;
                 pid < self->config_.num_shuffle_partitions; ++pid) {
              if (partitionRows[pid].empty()) continue;
              // Pass by copy (not move) to preserve thread_local capacity.
              // The copy cost is O(rows_in_partition) integers — trivial
              // compared to the serialization work that follows.
              if (!self->shuffle_accumulator_->accumulate(
                      pid, vector, partitionRows[pid])) {
                self->cancelled_.store(true);
                std::lock_guard<std::mutex> lock(self->error_mutex_);
                self->error_message_ =
                    "Shuffle accumulator flush timed out (query_id=" +
                    self->config_.query_id +
                    ", task_id=" + self->config_.task_id + ")";
                break;
              }
            }
          }
        } else {
          // Single partition: serialize entire batch with partition_id=0.
          auto* pool = vector->pool();
          auto serialized = self->shuffle_writer_->serialize(vector, pool);
          shuffle::ShuffleBatch batch;
          batch.partition_id = 0;
          batch.data = std::move(serialized);
          batch.row_count = vector->size();
          self->shuffle_queue_->push(
              std::move(batch), kShufflePushTimeout);
        }

        return facebook::velox::exec::BlockingReason::kNotBlocked;
      };
    } else {
      // Non-shuffle path: push RowVector directly to OutputQueue.
      // Uses Velox ContinueFuture for cooperative backpressure: when the
      // queue is full, returns kWaitForConsumer with a future that is
      // fulfilled when space becomes available. This yields the Driver
      // thread back to Velox's scheduler instead of busy-waiting.
      //
      // IMPORTANT: Velox CallbackSink::addInput() moves the vector into
      // the consumer callback. After consumer returns kWaitForConsumer,
      // Velox does NOT re-invoke the consumer with the same vector —
      // it fetches a new vector from the upstream operator. Therefore,
      // the space callback MUST guarantee the vector is pushed before
      // fulfilling the promise. A tryPush failure in the callback would
      // silently drop the batch.
      auto weak_self = weak_from_this();
      consumer = [weak_self](
          facebook::velox::RowVectorPtr vector,
          bool /*drained*/,
          facebook::velox::ContinueFuture* future) -> facebook::velox::exec::BlockingReason {
        auto self = weak_self.lock();
        if (!self || !vector || vector->size() == 0) {
          return facebook::velox::exec::BlockingReason::kNotBlocked;
        }
        if (self->cancelled_.load()) {
          return facebook::velox::exec::BlockingReason::kNotBlocked;
        }
        // Fast path: non-blocking push succeeds immediately.
        if (self->output_queue_->tryPush(vector)) {
          return facebook::velox::exec::BlockingReason::kNotBlocked;
        }
        // Slow path: queue is full. Create a promise/future pair and
        // register the promise fulfillment as a space-available callback.
        // Velox will suspend this Driver and resume it when the future
        // is fulfilled (i.e., when the consumer pops an item).
        if (future) {
          auto [promise, sem_future] =
              facebook::velox::makeVeloxContinuePromiseContract(
                  "OutputQueue backpressure");
          auto shared_promise =
              std::make_shared<facebook::velox::ContinuePromise>(
                  std::move(promise));
          auto weak_retry = weak_self;
          auto retry_vector = vector;
          self->output_queue_->registerSpaceCallback(
              [shared_promise, weak_retry, retry_vector]() {
                // Space is now available. Push the deferred vector with a
                // blocking timeout to guarantee delivery. Velox moves the
                // vector into CallbackSink::addInput() and will NOT retry
                // with the same vector after unblocking — a failed push
                // here means permanent data loss.
                if (auto s = weak_retry.lock()) {
                  if (!s->output_queue_->tryPush(retry_vector)) {
                    // tryPush failed (race with another producer). Use
                    // blocking push with generous timeout as last resort.
                    if (!s->output_queue_->push(
                            retry_vector, std::chrono::milliseconds(30000))) {
                      // Push timed out — cancel task to surface the error
                      // instead of silently dropping the batch.
                      s->cancelled_.store(true);
                      {
                        std::lock_guard<std::mutex> lock(s->error_mutex_);
                        s->error_message_ =
                            "OutputQueue push timed out during backpressure "
                            "recovery (query_id=" + s->config_.query_id +
                            ", task_id=" + s->config_.task_id + ")";
                      }
#if __has_include(<glog/logging.h>)
                      LOG(ERROR) << "output_queue_push_timeout"
                                 << " query_id=" << s->config_.query_id
                                 << " task_id=" << s->config_.task_id
                                 << " error_type=backpressure_data_loss";
#endif
                    }
                  }
                }
                shared_promise->setValue();
              });
          *future = std::move(sem_future);
          return facebook::velox::exec::BlockingReason::kWaitForConsumer;
        }
        // Defensive fallback when future is null (should not happen in
        // parallel mode): blocking push with timeout to avoid infinite hang.
        self->output_queue_->push(vector, std::chrono::milliseconds(5000));
        return facebook::velox::exec::BlockingReason::kNotBlocked;
      };
    }
  }

  // Build spill disk options if spilling is enabled.
  std::optional<facebook::velox::common::SpillDiskOptions> spillDiskOpts;
  if (config_.spill_enabled && !config_.spill_directory.empty()) {
    auto taskSpillDir = config_.spill_directory + "/" + config_.task_id;
    try {
      std::filesystem::create_directories(taskSpillDir);
      // Validate available disk space before allowing spill.
      // If max_spill_bytes is configured, check that the filesystem has
      // at least that much free space. This prevents silent spill failures
      // mid-query when the disk fills up.
      if (config_.max_spill_bytes > 0) {
        auto spaceInfo = std::filesystem::space(taskSpillDir);
        if (static_cast<int64_t>(spaceInfo.available) < config_.max_spill_bytes) {
          PANGROVA_LOG_WARN << "spill_disk_space_low"
                          << " task_id=" << config_.task_id
                          << " path=" << taskSpillDir
                          << " available_bytes=" << spaceInfo.available
                          << " max_spill_bytes=" << config_.max_spill_bytes;
        }
      }
    } catch (const std::filesystem::filesystem_error& e) {
      throw std::runtime_error(
          "Failed to create spill directory '" + taskSpillDir + "': " + e.what());
    }
    facebook::velox::common::SpillDiskOptions opts;
    opts.spillDirPath = taskSpillDir;
    opts.spillDirCreated = true;
    spillDiskOpts = std::move(opts);
  }

  velox_task_ = facebook::velox::exec::Task::create(
      config_.task_id,
      std::move(fragment),
      0,
      std::move(queryCtx),
      execMode,
      std::move(consumer),
      0,  // memoryArbitrationPriority
      std::move(spillDiskOpts));

  std::vector<SplitLeafNode> splitLeafNodes;
  collectSplitLeafNodes(plan_, splitLeafNodes);

  // Deduplicate splits by (split_id, table_name, file_path, start_offset, length).
  // When a plan has multiple scan nodes for the same table (e.g. UNION ALL),
  // the coordinator may send one split per scan node — all pointing to the
  // same file.  Without dedup, the routing loop below broadcasts each
  // duplicate to every matching leaf node, causing N× data multiplication.
  // Note: start_offset and length are included in the key to avoid
  // over-deduplicating partitioned tables where multiple splits share
  // the same file path but cover different byte ranges.
  // split_id is included to prevent over-deduplication in self-join
  // scenarios where the same file appears on both sides of the join
  // with different split_ids but an empty table_name.
  std::vector<split::SplitConfig> uniqueSplits;
  {
    std::set<std::tuple<std::string, std::string, std::string, int64_t, int64_t>> seen;
    for (const auto& s : splits_) {
      auto key = std::make_tuple(s.split_id, s.table_name, s.file_path,
                                 s.start_offset, s.length);
      if (seen.insert(key).second) {
        uniqueSplits.push_back(s);
      }
    }
  }

  // Route splits to scan nodes by matching split table name to scan node
  // table name.  Supports both exact match and suffix match (the split may
  // carry "schema.table" while the scan node only has "table").
  // If no match is found, broadcast to all leaf nodes as fallback.
  for (const auto& splitConfig : uniqueSplits) {
    auto connectorSplit = std::make_shared<PangrovaConnectorSplit>(
        PangrovaVeloxConnector::kConnectorId, splitConfig);

    bool routed = false;
    if (!splitConfig.table_name.empty()) {
      for (const auto& leaf : splitLeafNodes) {
        if (leaf.tableName == splitConfig.table_name ||
            matchesSuffix(leaf.tableName, splitConfig.table_name) ||
            matchesSuffix(splitConfig.table_name, leaf.tableName)) {
          velox_task_->addSplit(leaf.nodeId,
              facebook::velox::exec::Split(connectorSplit));
          routed = true;
        }
      }
    }
    // Fallback: broadcast to all leaf nodes (backward compatible).
    if (!routed) {
      for (const auto& leaf : splitLeafNodes) {
        velox_task_->addSplit(leaf.nodeId,
            facebook::velox::exec::Split(connectorSplit));
      }
    }
  }

  for (const auto& leaf : splitLeafNodes) {
    velox_task_->noMoreSplits(leaf.nodeId);
  }

  // Register with NativeProfiler for operator-level profiling.
  observability::NativeProfiler::instance().registerTask(
      config_.task_id, velox_task_);

  // Cache output type for FlightResultServer — DoGet may arrive after
  // the task completes and leaves getRunningTasks().
  if (config_.is_result_task) {
    auto output_type = std::dynamic_pointer_cast<const facebook::velox::RowType>(
        plan_->outputType());
#if PANGROVA_HAS_FLIGHT
    if (output_type) {
      pangrova::result::FlightResultServer::registerTaskOutputType(
          config_.task_id, output_type);
      pangrova::result::FlightResultServer::registerTaskSerdeMode(
          config_.task_id, config_.result_serde_mode);
    }
#endif
  }
}

void VeloxTaskRunner::cancel() {
  cancelled_.store(true);
  if (velox_task_) {
    velox_task_->requestCancel().wait();
  }
  cleanup();
  finished_.store(true);
}

bool VeloxTaskRunner::waitForCompletion() {
  if (!velox_task_) {
    {
      std::lock_guard<std::mutex> lock(error_mutex_);
      error_message_ = "Velox task is not initialized";
    }
    finished_.store(true);
    return false;
  }

  try {
    // Parallel mode: Task::start() initiates multi-Driver execution.
    // Use per-query override if set (> 0), otherwise fall back to default max_drivers.
    int32_t effective_drivers = config_.max_drivers_override > 0
        ? config_.max_drivers_override
        : config_.max_drivers;
    velox_task_->start(effective_drivers);

    // Wait until the task finishes via taskCompletionFuture.
    // For result tasks with PartitionedOutputNode, this future resolves
    // only after all output is consumed by FlightResultServer::DoGet().
    // The Java DAG dispatch layer handles the result-task lifecycle:
    // it completes the DAG future when the task reaches RUNNING state
    // (not FINISHED), allowing the coordinator to fetch results via
    // FlightResultServer, which unblocks the Velox task completion.
    velox_task_->taskCompletionFuture().wait();

    if (cancelled_.load()) {
      if (output_queue_) {
        output_queue_->close();
      }
      finished_.store(true);
      return true;
    }

    auto state = velox_task_->state();
    if (state == facebook::velox::exec::TaskState::kFailed) {
      {
        std::lock_guard<std::mutex> lock(error_mutex_);
        error_message_ = velox_task_->errorMessage();
      }
#if __has_include(<glog/logging.h>)
      LOG(ERROR) << "velox_task_failed"
                 << " query_id=" << config_.query_id
                 << " task_id=" << config_.task_id
                 << " error_type=velox_execution"
                 << " error_detail=" << velox_task_->errorMessage();
#endif
      cleanup();
      finished_.store(true);
      return false;
    }

    // Defer cleanup() to PlanExecutionDriver — it must drain the OutputQueue
    // before cleanup closes it and releases RowVector memory pools.
    // cleanup() is idempotent (std::call_once) so the caller can safely
    // call it again, and the destructor path also calls it.
    finished_.store(true);
    {
      std::lock_guard<std::mutex> lock(error_mutex_);
      return error_message_.empty();
    }
  } catch (const pangrova::executor::NativeUnsupportedFeatureException& e) {
    // Preserve structured metadata for fallback reporting.
    {
      std::lock_guard<std::mutex> lock(error_mutex_);
      error_message_ = std::string("NATIVE_FALLBACK_REQUIRED: ") + e.what();
    }
#if __has_include(<glog/logging.h>)
    LOG(ERROR) << "native_unsupported_feature"
               << " query_id=" << config_.query_id
               << " task_id=" << config_.task_id
               << " error_type=unsupported_feature"
               << " feature_name=" << e.featureName()
               << " connector_type=" << e.connectorType()
               << " split_id=" << e.splitId();
#endif
    cleanup();
    finished_.store(true);
    return false;
  } catch (const std::exception& e) {
    {
      std::lock_guard<std::mutex> lock(error_mutex_);
      error_message_ = e.what();
    }
#if __has_include(<glog/logging.h>)
    LOG(ERROR) << "velox_task_exception"
               << " query_id=" << config_.query_id
               << " task_id=" << config_.task_id
               << " error_type=runtime_exception"
               << " error_detail=" << e.what();
#endif
    cleanup();
    finished_.store(true);
    return false;
  }
}

folly::SemiFuture<bool> VeloxTaskRunner::startAsync() {
  if (!velox_task_) {
    {
      std::lock_guard<std::mutex> lock(error_mutex_);
      error_message_ = "Velox task is not initialized";
    }
    finished_.store(true);
    return folly::makeSemiFuture(false);
  }
  try {
    velox_task_->start(config_.max_drivers);
  } catch (const std::exception& e) {
    {
      std::lock_guard<std::mutex> lock(error_mutex_);
      error_message_ = std::string("Velox task start failed: ") + e.what();
    }
    finished_.store(true);
    return folly::makeSemiFuture(false);
  }
  auto self = shared_from_this();
  return velox_task_->taskCompletionFuture().deferValue(
      [self](auto /*state*/) -> bool {
        return self->handleTaskCompletion();
      });
}

bool VeloxTaskRunner::handleTaskCompletion() {
  if (cancelled_.load()) {
    if (output_queue_) {
      output_queue_->close();
    }
    finished_.store(true);
    return true;
  }
  auto task_state = velox_task_->state();
  if (task_state == facebook::velox::exec::TaskState::kFailed) {
    {
      std::lock_guard<std::mutex> lock(error_mutex_);
      error_message_ = velox_task_->errorMessage();
    }
#if __has_include(<glog/logging.h>)
    LOG(ERROR) << "velox_task_failed"
               << " query_id=" << config_.query_id
               << " task_id=" << config_.task_id
               << " error_type=velox_execution"
               << " error_detail=" << velox_task_->errorMessage();
#endif
    cleanup();
    finished_.store(true);
    return false;
  }
  finished_.store(true);
  std::lock_guard<std::mutex> lock(error_mutex_);
  return error_message_.empty();
}

bool VeloxTaskRunner::isFinished() const {
  return finished_.load();
}

bool VeloxTaskRunner::isResultSerdeUnsupportedTypeError() const {
  std::lock_guard<std::mutex> lock(error_mutex_);
  // ArrowBatchUnsupportedTypeException produces messages starting with
  // "ArrowBatch serde does not support type: ".
  static constexpr const char* kUnsupportedTypePrefix =
      "ArrowBatch serde does not support type: ";
  return !error_message_.empty() &&
         error_message_.find(kUnsupportedTypePrefix) != std::string::npos;
}

std::string VeloxTaskRunner::errorMessage() const {
  std::lock_guard<std::mutex> lock(error_mutex_);
  return error_message_;
}

void VeloxTaskRunner::injectDynamicFilter(
    const std::string& plan_node_id,
    facebook::velox::column_index_t column_index,
    std::unique_ptr<facebook::velox::common::Filter> filter) {
  if (!velox_task_ || !filter) {
    return;
  }

  // Route dynamic filter to the process-global PendingFilterRegistry.
  // PangrovaVeloxDataSource::next() drains pending filters from the registry
  // on each batch read, applying them as row-level filters.
  //
  // This bridges the gap between the external injection path (Java → JNI →
  // TaskExecutor → VeloxTaskRunner) and the DataSource that applies filters.
  PendingFilterRegistry::instance().push(
      config_.task_id, column_index,
      std::shared_ptr<facebook::velox::common::Filter>(std::move(filter)));

#if __has_include(<glog/logging.h>)
  LOG(INFO) << "dynamic_filter_registered"
            << " query_id=" << config_.query_id
            << " task_id=" << config_.task_id
            << " plan_node_id=" << plan_node_id
            << " column_index=" << column_index;
#endif
}

facebook::velox::exec::Task* VeloxTaskRunner::veloxTask() const {
  return velox_task_.get();
}

void VeloxTaskRunner::attachPoolHolders(
    std::vector<std::shared_ptr<facebook::velox::memory::MemoryPool>>
        holders) {
  plan_pool_holders_ = std::move(holders);
}

std::vector<uint8_t> VeloxTaskRunner::collectTaskStats() const {
  if (!velox_task_) {
    return {};
  }
  // Delegate to shared serialization utility to avoid duplicate logic
  // with StatusSerializer::serializeNativeTaskStats().
  return pangrova::jni::serializeNativeTaskStats(velox_task_->taskStats());
}

void VeloxTaskRunner::cleanup() {
  std::call_once(cleanup_once_, [this] {
    observability::NativeProfiler::instance().unregisterTask(config_.task_id);
    PendingFilterRegistry::instance().remove(config_.task_id);
    // Fallback cleanup for output type cache. The primary cleanup is in
    // OutputBufferBatchReader destructor (after DoGet completes). This
    // handles the case where the task fails and DoGet is never called,
    // preventing unbounded cache growth. The unregister call is idempotent
    // — if the reader already cleaned up, this is a no-op (map erase on
    // missing key).
    if (config_.is_result_task) {
#if PANGROVA_HAS_FLIGHT
      pangrova::result::FlightResultServer::unregisterTaskOutputType(
          config_.task_id);
      pangrova::result::FlightResultServer::unregisterTaskSerdeMode(
          config_.task_id);
#endif
    }
    // Result tasks use Velox OutputBufferManager — no OutputQueue to close.
    // Flush any remaining accumulated shuffle rows before closing queues.
    // This ensures rows below kMinPartitionFlushRows are not lost.
    if (shuffle_accumulator_) {
      // Use the task's memory pool from velox_task_ if available.
      facebook::velox::memory::MemoryPool* flush_pool = nullptr;
      if (velox_task_) {
        flush_pool = velox_task_->pool();
      }
      if (flush_pool) {
        shuffle_accumulator_->flushAll(flush_pool);
      }
      shuffle_accumulator_.reset();
    }
    if (output_queue_) {
      output_queue_->close();
    }
    // P1-VELOX-5: Clean up spill directory for this task.
    if (config_.spill_enabled && !config_.spill_directory.empty()) {
      auto taskSpillDir = config_.spill_directory + "/" + config_.task_id;
      try {
        // Remove spill directory directly without pre-traversal size
        // computation. The previous recursive_directory_iterator scan
        // blocked the cleanup path (including cancel) for large spill
        // directories. Spill byte accounting is handled by Velox's
        // SpillStats and reported via collectTaskStats() instead.
        std::filesystem::remove_all(taskSpillDir);
      } catch (const std::filesystem::filesystem_error& e) {
        std::cerr << "level=WARN event=spill_dir_cleanup_failed"
                  << " task_id=" << config_.task_id
                  << " path=" << taskSpillDir
                  << " error=" << e.what()
                  << std::endl;
      }
    }
  });
}

}  // namespace pangrova::velox
