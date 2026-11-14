// VeloxTaskRunner: Drives Velox exec::Task and feeds output to OutputQueue.
#include "engine/velox/runner/VeloxTaskRunner.h"

#include <chrono>
#include <filesystem>
#include <set>
#include <stdexcept>
#include <unordered_set>
#include <vector>

#include "jni/RuntimeInitializer.h"
#include "indexing/index/IndexHintFilter.h"
#include "engine/observability/PangrovaLog.h"
#include "engine/observability/NativeProfiler.h"
#include "engine/shuffle/SerializedShuffleQueue.h"
#include "engine/shuffle/ShuffleBatchSerializer.h"

#include "engine/velox/runner/TaskStatsEncoding.h"
#include "engine/result/FlightResultServer.h"
#include "capabilities/capability/LogicalTypeContract.h"

// Logging for dynamic filter injection warnings.
#if __has_include(<glog/logging.h>)
#include <glog/logging.h>
#endif
#include "engine/velox/connector/PangrovaVeloxConnector.h"
#include "engine/velox/filter/PendingFilterRegistry.h"
#include "engine/velox/exchange/PendingEndpointRegistry.h"
#include "engine/executor/NativeUnsupportedFeatureException.h"
#include "engine/velox/memory/VeloxMemoryGovernor.h"
#include "velox/core/PlanFragment.h"
#include "velox/core/QueryConfig.h"
#include "engine/velox/serde/CanonicalSerdeOptionsFactory.h"
#include "engine/velox/serde/CanonicalArrowSchema.h"
#include "engine/columnar/CanonicalBatchStream.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/Task.h"
#include "velox/exec/Driver.h"
#include "velox/exec/TaskStats.h"
#include "velox/common/future/VeloxPromise.h"
#include "velox/exec/HashPartitionFunction.h"
#include "velox/exec/Exchange.h"

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
  auto scanNode = std::dynamic_pointer_cast<
      const facebook::velox::core::TableScanNode>(node);
  if (scanNode) {
    std::string tableName;
    if (scanNode->tableHandle()) {
      tableName = scanNode->tableHandle()->name();
    }
    leafNodes.push_back({node->id(), std::move(tableName)});
    return;
  }
  for (const auto& source : node->sources()) {
    collectSplitLeafNodes(source, leafNodes);
  }
}

void collectExchangeNodes(
    const std::shared_ptr<const facebook::velox::core::PlanNode>& node,
    std::vector<facebook::velox::core::PlanNodeId>& node_ids) {
  if (!node) {
    return;
  }
  if (std::dynamic_pointer_cast<
          const facebook::velox::core::ExchangeNode>(node)) {
    node_ids.push_back(node->id());
  }
  for (const auto& source : node->sources()) {
    collectExchangeNodes(source, node_ids);
  }
}

// Velox's scaled scan parks all but the first scan driver until a non-empty
// split permits scale-up (or the scan operator closes). Join build operators
// wait for every peer driver before closing their source operators. An empty
// build-side scan therefore leaves parked drivers and the join waiting on
// each other. Keep the optimization on scan pipelines without a join barrier.
bool containsJoinBarrier(
    const std::shared_ptr<const facebook::velox::core::PlanNode>& node) {
  if (!node) {
    return false;
  }
  if (std::dynamic_pointer_cast<const facebook::velox::core::HashJoinNode>(node) ||
      std::dynamic_pointer_cast<const facebook::velox::core::NestedLoopJoinNode>(node) ||
      std::dynamic_pointer_cast<const facebook::velox::core::SpatialJoinNode>(node)) {
    return true;
  }
  for (const auto& source : node->sources()) {
    if (containsJoinBarrier(source)) {
      return true;
    }
  }
  return false;
}

// A queue-space callback runs on the consumer thread. Never wait for space
// there: another producer may win the freed slot before this callback does.
void scheduleDeferredOutput(
    std::weak_ptr<VeloxTaskRunner> owner,
    pangrova::executor::OutputQueue* queue,
    facebook::velox::RowVectorPtr batch,
    std::shared_ptr<facebook::velox::ContinuePromise> promise) {
  auto task = owner.lock();
  if (!task) {
    promise->setValue();
    return;
  }
  auto result = queue->tryPushOrDefer(
      batch, [owner, queue, batch, promise]() {
        scheduleDeferredOutput(owner, queue, batch, promise);
      });
  if (result != pangrova::executor::DeferredPushResult::DEFERRED) {
    promise->setValue();
  }
}

}  // namespace

VeloxTaskRunner::VeloxTaskRunner(
    const VeloxTaskRunnerConfig& config,
    std::shared_ptr<const facebook::velox::core::PlanNode> plan,
    const std::vector<split::SplitConfig>& splits,
    pangrova::executor::OutputQueue* output_queue,
    pangrova::shuffle::SerializedShuffleQueue* shuffle_queue,
    std::unordered_map<std::string, std::vector<split::SplitConfig>>
        scan_split_overrides)
    : config_(config),
      plan_(std::move(plan)),
      splits_(splits),
      scan_split_overrides_(std::move(scan_split_overrides)),
      output_queue_(output_queue),
      shuffle_queue_(shuffle_queue) {
  if (!config_.function_registry) {
    throw std::invalid_argument("VeloxTaskRunner requires an admitted owned function registry");
  }
  if (config_.is_result_task && config_.executor_handle > 0 && !config_.failure_reporter) {
    throw std::invalid_argument("A bound result task requires its failure reporter");
  }
  if (!plan_) {
    throw std::invalid_argument("VeloxTaskRunner: plan must not be null");
  }
  if (config_.is_result_task && !config_.canonical_output_schema) {
    throw std::invalid_argument("Result task requires its admitted canonical output schema");
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
  // Distinct volatile expression sites must retain separate invocation identities.
  query_config_values[facebook::velox::core::QueryConfig::kExprDedupNonDeterministic] = "false";
  // Each occurrence of a non-deterministic SQL function must be evaluated
  // independently. The pinned SDK defaults to sharing these expressions;
  // this is a semantic invariant, not a session performance override.
  query_config_values[facebook::velox::core::QueryConfig::kExprDedupNonDeterministic] = "false";
  if (config_.is_result_task || config_.is_shuffle_task) {
    if (config_.serialized_output_buffer_threshold_bytes <= 0) {
      throw std::runtime_error("Serialized output requires a positive serialized-buffer grant");
    }
    const auto threshold = std::to_string(config_.serialized_output_buffer_threshold_bytes);
    query_config_values[facebook::velox::core::QueryConfig::kMaxOutputBufferSize] = threshold;
    query_config_values[facebook::velox::core::QueryConfig::kMaxPartitionedOutputBufferSize] = threshold;
  }
  // The Flight shuffle writer and Velox Exchange deserializer must use the
  // same codec. Small pages may be emitted uncompressed, but larger pages
  // carry the configured codec and otherwise fail during Exchange::getOutput.
  query_config_values[facebook::velox::core::QueryConfig::kShuffleCompressionKind] =
      config_.shuffle_compression_codec;
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

  // Engine-owned execution identity. These values originate from the
  // internally authenticated coordinator task request, not user SQL, and are
  // consumed by JVM-backed AI functions for authorization, deadline and audit.
  query_config_values["pangrova.task_id"] = config_.task_id;
  query_config_values["pangrova.executor_handle"] =
      std::to_string(config_.executor_handle);
  query_config_values["pangrova.query_id"] = config_.query_id;
  query_config_values["pangrova.tenant_id"] = config_.tenant_id;
  query_config_values["pangrova.principal"] = config_.principal;
  query_config_values["pangrova.policy_context_hash"] =
      config_.policy_context_hash;
  query_config_values["pangrova.dependency_scope_hash"] =
      config_.dependency_scope_hash;
  query_config_values["pangrova.dependency_version_hash"] =
      config_.dependency_version_hash;
  query_config_values["pangrova.trace_id"] = config_.trace_id;
  query_config_values["pangrova.deadline_epoch_ms"] =
      std::to_string(config_.deadline_epoch_ms);
  query_config_values["pangrova.provider_concurrency"] =
      std::to_string(config_.max_provider_concurrency);
  query_config_values["pangrova.execution_callback_context"] = config_.callback_context;
  query_config_values["pangrova.media_bytes"] =
      std::to_string(config_.max_media_bytes);

  // Lazy materialization: Velox enables lazy vector loading by default.
  // Ensure it is not explicitly disabled.
  if (config_.lazy_materialization) {
    query_config_values["debug_disable_expression_with_lazy_inputs"] = "false";
  }

  // Performance tuning knobs (Req 6): enable high-value Velox QueryConfig
  // settings that improve hash join, table scan, and filter performance.
  query_config_values[facebook::velox::core::QueryConfig::kHashProbeFinishEarlyOnEmptyBuild] =
      config_.hash_probe_finish_early_on_empty_build ? "true" : "false";
  query_config_values[facebook::velox::core::QueryConfig::kHashProbeStringDynamicFilterPushdownEnabled] =
      config_.hash_probe_string_dynamic_filter_pushdown ? "true" : "false";
  query_config_values[facebook::velox::core::QueryConfig::kAdaptiveFilterReorderingEnabled] =
      config_.adaptive_filter_reordering ? "true" : "false";
  query_config_values[facebook::velox::core::QueryConfig::kParallelOutputJoinBuildRowsEnabled] =
      config_.parallel_output_join_build_rows ? "true" : "false";
  const bool scaled_scan_eligible = !containsJoinBarrier(plan_);
  query_config_values[facebook::velox::core::QueryConfig::kTableScanScaledProcessingEnabled] =
      config_.table_scan_scaled_processing && scaled_scan_eligible ? "true" : "false";
  if (config_.table_scan_scaled_processing && !scaled_scan_eligible) {
    PANGROVA_LOG_INFO << "event=scaled_scan_ineligible reason=join_peer_barrier"
                      << " task_id=" << config_.task_id;
  }
  if (config_.max_split_preload_per_driver < 0) {
    throw std::invalid_argument("max_split_preload_per_driver must be non-negative");
  }
  query_config_values[facebook::velox::core::QueryConfig::kMaxSplitPreloadPerDriver] =
      std::to_string(config_.max_split_preload_per_driver);

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
  if (!executor || !memGovernor) {
    throw std::runtime_error(
        "VeloxTaskRunner: Native runtime is not initialized");
  }
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

  query_config_values["require_function_registry_snapshot"] = "true";
  query_config_values["require_vector_serde_options_factory"] = "true";
  auto serdeFactory = std::make_shared<const CanonicalSerdeOptionsFactory>(
      config_.serde_schema_bindings, columnar::kMaximumBatchStreamBytes);
  queryCtx = facebook::velox::core::QueryCtx::create(
      executor.get(),
      facebook::velox::core::QueryConfig{std::move(query_config_values), config_.function_registry, serdeFactory},
      std::move(connectorConfigs), asyncCache, std::move(taskPool));

  // Execution mode: always parallel. PlanExecutionDriver enforces
  // max_drivers >= 2, and parallel mode (Task::start() +
  // taskCompletionFuture()) delegates scheduling to Velox's internal
  // executor, correctly handling multi-pipeline plans (JOIN/agg).
  auto execMode = facebook::velox::exec::Task::ExecutionMode::kParallel;

  // Create ShuffleBatchSerializer for shuffle tasks.
  if (config_.is_shuffle_task && shuffle_queue_) {
    shuffle::ShuffleBatchSerializer::Config writerConfig;
    writerConfig.canonical_schema = config_.canonical_output_schema;
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
  // PartitionedOutputNode that serializes output via ArrowBatchVectorSerde and
  // routes it through Velox's OutputBufferManager automatically. No consumer
  // callback is needed — passing nullptr lets Velox use OutputBufferManager.
  // FlightResultServer pulls the canonical Arrow streams from OutputBufferManager.
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
          if (!self->config_.shuffle_output_spec) {
            throw std::runtime_error(
                "Multi-partition shuffle lacks producer ExchangeRel binding");
          }
          const auto distribution =
              self->config_.shuffle_output_spec->distribution;
          if (distribution != ShuffleOutputSpec::Distribution::HASH) {
            const auto numRows = vector->size();
            const auto numPartitions = self->config_.num_shuffle_partitions;
            std::vector<std::vector<facebook::velox::vector_size_t>> partitionRows(
                numPartitions);
            if (distribution == ShuffleOutputSpec::Distribution::BROADCAST) {
              for (int32_t pid = 0; pid < numPartitions; ++pid) {
                partitionRows[pid].reserve(numRows);
                for (facebook::velox::vector_size_t row = 0; row < numRows; ++row) {
                  partitionRows[pid].push_back(row);
                }
              }
            } else {
              const uint64_t first = self->shuffle_next_row_.fetch_add(numRows);
              for (facebook::velox::vector_size_t row = 0; row < numRows; ++row) {
                partitionRows[(first + row) % numPartitions].push_back(row);
              }
            }
            for (int32_t pid = 0; pid < numPartitions; ++pid) {
              if (!partitionRows[pid].empty() &&
                  !self->shuffle_accumulator_->accumulate(
                      pid, vector, std::move(partitionRows[pid]))) {
                self->cancelled_.store(true);
                std::lock_guard<std::mutex> lock(self->error_mutex_);
                self->error_message_ =
                    "Shuffle accumulator flush timed out (query_id=" +
                    self->config_.query_id + ", task_id=" +
                    self->config_.task_id + ")";
                break;
              }
            }
            return facebook::velox::exec::BlockingReason::kNotBlocked;
          }
          // Multi-partition: hash-partition rows and accumulate per-partition
          // row sets. The accumulator coalesces small partitions and flushes
          // automatically when a partition exceeds kMinPartitionFlushRows.
          auto numRows = vector->size();
          std::vector<uint32_t> partitions(numRows);
          std::optional<uint32_t> singlePartition;
          {
            // HashPartitionFunction::partition() mutates its VectorHasher
            // DecodedVector and scratch buffers, so it is not thread-safe.
            // The consumer is shared by all Drivers of a Task, so the cached
            // partitioner must be serialized.
            std::lock_guard<std::mutex> lock(self->shuffle_partitioner_mutex_);
            if (!self->shuffle_partitioner_) {
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
            }
            singlePartition =
                self->shuffle_partitioner_->partition(*vector, partitions);
          }

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
          // Single partition: serialize into canonical bounded pages.
          auto* pool = vector->pool();
          auto serialized_batches =
              self->shuffle_writer_->serializeBatches(vector, pool);
          for (auto& serialized : serialized_batches) {
            shuffle::ShuffleBatch batch;
            batch.partition_id = 0;
            batch.data = std::move(serialized.data);
            batch.row_count = serialized.row_count;
            if (!self->shuffle_queue_->push(
                    std::move(batch), kShufflePushTimeout)) {
              self->cancelled_.store(true);
              std::lock_guard<std::mutex> lock(self->error_mutex_);
              self->error_message_ =
                  "Shuffle batch push timed out (query_id=" +
                  self->config_.query_id +
                  ", task_id=" + self->config_.task_id + ")";
              break;
            }
          }
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
          // The deferred vector remains owned by the callback until it has
          // actually entered the queue; only then may Velox resume its driver.
          scheduleDeferredOutput(weak_self, self->output_queue_, vector,
                                 shared_promise);
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

  // Publish the result schema before the OutputBuffer becomes visible to
  // Flight DoGet. The cache carries exact logical column types with the
  // physical Velox row type as one atomic registration.
  if (config_.is_result_task) {
    auto output_type = std::dynamic_pointer_cast<const facebook::velox::RowType>(
        plan_->outputType());
    if (output_type) {
      pangrova::result::FlightResultServer::TaskOutputInfo info;
      info.output_type = output_type;
      info.result_column_types = config_.result_column_types;
      info.column_labels = config_.result_column_labels;
      info.pool = std::make_shared<pangrova::memory::ColumnarMemoryPool>(
          velox_task_->pool()->addLeafChild("result-stream"));
      info.wire_schema = canonicalArrowSchema(
          *config_.canonical_output_schema, info.pool->source().get());
      if (config_.result_output_schema) {
        info.result_schema = canonicalArrowSchema(
            *config_.result_output_schema, info.pool->source().get());
      }
      // Check exact nested types/names while permitting task-local ordinal aliases.
      physicalArrowSchema(info.wire_schema, *config_.canonical_output_schema, output_type);
      for (const auto& column : info.result_column_types) {
        if (column.column_index >= static_cast<uint32_t>(info.wire_schema->num_fields()) ||
            column.logical_type.substrait_type().SerializeAsString() !=
                config_.canonical_output_schema->struct_().types(column.column_index).SerializeAsString()) {
          throw std::invalid_argument("VECTOR result declaration differs from canonical output schema");
        }
        capability::validateVectorResultField(column.logical_type, *info.wire_schema->field(column.column_index));
      }
      result_ownership_ = std::make_shared<pangrova::result::ResultOwnershipState>();
      info.ownership = result_ownership_;
      result_buffers_ = facebook::velox::exec::OutputBufferManager::getInstanceRef();
      info.buffers = result_buffers_;
      // Direct runner tests have no executor registry owner. Canonical Task
      // execution always binds a positive handle before the runner is built.
      if (config_.executor_handle > 0) {
        info.report_stream_failure = [handle = config_.executor_handle, reporter = config_.failure_reporter](const std::string& failure) {
          reporter->reportResultStreamFailure(handle, failure);
        };
      }
      if (!info.authority.ParseFromString(config_.callback_context)) {
        throw std::runtime_error("Result task lacks its admitted callback authority");
      }
      pangrova::result::FlightResultServer::registerTaskOutputType(config_.task_id, std::move(info));
    }
  }

  std::vector<facebook::velox::core::PlanNodeId> exchangeNodeIds;
  collectExchangeNodes(plan_, exchangeNodeIds);
  if (!exchangeNodeIds.empty() && config_.executor_handle <= 0) {
    throw std::runtime_error(
        "Exchange plan requires a registry-bound executor handle");
  }
  for (const auto& nodeId : exchangeNodeIds) {
    auto binding = PendingEndpointRegistry::instance().takeSealedInput(
        config_.executor_handle, nodeId);
    if (!binding.has_value()) {
      throw std::runtime_error(
          "Exchange input is missing or not sealed: plan_node_id=" + nodeId);
    }
    if (binding->sources.size() != binding->expected_source_partitions) {
      throw std::runtime_error(
          "Sealed exchange input cardinality changed: plan_node_id=" + nodeId);
    }
    for (const auto& source : binding->sources) {
      auto remoteSplit = std::make_shared<
          facebook::velox::exec::RemoteConnectorSplit>(
          source.runtime_source_id);
      velox_task_->addSplit(
          nodeId, facebook::velox::exec::Split(std::move(remoteSplit)));
    }
    velox_task_->noMoreSplits(nodeId);
  }

  std::vector<SplitLeafNode> splitLeafNodes;
  collectSplitLeafNodes(plan_, splitLeafNodes);

  std::unordered_set<std::string> overridden_nodes;
  size_t index_split_total = 0;
  size_t index_split_skipped = 0;
  for (const auto& leaf : splitLeafNodes) {
    auto override = scan_split_overrides_.find(leaf.nodeId);
    if (override == scan_split_overrides_.end()) {
      continue;
    }
    overridden_nodes.emplace(leaf.nodeId);
    for (const auto& split_config : override->second) {
      bool index_evaluated = false;
      if (!index::IndexHintFilter::shouldKeepTypedSplit(
              split_config.file_scan_detail, split_config.file_path, &index_evaluated)) {
        ++index_split_total;
        ++index_split_skipped;
        continue;
      }
      if (index_evaluated) {
        ++index_split_total;
      }
      auto connector_split = std::make_shared<PangrovaConnectorSplit>(
          PangrovaVeloxConnector::kConnectorId, split_config);
      velox_task_->addSplit(
          leaf.nodeId, facebook::velox::exec::Split(connector_split));
    }
  }
  if (overridden_nodes.size() != scan_split_overrides_.size()) {
    throw std::runtime_error(
        "Retrieval split override references an unknown scan node");
  }

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
    bool index_evaluated = false;
    if (!index::IndexHintFilter::shouldKeepTypedSplit(
            splitConfig.file_scan_detail, splitConfig.file_path, &index_evaluated)) {
      ++index_split_total;
      ++index_split_skipped;
      continue;
    }
    if (index_evaluated) {
      ++index_split_total;
    }
    auto connectorSplit = std::make_shared<PangrovaConnectorSplit>(
        PangrovaVeloxConnector::kConnectorId, splitConfig);

    bool routed = false;
    if (!splitConfig.table_name.empty()) {
      for (const auto& leaf : splitLeafNodes) {
        if (overridden_nodes.contains(leaf.nodeId)) {
          continue;
        }
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
        if (overridden_nodes.contains(leaf.nodeId)) {
          continue;
        }
        velox_task_->addSplit(leaf.nodeId,
            facebook::velox::exec::Split(connectorSplit));
      }
    }
  }

  if (index_split_skipped > 0) {
    PANGROVA_LOG_INFO << "event=native_index_split_filter task_id=" << config_.task_id
                      << " total=" << index_split_total
                      << " skipped=" << index_split_skipped;
  }
  index_splits_evaluated_.store(index_split_total, std::memory_order_release);
  index_splits_skipped_.store(index_split_skipped, std::memory_order_release);

  for (const auto& leaf : splitLeafNodes) {
    velox_task_->noMoreSplits(leaf.nodeId);
  }

  // Register with NativeProfiler for operator-level profiling.
  observability::NativeProfiler::instance().registerTask(
      config_.task_id, velox_task_);

}

void VeloxTaskRunner::cancel() {
  cancelled_.store(true);
  revokeResultOutput();
  if (velox_task_) {
    velox_task_->requestCancel().wait();
  }
  if (result_ownership_) result_ownership_->stopProducer();
  cleanup();
  finished_.store(true);
}

void VeloxTaskRunner::releaseTaskAndWaitForDeletion() {
  if (!velox_task_) {
    return;
  }

  // A completion future is fulfilled when the Task becomes terminal, before
  // the executor necessarily releases its last Driver/DriverCtx reference.
  // Keep this runner (and its plan-conversion pool holders) alive while the
  // Task destructor clears the plan and all vectors backed by those pools.
  cancelled_.store(true);
  revokeResultOutput();
  velox_task_->requestCancel().wait();
  cleanup();
  if (result_ownership_) {
    result_ownership_->stopProducer();
  }
  auto deletion_future = velox_task_->taskDeletionFuture();
  velox_task_.reset();
  deletion_future.wait();
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
    const int32_t effective_drivers = config_.max_drivers_override > 0
        ? config_.max_drivers_override
        : config_.max_drivers;
    velox_task_->start(effective_drivers);
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
    const std::string failure = velox_task_->errorMessage();
    {
      std::lock_guard<std::mutex> lock(error_mutex_);
      error_message_ = failure;
    }
    // A Flight reader may observe result revocation as soon as cleanup runs.
    // Publish the producer's actual failure first so that the later stream
    // cancellation cannot replace the canonical execution diagnostic.
    if (config_.is_result_task && config_.executor_handle > 0) {
      protocol::v2::ExecutionError error;
      error.set_error_class(protocol::v2::ERROR_CLASS_INTERNAL);
      error.set_code("NATIVE_TASK_FAILED");
      error.set_message(failure.empty() ? "Native result producer failed" : failure);
      error.set_retryable(false);
      error.set_failed_component("native-executor");
      config_.failure_reporter->reportExecutionError(config_.executor_handle, error);
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

pangrova::result::ResultOwnershipState::Evidence VeloxTaskRunner::resultDrainEvidence() const {
  if (!result_ownership_) return {false, false, false, 0, 0};
  if (velox_task_ && velox_task_->state() != facebook::velox::exec::TaskState::kRunning) {
    result_ownership_->stopProducer();
  }
  return result_ownership_->evidence();
}

std::string VeloxTaskRunner::errorMessage() const {
  std::lock_guard<std::mutex> lock(error_mutex_);
  return error_message_;
}

void VeloxTaskRunner::injectDynamicFilter(
    const std::string& plan_node_id,
    facebook::velox::column_index_t column_index,
    std::unique_ptr<facebook::velox::common::Filter> filter) {
  if (!filter) {
    return;
  }

  // Route dynamic filter to the process-global PendingFilterRegistry.
  // PangrovaVeloxDataSource::next() drains pending filters from the registry
  // on each batch read, applying them as row-level filters.
  //
  // This bridges the gap between the external injection path (Java → JNI →
  // TaskExecutor → VeloxTaskRunner) and the DataSource that applies filters.
  PendingFilterRegistry::instance().push(
      config_.task_id, plan_node_id, column_index,
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

std::vector<uint8_t> VeloxTaskRunner::collectTaskStats() const {
  if (!velox_task_) {
    return {};
  }
  // Delegate to shared serialization utility to avoid duplicate logic
  // with the wire-format per-operator counters.
  return serializeNativeTaskStats(
      velox_task_->taskStats(),
      index_splits_evaluated_.load(std::memory_order_acquire),
      index_splits_skipped_.load(std::memory_order_acquire));
}

void VeloxTaskRunner::finalizeShuffleOutput() {
  std::lock_guard<std::mutex> lock(shuffle_accumulator_mutex_);
  if (!shuffle_queue_ || !shuffle_accumulator_) {
    return;
  }
  auto* flush_pool = velox_task_ ? velox_task_->pool() : nullptr;
  if (!flush_pool || !shuffle_accumulator_->flushAll(flush_pool)) {
    throw std::runtime_error(
        "Failed to flush all buffered shuffle rows before producer completion");
  }
  shuffle_accumulator_.reset();
}

void VeloxTaskRunner::revokeResultOutput() {
  if (config_.is_result_task) {
    if (result_ownership_) result_ownership_->revokeAuthority();
    pangrova::result::FlightResultServer::unregisterTaskOutputType(config_.task_id);
    if (result_buffers_) {
      result_buffers_->deleteResults(config_.task_id, 0);
      if (result_ownership_) result_ownership_->revokeAndClearQueue();
    }
  }
}

void VeloxTaskRunner::cleanup() {
  std::call_once(cleanup_once_, [this] {
    observability::NativeProfiler::instance().unregisterTask(config_.task_id);
    PendingFilterRegistry::instance().remove(config_.task_id);
    // Successful producer completion is not consumption or result revocation.
    if (cancelled_.load() || !errorMessage().empty()) revokeResultOutput();
    // Result tasks use Velox OutputBufferManager — no OutputQueue to close.
    // A successful producer flushes explicitly before publishing FINISHED.
    // Cancellation/failure discards buffered rows; its queue is aborted.
    {
      std::lock_guard<std::mutex> lock(shuffle_accumulator_mutex_);
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
