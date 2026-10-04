// TaskExecutor: Orchestrator that delegates to TaskLifecycle,
// PlanExecutionDriver, and TaskOutputRouter.
//
// Lifecycle: PENDING → RUNNING → FLUSHING → FINISHED/FAILED/CANCELLED
// Terminal states are monotonic — no further transitions once reached.
#include "executor/driver/TaskExecutor.h"

#include <chrono>
#include <stdexcept>

#include "cache/SemanticResultCache.h"
#include "jni/RuntimeInitializer.h"
#include "pangrova/protocol/v2/result.pb.h"
#include "velox/memory/VeloxMemoryGovernor.h"
#include "velox/runner/VeloxTaskRunner.h"

// Logging for dynamic filter column index out-of-bounds warnings.
#include "observability/PangrovaLog.h"
#include "velox/exchange/PendingEndpointRegistry.h"

namespace pangrova::executor {

const std::string& TaskExecutor::callbackContext() const { return config_.callback_context; }
bool TaskExecutor::isResultTask() const { return config_.is_result_task; }

TaskExecutor::TaskExecutor(const TaskExecutorConfig& config)
    : config_(config),
      router_(config.num_shuffle_partitions,
              config.output_queue_capacity,
              config.shuffle_queue_capacity,
              static_cast<size_t>(config.resource_limit.max_shuffle_buffer_bytes),
              config.queue_poll_timeout),
      trace_id_(config.trace_id) {
  execution_driver_ = std::make_unique<PlanExecutionDriver>(
      config_, lifecycle_, router_.outputQueue(),
      router_.serializedShuffleQueue(), metrics_collector_);
  startDeadlineWatchdog();
}

TaskExecutor::~TaskExecutor() {
  stopDeadlineWatchdog();
  cancel();
  if (execution_driver_) execution_driver_->join();
  drainUnconsumedOutput();
  cache::SemanticResultCache::instance().releaseTaskUsage(config_.task_id);
}

void TaskExecutor::start() {
  if (isExecutionReleased()) {
    throw std::runtime_error("Cannot start a released Native executor: " + config_.task_id);
  }
  if (!lifecycle_.transitionTo(ExecutorState::RUNNING)) {
    throw std::runtime_error(
        "Cannot start task " + config_.task_id +
        ": invalid state transition");
  }
  execution_driver_->executeAsync();
}

void TaskExecutor::bindHandle(int64_t executor_handle) {
  if (executor_handle <= 0) {
    throw std::invalid_argument("executor handle must be positive");
  }
  if (config_.executor_handle != 0) {
    throw std::logic_error("executor handle is already bound for task " +
                           config_.task_id);
  }
  if (lifecycle_.state() != ExecutorState::PENDING) {
    throw std::logic_error("executor handle must be bound before task start");
  }
  config_.executor_handle = executor_handle;
}

void TaskExecutor::cancel() {
  stopDeadlineWatchdog();
  if (lifecycle_.isTerminal()) return;
  if (!router_.abortShuffle()) return;  // committed producer wins
  execution_driver_->cancel();
  router_.closeAll();
  lifecycle_.transitionTo(ExecutorState::CANCELLED);
}

void TaskExecutor::release() {
  stopDeadlineWatchdog();
  if (lifecycle_.state() != ExecutorState::FINISHED) {
    router_.abortShuffle();
  }
  cancel();
  if (execution_driver_) {
    execution_driver_->join();
  }
  // F-NATIVE-EXEC-05: Use drainUnconsumedOutput() for consistent cleanup.
  drainUnconsumedOutput();
  if (execution_driver_) execution_driver_->release();
  if (config_.executor_handle > 0) {
    pangrova::velox::PendingEndpointRegistry::instance().discardExecutor(
        config_.executor_handle);
  }
  execution_released_.store(true, std::memory_order_release);
}

void TaskExecutor::startDeadlineWatchdog() {
  if (config_.deadline_epoch_ms <= 0) {
    return;
  }
  const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
  if (config_.deadline_epoch_ms <= now_ms) {
    failDeadline();
    return;
  }
  deadline_thread_ = std::thread([this] {
    const auto deadline = std::chrono::system_clock::time_point(
        std::chrono::milliseconds(config_.deadline_epoch_ms));
    std::unique_lock<std::mutex> lock(deadline_mutex_);
    while (!deadline_stop_ && !lifecycle_.isTerminal()) {
      const auto now = std::chrono::system_clock::now();
      if (now >= deadline) {
        lock.unlock();
        failDeadline();
        return;
      }
      const auto poll_at = now + std::chrono::milliseconds(100) < deadline
          ? now + std::chrono::milliseconds(100)
          : deadline;
      if (deadline_cv_.wait_until(
              lock, poll_at, [this] { return deadline_stop_; })) {
        return;
      }
    }
  });
}

void TaskExecutor::stopDeadlineWatchdog() {
  {
    std::lock_guard<std::mutex> lock(deadline_mutex_);
    deadline_stop_ = true;
  }
  deadline_cv_.notify_all();
  if (deadline_thread_.joinable() &&
      deadline_thread_.get_id() != std::this_thread::get_id()) {
    deadline_thread_.join();
  }
}

void TaskExecutor::failDeadline() {
  if (lifecycle_.isTerminal()) {
    return;
  }
  if (!router_.abortShuffle()) {
    return;  // committed producer wins
  }
  execution_driver_->cancel();
  router_.closeAll();
  lifecycle_.notifyTaskFailed(
      "Task deadline exceeded: task_id=" + config_.task_id +
      " deadline_epoch_ms=" + std::to_string(config_.deadline_epoch_ms));
}

void TaskExecutor::drainUnconsumedOutput() {
  router_.drainOutputQueue();
}

int TaskExecutor::getNextBatch(
    facebook::velox::RowVectorPtr& row_vector) {
  auto s = lifecycle_.state();
  if (s == ExecutorState::FAILED) return kError;
  if (s == ExecutorState::CANCELLED) return kEof;
  // Try to pop a batch from the output queue. This works in any
  // non-terminal-error state including FINISHED — the queue may still
  // hold unconsumed data after the Velox task completed.
  if (router_.getNextBatch(row_vector, config_.queue_poll_timeout)) {
    return kDataAvailable;
  }
  // Re-read state after blocking pop — it may have changed.
  s = lifecycle_.state();
  if (s == ExecutorState::FINISHED || s == ExecutorState::CANCELLED) {
    return kEof;
  }
  if (s == ExecutorState::FLUSHING) {
    auto* queue = router_.outputQueue();
    if (queue && queue->isClosed() && queue->size() == 0) {
      // P0-7: Before transitioning to FINISHED, check if the execution
      // driver reported an error. The queue may be closed+empty because
      // the Velox task failed (cleanup closes the queue), not because
      // all data was successfully produced.
      auto* runner = veloxTaskRunner();
      if (runner && !runner->errorMessage().empty()) {
        lifecycle_.notifyTaskFailed(runner->errorMessage());
        return kError;
      }
      lifecycle_.transitionTo(ExecutorState::FINISHED);
      return kEof;
    }
  }
  return (s == ExecutorState::FAILED) ? kError : kNotReady;
}

ExecutorState TaskExecutor::state() const { return lifecycle_.state(); }
std::string TaskExecutor::taskId() const { return config_.task_id; }

std::string TaskExecutor::queryId() const { return config_.query_id; }

int32_t TaskExecutor::stageId() const { return config_.stage_id; }

int32_t TaskExecutor::partitionId() const { return config_.partition_id; }

uint64_t TaskExecutor::planEpoch() const { return config_.plan_epoch; }

uint32_t TaskExecutor::attemptId() const { return config_.attempt_id; }

uint64_t TaskExecutor::fencingToken() const { return config_.fencing_token; }

uint64_t TaskExecutor::controlEpoch() const {
  return config_.control_epoch;
}

const std::string& TaskExecutor::ownerId() const {
  return config_.owner_id;
}

const std::string& TaskExecutor::ownerIncarnation() const {
  return config_.owner_incarnation;
}

uint64_t TaskExecutor::ownerFencingToken() const {
  return config_.owner_fencing_token;
}

const std::vector<split::SplitConfig>& TaskExecutor::splits() const {
  return config_.splits;
}

const std::unordered_map<
    std::string, pangrova::protocol::v2::OperatorBinding>&
TaskExecutor::operatorBindings() const {
  return config_.operator_bindings;
}

int64_t TaskExecutor::deadlineEpochMs() const {
  return config_.deadline_epoch_ms;
}

int64_t TaskExecutor::nativeMemoryLimitBytes() const {
  return config_.resource_limit.max_memory_bytes;
}

int32_t TaskExecutor::shufflePartitionCount() const {
  return config_.num_shuffle_partitions;
}

bool TaskExecutor::usesExternalShuffle() const {
  return config_.shuffle_execution.has_value();
}

int64_t TaskExecutor::executorHandle() const {
  return config_.executor_handle;
}
std::string TaskExecutor::errorMessage() const {
  return lifecycle_.errorMessage();
}

TaskLifecycle::Snapshot TaskExecutor::lifecycleSnapshot() const {
  return lifecycle_.snapshot();
}

void TaskExecutor::recordExecutionError(
    const protocol::v2::ExecutionError& error) {
  lifecycle_.recordExecutionError(error);
}

std::string TaskExecutor::nativeStackTrace() const {
  return lifecycle_.nativeStackTrace();
}

std::optional<protocol::v2::ArtifactWorkReport>
TaskExecutor::artifactWorkReport() const {
  return config_.artifact_report_state
      ? config_.artifact_report_state->snapshot()
      : std::nullopt;
}

void TaskExecutor::injectDynamicFilter(
    const std::string& plan_node_id,
    const std::string& filter_proto) {
  if (!execution_driver_) {
    throw std::runtime_error(
        "TaskExecutor: Native execution driver is unavailable");
  }
  execution_driver_->injectDynamicFilter(plan_node_id, filter_proto);
}

void TaskExecutor::updateMemoryLimit(int64_t new_limit_bytes) {
  // Velox SharedArbitrator capacity is fixed at initialization and cannot
  // be dynamically resized. Instead, we use memory pressure signaling:
  // when the new limit is lower than current usage, trigger AsyncDataCache
  // shrink to free cached data and reduce memory footprint.
  auto* memGovernor = pangrova::jni::globalVeloxMemoryGovernor();
  if (!memGovernor) {
    return;
  }

  int64_t current_used = memGovernor->usedMemory();

#if __has_include(<glog/logging.h>)
  LOG(INFO) << "memory_limit_update"
            << " task_id=" << config_.task_id
            << " trace_id=" << trace_id_
            << " new_limit_bytes=" << new_limit_bytes
            << " current_used_bytes=" << current_used
            << " total_capacity_bytes=" << memGovernor->totalMemory();
#endif

  if (new_limit_bytes > 0 && current_used > new_limit_bytes) {
    // Current usage exceeds the new limit — signal memory pressure
    // to AsyncDataCache to evict cached data and free memory.
    auto* cache = pangrova::jni::globalAsyncDataCache();
    if (cache) {
      double usage_pct = (memGovernor->totalMemory() > 0)
          ? (static_cast<double>(current_used) /
             static_cast<double>(memGovernor->totalMemory()) * 100.0)
          : 100.0;
      memGovernor->signalMemoryPressure(cache, usage_pct);
    }
  }
}

void TaskExecutor::setTraceId(const std::string& trace_id) {
  trace_id_ = trace_id;
  config_.trace_id = trace_id;
}

void TaskExecutor::notifyTaskFailed(const std::string& message) {
  if (!router_.abortShuffle()) return;  // committed producer wins
  execution_driver_->cancel();
  router_.closeAll();
  lifecycle_.notifyTaskFailed(message);
}

OutputQueue* TaskExecutor::outputQueue() { return router_.outputQueue(); }

pangrova::shuffle::SerializedShuffleQueue* TaskExecutor::serializedShuffleQueue() {
  return router_.serializedShuffleQueue();
}

observability::NativeMetricsCollector& TaskExecutor::metricsCollector() {
  return metrics_collector_;
}

std::vector<uint8_t> TaskExecutor::collectTaskStats() const {
  if (execution_driver_) {
    auto serialized = execution_driver_->collectTaskStats();
    if (serialized.empty()) return {};
    protocol::v2::NativeTaskStats stats;
    if (!serialized.empty() &&
        !stats.ParseFromArray(
            serialized.data(), static_cast<int>(serialized.size()))) {
      throw std::runtime_error("Native task stats protobuf is invalid");
    }
    const auto cache_usage =
        cache::SemanticResultCache::instance().taskUsage(config_.task_id);
    stats.set_cache_hit_bytes(cache_usage.hit_bytes);
    stats.set_cache_miss_bytes(cache_usage.miss_bytes);
    stats.set_cache_hit_count(cache_usage.hits);
    stats.set_cache_miss_count(cache_usage.misses);
    stats.set_cache_eviction_count(cache_usage.evictions);
    stats.set_cache_admission_rejection_count(
        cache_usage.admission_rejections);
    serialized.resize(stats.ByteSizeLong());
    stats.SerializeToArray(
        serialized.data(), static_cast<int>(serialized.size()));
    return serialized;
  }
  return {};
}

bool TaskExecutor::hasFinalTaskStats() const {
  return execution_driver_ && execution_driver_->hasFinalTaskStats();
}

velox::VeloxTaskRunner* TaskExecutor::veloxTaskRunner() const {
  if (!execution_driver_) {
    return nullptr;
  }
  return execution_driver_->veloxTaskRunner();
}

}  // namespace pangrova::executor
