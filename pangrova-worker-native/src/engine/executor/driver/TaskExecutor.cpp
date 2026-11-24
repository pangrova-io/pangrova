// TaskExecutor: Orchestrator that delegates to TaskLifecycle,
// PlanExecutionDriver, and TaskOutputRouter.
//
// Lifecycle: PENDING → RUNNING → FLUSHING → FINISHED/FAILED/CANCELLED
// Terminal states are monotonic — no further transitions once reached.
#include "engine/executor/driver/TaskExecutor.h"

#include <chrono>
#include <stdexcept>
#include <utility>

#include "engine/cache/SemanticResultCache.h"
#include "jni/RuntimeInitializer.h"
#include "pangrova/protocol/v2/result.pb.h"
#include "engine/velox/memory/VeloxMemoryGovernor.h"
#include "engine/velox/runner/VeloxTaskRunner.h"

// Logging for dynamic filter column index out-of-bounds warnings.
#include "engine/observability/PangrovaLog.h"
#include "engine/velox/exchange/PendingEndpointRegistry.h"

namespace pangrova::executor {

const std::string& TaskExecutor::callbackContext() const { return config_.callback_context; }
bool TaskExecutor::isResultTask() const { return config_.is_result_task; }

TaskExecutor::TaskExecutor(TaskExecutorConfig config)
    : config_(std::move(config)),
      router_(config_.num_shuffle_partitions,
              config_.output_queue_capacity,
              config_.shuffle_queue_capacity,
              static_cast<size_t>(config_.resource_limit.max_shuffle_buffer_bytes),
              config_.queue_poll_timeout),
      trace_id_(config_.trace_id) {
  execution_driver_ = std::make_unique<PlanExecutionDriver>(
      config_, lifecycle_, router_.outputQueue(),
      router_.serializedShuffleQueue(), metrics_collector_);
  startDeadlineWatchdog();
}

TaskExecutor::~TaskExecutor() {
  try {
    release();
  } catch (const std::exception& error) {
    PANGROVA_LOG(ERROR) << "event=task_destructor_release_failed task_id=" << config_.task_id
                       << " error=" << error.what();
  }
  cache::SemanticResultCache::instance().releaseTaskUsage(config_.task_id);
}

void TaskExecutor::start(std::function<void()> on_result_ready) {
  if (on_result_ready && !isResultTask()) {
    throw std::invalid_argument("Result readiness requires an OUTPUT executor");
  }
  auto execution_control = acquireExecutionStart();
  if (isExecutionReleased()) {
    throw std::runtime_error("Cannot start a released Native executor: " + config_.task_id);
  }
  if (!lifecycle_.transitionTo(ExecutorState::RUNNING)) {
    throw std::runtime_error(
        "Cannot start task " + config_.task_id +
        ": invalid state transition");
  }
  execution_driver_->executeAsync(std::move(on_result_ready));
}

void TaskExecutor::bindHandle(int64_t executor_handle, const TaskFailureReporter& failure_reporter) {
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
  config_.failure_reporter = &failure_reporter;
}

void TaskExecutor::cancel() {
  sealShuffleStarts();
  stopDeadlineWatchdog();
  if (lifecycle_.isTerminal()) return;
  if (!router_.abortShuffle()) return;  // committed producer wins
  execution_driver_->cancel();
  router_.closeAll();
  lifecycle_.transitionTo(ExecutorState::CANCELLED);
}

void TaskExecutor::release() {
  {
    std::unique_lock<std::mutex> lock(release_mutex_);
    release_cv_.wait(lock, [this] { return !release_in_progress_; });
    if (isExecutionReleased()) return;
    release_in_progress_ = true;
  }
  try {
    releaseInternal();
    std::lock_guard<std::mutex> lock(release_mutex_);
    release_in_progress_ = false;
    release_cv_.notify_all();
  } catch (...) {
    std::lock_guard<std::mutex> lock(release_mutex_);
    release_in_progress_ = false;
    release_cv_.notify_all();
    throw;
  }
}

void TaskExecutor::releaseInternal() {
  closeShuffleRegistration();
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
  deadline_thread_id_ = deadline_thread_.get_id();
}

void TaskExecutor::stopDeadlineWatchdog() {
  std::thread joining;
  {
    std::unique_lock<std::mutex> lock(deadline_mutex_);
    deadline_stop_ = true;
    deadline_cv_.notify_all();
    if (deadline_thread_id_ == std::this_thread::get_id()) return;
    deadline_cv_.wait(lock, [this] { return !deadline_join_in_progress_; });
    if (deadline_thread_.joinable() &&
        deadline_thread_.get_id() != std::this_thread::get_id()) {
      joining = std::move(deadline_thread_);
      deadline_join_in_progress_ = true;
    }
  }
  deadline_cv_.notify_all();
  if (joining.joinable()) {
    joining.join();
    std::lock_guard<std::mutex> lock(deadline_mutex_);
    deadline_join_in_progress_ = false;
    deadline_cv_.notify_all();
  }
}

void TaskExecutor::failDeadline() {
  sealShuffleStarts();
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

ExecutorState TaskExecutor::awaitTerminal(std::chrono::milliseconds timeout) const {
  return lifecycle_.awaitTerminal(timeout);
}
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

bool TaskExecutor::usesLocalDiskShuffle() const {
  return config_.shuffle_persistence_mode ==
      pangrova::protocol::v2::SHUFFLE_PERSISTENCE_MODE_MEMORY_AND_DISK;
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

void TaskExecutor::recordResultStreamFinished() {
  if (!config_.is_result_task || !lifecycle_.finishResultStream()) {
    throw std::logic_error("Cannot finish delivery for an inactive result task");
  }
}

void TaskExecutor::recordResultStreamFailure(const std::string& message) {
  protocol::v2::ExecutionError error;
  error.set_error_class(protocol::v2::ERROR_CLASS_RESULT_VALIDATION);
  error.set_code("RESULT_STREAM_FAILED");
  error.set_message(message.substr(0, 4096));
  error.set_retryable(false);
  error.set_failed_component("native-result-stream");
  lifecycle_.recordExecutionError(error);
  lifecycle_.notifyTaskFailed(error.message());
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
    const google::protobuf::RepeatedPtrField<protocol::v2::RuntimeFilter>& filters) {
  if (!execution_driver_) {
    throw std::runtime_error(
        "TaskExecutor: Native execution driver is unavailable");
  }
  execution_driver_->injectDynamicFilter(plan_node_id, filters);
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
  sealShuffleStarts();
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

uint64_t TaskExecutor::requestedObjectReadBytes() const {
  return execution_driver_ ? execution_driver_->requestedObjectReadBytes() : 0;
}

std::optional<protocol::v2::NativeTaskStats> TaskExecutor::collectTaskStats() const {
  auto stats = execution_driver_ ? execution_driver_->collectTaskStats() : std::nullopt;
  if (!stats) return std::nullopt;
  if (state() != ExecutorState::FINISHED) stats->clear_build_keys();
  const auto cache_usage =
      cache::SemanticResultCache::instance().taskUsage(config_.task_id);
  stats->set_cache_hit_bytes(cache_usage.hit_bytes);
  stats->set_cache_miss_bytes(cache_usage.miss_bytes);
  stats->set_cache_hit_count(cache_usage.hits);
  stats->set_cache_miss_count(cache_usage.misses);
  stats->set_cache_eviction_count(cache_usage.evictions);
  stats->set_cache_admission_rejection_count(cache_usage.admission_rejections);
  stats->set_shuffle_payload_bytes(router_.shufflePayloadBytes());
  return stats;
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

void TaskExecutor::watchResultDrain(std::function<void()> listener) const {
  if (!isResultTask() || state() != ExecutorState::FINISHED || isExecutionReleased()) {
    throw std::logic_error("Result drain listener requires a finished, live result executor");
  }
  execution_driver_->watchResultDrain(std::move(listener));
}

}  // namespace pangrova::executor
