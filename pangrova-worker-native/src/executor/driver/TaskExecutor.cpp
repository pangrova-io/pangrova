// TaskExecutor: Orchestrator that delegates to TaskLifecycle,
// PlanExecutionDriver, and TaskOutputRouter.
//
// Lifecycle: PENDING → RUNNING → FLUSHING → FINISHED/FAILED/CANCELLED
// Terminal states are monotonic — no further transitions once reached.
#include "executor/driver/TaskExecutor.h"

#include <stdexcept>

#include "jni/RuntimeInitializer.h"
#include "velox/memory/VeloxMemoryGovernor.h"
#include "velox/filter/DynamicFilterConverter.h"
#include "velox/runner/VeloxTaskRunner.h"

#if __has_include("pangrova/native/protocol/v1/dynamic_filter.pb.h")
#include "pangrova/native/protocol/v1/dynamic_filter.pb.h"
#define PANGROVA_HAS_FILTER_PROTO 1
#else
#define PANGROVA_HAS_FILTER_PROTO 0
#endif

// Logging for dynamic filter column index out-of-bounds warnings.
#include "observability/PangrovaLog.h"

namespace pangrova::executor {

TaskExecutor::TaskExecutor(const TaskExecutorConfig& config)
    : config_(config),
      router_(config.num_shuffle_partitions,
              config.output_queue_capacity,
              config.shuffle_queue_capacity,
              config.queue_poll_timeout),
      trace_id_(config.trace_id) {
  execution_driver_ = std::make_unique<PlanExecutionDriver>(
      config_, lifecycle_, router_.outputQueue(),
      router_.serializedShuffleQueue(), metrics_collector_);
}

TaskExecutor::~TaskExecutor() {
  release();
}

void TaskExecutor::start() {
  if (!lifecycle_.transitionTo(ExecutorState::RUNNING)) {
    throw std::runtime_error(
        "Cannot start task " + config_.task_id +
        ": invalid state transition");
  }
  execution_driver_->executeAsync();
}

void TaskExecutor::cancel() {
  if (lifecycle_.isTerminal()) return;
  lifecycle_.transitionTo(ExecutorState::CANCELLED);
  execution_driver_->cancel();
  router_.closeAll();
}

void TaskExecutor::release() {
  cancel();
  if (execution_driver_) {
    execution_driver_->join();
  }
  // F-NATIVE-EXEC-05: Use drainUnconsumedOutput() for consistent cleanup.
  drainUnconsumedOutput();
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

int TaskExecutor::getSerializedShuffleBatch(
    pangrova::shuffle::ShuffleBatch& batch) {
  auto s = lifecycle_.state();
  if (s == ExecutorState::FAILED) return kError;
  if (s == ExecutorState::CANCELLED) return kEof;
  if (router_.getSerializedShuffleBatch(batch, config_.queue_poll_timeout)) {
    return kDataAvailable;
  }
  s = lifecycle_.state();
  if (s == ExecutorState::FLUSHING) {
    auto* queue = router_.serializedShuffleQueue();
    if (queue && queue->isClosed() && queue->size() == 0) {
      // P0-7: Check for runner error before transitioning to FINISHED.
      auto* runner = veloxTaskRunner();
      if (runner && !runner->errorMessage().empty()) {
        lifecycle_.notifyTaskFailed(runner->errorMessage());
        return kError;
      }
      lifecycle_.transitionTo(ExecutorState::FINISHED);
      return kEof;
    }
  }
  if (s == ExecutorState::FINISHED || s == ExecutorState::CANCELLED) {
    return kEof;
  }
  return (s == ExecutorState::FAILED) ? kError : kNotReady;
}

ExecutorState TaskExecutor::state() const { return lifecycle_.state(); }
std::string TaskExecutor::taskId() const { return config_.task_id; }
std::string TaskExecutor::errorMessage() const {
  return lifecycle_.errorMessage();
}

std::string TaskExecutor::nativeStackTrace() const {
  return lifecycle_.nativeStackTrace();
}

void TaskExecutor::addExchangeBatch(
    const std::string& source_id,
    facebook::velox::RowVectorPtr row_vector) {
  if (lifecycle_.isTerminal()) return;
  router_.addExchangeBatch(source_id, std::move(row_vector));
}

void TaskExecutor::noMoreExchangeData(const std::string& source_id) {
  router_.noMoreExchangeData(source_id);
}

void TaskExecutor::noMoreExchangeSources() {
  router_.noMoreExchangeSources();
}

void TaskExecutor::injectDynamicFilter(
    const std::string& plan_node_id,
    const std::string& filter_proto) {
  if (!execution_driver_) {
    return;
  }

#if PANGROVA_HAS_FILTER_PROTO
  pangrova::native::protocol::v1::InjectDynamicFilterPayload payload;
  if (!payload.ParseFromString(filter_proto)) {
    PANGROVA_LOG_WARN
        << "TaskExecutor: failed to parse dynamic filter proto"
        << " plan_node_id=" << plan_node_id
        << " task_id=" << config_.task_id;
    return;
  }

  auto converted = pangrova::velox::convertDynamicFilters(payload);
  auto* runner = execution_driver_->veloxTaskRunner();
  if (!runner) {
    return;
  }

  // Resolve column names using the scan node output type persisted
  // in PlanExecutionDriver after plan conversion.
  auto output_type_opt =
      execution_driver_->getScanNodeOutputType(plan_node_id);

  for (auto& cf : converted) {
    facebook::velox::column_index_t col_idx =
        static_cast<facebook::velox::column_index_t>(-1);

    if (output_type_opt.has_value()) {
      int resolved = pangrova::velox::resolveColumnIndex(
          cf.column_name, output_type_opt.value());
      if (resolved >= 0) {
        col_idx = static_cast<facebook::velox::column_index_t>(resolved);
      }
    }

    if (col_idx == static_cast<facebook::velox::column_index_t>(-1)) {
      PANGROVA_LOG_WARN
          << "TaskExecutor: cannot resolve dynamic filter column"
          << " trace_id=" << trace_id_
          << " plan_node_id=" << plan_node_id
          << " column_name=" << cf.column_name
          << " task_id=" << config_.task_id;
      continue;
    }

    runner->injectDynamicFilter(
        plan_node_id,
        col_idx,
        std::move(cf.filter));
  }
#else
  (void)plan_node_id;
  (void)filter_proto;
#endif
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
}

void TaskExecutor::notifyTaskComplete() {
  lifecycle_.notifyTaskComplete();
}

void TaskExecutor::notifyTaskFailed(const std::string& message) {
  lifecycle_.notifyTaskFailed(message);
  router_.closeAll();
}

void TaskExecutor::notifyQueuesDrained() {
  lifecycle_.transitionTo(ExecutorState::FINISHED);
  router_.closeAll();
}

OutputQueue* TaskExecutor::outputQueue() { return router_.outputQueue(); }

pangrova::shuffle::SerializedShuffleQueue* TaskExecutor::serializedShuffleQueue() {
  return router_.serializedShuffleQueue();
}

ExchangeBuffer* TaskExecutor::exchangeBuffer() {
  return router_.exchangeBuffer();
}

observability::NativeMetricsCollector& TaskExecutor::metricsCollector() {
  return metrics_collector_;
}

std::vector<uint8_t> TaskExecutor::collectTaskStats() const {
  auto* runner = veloxTaskRunner();
  if (runner) {
    return runner->collectTaskStats();
  }
  return {};
}

velox::VeloxTaskRunner* TaskExecutor::veloxTaskRunner() const {
  if (!execution_driver_) {
    return nullptr;
  }
  return execution_driver_->veloxTaskRunner();
}

}  // namespace pangrova::executor
