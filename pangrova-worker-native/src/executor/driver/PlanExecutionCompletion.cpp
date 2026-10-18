#include "executor/driver/PlanExecutionDriver.h"

#include <stdexcept>

#include "executor/driver/PlanExecutionState.h"
#include "executor/driver/TaskExecutor.h"
#include "jni/RuntimeInitializer.h"
#include "shuffle/ShuffleEgressExecutor.h"
#include "velox/exec/Task.h"
#include "velox/memory/VeloxMemoryGovernor.h"
#include "velox/runner/SingleMatchJoinGuard.h"

namespace pangrova::executor {

std::string PlanExecutionDriver::validateProducer(
    const std::shared_ptr<Execution>& execution, bool& success) {
  if (!success || cancelled_.load()) return {};
  try {
    if (!execution->singleMatchRequirements.empty()) {
      if (!execution->runner->veloxTask()) {
        throw std::runtime_error("LEFT_SINGLE join task has no Native statistics");
      }
      velox::validateSingleMatchJoinStats(execution->runner->veloxTask()->taskStats(),
                                         execution->singleMatchRequirements);
    }
    if (shuffle_queue_) execution->runner->finalizeShuffleOutput();
  } catch (const std::exception& failure) {
    success = false;
    return failure.what();
  }
  return {};
}

void PlanExecutionDriver::completeShuffleDrain(
    const std::shared_ptr<Execution>& execution, bool success) {
  if ((!success || cancelled_.load()) && shuffle_queue_) shuffle_queue_->abort();
  if (!execution->shuffleDrain) return;
  if (shuffle_queue_) shuffle_queue_->close();
  execution->shuffleDrain->join();
  if (execution->shuffleFailure) {
    std::rethrow_exception(execution->shuffleFailure);
  }
  if (success && !cancelled_.load()) {
    config_.shuffle_egress->onTaskCompleted(config_.query_id, config_.task_id);
  } else {
    config_.shuffle_egress->onTaskAborted(config_.query_id, config_.task_id,
        cancelled_.load() ? "Native task cancelled" : "Native task execution failed");
  }
}

bool PlanExecutionDriver::completeWriteDrain(
    const std::shared_ptr<Execution>& execution, bool success) {
  if (!execution->writeDrain) return success && !cancelled_.load();
  if (cancelled_.load()) markWriteProducerCancelled(write_producer_outcome_);
  else if (success) markWriteProducerSucceeded(write_producer_outcome_);
  else markWriteProducerFailed(write_producer_outcome_);
  if (output_queue_) output_queue_->close();
  execution->writeDrain->join();
  if (execution->writeFailure) throw std::runtime_error(*execution->writeFailure);
  return success && !cancelled_.load() &&
      write_producer_outcome_->load(std::memory_order_acquire) ==
          WriteProducerOutcome::FINISHED;
}

void PlanExecutionDriver::completeExecution(
    const std::shared_ptr<Execution>& execution, bool success) {
  const auto failure = validateProducer(execution, success);
  completeShuffleDrain(execution, success);
  if (!success) {
    if (cancelled_.load()) markWriteProducerCancelled(write_producer_outcome_);
    else markWriteProducerFailed(write_producer_outcome_);
    if (output_queue_) output_queue_->close();
    completeWriteDrain(execution, false);
    if (!cancelled_.load()) {
      notifyFailure("Native task failed: " +
          (failure.empty() ? execution->runner->errorMessage() : failure));
    }
    return;
  }
  if (!completeWriteDrain(execution, true)) return;
  finishSuccessfulExecution(execution);
}

void PlanExecutionDriver::finishSuccessfulExecution(
    const std::shared_ptr<Execution>& execution) {
  // Completion is not a buffer-owner barrier. The runner's task pools remain
  // owned by active_runner_ until explicit result/write drain and release.
  auto task = execution->runner->veloxTask();
  if (task) {
    int64_t spillBytes = 0;
    for (const auto& pipeline : task->taskStats().pipelineStats) {
      for (const auto& op : pipeline.operatorStats) spillBytes += op.spilledBytes;
    }
    if (auto* governor = jni::globalVeloxMemoryGovernor()) {
      governor->reportSpillBytes(spillBytes);
    }
  }
  execution->runner->cleanup();
  if (cancelled_.load()) return;
  // All fallible success steps precede terminal publication and direct EOF.
  if (shuffle_queue_ && !shuffle_queue_->commit()) return;
  lifecycle_.notifyTaskComplete();
  lifecycle_.transitionTo(ExecutorState::FINISHED);
}

}  // namespace pangrova::executor
