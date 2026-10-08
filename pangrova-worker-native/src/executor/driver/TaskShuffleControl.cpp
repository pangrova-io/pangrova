#include "executor/driver/TaskExecutor.h"

#include <stdexcept>

namespace pangrova::executor {

shuffle::ShuffleControlGate::Lease TaskExecutor::acquireExecutionStart() {
  try {
    return shuffle_start_gate_.acquire();
  } catch (const std::logic_error&) {
    throw std::runtime_error("Cannot start a closed Native executor: " + config_.task_id);
  }
}

shuffle::ShuffleControlGate::Lease TaskExecutor::acquireShuffleStart() {
  auto lease = shuffle_start_gate_.acquire();
  if (isExecutionReleased() || lifecycle_.state() == ExecutorState::FAILED ||
      lifecycle_.state() == ExecutorState::CANCELLED) {
    throw std::logic_error("shuffle producer cannot start after task failure or release");
  }
  return lease;
}

void TaskExecutor::installShuffleRegistration(
    const shuffle::ShuffleControlGate::Lease& lease,
    std::shared_ptr<shuffle::ShuffleProducerRegistration> registration) {
  if (!shuffle_start_gate_.owns(lease) || !registration) {
    throw std::invalid_argument("shuffle registration requires the actual task control lease");
  }
  const auto& identity = registration->identity();
  if (identity.handle != config_.executor_handle || identity.queue != serializedShuffleQueue() ||
      identity.query_id != config_.query_id || identity.stage_id != config_.stage_id ||
      identity.partition_count != shufflePartitionCount() ||
      identity.plan_epoch != config_.plan_epoch || identity.attempt_id != config_.attempt_id ||
      identity.fencing_token != config_.fencing_token) {
    throw std::logic_error("shuffle registration does not match its actual executor owner");
  }
  std::lock_guard<std::mutex> lock(shuffle_registration_mutex_);
  if (shuffle_registration_ && shuffle_registration_ != registration) {
    throw std::logic_error("executor already owns a different shuffle registration");
  }
  shuffle_registration_ = std::move(registration);
}

void TaskExecutor::sealShuffleStarts() {
  shuffle_start_gate_.sealAndWait();
}

void TaskExecutor::closeShuffleRegistration() {
  sealShuffleStarts();
  std::shared_ptr<shuffle::ShuffleProducerRegistration> registration;
  {
    std::lock_guard<std::mutex> lock(shuffle_registration_mutex_);
    registration = shuffle_registration_;
  }
  if (registration) registration->close();
}

}  // namespace pangrova::executor
