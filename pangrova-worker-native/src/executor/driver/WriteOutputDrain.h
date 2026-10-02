#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <utility>

#include "executor/queue/OutputQueue.h"

namespace pangrova::executor {

// Producer outcome is intentionally independent from OutputQueue::close().
// close() is also used to unblock consumers after cancellation and failure, so
// observing a closed queue alone must never authorize publication of staged
// write data.
enum class WriteProducerOutcome {
  RUNNING,
  SUCCEEDED,
  FAILED,
  CANCELLED,
  FINALIZING,
  FINISHED,
};

using WriteProducerOutcomeRef =
    std::shared_ptr<std::atomic<WriteProducerOutcome>>;

inline bool markWriteProducerSucceeded(
    const WriteProducerOutcomeRef& outcome) {
  auto expected = WriteProducerOutcome::RUNNING;
  return outcome->compare_exchange_strong(
      expected, WriteProducerOutcome::SUCCEEDED,
      std::memory_order_acq_rel, std::memory_order_acquire);
}

inline void markWriteProducerFailed(const WriteProducerOutcomeRef& outcome) {
  auto expected = WriteProducerOutcome::RUNNING;
  outcome->compare_exchange_strong(
      expected, WriteProducerOutcome::FAILED,
      std::memory_order_acq_rel, std::memory_order_acquire);
}

// Cancellation may revoke a producer success until the drain atomically
// claims finalization. Whichever CAS wins is the linearization point: a cancel
// before FINALIZING prevents publication; a later cancel is an after-completion
// no-op for the write result.
inline bool markWriteProducerCancelled(
    const WriteProducerOutcomeRef& outcome) {
  auto current = outcome->load(std::memory_order_acquire);
  while (current == WriteProducerOutcome::RUNNING ||
         current == WriteProducerOutcome::SUCCEEDED) {
    if (outcome->compare_exchange_weak(
            current, WriteProducerOutcome::CANCELLED,
            std::memory_order_acq_rel, std::memory_order_acquire)) {
      return true;
    }
  }
  return current == WriteProducerOutcome::CANCELLED;
}

// Drain until the producer closes the queue. A poll timeout only gives callers
// a chance to observe cancellation; it is not end-of-stream. finalize is
// invoked exactly once, and only after an explicitly successful producer
// outcome was published before queue closure.
template <typename Consume, typename Finalize>
bool drainWriteOutput(
    OutputQueue* queue,
    const WriteProducerOutcomeRef& producer_outcome,
    std::chrono::milliseconds poll_timeout,
    Consume&& consume,
    Finalize&& finalize) {
  facebook::velox::RowVectorPtr row_vector;
  for (;;) {
    const auto result = queue->pop(row_vector, poll_timeout);
    if (result == PopResult::TIMEOUT) {
      continue;
    }
    if (result == PopResult::CLOSED) {
      break;
    }
    consume(row_vector);
  }

  auto expected = WriteProducerOutcome::SUCCEEDED;
  if (!producer_outcome->compare_exchange_strong(
          expected, WriteProducerOutcome::FINALIZING,
          std::memory_order_acq_rel, std::memory_order_acquire)) {
    return false;
  }
  try {
    finalize();
    producer_outcome->store(
        WriteProducerOutcome::FINISHED, std::memory_order_release);
  } catch (...) {
    producer_outcome->store(
        WriteProducerOutcome::FAILED, std::memory_order_release);
    throw;
  }
  return true;
}

}  // namespace pangrova::executor
