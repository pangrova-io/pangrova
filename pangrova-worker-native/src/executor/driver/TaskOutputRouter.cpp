// TaskOutputRouter: Queue management and batch routing extracted from
// TaskExecutor. Owns OutputQueue and SerializedShuffleQueue.
// Routes output batches to the correct destination.
#include "executor/driver/TaskOutputRouter.h"

#include <stdexcept>

namespace pangrova::executor {

TaskOutputRouter::TaskOutputRouter(
    int32_t num_shuffle_partitions,
    size_t output_queue_capacity,
    size_t shuffle_queue_capacity,
    size_t shuffle_buffer_bytes,
    std::chrono::milliseconds queue_poll_timeout)
    : num_shuffle_partitions_(num_shuffle_partitions),
      queue_poll_timeout_(queue_poll_timeout) {
  output_queue_ = std::make_unique<OutputQueue>(output_queue_capacity);
  if (num_shuffle_partitions > 0) {
    serialized_shuffle_queue_ =
        std::make_unique<pangrova::shuffle::SerializedShuffleQueue>(
            shuffle_queue_capacity, shuffle_buffer_bytes);
  }
}

bool TaskOutputRouter::routeOutputBatch(
    facebook::velox::RowVectorPtr row_vector) {
  // Producer ExchangeRel output is partitioned by VeloxTaskRunner's callback
  // into SerializedShuffleQueue. This method handles result-task rows only.
  return output_queue_->push(std::move(row_vector), queue_poll_timeout_);
}

bool TaskOutputRouter::getNextBatch(
    facebook::velox::RowVectorPtr& row_vector,
    std::chrono::milliseconds timeout) {
  if (!output_queue_) return false;
  return output_queue_->pop(row_vector, timeout) == PopResult::DATA;
}

OutputQueue* TaskOutputRouter::outputQueue() {
  return output_queue_.get();
}

pangrova::shuffle::SerializedShuffleQueue*
TaskOutputRouter::serializedShuffleQueue() {
  return serialized_shuffle_queue_.get();
}

void TaskOutputRouter::closeAll() {
  if (output_queue_) output_queue_->close();
  if (serialized_shuffle_queue_) serialized_shuffle_queue_->abort();
}

bool TaskOutputRouter::abortShuffle() {
  return !serialized_shuffle_queue_ || serialized_shuffle_queue_->abort();
}

void TaskOutputRouter::drainOutputQueue() {
  // Both RowVectors and serialized IOBufs can own Velox StreamArena memory.
  // JNI/Java must quiesce Flight's borrowed source before reaching this point;
  // then drain while the runner and its pools are still alive.
  if (output_queue_) {
    output_queue_->close();
    facebook::velox::RowVectorPtr discard;
    while (output_queue_->pop(discard, std::chrono::milliseconds(0)) ==
           PopResult::DATA) {}
  }
  if (serialized_shuffle_queue_) {
    serialized_shuffle_queue_->abort();
    pangrova::shuffle::ShuffleBatch discard;
    while (serialized_shuffle_queue_->pop(discard, std::chrono::milliseconds(0)) ==
           PopResult::DATA) {}
  }
}

int32_t TaskOutputRouter::numShufflePartitions() const {
  return num_shuffle_partitions_;
}

}  // namespace pangrova::executor
