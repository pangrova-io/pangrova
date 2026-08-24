// TaskOutputRouter: Queue management and batch routing extracted from
// TaskExecutor. Owns OutputQueue, SerializedShuffleQueue, and ExchangeBuffer.
// Routes output batches to the correct destination.
#include "executor/driver/TaskOutputRouter.h"

#include <stdexcept>

namespace pangrova::executor {

TaskOutputRouter::TaskOutputRouter(
    int32_t num_shuffle_partitions,
    size_t output_queue_capacity,
    size_t shuffle_queue_capacity,
    std::chrono::milliseconds queue_poll_timeout)
    : num_shuffle_partitions_(num_shuffle_partitions),
      queue_poll_timeout_(queue_poll_timeout) {
  output_queue_ = std::make_unique<OutputQueue>(output_queue_capacity);
  exchange_buffer_ = std::make_unique<ExchangeBuffer>(output_queue_capacity);
  if (num_shuffle_partitions > 0) {
    serialized_shuffle_queue_ =
        std::make_unique<pangrova::shuffle::SerializedShuffleQueue>(
            shuffle_queue_capacity);
  }
}

bool TaskOutputRouter::routeOutputBatch(
    facebook::velox::RowVectorPtr row_vector) {
  // Shuffle partitioning is handled by Velox PartitionedOutputNode internally.
  // All output RowVectors go directly to the output queue.
  return output_queue_->push(std::move(row_vector), queue_poll_timeout_);
}

bool TaskOutputRouter::getNextBatch(
    facebook::velox::RowVectorPtr& row_vector,
    std::chrono::milliseconds timeout) {
  if (!output_queue_) return false;
  return output_queue_->pop(row_vector, timeout) == PopResult::DATA;
}

bool TaskOutputRouter::getSerializedShuffleBatch(
    pangrova::shuffle::ShuffleBatch& batch,
    std::chrono::milliseconds timeout) {
  if (!serialized_shuffle_queue_) return false;
  return serialized_shuffle_queue_->pop(batch, timeout) == PopResult::DATA;
}

bool TaskOutputRouter::addExchangeBatch(
    const std::string& source_id,
    facebook::velox::RowVectorPtr row_vector) {
  if (exchange_buffer_) {
    return exchange_buffer_->addBatch(source_id, std::move(row_vector));
  }
  return false;
}

void TaskOutputRouter::noMoreExchangeData(const std::string& source_id) {
  if (exchange_buffer_) exchange_buffer_->markSourceComplete(source_id);
}

void TaskOutputRouter::noMoreExchangeSources() {
  if (exchange_buffer_) exchange_buffer_->markNoMoreSources();
}

OutputQueue* TaskOutputRouter::outputQueue() {
  return output_queue_.get();
}

pangrova::shuffle::SerializedShuffleQueue*
TaskOutputRouter::serializedShuffleQueue() {
  return serialized_shuffle_queue_.get();
}

ExchangeBuffer* TaskOutputRouter::exchangeBuffer() {
  return exchange_buffer_.get();
}

void TaskOutputRouter::closeAll() {
  if (output_queue_) output_queue_->close();
  if (serialized_shuffle_queue_) serialized_shuffle_queue_->close();
  if (exchange_buffer_) exchange_buffer_->close();
}

void TaskOutputRouter::drainOutputQueue() {
  // F-NATIVE-EXEC-05: Drain output + shuffle queues so RowVector memory
  // pools are released while still alive. close() must be called first.
  if (output_queue_) {
    output_queue_->close();
    facebook::velox::RowVectorPtr discard;
    while (output_queue_->pop(discard, std::chrono::milliseconds(0)) ==
           PopResult::DATA) {}
  }
  if (serialized_shuffle_queue_) {
    serialized_shuffle_queue_->close();
    pangrova::shuffle::ShuffleBatch discard;
    while (serialized_shuffle_queue_->pop(discard, std::chrono::milliseconds(0)) ==
           PopResult::DATA) {}
  }
}

int32_t TaskOutputRouter::numShufflePartitions() const {
  return num_shuffle_partitions_;
}

}  // namespace pangrova::executor
