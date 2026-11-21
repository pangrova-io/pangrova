#include "engine/shuffle/ShuffleEgressPump.h"

#include <stdexcept>

namespace pangrova::shuffle {

void drainSerializedShuffle(
    SerializedShuffleQueue* queue,
    std::chrono::milliseconds poll_timeout,
    const ShuffleBatchConsumer& consumer) {
  if (!queue || !consumer) {
    throw std::invalid_argument(
        "External shuffle drain requires a queue and consumer");
  }
  ShuffleBatch batch;
  while (true) {
    const auto result = queue->pop(batch, poll_timeout);
    if (result == executor::PopResult::TIMEOUT) {
      continue;
    }
    if (result == executor::PopResult::CLOSED) {
      return;
    }
    if (!batch.data) {
      throw std::runtime_error("Native shuffle produced an empty batch buffer");
    }
    if (batch.row_count <= 0) {
      throw std::runtime_error("Native shuffle produced an invalid row count");
    }
    auto bytes = batch.data->coalesce();
    if (bytes.empty()) {
      throw std::runtime_error("Native shuffle produced a zero-byte batch");
    }
    consumer(batch.partition_id, batch.row_count, bytes.data(),
             static_cast<int64_t>(bytes.size()));
  }
}

}  // namespace pangrova::shuffle
