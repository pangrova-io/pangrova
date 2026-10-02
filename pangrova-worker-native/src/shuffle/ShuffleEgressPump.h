#pragma once

#include <chrono>
#include <cstdint>
#include <functional>

#include "shuffle/SerializedShuffleQueue.h"

namespace pangrova::shuffle {

using ShuffleBatchConsumer =
    std::function<void(int32_t, int64_t, const uint8_t*, int64_t)>;

// Drains one producer queue in FIFO order until it is closed and empty.
void drainSerializedShuffle(
    SerializedShuffleQueue* queue,
    std::chrono::milliseconds poll_timeout,
    const ShuffleBatchConsumer& consumer);

}  // namespace pangrova::shuffle
