// Unified return codes for native pull-model interfaces.
// Single source of truth — used by TaskExecutor and JniEntryPoints.
#pragma once

#include <cstdint>

namespace pangrova::executor {

// Internal return codes for TaskExecutor::getNextBatch / getSerializedShuffleBatch.
constexpr int kDataAvailable = 0;
constexpr int kEof = 1;
constexpr int kError = -1;
constexpr int kNotReady = -2;

}  // namespace pangrova::executor
