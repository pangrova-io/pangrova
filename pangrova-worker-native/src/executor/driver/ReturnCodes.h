// Unified return codes for native pull-model interfaces.
// Single source of truth — used by TaskExecutor and JniBridge.
#pragma once

#include <cstdint>

namespace pangrova::executor {

// Internal return codes for TaskExecutor::getNextBatch / getSerializedShuffleBatch.
constexpr int kDataAvailable = 0;
constexpr int kEof = 1;
constexpr int kError = -1;
constexpr int kNotReady = -2;

}  // namespace pangrova::executor

namespace pangrova::jni {

// JNI serialization status bytes for nativeGetNextBatchColumnar / nativeGetShuffleBatch.
// Encoded as the first byte of the returned jbyteArray.
// These values are consumed by Java BatchResult.parseStructuredResult().
// MUST match BatchResultStatus enum in pangrova-protocol task.proto.
constexpr int8_t kJniStatusData = 0;
constexpr int8_t kJniStatusEof = 1;
constexpr int8_t kJniStatusNotReady = 2;
constexpr int8_t kJniStatusError = 3;

}  // namespace pangrova::jni
