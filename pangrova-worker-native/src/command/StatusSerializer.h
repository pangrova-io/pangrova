// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

// Serializes TaskExecutor status, task stats, and cache stats to
// proto wire format. Extracted from JniEntryPoints.cpp to isolate serialization
// from JNI entry dispatch.

#pragma once

#include <cstdint>
#include <vector>

#include "engine/executor/driver/TaskExecutor.h"
#include "pangrova/protocol/v2/result.pb.h"

// Forward declare execution framework types to avoid pulling heavy headers.
namespace facebook::velox::cache {
struct CacheStats;
}

namespace pangrova::jni {

// Keep the polling state and the state in the full report on the same wire enum.
pangrova::protocol::v2::TaskState toProtoTaskState(executor::ExecutorState state);

// Serialize TaskExecutor state to proto wire format.
std::vector<uint8_t> serializeTaskStatus(executor::TaskExecutor& executor);

// Serialize task stats from native TaskStats (per-operator metrics).
// Falls back to NativeMetricsCollector snapshot for top-level aggregates
// when native TaskStats is unavailable.
std::vector<uint8_t> serializeTaskStats(executor::TaskExecutor& executor);

// Serialize AsyncDataCache::CacheStats to proto.
std::vector<uint8_t> serializeNativeCacheStats(
    const facebook::velox::cache::CacheStats& stats);

}  // namespace pangrova::jni
