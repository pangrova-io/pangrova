// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

// Serializes TaskExecutor status, task stats, and cache stats to
// proto wire format. Extracted from JniBridge.cpp to isolate serialization
// from JNI entry dispatch.

#pragma once

#include <cstdint>
#include <vector>

#include "executor/driver/TaskExecutor.h"

// Forward declare execution framework types to avoid pulling heavy headers.
namespace facebook::velox::cache {
struct CacheStats;
}

namespace facebook::velox::exec {
struct TaskStats;
}

namespace pangrova::jni {

// Serialize TaskExecutor state to proto wire format.
std::vector<uint8_t> serializeTaskStatus(executor::TaskExecutor& executor);

// Serialize task stats from native TaskStats (per-operator metrics).
// Falls back to NativeMetricsCollector snapshot for top-level aggregates
// when native TaskStats is unavailable.
std::vector<uint8_t> serializeTaskStats(executor::TaskExecutor& executor);

// Serialize exec::TaskStats directly to proto wire format.
// Used by VeloxTaskRunner::collectTaskStats() for direct serialization.
std::vector<uint8_t> serializeNativeTaskStats(
    const facebook::velox::exec::TaskStats& task_stats,
    uint64_t index_splits_evaluated = 0,
    uint64_t index_splits_skipped = 0);

// Serialize AsyncDataCache::CacheStats to proto.
std::vector<uint8_t> serializeNativeCacheStats(
    const facebook::velox::cache::CacheStats& stats);

}  // namespace pangrova::jni
