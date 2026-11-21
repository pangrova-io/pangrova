// NativeFeatureDetection: Centralized compile-time feature detection for
// optional native engine components. Each macro is defined exactly once
// here, eliminating the maintenance risk of duplicated __has_include
// checks scattered across 6+ source files.
//
// Placement: compat/ — platform and compatibility utilities.
#pragma once

// --- AsyncDataCache (RAM/SSD data caching) ---
#if __has_include("velox/common/caching/AsyncDataCache.h")
#include "velox/common/caching/AsyncDataCache.h"
#if __has_include("velox/common/caching/SsdCache.h")
#include "velox/common/caching/SsdCache.h"
#endif
#define PANGROVA_HAS_VELOX_ASYNC_CACHE 1
#else
#define PANGROVA_HAS_VELOX_ASYNC_CACHE 0
#endif

// --- CachedBufferedInput (cache-aware file reader) ---
#if __has_include("velox/dwio/common/CachedBufferedInput.h")
#include "velox/dwio/common/CachedBufferedInput.h"
#define PANGROVA_HAS_CACHED_BUFFERED_INPUT 1
#else
#define PANGROVA_HAS_CACHED_BUFFERED_INPUT 0
#endif

// --- StringIdLease and ScanTracker (CachedBufferedInput dependencies) ---
#if PANGROVA_HAS_CACHED_BUFFERED_INPUT
#if __has_include("velox/common/caching/StringIdMap.h")
#include "velox/common/caching/StringIdMap.h"
#endif
#if __has_include("velox/common/caching/ScanTracker.h")
#include "velox/common/caching/ScanTracker.h"
#endif
#endif

// --- MemoryAllocator (for AsyncDataCache construction) ---
#if __has_include("velox/common/memory/MallocAllocator.h")
#define PANGROVA_HAS_VELOX_MEMORY_ALLOCATOR 1
#else
#define PANGROVA_HAS_VELOX_MEMORY_ALLOCATOR 0
#endif

// --- File abstraction ---
#if __has_include("velox/common/file/File.h")
#define PANGROVA_HAS_VELOX_FILE 1
#else
#define PANGROVA_HAS_VELOX_FILE 0
#endif

// --- GroupIdNode (GROUPING SETS single-pass expansion) ---
// GroupIdNode lives in velox/core/PlanNode.h and is required for efficient
// GROUPING SETS execution. All Velox versions supported by Pangrova include it.
// This macro enables compile-time verification at the usage site.
#if __has_include("velox/core/PlanNode.h")
#define PANGROVA_HAS_VELOX_GROUP_ID_NODE 1
#else
#define PANGROVA_HAS_VELOX_GROUP_ID_NODE 0
#endif

// --- Folly executor (for async prefetch) ---
#if __has_include(<folly/executors/CPUThreadPoolExecutor.h>)
#define PANGROVA_HAS_FOLLY_EXECUTOR 1
#else
#define PANGROVA_HAS_FOLLY_EXECUTOR 0
#endif
