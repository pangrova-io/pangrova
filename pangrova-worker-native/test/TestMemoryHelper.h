// TestMemoryHelper: Provides Velox runtime initialization and memory pool
// helpers for C++ unit tests that don't go through JNI.
//
// Uses initializeVeloxRuntime(config) to set up the full Velox runtime:
//   - MemoryManager (global singleton via VeloxMemoryGovernor)
//   - Parquet/DWRF reader factories
//   - Velox built-in functions (scalar, aggregate, window)
//   - PrestoVectorSerde (shuffle wire format)
//   - Local filesystem registration
//   - PangrovaVeloxConnector registration
//
// Re-entrant initialization: supports re-initialization after
// shutdownVeloxRuntime() (e.g., VeloxRuntimeInitializerTest TearDown).
// Uses mutex-guarded check instead of std::call_once to allow re-init.
#pragma once

#include <memory>
#include <mutex>
#include <string>

#include "jni/RuntimeInitializer.h"
#include "velox/common/memory/Memory.h"

namespace pangrova::test {

namespace detail {

// Mutex-guarded re-entrant initialization.
// Unlike std::call_once, this allows re-initialization after shutdown
// by checking the runtime state on every call.
inline std::mutex& initMutex() {
  static std::mutex mu;
  return mu;
}

inline void ensureRuntimeInitialized() {
  std::lock_guard<std::mutex> lock(initMutex());
  // globalVeloxMemoryGovernor() returns nullptr after shutdownVeloxRuntime().
  // When that happens, we must re-initialize the runtime.
  if (pangrova::jni::globalVeloxMemoryGovernor() == nullptr) {
    pangrova::jni::NativeRuntimeConfig config;
    config.max_memory_bytes = 1L * 1024 * 1024 * 1024;  // 1GB
    config.num_drivers = 2;
    config.spill_enabled = false;
    config.cache_enabled = false;
    pangrova::jni::initializeVeloxRuntime(config);
  }
}

}  // namespace detail

// Ensures the full Velox runtime is initialized for tests.
// Re-entrant: if a previous test called shutdownVeloxRuntime(),
// this will re-initialize the runtime automatically.
// Returns the global MemoryManager instance.
inline facebook::velox::memory::MemoryManager* testMemoryManager() {
  detail::ensureRuntimeInitialized();
  return facebook::velox::memory::MemoryManager::getInstance();
}

// Convenience: create a leaf pool for a test.
inline std::shared_ptr<facebook::velox::memory::MemoryPool> testLeafPool(
    const std::string& name) {
  return testMemoryManager()->addLeafPool(name);
}

// Convenience: create a root pool for a test.
inline std::shared_ptr<facebook::velox::memory::MemoryPool> testRootPool(
    const std::string& name) {
  return testMemoryManager()->addRootPool(name);
}

}  // namespace pangrova::test
