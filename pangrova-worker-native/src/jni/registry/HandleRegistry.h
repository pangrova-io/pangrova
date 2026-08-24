// HandleRegistry: Thread-safe handle map with generation tracking and
// reference counting. Replaces raw ExecutorHandleMap to eliminate
// use-after-free SIGSEGV at the JNI boundary.
//
// Key safety guarantees:
//   - acquire() on a stale handle (from previous init cycle) throws instead of SIGSEGV
//   - double release() throws NativeBridgeException instead of SIGSEGV
//   - destroyAll() force-releases leaked handles and increments generation
//   - concurrent acquire/release from multiple JNI threads is safe
//
// @ThreadSafe — all public methods are safe for concurrent access.
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "executor/driver/TaskExecutor.h"

namespace pangrova::jni {

class HandleRegistry {
 public:
  static HandleRegistry& instance();

  // Register an executor and return a unique handle.
  // The handle encodes the current generation for staleness detection.
  int64_t create(std::shared_ptr<pangrova::executor::TaskExecutor> executor);

  // Acquire a shared reference to the executor for the given handle.
  // Throws std::runtime_error if:
  //   - handle does not exist (already released or never created)
  //   - handle belongs to a previous generation (stale after destroyAll)
  std::shared_ptr<pangrova::executor::TaskExecutor> acquire(int64_t handle) const;

  // Release the executor for the given handle.
  // The executor is destroyed when the last shared_ptr reference drops.
  // Throws std::runtime_error on double-release (handle not found).
  void release(int64_t handle);

  // Force-release all handles, log warnings for leaked (non-terminal) handles,
  // and increment the generation counter. Called during nativeShutdown().
  // After this call, all previously issued handles become stale.
  void destroyAll();

  // Remove handles whose executor is in a terminal state (FINISHED/FAILED/CANCELLED).
  // Opportunistic cleanup — called periodically from nativeGetTaskStatus.
  void removeTerminated();

  // Current number of active handles (for diagnostics/testing).
  size_t size() const;

  // Current generation (for diagnostics/testing).
  uint64_t generation() const;

 private:
  HandleRegistry() = default;

  struct Entry {
    std::shared_ptr<pangrova::executor::TaskExecutor> executor;
    uint64_t generation;
  };

  mutable std::shared_mutex mutex_;
  std::unordered_map<int64_t, Entry> handles_;
  std::atomic<int64_t> next_handle_{1};
  std::atomic<uint64_t> current_generation_{0};
};

}  // namespace pangrova::jni
