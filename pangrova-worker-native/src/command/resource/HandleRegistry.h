// HandleRegistry: Thread-safe handle map with generation tracking and
// reference counting. Replaces raw ExecutorHandleMap to eliminate
// use-after-free SIGSEGV at the JNI boundary.
//
// Key safety guarantees:
//   - acquire() on a stale handle (from previous init cycle) throws instead of SIGSEGV
//   - double release() throws JniCallException instead of SIGSEGV
//   - failed release retains the exact handle for retry
//   - concurrent acquire/release from multiple JNI threads is safe
//
// @ThreadSafe — all public methods are safe for concurrent access.
#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "engine/executor/driver/TaskExecutor.h"
#include "pangrova/protocol/v2/runtime.pb.h"

namespace pangrova::jni {

class HandleRegistry {
 public:
  static HandleRegistry& instance();

  // Register an executor and return a unique handle.
  // The handle encodes the current generation for staleness detection.
  int64_t create(std::shared_ptr<pangrova::executor::TaskExecutor> executor);

  // Reserve the same authoritative owner before construction starts. SDK/JNI
  // resources are constructed outside the lock and attached without allocation.
  int64_t reserveCreation(const protocol::v2::ExecutorCreationRecoveryRequest& identity);
  void attachCreation(int64_t handle, std::shared_ptr<executor::TaskExecutor> executor);
  void finishCreation(int64_t handle);
  protocol::v2::ExecutorCreationRecoveryResult recoverCreation(
      const protocol::v2::ExecutorCreationRecoveryRequest& identity);

  // Acquire a shared reference to the executor for the given handle.
  // Throws std::runtime_error if:
  //   - handle does not exist (already released or never created)
  //   - handle belongs to a previous generation (stale after destroyAll)
  std::shared_ptr<pangrova::executor::TaskExecutor> acquire(int64_t handle) const;

  // Release the executor for the given handle.
  // The executor is destroyed when the last shared_ptr reference drops.
  // Throws std::runtime_error on double-release (handle not found).
  // Complete the caller's receipt allocation after drain and before erase.
  // Any preparation failure retains the exact executor for retry.
  void release(int64_t handle,
               const std::function<void(pangrova::executor::TaskExecutor&)>&
                   prepare_receipt = {});

  // Join and release each actual executor outside the registry lock. Failed
  // release retains that executor and does not advance the generation.
  void destroyAll();

  // Current number of active handles (for diagnostics/testing).
  size_t size() const;

  // Current generation (for diagnostics/testing).
  uint64_t generation() const;

 private:
  HandleRegistry() = default;

  struct Entry {
    std::shared_ptr<pangrova::executor::TaskExecutor> executor;
    uint64_t generation;
    protocol::v2::ExecutorCreationRecoveryRequest identity;
    bool constructing{false};
  };

  int64_t reserveCreationLocked(const protocol::v2::ExecutorCreationRecoveryRequest& identity,
                               bool constructing);
  void purgeClosedCreationsLocked();
  void eraseLocked(std::unordered_map<int64_t, Entry>::iterator entry);

  mutable std::shared_mutex mutex_;
  std::unordered_map<int64_t, Entry> handles_;
  // Lookup only; ownership and its construction-exit fact live in handles_.
  std::unordered_map<std::string, int64_t> creation_index_;
  std::atomic<int64_t> next_handle_{1};
  std::atomic<uint64_t> current_generation_{0};
};

}  // namespace pangrova::jni
