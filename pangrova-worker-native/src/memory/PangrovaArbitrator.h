// PangrovaArbitrator: Custom Velox MemoryArbitrator that delegates capacity
// management to Java-side NativeMemoryCoordinator via the Listener chain:
//   PangrovaArbitrator → BlockAllocationListener → PangrovaAllocationListener → JNI
//
// Registration: call registerFactory() before MemoryManager creation.
// Factory pattern follows Gluten's ArbitratorFactoryRegister approach.
//
// Pool tracking: addPool/removePool maintain a registry of active memory pools
// with their priority. Under memory pressure, the global shrinkCapacity
// iterates tracked pools and shrinks the largest ones first.
//
// @ThreadSafe — growCapacity/shrinkCapacity may be called concurrently
// from multiple Velox Driver threads. Internal counters use std::atomic;
// listener chain is mutex-guarded (BlockAllocationListener::mutex_).
// Pool registry is protected by pools_mutex_.
//
// Placement: memory/ — memory coordination components.
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "velox/common/memory/MemoryArbitrator.h"

namespace pangrova::memory {

class BlockAllocationListener;

class PangrovaArbitrator : public facebook::velox::memory::MemoryArbitrator {
 public:
  static constexpr const char* kKind = "PANGROVA";

  // Default priority for pools that don't specify one.
  static constexpr int32_t kDefaultPriority = 0;

  PangrovaArbitrator(
      const Config& config,
      BlockAllocationListener* listener);

  ~PangrovaArbitrator() override;

  static void registerFactory(BlockAllocationListener* listener);

  // MemoryArbitrator pure virtual interface.
  std::string kind() const override;
  void shutdown() override;

  // Register a memory pool for tracking and arbitration.
  // The pool's capacity and priority are recorded so the arbitrator can
  // shrink pools under memory pressure (largest pools shrunk first).
  void addPool(
      const std::shared_ptr<facebook::velox::memory::MemoryPool>& pool)
      override;

  // Unregister a pool. Reclaims any unused capacity back to the Java side.
  void removePool(facebook::velox::memory::MemoryPool* pool) override;

  void growCapacity(
      facebook::velox::memory::MemoryPool* pool,
      uint64_t targetBytes) override;

  uint64_t shrinkCapacity(
      facebook::velox::memory::MemoryPool* pool,
      uint64_t targetBytes) override;

  uint64_t shrinkCapacity(
      uint64_t targetBytes,
      bool allowSpill,
      bool allowAbort) override;

  Stats stats() const override;
  std::string toString() const override;

  // Number of currently tracked pools (for testing/diagnostics).
  size_t poolCount() const;

  // ----- Task 27.7: Non-JVM "root pool" management -----
  //
  // Velox runs background I/O / async threads that allocate memory outside
  // any Java-initiated JNI call chain. Those allocations cannot consult the
  // Java MemoryCoordinator (no JNIEnv attached) and previously bypassed all
  // accounting via a 512MB hard cap + atomic CAS drain inside
  // PangrovaAllocationListener. That scheme has two known correctness issues:
  //   (a) the cap is hardcoded and decoupled from the global memory budget,
  //   (b) allocate-on-non-JVM-thread / free-on-JVM-thread pairs race the
  //       CAS drain, causing the counter to drift unbounded under churn.
  //
  // Replacement design: a single "non-JVM root pool" hosted inside the
  // arbitrator. Counter is uint64_t reserved bytes against a configurable
  // capacity. reserveNonJvm()/releaseNonJvm() form a balanced pair tracked
  // by the same component that already drives JVM-side pool arbitration,
  // so the arbitrator can include non-JVM usage in shrink decisions.

  // Initialize the non-JVM root pool with the given byte cap.
  // capacity_bytes == 0 leaves the previous cap (defaults to 512MB).
  // Idempotent. Safe to call before the arbitrator goes live.
  void initNonJvmRootPool(uint64_t capacity_bytes);

  // Try to reserve `bytes` against the non-JVM root pool. Returns the
  // granted byte count: either `bytes` (success) or 0 (cap exceeded).
  // Thread-safe via atomic CAS — no locks held.
  int64_t reserveNonJvm(int64_t bytes);

  // Release `bytes` previously reserved against the non-JVM root pool.
  // Caller is responsible for not double-releasing; returns the count
  // actually drained (clamped to current outstanding reservation).
  int64_t releaseNonJvm(int64_t bytes);

  // Current bytes reserved by non-JVM threads (for observability).
  int64_t nonJvmReservedBytes() const;

  // Configured non-JVM cap (for observability).
  int64_t nonJvmCapBytes() const;

 private:
  // Metadata for a tracked memory pool.
  struct PoolEntry {
    facebook::velox::memory::MemoryPool* pool;
    int32_t priority;  // Lower value = higher priority (shrunk last).
    uint64_t capacity;  // Capacity at registration time.
  };

  BlockAllocationListener* listener_;  // Non-owning.
  std::atomic<bool> shutdown_{false};

  // Pool registry — protected by pools_mutex_.
  // PERF-T9: unordered_map for O(1) add/remove instead of O(N) linear scan.
  mutable std::mutex pools_mutex_;
  std::unordered_map<facebook::velox::memory::MemoryPool*, PoolEntry> pools_;

  std::atomic<int64_t> grow_requests_{0};
  std::atomic<int64_t> grow_successes_{0};
  std::atomic<int64_t> grow_failures_{0};
  std::atomic<int64_t> grow_aborted_{0};
  std::atomic<int64_t> shrink_requests_{0};
  std::atomic<int64_t> total_grown_bytes_{0};
  std::atomic<int64_t> total_shrunk_bytes_{0};

  // Task 27.7: non-JVM root pool counters.
  static constexpr int64_t kDefaultNonJvmCapBytes = 512LL * 1024 * 1024;
  std::atomic<int64_t> non_jvm_cap_bytes_{kDefaultNonJvmCapBytes};
  std::atomic<int64_t> non_jvm_reserved_bytes_{0};
};

}  // namespace pangrova::memory
