// VeloxMemoryGovernor: Governs Velox memory arbitration with Java-side
// NativeMemoryCoordinator via PangrovaArbitrator + Listener chain.
//
// Memory governance path:
//   PangrovaArbitrator → BlockAllocationListener → PangrovaAllocationListener → JNI
//
// Placement: velox/ — Velox integration adapters.
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

#include "velox/common/memory/Memory.h"

// Forward declaration for AsyncDataCache pressure signaling and stats.
namespace facebook::velox::cache { class AsyncDataCache; }
namespace pangrova::memory { class BlockAllocationListener; }

namespace pangrova::velox {

// Configuration for VeloxMemoryGovernor initialization.
struct VeloxMemoryGovernorConfig {
  int64_t arbitrator_capacity = 0;       // 0 = use totalMemoryBytes
  int64_t reserved_capacity = 0;         // Reserved for running queries
  int64_t initial_query_capacity = 256L * 1024 * 1024;  // 256MB default
  bool enable_spill = false;
  std::string spill_directory;
  int64_t max_spill_bytes = 0;           // 0 = unlimited spill
};

// Memory snapshot for heartbeat observability reporting.
struct MemorySnapshot {
  int64_t current_used_bytes = 0;
  int64_t peak_used_bytes = 0;
  int64_t spill_bytes = 0;
  int64_t cache_used_bytes = 0;
  int64_t arbitrator_free_bytes = 0;
  int64_t arbitrator_capacity_bytes = 0;
  int32_t active_pool_count = 0;
};

// Governs Velox memory arbitration with Java-side NativeMemoryCoordinator.
// Creates a Velox MemoryManager with PangrovaArbitrator that delegates
// grow/shrink to the Listener chain for synchronous Java-side coordination.
class VeloxMemoryGovernor {
 public:
  // Initialize with total memory budget and a BlockAllocationListener.
  // The listener is passed to PangrovaArbitrator via factory registration
  // and must outlive this governor.
  VeloxMemoryGovernor(
      int64_t total_memory_bytes,
      pangrova::memory::BlockAllocationListener* listener,
      const VeloxMemoryGovernorConfig& config = {});

  ~VeloxMemoryGovernor();

  // Non-copyable, non-movable.
  VeloxMemoryGovernor(const VeloxMemoryGovernor&) = delete;
  VeloxMemoryGovernor& operator=(const VeloxMemoryGovernor&) = delete;

  // Create a Velox memory pool for a specific task.
  // The pool participates in PangrovaArbitrator's memory governance.
  std::shared_ptr<facebook::velox::memory::MemoryPool> createTaskPool(
      const std::string& query_id,
      const std::string& task_id,
      int64_t max_bytes);

  // Current memory usage across all Velox pools.
  int64_t usedMemory() const;

  // Total memory capacity configured for Velox.
  int64_t totalMemory() const;

  // Whether spilling is enabled in this governor.
  bool spillEnabled() const { return config_.enable_spill; }

  // Spill directory path.
  const std::string& spillDirectory() const { return config_.spill_directory; }

  // Maximum spill bytes.
  int64_t maxSpillBytes() const { return config_.max_spill_bytes; }

  // Access the underlying Velox MemoryManager (for QueryCtx construction).
  facebook::velox::memory::MemoryManager* veloxMemoryManager() const {
    return velox_mgr_;
  }

  // Propagate memory pressure signal to AsyncDataCache.
  // HIGH/CRITICAL pressure triggers cache eviction to free memory.
  void signalMemoryPressure(
      facebook::velox::cache::AsyncDataCache* cache,
      double usage_percent);

  // Collect memory stats for heartbeat observability reporting.
  MemorySnapshot getMemorySnapshot() const;

  // Set the AsyncDataCache pointer for cache stats reporting.
  // Called once during runtime initialization. Non-owning.
  void setAsyncDataCache(facebook::velox::cache::AsyncDataCache* cache);

  // Report spill bytes from a completed task. Accumulates into the
  // process-wide spill counter for MemorySnapshot reporting.
  // @ThreadSafe — uses atomic increment.
  void reportSpillBytes(int64_t bytes);

  // Returns true — Velox memory governor is always available.
  static bool isAvailable() { return true; }

 private:
  const int64_t total_memory_bytes_;
  VeloxMemoryGovernorConfig config_;
  pangrova::memory::BlockAllocationListener* listener_;  // Non-owning
  // Points to the process-wide Velox MemoryManager singleton.
  // Initialized via MemoryManager::initialize() in constructor.
  // Non-owning — the singleton is managed by Velox's static lifetime.
  facebook::velox::memory::MemoryManager* velox_mgr_ = nullptr;
  // P0-BUG-5: Track actual peak memory usage instead of reporting totalMemory.
  mutable std::atomic<int64_t> peak_used_bytes_{0};
  // Cumulative spill bytes across all completed tasks.
  mutable std::atomic<int64_t> cumulative_spill_bytes_{0};
  // Active task pool count (incremented on createTaskPool, decremented on release).
  mutable std::atomic<int32_t> active_pool_count_{0};
  // Non-owning pointer to the global AsyncDataCache for cache stats.
  std::atomic<facebook::velox::cache::AsyncDataCache*> async_data_cache_{nullptr};
};

}  // namespace pangrova::velox
