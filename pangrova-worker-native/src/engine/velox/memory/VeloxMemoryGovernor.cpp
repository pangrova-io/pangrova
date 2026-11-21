// VeloxMemoryGovernor: Integrates PangrovaArbitrator with JNI layer.
// Memory governance delegated to Java via Listener chain.
#include "engine/velox/memory/VeloxMemoryGovernor.h"

#include <iostream>
#include <limits>
#include <stdexcept>

#include "engine/memory/PangrovaArbitrator.h"
#include "engine/memory/BlockAllocationListener.h"
#include "engine/memory/MemoryPressure.h"

// AsyncDataCache for memory pressure signaling and cache stats.
#include "engine/compat/NativeFeatureDetection.h"

namespace pangrova::velox {

namespace {

// Velox MemoryManager singleton initialization — isolated for version compat.
//
// Velox only exposes testingSetInstance() to set the process-wide singleton.
// Despite the "testing" prefix, this is the correct production path — Velox
// does not provide a separate production setter.
//
// UPGRADE GUIDE: When bumping Velox version in cmake/FindVelox.cmake:
//   1. Search Velox changelog for "testingSetInstance" or "MemoryManager::initialize"
//   2. If renamed/replaced, update ONLY this function
//   3. Run: ctest --test-dir build -R PangrovaArbitratorTest
void initVeloxMemoryManagerSingleton(
    const facebook::velox::memory::MemoryManager::Options& options) {
  facebook::velox::memory::MemoryManager::testingSetInstance(options);
}

}  // namespace

VeloxMemoryGovernor::VeloxMemoryGovernor(
    int64_t total_memory_bytes,
    pangrova::memory::BlockAllocationListener* listener,
    const VeloxMemoryGovernorConfig& config)
    : total_memory_bytes_(total_memory_bytes),
      config_(config),
      listener_(listener) {
  if (total_memory_bytes_ <= 0) {
    throw std::invalid_argument(
        "VeloxMemoryGovernor: total_memory_bytes must be positive, got " +
        std::to_string(total_memory_bytes_));
  }

  int64_t capacity = config_.arbitrator_capacity > 0
      ? config_.arbitrator_capacity
      : total_memory_bytes_;

  facebook::velox::memory::MemoryManager::Options options;
  options.allocatorCapacity = capacity;
  options.arbitratorCapacity = capacity;

  if (listener_) {
    // Production path: PangrovaArbitrator delegates to Java via Listener chain.
    pangrova::memory::PangrovaArbitrator::registerFactory(listener_);
    options.arbitratorKind = pangrova::memory::PangrovaArbitrator::kKind;
  }
  // else: test-only path — uses Velox default SharedArbitrator (no JNI).

  // Isolated Velox MemoryManager initialization.
  // See initVeloxMemoryManagerSingleton() for upgrade guide.
  initVeloxMemoryManagerSingleton(options);
  velox_mgr_ = facebook::velox::memory::MemoryManager::getInstance();
}

VeloxMemoryGovernor::~VeloxMemoryGovernor() = default;

std::shared_ptr<facebook::velox::memory::MemoryPool>
VeloxMemoryGovernor::createTaskPool(
    const std::string& query_id,
    const std::string& task_id,
    int64_t max_bytes) {
  auto pool_name = query_id + "." + task_id;
  // Without a root reclaimer Velox refuses to build task/node/exchange
  // reclaimers, so reclaimableBytes() stays nullopt and arbitration-driven
  // spill can never release anything. The base reclaimer walks child pools.
  auto pool = velox_mgr_->addRootPool(
      pool_name,
      max_bytes,
      facebook::velox::memory::MemoryReclaimer::create());
  {
    std::lock_guard lock(task_pools_mutex_);
    pruneExpiredTaskPoolsLocked();
    if (task_pools_.size() >=
        static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
      throw std::overflow_error("Active task pool count exceeds snapshot capacity");
    }
    task_pools_.emplace_back(pool);
  }
  return pool;
}

void VeloxMemoryGovernor::pruneExpiredTaskPoolsLocked() const {
  std::erase_if(task_pools_, [](const auto& pool) { return pool.expired(); });
}

int32_t VeloxMemoryGovernor::activeTaskPoolCount() const {
  std::lock_guard lock(task_pools_mutex_);
  pruneExpiredTaskPoolsLocked();
  return static_cast<int32_t>(task_pools_.size());
}

int64_t VeloxMemoryGovernor::usedMemory() const {
  return velox_mgr_->getTotalBytes();
}

int64_t VeloxMemoryGovernor::totalMemory() const {
  return total_memory_bytes_;
}

MemorySnapshot VeloxMemoryGovernor::getMemorySnapshot() const {
  MemorySnapshot snapshot;
  // P1-COLLAB-7: Return valid snapshot even when governor is partially initialized.
  if (!velox_mgr_) {
    snapshot.current_used_bytes = 0;
    snapshot.peak_used_bytes = 0;
    snapshot.arbitrator_capacity_bytes = total_memory_bytes_;
    snapshot.arbitrator_free_bytes = total_memory_bytes_;
    return snapshot;
  }
  snapshot.current_used_bytes = usedMemory();
  // P1-12: Include non-JVM thread allocations in the snapshot.
  // These are tracked by PangrovaAllocationListener::non_jvm_allocated_bytes_
  // and are invisible to Java-side NativeMemoryCoordinator.
  if (listener_) {
    snapshot.current_used_bytes += listener_->nonJvmAllocatedBytes();
  }
  // P0-BUG-5: Track actual peak via monotonic CAS update.
  // No ABA risk because peak only increases. Use acq_rel for visibility
  // across threads reading the snapshot concurrently.
  int64_t prev_peak = peak_used_bytes_.load(std::memory_order_acquire);
  while (snapshot.current_used_bytes > prev_peak &&
         !peak_used_bytes_.compare_exchange_weak(
             prev_peak, snapshot.current_used_bytes,
             std::memory_order_acq_rel, std::memory_order_acquire)) {
    // prev_peak is updated by compare_exchange_weak on failure.
  }
  snapshot.peak_used_bytes = peak_used_bytes_.load(std::memory_order_acquire);
  snapshot.arbitrator_capacity_bytes = total_memory_bytes_;
  snapshot.arbitrator_free_bytes =
      total_memory_bytes_ - snapshot.current_used_bytes;

  // G.2: Populate spill_bytes from cumulative task spill counter.
  snapshot.spill_bytes = cumulative_spill_bytes_.load(std::memory_order_relaxed);

  // G.2: Populate cache_used_bytes from AsyncDataCache stats.
  auto* cache = async_data_cache_.load(std::memory_order_acquire);
#if PANGROVA_HAS_VELOX_ASYNC_CACHE
  if (cache) {
    auto cacheStats = cache->refreshStats();
    snapshot.cache_used_bytes = cacheStats.tinySize + cacheStats.largeSize;
  }
#else
  (void)cache;
#endif

  snapshot.active_pool_count = activeTaskPoolCount();

  return snapshot;
}

void VeloxMemoryGovernor::signalMemoryPressure(
    facebook::velox::cache::AsyncDataCache* cache,
    double usage_percent) {
  if (!cache) {
    return;
  }
#if PANGROVA_HAS_VELOX_ASYNC_CACHE
  if (usage_percent >= pangrova::memory::kPressureThresholdHigh) {
    // Shrink 25% of the backing store capacity to relieve memory pressure.
    // SSD-backed: shrink SSD cache by 25%.
    // RAM-only: shrink RAM cache by 25% of its max capacity.
    // Note: shrink(0) is a no-op for RAM-only caches in Velox, so we must
    // pass a meaningful byte count to actually free memory.
    int64_t shrink_bytes = cache->ssdCache()
        ? cache->ssdCache()->maxBytes() / 4
        : static_cast<int64_t>(cache->allocator()->capacity()) / 4;
    cache->shrink(shrink_bytes);
    std::cerr << "level=INFO event=memory_pressure_cache_shrink"
              << " usage_percent=" << usage_percent
              << " shrink_bytes=" << shrink_bytes
              << std::endl;
  }
#else
  (void)cache;
  (void)usage_percent;
#endif
}

void VeloxMemoryGovernor::setAsyncDataCache(
    facebook::velox::cache::AsyncDataCache* cache) {
  async_data_cache_.store(cache, std::memory_order_release);
}

void VeloxMemoryGovernor::reportSpillBytes(int64_t bytes) {
  if (bytes > 0) {
    cumulative_spill_bytes_.fetch_add(bytes, std::memory_order_relaxed);
  }
}

}  // namespace pangrova::velox
