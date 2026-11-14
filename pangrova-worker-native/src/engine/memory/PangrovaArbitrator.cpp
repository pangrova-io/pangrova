// PangrovaArbitrator: Custom Velox MemoryArbitrator delegating to Java via
// BlockAllocationListener chain.
// See PangrovaArbitrator.h for design rationale.
#include "engine/memory/PangrovaArbitrator.h"

#include <algorithm>
#include <limits>
#include <sstream>

#include "engine/memory/BlockAllocationListener.h"
#include "engine/memory/MemoryPressure.h"
#include "engine/observability/PangrovaLog.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/MemoryArbitrator.h"
#include "velox/common/memory/MemoryPool.h"

namespace pangrova::memory {

PangrovaArbitrator::PangrovaArbitrator(
    const Config& config,
    BlockAllocationListener* listener)
    : MemoryArbitrator(config), listener_(listener) {
  VELOX_CHECK_NOT_NULL(
      listener_, "BlockAllocationListener must not be null");

  // Task 27.7: install this arbitrator as the non-JVM root pool delegate
  // for the listener. This replaces the listener's local 512MB CAS-drain
  // counter with the arbitrator-managed reserveNonJvm/releaseNonJvm path.
  // Lifetime: shutdown() and the destructor both clear the delegate
  // pointer before the arbitrator is freed (the listener outlives the
  // arbitrator in NativeEngineLifecycle::shutdownInternal).
  listener_->setNonJvmRootPoolDelegate(this);

  PANGROVA_LOG(INFO) << "event=pangrova_arbitrator_created"
            << " kind=" << kKind
            << " capacity=" << config.capacity
            << " non_jvm_cap_bytes=" << nonJvmCapBytes();
}

PangrovaArbitrator::~PangrovaArbitrator() {
  // Task 27.7: defensive — guarantees the listener never sees a dangling
  // delegate pointer even if shutdown() was never called.
  if (listener_) {
    listener_->setNonJvmRootPoolDelegate(nullptr);
  }
}

void PangrovaArbitrator::registerFactory(BlockAllocationListener* listener) {
  VELOX_CHECK_NOT_NULL(
      listener, "BlockAllocationListener must not be null for factory registration");

  facebook::velox::memory::MemoryArbitrator::registerFactory(
      kKind,
      [listener](const Config& config) -> std::unique_ptr<MemoryArbitrator> {
        return std::make_unique<PangrovaArbitrator>(config, listener);
      });

  PANGROVA_LOG(INFO) << "event=pangrova_arbitrator_factory_registered"
            << " kind=" << kKind;
}

std::string PangrovaArbitrator::kind() const {
  return kKind;
}

void PangrovaArbitrator::shutdown() {
  shutdown_.store(true, std::memory_order_release);
  // Task 27.7: detach the non-JVM root pool delegate before Velox
  // MemoryManager frees us. Without this clear, PangrovaAllocationListener
  // would dangle on a destroyed arbitrator if any non-JVM allocation
  // arrives between MemoryManager teardown and BlockAllocationListener
  // teardown (NativeEngineLifecycle destroys governor then listener).
  if (listener_) {
    listener_->setNonJvmRootPoolDelegate(nullptr);
  }
}

void PangrovaArbitrator::addPool(
    const std::shared_ptr<facebook::velox::memory::MemoryPool>& pool) {
  VELOX_CHECK_NOT_NULL(pool, "Cannot add null pool to arbitrator");

  std::lock_guard<std::mutex> lock(pools_mutex_);
  // PERF-T9: O(1) duplicate check via unordered_map.
  if (pools_.count(pool.get())) {
    PANGROVA_LOG(WARNING) << "event=pangrova_arbitrator_add_pool_duplicate"
                        << " pool=" << pool->name();
    return;
  }

  PoolEntry entry;
  entry.pool = pool;
  entry.priority = kDefaultPriority;
  entry.capacity = pool->capacity();
  pools_[pool.get()] = entry;

  PANGROVA_LOG(INFO) << "event=pangrova_arbitrator_pool_added"
                   << " pool=" << pool->name()
                   << " capacity=" << entry.capacity
                   << " priority=" << entry.priority
                   << " pool_count=" << pools_.size();
}

void PangrovaArbitrator::removePool(
    facebook::velox::memory::MemoryPool* pool) {
  VELOX_CHECK_NOT_NULL(pool, "Cannot remove null pool from arbitrator");

  std::lock_guard<std::mutex> lock(pools_mutex_);
  // PERF-T9: O(1) lookup and erase via unordered_map.
  auto it = pools_.find(pool);

  if (it == pools_.end()) {
    PANGROVA_LOG(WARNING) << "event=pangrova_arbitrator_remove_pool_not_found"
                        << " pool=" << pool->name();
    return;
  }

  pools_.erase(it);

  // Reclaim any unused capacity from the pool back to Java side. usedBytes can
  // transiently exceed capacity, so guard the unsigned subtraction.
  const uint64_t capacity = pool->capacity();
  const uint64_t used = pool->usedBytes();
  uint64_t reclaimable = capacity > used ? capacity - used : 0;
  if (reclaimable > 0) {
    uint64_t freed = shrinkPool(pool, reclaimable);
    if (freed > 0) {
      listener_->allocationChanged(-static_cast<int64_t>(freed));
      total_shrunk_bytes_.fetch_add(
          static_cast<int64_t>(freed), std::memory_order_relaxed);
    }
  }

  PANGROVA_LOG(INFO) << "event=pangrova_arbitrator_pool_removed"
                   << " pool=" << pool->name()
                   << " reclaimed_bytes=" << reclaimable
                   << " pool_count=" << pools_.size();
}

void PangrovaArbitrator::commitCapacity(
    facebook::velox::memory::MemoryPool* pool, int64_t granted, uint64_t requested) {
  bool committed;
  try {
    committed = growPool(pool, static_cast<uint64_t>(granted), requested);
  } catch (...) {
    listener_->allocationChanged(-granted);
    grow_failures_.fetch_add(1, std::memory_order_relaxed);
    throw;
  }
  if (!committed) {
    listener_->allocationChanged(-granted);
    grow_failures_.fetch_add(1, std::memory_order_relaxed);
    VELOX_MEM_POOL_CAP_EXCEEDED(fmt::format(
        "Task pool rejected capacity growth: pool={} granted_bytes={} requested_bytes={} "
        "capacity_bytes={} max_capacity_bytes={} used_bytes={} reserved_bytes={}",
        pool->name(), granted, requested, pool->capacity(), pool->maxCapacity(),
        pool->usedBytes(), pool->reservedBytes()));
  }
  grow_successes_.fetch_add(1, std::memory_order_relaxed);
  total_grown_bytes_.fetch_add(granted, std::memory_order_relaxed);
}

void PangrovaArbitrator::growCapacity(
    facebook::velox::memory::MemoryPool* pool, uint64_t targetBytes) {
  VELOX_CHECK_NOT_NULL(pool, "Capacity growth requires a pool");
  grow_requests_.fetch_add(1, std::memory_order_relaxed);
  if (targetBytes == 0) return;
  if (targetBytes > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
    grow_failures_.fetch_add(1, std::memory_order_relaxed);
    VELOX_MEM_POOL_CAP_EXCEEDED("Capacity request exceeds the accounting domain");
  }
  int64_t granted = listener_->allocationChanged(static_cast<int64_t>(targetBytes));
  if (granted > 0) {
    commitCapacity(pool, granted, targetBytes);
    return;
  }
  // Java denied the growth. Free capacity elsewhere before failing the query:
  // first this pool's own unused capacity, then arbitration-driven reclaim
  // (spill) across tracked pools, which is what makes used bytes releasable.
  uint64_t reclaimed = shrinkPool(pool, targetBytes);
  if (reclaimed > 0) {
    listener_->allocationChanged(-static_cast<int64_t>(reclaimed));
    total_shrunk_bytes_.fetch_add(static_cast<int64_t>(reclaimed), std::memory_order_relaxed);
  }
  if (reclaimed < targetBytes) {
    reclaimed += reclaimCapacity(targetBytes - reclaimed, /*allowSpill=*/true);
  }
  if (reclaimed > 0) {
    granted = listener_->allocationChanged(static_cast<int64_t>(targetBytes));
    if (granted > 0) {
      commitCapacity(pool, granted, targetBytes);
      return;
    }
  }
  grow_failures_.fetch_add(1, std::memory_order_relaxed);
  VELOX_MEM_POOL_CAP_EXCEEDED(fmt::format(
      "Capacity growth denied: pool={} requested_bytes={} reclaimed_bytes={}",
      pool->name(), targetBytes, reclaimed));
}
uint64_t PangrovaArbitrator::shrinkCapacity(
    facebook::velox::memory::MemoryPool* pool,
    uint64_t targetBytes) {
  shrink_requests_.fetch_add(1, std::memory_order_relaxed);

  if (targetBytes == 0) {
    return 0;
  }

  // Actually shrink the pool's capacity first, then notify Java side.
  uint64_t freed = shrinkPool(pool, targetBytes);
  if (freed > 0) {
    listener_->allocationChanged(-static_cast<int64_t>(freed));
    total_shrunk_bytes_.fetch_add(
        static_cast<int64_t>(freed), std::memory_order_relaxed);
  }

  return freed;
}

uint64_t PangrovaArbitrator::shrinkCapacity(
    uint64_t targetBytes,
    bool allowSpill,
    bool allowAbort) {
  // Aborting a query is never an acceptable way to satisfy a capacity request.
  (void)allowAbort;
  return reclaimCapacity(targetBytes, allowSpill);
}

uint64_t PangrovaArbitrator::reclaimCapacity(
    uint64_t targetBytes,
    bool allowSpill) {
  if (targetBytes == 0 || shutdown_.load(std::memory_order_acquire)) {
    return 0;
  }

  // Marks this thread as under arbitration so spill-time allocations do not
  // recursively trigger arbitration (velox MemoryPool checks
  // underMemoryArbitration()). Mirrors SharedArbitrator::shrinkCapacity.
  facebook::velox::memory::ScopedMemoryArbitrationContext arbitrationCtx{};

  // Snapshot candidates under the registry lock, then release it before doing
  // any pool work: Velox reclaim re-enters growCapacity, so holding
  // pools_mutex_ across it would deadlock. weak_ptr keeps the snapshot safe
  // against a pool being destroyed in the meantime.
  struct Candidate {
    std::weak_ptr<facebook::velox::memory::MemoryPool> pool;
    int32_t priority;
    uint64_t capacity;
  };
  std::vector<Candidate> candidates;
  {
    std::lock_guard<std::mutex> lock(pools_mutex_);
    candidates.reserve(pools_.size());
    for (const auto& [key, entry] : pools_) {
      candidates.push_back(Candidate{entry.pool, entry.priority, entry.capacity});
    }
  }
  if (candidates.empty()) {
    return 0;
  }

  // Largest low-priority pools yield first.
  std::sort(candidates.begin(), candidates.end(),
      [](const Candidate& a, const Candidate& b) {
        if (a.priority != b.priority) {
          return a.priority > b.priority;
        }
        return a.capacity > b.capacity;
      });

  uint64_t totalFreed = 0;

  // Pass 1: give back unused capacity. Cheap and never blocks.
  for (const auto& candidate : candidates) {
    if (totalFreed >= targetBytes) {
      break;
    }
    auto pool = candidate.pool.lock();
    if (pool == nullptr) {
      continue;
    }
    const uint64_t freed = shrinkPool(pool.get(), targetBytes - totalFreed);
    if (freed > 0) {
      listener_->allocationChanged(-static_cast<int64_t>(freed));
      totalFreed += freed;
    }
  }

  // Pass 2: unused capacity is ~0 under real pressure, so drive operator
  // reclaim (spill) for the shortfall.
  if (allowSpill && totalFreed < targetBytes) {
    for (const auto& candidate : candidates) {
      if (totalFreed >= targetBytes) {
        break;
      }
      auto pool = candidate.pool.lock();
      if (pool == nullptr) {
        continue;
      }
      const uint64_t shortfall = targetBytes - totalFreed;
      if (pool->reclaimableBytes().value_or(0) == 0) {
        continue;
      }
      // Pairs enter/leave arbitration on this pool's reclaimer so a driver
      // that must pause for reclaim can do so.
      facebook::velox::memory::MemoryPoolArbitrationSection section(pool.get());
      facebook::velox::memory::MemoryReclaimer::Stats stats;
      // maxWaitMs must be non-zero: zero makes Velox wait unboundedly for the
      // requesting task to pause, which self-deadlocks when reclaiming it.
      const uint64_t freed =
          pool->reclaim(shortfall, kReclaimMaxWaitMs, stats);
      if (freed > 0) {
        listener_->allocationChanged(-static_cast<int64_t>(freed));
        totalFreed += freed;
      }
    }
  }

  if (totalFreed > 0) {
    total_shrunk_bytes_.fetch_add(
        static_cast<int64_t>(totalFreed), std::memory_order_relaxed);

    PANGROVA_LOG(INFO) << "event=pangrova_arbitrator_global_shrink"
                     << " target_bytes=" << targetBytes
                     << " freed_bytes=" << totalFreed
                     << " allow_spill=" << allowSpill
                     << " pool_count=" << candidates.size();
  }

  return totalFreed;
}

size_t PangrovaArbitrator::poolCount() const {
  std::lock_guard<std::mutex> lock(pools_mutex_);
  return pools_.size();
}

// ---------------------------------------------------------------------------
// Task 27.7: non-JVM root pool implementation.
//
// Unlike the JVM-driven pools tracked in pools_, the non-JVM root pool is
// a flat byte counter against a configurable cap. Allocations and frees
// are perfectly paired (the caller — PangrovaAllocationListener — drives both
// sides), so a simple atomic CAS suffices without any drain heuristic.
// ---------------------------------------------------------------------------

void PangrovaArbitrator::initNonJvmRootPool(uint64_t capacity_bytes) {
  if (capacity_bytes == 0) {
    return;
  }
  non_jvm_cap_bytes_.store(static_cast<int64_t>(capacity_bytes),
                           std::memory_order_relaxed);
  PANGROVA_LOG(INFO) << "event=pangrova_arbitrator_non_jvm_root_pool_init"
                   << " cap_bytes=" << capacity_bytes;
}

int64_t PangrovaArbitrator::reserveNonJvm(int64_t bytes) {
  if (bytes <= 0) {
    return 0;
  }
  const int64_t cap = non_jvm_cap_bytes_.load(std::memory_order_relaxed);
  while (true) {
    int64_t current =
        non_jvm_reserved_bytes_.load(std::memory_order_relaxed);
    if (current + bytes > cap) {
      PANGROVA_LOG(WARNING) << "event=non_jvm_root_pool_cap_exceeded"
                          << " current_bytes=" << current
                          << " requested_bytes=" << bytes
                          << " cap_bytes=" << cap;
      return 0;
    }
    if (non_jvm_reserved_bytes_.compare_exchange_weak(
            current, current + bytes, std::memory_order_relaxed)) {
      return bytes;
    }
  }
}

int64_t PangrovaArbitrator::releaseNonJvm(int64_t bytes) {
  if (bytes <= 0) {
    return 0;
  }
  while (true) {
    int64_t current =
        non_jvm_reserved_bytes_.load(std::memory_order_relaxed);
    if (current <= 0) {
      return 0;
    }
    int64_t drain = std::min(bytes, current);
    if (non_jvm_reserved_bytes_.compare_exchange_weak(
            current, current - drain, std::memory_order_relaxed)) {
      return drain;
    }
  }
}

int64_t PangrovaArbitrator::nonJvmReservedBytes() const {
  return non_jvm_reserved_bytes_.load(std::memory_order_relaxed);
}

int64_t PangrovaArbitrator::nonJvmCapBytes() const {
  return non_jvm_cap_bytes_.load(std::memory_order_relaxed);
}

facebook::velox::memory::MemoryArbitrator::Stats
PangrovaArbitrator::stats() const {
  Stats result;
  result.numRequests = static_cast<uint64_t>(
      grow_requests_.load(std::memory_order_relaxed));
  result.numSucceeded = static_cast<uint64_t>(
      grow_successes_.load(std::memory_order_relaxed));
  // P1-MISC-2: Track numAborted and numFailures separately.
  // numAborted = tasks killed due to memory pressure (currently 0, no abort path).
  // numFailures = grow requests denied by Java-side memory coordinator.
  result.numAborted = static_cast<uint64_t>(
      grow_aborted_.load(std::memory_order_relaxed));
  result.numFailures = static_cast<uint64_t>(
      grow_failures_.load(std::memory_order_relaxed));
  result.freeCapacityBytes = 0;
  result.freeReservedCapacityBytes = 0;
  result.reclaimedFreeBytes = static_cast<uint64_t>(
      total_shrunk_bytes_.load(std::memory_order_relaxed));
  return result;
}

std::string PangrovaArbitrator::toString() const {
  std::ostringstream oss;
  oss << "PangrovaArbitrator["
      << "kind=" << kKind
      << " pool_count=" << poolCount()
      << " grow_requests=" << grow_requests_.load(std::memory_order_relaxed)
      << " grow_successes=" << grow_successes_.load(std::memory_order_relaxed)
      << " grow_failures=" << grow_failures_.load(std::memory_order_relaxed)
      << " shrink_requests=" << shrink_requests_.load(std::memory_order_relaxed)
      << " total_grown_bytes=" << total_grown_bytes_.load(std::memory_order_relaxed)
      << " total_shrunk_bytes=" << total_shrunk_bytes_.load(std::memory_order_relaxed)
      << " listener_reserved_bytes=" << listener_->reservedBytes()
      << "]";
  return oss.str();
}

}  // namespace pangrova::memory
