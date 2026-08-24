// PangrovaArbitrator: Custom Velox MemoryArbitrator delegating to Java via
// BlockAllocationListener chain.
// See PangrovaArbitrator.h for design rationale.
#include "memory/PangrovaArbitrator.h"

#include <algorithm>
#include <sstream>

#include "memory/BlockAllocationListener.h"
#include "memory/MemoryPressure.h"
#include "observability/PangrovaLog.h"
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
  entry.pool = pool.get();
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

  // Reclaim any unused capacity from the pool back to Java side.
  uint64_t reclaimable = pool->capacity() - pool->usedBytes();
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

void PangrovaArbitrator::growCapacity(
    facebook::velox::memory::MemoryPool* pool,
    uint64_t targetBytes) {
  grow_requests_.fetch_add(1, std::memory_order_relaxed);

  if (targetBytes == 0) {
    return;
  }

  // Delegate to listener chain:
  // PangrovaArbitrator → BlockAllocationListener → PangrovaAllocationListener → JNI
  // Java side tracks the reservation; Velox MemoryManager handles pool capacity.
  int64_t granted = listener_->allocationChanged(
      static_cast<int64_t>(targetBytes));

  if (granted > 0) {
    grow_successes_.fetch_add(1, std::memory_order_relaxed);
    total_grown_bytes_.fetch_add(granted, std::memory_order_relaxed);
    // Actually grow the pool's capacity so Velox's internal accounting
    // (reservation vs capacity) stays consistent. The reservationBytes
    // parameter commits the pending reservation that triggered this
    // arbitration call — without it, the caller retries indefinitely
    // and the reservation counter overflows.
    growPool(pool, static_cast<uint64_t>(granted), targetBytes);
    return;
  }

  grow_failures_.fetch_add(1, std::memory_order_relaxed);

  // Attempt to reclaim memory from the pool before giving up.
  // This triggers Velox's internal spill path for operators that support
  // spilling (e.g., HashBuild, OrderBy), freeing memory that can then
  // be re-allocated.
  uint64_t reclaimed = shrinkPool(pool, targetBytes);
  if (reclaimed > 0) {
    // Release the reclaimed capacity from Java-side reservation first.
    // shrinkPool() reduced the Velox pool's capacity, but the corresponding
    // Java-side reservation (tracked by BlockAllocationListener) was not
    // reduced. Without this release, Java-side reserved_bytes_ would
    // over-count by `reclaimed` bytes, causing the Java memory coordinator
    // to believe native is using more memory than it actually is.
    listener_->allocationChanged(-static_cast<int64_t>(reclaimed));
    total_shrunk_bytes_.fetch_add(
        static_cast<int64_t>(reclaimed), std::memory_order_relaxed);

    PANGROVA_LOG(INFO) << "event=pangrova_arbitrator_reclaimed"
              << " pool=" << pool->name()
              << " reclaimed_bytes=" << reclaimed
              << " requested_bytes=" << targetBytes;
    // Retry the allocation after reclaim.
    int64_t retryGranted = listener_->allocationChanged(
        static_cast<int64_t>(targetBytes));
    if (retryGranted > 0) {
      grow_successes_.fetch_add(1, std::memory_order_relaxed);
      total_grown_bytes_.fetch_add(retryGranted, std::memory_order_relaxed);
      growPool(pool, static_cast<uint64_t>(retryGranted), targetBytes);
      return;
    }
  }

  PANGROVA_LOG(ERROR) << "event=pangrova_arbitrator_grow_denied"
            << " pool=" << pool->name()
            << " requested_bytes=" << targetBytes
            << " reclaimed_bytes=" << reclaimed;

  // Guard: if the arbitrator is shutting down, deny silently. Throwing
  // during teardown can crash threads that are draining work.
  if (shutdown_.load(std::memory_order_acquire)) {
    PANGROVA_LOG(WARNING) << "event=oom_during_shutdown pool=" << pool->name()
                        << " requested_bytes=" << targetBytes;
    return;
  }

  // Guard: if the pool is not tracked by this arbitrator, it may be a
  // background allocation on a thread without VELOX_FAIL catch semantics.
  // Deny gracefully instead of risking process termination.
  {
    std::lock_guard<std::mutex> lock(pools_mutex_);
    // PERF-T9: O(1) lookup in unordered_map.
    bool tracked = pools_.count(pool) > 0;
    if (!tracked) {
      PANGROVA_LOG(ERROR) << "event=oom_untracked_pool pool=" << pool->name()
                        << " requested_bytes=" << targetBytes;
      return;
    }
  }

  // Throw VeloxMemoryCapExceeded so Velox can distinguish OOM from generic
  // runtime errors. This enables the framework to trigger spill/abort paths
  // rather than treating it as an unexpected crash. On Velox driver threads
  // it is caught by Task::next() which transitions the task to FAILED. On JNI
  // call paths it is caught by JniExceptionScope and converted to a Java
  // OutOfMemoryError.
  VELOX_MEM_POOL_CAP_EXCEEDED(fmt::format(
      "PangrovaArbitrator: memory allocation denied by Java-side coordinator. "
      "pool={} requested_bytes={} reclaimed_bytes={}",
      pool->name(),
      targetBytes,
      reclaimed));
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
    bool /*allowSpill*/,
    bool /*allowAbort*/) {
  if (targetBytes == 0) {
    return 0;
  }

  // Under memory pressure, shrink tracked pools starting from the largest.
  // This implements the arbitration policy: largest pools yield first,
  // respecting priority (lower priority number = higher priority = shrunk last).
  uint64_t totalFreed = 0;

  std::lock_guard<std::mutex> lock(pools_mutex_);

  if (pools_.empty()) {
    return 0;
  }

  // Build a sorted list: sort by priority descending (shrink low-priority first),
  // then by capacity descending (shrink largest first within same priority).
  std::vector<PoolEntry*> candidates;
  candidates.reserve(pools_.size());
  for (auto& [key, entry] : pools_) {
    candidates.push_back(&entry);
  }
  std::sort(candidates.begin(), candidates.end(),
      [](const PoolEntry* a, const PoolEntry* b) {
        if (a->priority != b->priority) {
          return a->priority > b->priority;  // Higher number = lower priority = shrunk first.
        }
        return a->pool->capacity() > b->pool->capacity();  // Larger pools first.
      });

  for (auto* candidate : candidates) {
    if (totalFreed >= targetBytes) {
      break;
    }

    uint64_t remaining = targetBytes - totalFreed;
    uint64_t freed = shrinkPool(candidate->pool, remaining);
    if (freed > 0) {
      listener_->allocationChanged(-static_cast<int64_t>(freed));
      totalFreed += freed;
    }
  }

  if (totalFreed > 0) {
    total_shrunk_bytes_.fetch_add(
        static_cast<int64_t>(totalFreed), std::memory_order_relaxed);

    PANGROVA_LOG(INFO) << "event=pangrova_arbitrator_global_shrink"
                     << " target_bytes=" << targetBytes
                     << " freed_bytes=" << totalFreed
                     << " pool_count=" << pools_.size();
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
