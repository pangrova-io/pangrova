// BlockAllocationListener: Batches small allocations into block-sized JNI calls.
// See BlockAllocationListener.h for design rationale.
//
// Thread safety: atomic CAS for fast path (within existing reservation),
// mutex only for slow path (JNI delegation). >90% of allocations take the
// fast path in production workloads.
#include "memory/BlockAllocationListener.h"

#include "memory/PangrovaAllocationListener.h"
#include "observability/PangrovaLog.h"
#include "velox/common/base/Exceptions.h"

namespace pangrova::memory {

BlockAllocationListener::BlockAllocationListener(
    std::unique_ptr<PangrovaAllocationListener> delegate,
    int64_t block_size)
    : delegate_(std::move(delegate)), block_size_(block_size) {
  VELOX_CHECK_NOT_NULL(
      delegate_, "PangrovaAllocationListener delegate must not be null");
  VELOX_CHECK_GT(
      block_size_, 0, "Block size must be positive, got {}", block_size_);
}

BlockAllocationListener::~BlockAllocationListener() {
  // Release any remaining reserved bytes back to Java side.
  auto reserved = reserved_bytes_.load(std::memory_order_relaxed);
  if (reserved > 0) {
    PANGROVA_LOG(INFO) << "event=block_listener_destructor_release"
                     << " reserved_bytes=" << reserved;
    delegate_->allocationChanged(-reserved);
    reserved_bytes_.store(0, std::memory_order_relaxed);
  }
  current_block_used_.store(0, std::memory_order_relaxed);
}

int64_t BlockAllocationListener::allocationChanged(int64_t diff) {
  if (diff == 0) {
    return 0;
  }

  if (diff > 0) {
    // Fast path: CAS-increment current_block_used_ and check if within
    // existing reservation. No mutex needed for the common case.
    while (true) {
      int64_t current = current_block_used_.load(std::memory_order_acquire);
      int64_t reserved = reserved_bytes_.load(std::memory_order_acquire);
      int64_t newUsed = current + diff;

      if (newUsed <= reserved) {
        // Within existing reservation — try CAS.
        if (current_block_used_.compare_exchange_weak(
                current, newUsed, std::memory_order_release,
                std::memory_order_relaxed)) {
          return diff;
        }
        // CAS failed — another thread raced us; retry.
        continue;
      }

      // Need more reservation — fall through to slow path.
      break;
    }
  }

  // Slow path: mutex-protected JNI delegation.
  return allocationChangedSlow(diff);
}

int64_t BlockAllocationListener::allocationChangedSlow(int64_t diff) {
  std::lock_guard<std::mutex> lock(slow_path_mutex_);

  if (diff > 0) {
    int64_t current = current_block_used_.load(std::memory_order_relaxed);
    int64_t reserved = reserved_bytes_.load(std::memory_order_relaxed);
    int64_t newUsed = current + diff;

    // Re-check under lock — another thread may have expanded reservation.
    if (newUsed <= reserved) {
      current_block_used_.store(newUsed, std::memory_order_release);
      return diff;
    }

    // Calculate how many bytes exceed current reservation.
    int64_t deficit = newUsed - reserved;
    int64_t blocksNeeded = (deficit + block_size_ - 1) / block_size_;
    int64_t bytesToReserve = blocksNeeded * block_size_;

    int64_t granted = delegate_->allocationChanged(bytesToReserve);
    if (granted <= 0) {
      return 0;
    }

    reserved_bytes_.store(reserved + granted, std::memory_order_release);
    current_block_used_.store(newUsed, std::memory_order_release);
    return diff;
  }

  // Deallocation path.
  int64_t releaseBytes = -diff;
  int64_t current = current_block_used_.load(std::memory_order_relaxed);

  if (releaseBytes > current) {
    PANGROVA_LOG(WARNING) << "event=deallocation_clamped"
                        << " requested=" << releaseBytes
                        << " current_block_used=" << current
                        << " clamped_to=" << current;
    releaseBytes = current;
  }

  int64_t newUsed = current - releaseBytes;
  current_block_used_.store(newUsed, std::memory_order_release);

  // Release full blocks back to Java when excess reservation >= block_size.
  // F-WORKER-08: Batch multiple block releases into a single JNI call to
  // reduce JNI overhead under large deallocations.
  int64_t reserved = reserved_bytes_.load(std::memory_order_relaxed);
  int64_t excess = reserved - newUsed;
  if (excess >= block_size_) {
    int64_t blocksToRelease = excess / block_size_;
    int64_t bytesToRelease = blocksToRelease * block_size_;
    delegate_->allocationChanged(-bytesToRelease);
    reserved -= bytesToRelease;
  }
  reserved_bytes_.store(reserved, std::memory_order_release);

  return releaseBytes;
}

int64_t BlockAllocationListener::nonJvmAllocatedBytes() const {
  return delegate_ ? delegate_->nonJvmAllocatedBytes() : 0;
}

void BlockAllocationListener::setNonJvmRootPoolDelegate(
    PangrovaArbitrator* arbitrator) {
  if (delegate_) {
    delegate_->setNonJvmRootPoolDelegate(arbitrator);
  }
}

}  // namespace pangrova::memory
