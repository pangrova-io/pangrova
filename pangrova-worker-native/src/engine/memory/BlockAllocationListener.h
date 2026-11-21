// BlockAllocationListener: Batches small memory allocations into block-sized
// chunks before delegating to PangrovaAllocationListener, reducing JNI call
// frequency.
//
// Default block size: 8MB (configurable). When accumulated allocation within
// the current block reaches block_size, a single JNI reserve call is made
// for the entire block. Releases follow the same batching logic.
//
// @ThreadSafe — uses atomic CAS for the fast path (allocation within existing
// reservation) and a mutex only for the slow path (JNI delegation). The fast
// path is lock-free and handles >90% of allocations in production workloads.
//
// Placement: memory/ — memory coordination components.
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>

namespace pangrova::memory {

class PangrovaAllocationListener;
class PangrovaArbitrator;

class BlockAllocationListener {
 public:
  static constexpr int64_t kDefaultBlockSize = 8L * 1024 * 1024;  // 8MB

  // Construct with delegate listener and block size.
  // delegate: ownership transferred; must not be null.
  // block_size: must be positive.
  BlockAllocationListener(
      std::unique_ptr<PangrovaAllocationListener> delegate,
      int64_t block_size = kDefaultBlockSize);

  ~BlockAllocationListener();

  // Non-copyable, non-movable (owns delegate).
  BlockAllocationListener(const BlockAllocationListener&) = delete;
  BlockAllocationListener& operator=(const BlockAllocationListener&) = delete;
  BlockAllocationListener(BlockAllocationListener&&) = delete;
  BlockAllocationListener& operator=(BlockAllocationListener&&) = delete;

  // Batched allocation change notification.
  //
  // diff > 0 (allocation):
  //   Fast path (lock-free): if within existing reservation, CAS-increment.
  //   Slow path (mutex): delegates a block-sized reserve to PangrovaAllocationListener.
  //   Returns actual granted bytes (0 = denied by Java side).
  //
  // diff < 0 (deallocation):
  //   Accumulates release. When accumulated release reaches block size,
  //   delegates unreserve to PangrovaAllocationListener.
  //   Returns |diff|.
  //
  // diff == 0: no-op, returns 0.
  int64_t allocationChanged(int64_t diff);

  // Total bytes currently reserved from Java side.
  int64_t reservedBytes() const {
    return reserved_bytes_.load(std::memory_order_acquire);
  }

  // Current accumulated bytes within the active block (not yet delegated).
  int64_t currentBlockUsed() const {
    return current_block_used_.load(std::memory_order_acquire);
  }

  // P1-12: Expose non-JVM thread allocations for memory snapshot reporting.
  int64_t nonJvmAllocatedBytes() const;

  // Task 27.7: forward the PangrovaArbitrator-managed non-JVM root pool
  // delegate to the inner PangrovaAllocationListener.
  void setNonJvmRootPoolDelegate(PangrovaArbitrator* arbitrator);

 private:
  int64_t allocationChangedSlow(int64_t diff);

  std::unique_ptr<PangrovaAllocationListener> delegate_;
  const int64_t block_size_;

  // Atomic fields for the lock-free fast path.
  std::atomic<int64_t> current_block_used_{0};
  std::atomic<int64_t> reserved_bytes_{0};

  // Mutex only for the slow path (JNI delegation).
  std::mutex slow_path_mutex_;
};

}  // namespace pangrova::memory
