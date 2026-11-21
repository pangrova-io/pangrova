#pragma once

#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>

#include <atomic>
#include <memory>

#include "velox/common/memory/MemoryPool.h"

namespace pangrova::memory {
// Arrow interop allocations are actual allocations under the task's pool cap.
class ColumnarMemoryPool final : public arrow::MemoryPool {
 public:
  explicit ColumnarMemoryPool(std::shared_ptr<facebook::velox::memory::MemoryPool> pool);
  using arrow::MemoryPool::Allocate;
  using arrow::MemoryPool::Reallocate;
  using arrow::MemoryPool::Free;
  arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override;
  arrow::Status Reallocate(int64_t oldSize, int64_t newSize, int64_t alignment, uint8_t** pointer) override;
  void Free(uint8_t* buffer, int64_t size, int64_t alignment) override;
  int64_t bytes_allocated() const override { return live_.load(); }
  int64_t max_memory() const override { return peak_.load(); }
  int64_t total_bytes_allocated() const override { return total_.load(); }
  int64_t num_allocations() const override { return allocations_.load(); }
  std::string backend_name() const override { return "task-columnar"; }
  int64_t copiedBytes() const { return copied_.load(); }
  const std::shared_ptr<facebook::velox::memory::MemoryPool>& source() const { return pool_; }

 private:
  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
  std::atomic<int64_t> live_{0}, peak_{0}, total_{0}, allocations_{0}, copied_{0};
};

// Arrow Buffer stores a borrowed allocator pointer. These owners prevent the
// allocator and task pool from dying before asynchronous buffer consumers.
std::shared_ptr<arrow::Buffer> retainColumnarBuffer(
    std::shared_ptr<arrow::Buffer> buffer, std::shared_ptr<ColumnarMemoryPool> pool);
std::shared_ptr<arrow::RecordBatch> retainColumnarBatch(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::shared_ptr<ColumnarMemoryPool>& pool);
}  // namespace pangrova::memory
