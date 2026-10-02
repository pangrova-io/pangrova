#include "memory/ColumnarMemoryPool.h"

#include <arrow/array.h>
#include <arrow/buffer.h>

#include <algorithm>
#include <cstring>
#include <limits>

namespace pangrova::memory {
ColumnarMemoryPool::ColumnarMemoryPool(
    std::shared_ptr<facebook::velox::memory::MemoryPool> pool) : pool_(std::move(pool)) {
  if (!pool_) throw std::invalid_argument("Columnar allocator requires its actual task pool");
}

arrow::Status ColumnarMemoryPool::Allocate(int64_t size, int64_t alignment, uint8_t** out) {
  if (!out || size < 0 || alignment <= 0 || (alignment & (alignment - 1)) != 0 ||
      alignment > std::numeric_limits<uint32_t>::max()) {
    return arrow::Status::Invalid("Invalid task columnar allocation size/alignment");
  }
  *out = nullptr;
  if (size == 0) return arrow::Status::OK();
  try {
    *out = static_cast<uint8_t*>(pool_->allocate(size, static_cast<uint32_t>(alignment)));
  } catch (const std::exception& failure) {
    return arrow::Status::OutOfMemory(failure.what());
  }
  const auto live = live_.fetch_add(size) + size;
  auto peak = peak_.load();
  while (live > peak && !peak_.compare_exchange_weak(peak, live)) {}
  total_.fetch_add(size);
  allocations_.fetch_add(1);
  return arrow::Status::OK();
}

arrow::Status ColumnarMemoryPool::Reallocate(
    int64_t oldSize, int64_t newSize, int64_t alignment, uint8_t** pointer) {
  if (!pointer || oldSize < 0 || newSize < 0 || (!*pointer && oldSize != 0)) {
    return arrow::Status::Invalid("Invalid task columnar reallocation");
  }
  if (oldSize == newSize) return arrow::Status::OK();
  uint8_t* replacement = nullptr;
  ARROW_RETURN_NOT_OK(Allocate(newSize, alignment, &replacement));
  const auto copied = std::min(oldSize, newSize);
  if (copied > 0) std::memcpy(replacement, *pointer, static_cast<size_t>(copied));
  copied_.fetch_add(copied);
  Free(*pointer, oldSize, alignment);
  *pointer = replacement;
  return arrow::Status::OK();
}

void ColumnarMemoryPool::Free(uint8_t* buffer, int64_t size, int64_t) {
  if (!buffer) return;
  try { pool_->free(buffer, size); }
  catch (...) { std::terminate(); }
  live_.fetch_sub(size);
}

std::shared_ptr<arrow::Buffer> retainColumnarBuffer(
    std::shared_ptr<arrow::Buffer> buffer, std::shared_ptr<ColumnarMemoryPool> pool) {
  struct Owner {
    std::shared_ptr<ColumnarMemoryPool> pool;
    std::shared_ptr<arrow::Buffer> buffer;
  };
  if (!buffer) return nullptr;
  auto owner = std::make_shared<Owner>(Owner{std::move(pool), std::move(buffer)});
  return std::shared_ptr<arrow::Buffer>(owner, owner->buffer.get());
}

namespace {
std::shared_ptr<arrow::ArrayData> retainedArray(
    const std::shared_ptr<arrow::ArrayData>& data, const std::shared_ptr<ColumnarMemoryPool>& pool) {
  auto result = data->Copy();
  for (auto& buffer : result->buffers) buffer = retainColumnarBuffer(std::move(buffer), pool);
  for (auto& child : result->child_data) child = retainedArray(child, pool);
  if (result->dictionary) result->dictionary = retainedArray(result->dictionary, pool);
  return result;
}
}  // namespace

std::shared_ptr<arrow::RecordBatch> retainColumnarBatch(
    const std::shared_ptr<arrow::RecordBatch>& batch, const std::shared_ptr<ColumnarMemoryPool>& pool) {
  std::vector<std::shared_ptr<arrow::Array>> columns;
  for (const auto& column : batch->columns()) columns.push_back(arrow::MakeArray(retainedArray(column->data(), pool)));
  return arrow::RecordBatch::Make(batch->schema(), batch->num_rows(), std::move(columns));
}
}  // namespace pangrova::memory
