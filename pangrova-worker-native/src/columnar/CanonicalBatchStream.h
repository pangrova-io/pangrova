#pragma once

#include <arrow/buffer.h>
#include <arrow/record_batch.h>
#include "memory/ColumnarMemoryPool.h"

namespace pangrova::columnar {
// The wire ceiling is not a substitute for the actual task allocator cap.
inline constexpr int64_t kMaximumBatchStreamBytes = 256LL * 1024 * 1024;
arrow::Result<std::shared_ptr<arrow::Buffer>> encodeBatchStream(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
    const std::shared_ptr<memory::ColumnarMemoryPool>& pool,
    int64_t maximumBytes = kMaximumBatchStreamBytes);
// Reads Arrow schemas/dictionaries/batches, never SQL types. The consumer must
// validate against its authoritative task/result schema.
arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>> decodeBatchStream(
    std::shared_ptr<arrow::Buffer> buffer,
    const std::shared_ptr<memory::ColumnarMemoryPool>& pool,
    int64_t maximumBytes = kMaximumBatchStreamBytes);
}  // namespace pangrova::columnar
