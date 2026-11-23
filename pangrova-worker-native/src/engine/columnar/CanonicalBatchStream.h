#pragma once

#include <arrow/buffer.h>
#include <arrow/record_batch.h>
#include <arrow/util/type_fwd.h>
#include "engine/memory/ColumnarMemoryPool.h"

namespace pangrova::columnar {
// The wire ceiling is not a substitute for the actual task allocator cap.
inline constexpr int64_t kMaximumBatchStreamBytes = 256LL * 1024 * 1024;
struct BatchStreamOptions {
  int64_t maximum_bytes = kMaximumBatchStreamBytes;
  arrow::Compression::type compression = arrow::Compression::UNCOMPRESSED;
  double minimum_space_savings = 0.2;
};
arrow::Result<std::shared_ptr<arrow::Buffer>> encodeBatchStream(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
    const std::shared_ptr<memory::ColumnarMemoryPool>& pool,
    const BatchStreamOptions& options = {});
// Checks Arrow buffers once per decoded batch. An admitted schema also checks
// wire identity and required values before any batch is returned.
arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>> decodeBatchStream(
    std::shared_ptr<arrow::Buffer> buffer,
    const std::shared_ptr<memory::ColumnarMemoryPool>& pool,
    int64_t maximumBytes = kMaximumBatchStreamBytes,
    const arrow::Schema* admittedSchema = nullptr);
}  // namespace pangrova::columnar
