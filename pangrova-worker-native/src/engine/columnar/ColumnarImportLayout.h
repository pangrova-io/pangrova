#pragma once

#include <arrow/record_batch.h>
#include <cstdint>

#include "engine/memory/ColumnarMemoryPool.h"

namespace pangrova::columnar {
// New offsets are layout allocations, not copied value payloads. Bitmap copies
// are counted only when a sliced input requires bit alignment.
struct ColumnarImportUsage {
  uint64_t allocatedOffsetBytes = 0;
  uint64_t copiedBytes = 0;
};

// Normalize Arrow layouts unsupported by the task vector importer, without SQL
// type interpretation. Fields, nullability and metadata retain their authority.
arrow::Result<std::shared_ptr<arrow::Schema>> normalizeImportSchema(
    const arrow::Schema& schema);
arrow::Result<std::shared_ptr<arrow::RecordBatch>> normalizeImportLayout(
    const arrow::RecordBatch& batch,
    const std::shared_ptr<memory::ColumnarMemoryPool>& pool,
    ColumnarImportUsage& usage);
}  // namespace pangrova::columnar
