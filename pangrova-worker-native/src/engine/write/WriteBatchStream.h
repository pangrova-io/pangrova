#pragma once

#include "engine/columnar/CanonicalBatchStream.h"

namespace pangrova::write {

// Applies the synchronous JNI write boundary's dimensions/plain-vector
// contract, then uses the sole canonical Arrow stream codec and task allocator.
arrow::Result<std::shared_ptr<arrow::Buffer>> encodeWriteBatch(
    const arrow::RecordBatch& batch,
    const std::shared_ptr<memory::ColumnarMemoryPool>& pool);

}  // namespace pangrova::write
