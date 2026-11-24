#pragma once

#include <arrow/record_batch.h>
#include "velox/vector/ComplexVector.h"

namespace pangrova::velox {

void validateArrowExportTypes(const facebook::velox::RowVectorPtr& vector);

// The C Data bridge retains eligible Velox buffers and exports a canonical,
// non-dictionary Arrow batch. The caller admits exact names and required values.
std::shared_ptr<arrow::RecordBatch> exportArrowRecordBatch(
    const facebook::velox::RowVectorPtr& vector, facebook::velox::memory::MemoryPool* pool);

facebook::velox::RowVectorPtr importArrowRecordBatch(
    const arrow::RecordBatch& batch, facebook::velox::memory::MemoryPool* pool);

}  // namespace pangrova::velox
