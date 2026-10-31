#pragma once

#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"

namespace pangrova::write {

// Velox's Arrow bridge can flatten top-level encodings, but rejects a nested
// dictionary whose wrapped vector is itself encoded. DML ARRAY/ROW expressions
// may produce exactly that shape. Normalize recursively before Arrow export.
inline facebook::velox::VectorPtr prepareWriteArrowExport(
    const facebook::velox::RowVectorPtr& batch) {
  facebook::velox::VectorPtr normalized = batch;
  facebook::velox::BaseVector::flattenVector(normalized);
  return normalized;
}

}  // namespace pangrova::write
