#pragma once

#include <limits>
#include <stdexcept>
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"

namespace pangrova::merge {

inline int64_t decodedSequenceNumber(const facebook::velox::RowVectorPtr& batch,
    facebook::velox::column_index_t column, int64_t row) {
  const auto& vector = batch->childAt(column);
  facebook::velox::DecodedVector decoded(*vector);
  if (decoded.isNullAt(row)) {
    if (batch->type()->asRow().nameOf(column) == "_SEQUENCE_NUMBER") {
      throw std::invalid_argument("Paimon persisted sequence must not be null");
    }
    return std::numeric_limits<int64_t>::min();
  }
  switch (vector->typeKind()) {
    case facebook::velox::TypeKind::BIGINT: return decoded.valueAt<int64_t>(row);
    case facebook::velox::TypeKind::INTEGER: return decoded.valueAt<int32_t>(row);
    case facebook::velox::TypeKind::SMALLINT: return decoded.valueAt<int16_t>(row);
    case facebook::velox::TypeKind::TINYINT: return decoded.valueAt<int8_t>(row);
    default: throw std::invalid_argument("Merge sequence must be an integral physical column");
  }
}

}  // namespace pangrova::merge
