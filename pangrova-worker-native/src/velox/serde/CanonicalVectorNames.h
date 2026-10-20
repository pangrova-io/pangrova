#pragma once

#include "velox/vector/BaseVector.h"

namespace pangrova::velox {

// Binds ordinal ROW field names to the admitted type without changing values,
// physical types or shared payload buffers. The input vectors are never mutated.
facebook::velox::VectorPtr bindCanonicalVectorNames(
    const facebook::velox::VectorPtr& vector, const facebook::velox::TypePtr& type);

}  // namespace pangrova::velox
