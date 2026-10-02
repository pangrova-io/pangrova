#pragma once

#include <cstdint>

namespace pangrova::result {

// Validated logical type identity for one result column. The physical storage
// remains Arrow List<Float32>; this dimension is exported as field metadata.
struct ResultColumnVectorDimension {
  uint32_t column_index;
  uint32_t dimension;
};

}  // namespace pangrova::result
