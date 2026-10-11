#pragma once

#include <cstdint>
#include "pangrova/protocol/v2/common.pb.h"

namespace pangrova::result {

// Validated logical type identity for one result column. The physical storage
// remains Arrow LIST; the complete canonical declaration admits its element
// precision and nullability, and the dimension is exported as field metadata.
struct ResultColumnVectorDimension {
  uint32_t column_index;
  uint32_t dimension;
  protocol::v2::LogicalType logical_type;
};

}  // namespace pangrova::result
