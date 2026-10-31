#pragma once

#include "substrait/type.pb.h"
#include "velox/type/Type.h"

namespace pangrova::capability {

// Sole canonical-to-physical projection used by execution and contract binding.
facebook::velox::TypePtr canonicalPhysicalType(const substrait::Type& type);
facebook::velox::RowTypePtr canonicalPhysicalSchema(
    const substrait::NamedStruct& schema);
facebook::velox::TypePtr canonicalDecimalType(int32_t precision, int32_t scale);
facebook::velox::TypePtr canonicalTimeType(int32_t precision);
facebook::velox::RowTypePtr canonicalComputedRowType(
    std::vector<facebook::velox::TypePtr> children);

}  // namespace pangrova::capability
