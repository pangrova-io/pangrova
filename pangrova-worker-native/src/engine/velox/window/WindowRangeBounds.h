#pragma once

#include <cstdint>

#include "velox/core/Expressions.h"

namespace facebook::velox::exec {
class FunctionRegistrySnapshot;
}

namespace pangrova::velox {

// The comparison key and precomputed bound use one exact physical domain.
facebook::velox::core::TypedExprPtr rangeWindowComparison(
    facebook::velox::core::TypedExprPtr value);

facebook::velox::core::TypedExprPtr rangeWindowBoundary(
    const facebook::velox::exec::FunctionRegistrySnapshot& registry,
    facebook::velox::core::TypedExprPtr comparison, int64_t offset, bool subtract);

}  // namespace pangrova::velox
