// DynamicFilterEvaluator: Evaluates dynamic filters (runtime bloom/range
// filters from hash join build side) against a RowVector batch.
//
// Supports: BIGINT, INTEGER, SMALLINT, TINYINT, DOUBLE, REAL, VARCHAR,
// VARBINARY column types. Handles both Velox-native DoubleRange filters
// and DynamicFilterConverter BigintRange (ordered-int bit-pattern) filters.
//
// Stateless free function — safe to call from any thread.
//
// Placement: velox/connector/ — Velox integration utilities.
#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "velox/type/Filter.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"

namespace pangrova::velox {

// Evaluate dynamic filters against a RowVector batch.
// Returns a filtered RowVector with only rows passing all filters.
// Returns nullptr if all rows are filtered out.
// Returns the input unchanged if no filters match or filters list is empty.
facebook::velox::RowVectorPtr evaluateDynamicFilters(
    facebook::velox::RowVectorPtr rowVector,
    const std::vector<std::pair<facebook::velox::column_index_t,
                                std::shared_ptr<facebook::velox::common::Filter>>>& filters,
    const facebook::velox::RowTypePtr& outputType,
    facebook::velox::memory::MemoryPool* pool);

}  // namespace pangrova::velox
