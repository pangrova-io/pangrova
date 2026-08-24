// SchemaEvolution: Reusable utility for Iceberg/Paimon schema evolution.
// Applies column mapping (reorder + null-fill) and type promotion (widening
// casts) directly on RowVector, avoiding Arrow round-trip overhead.
//
// Stateless free functions — all memory allocated from the caller's MemoryPool.
//
// Placement: velox/ — Velox integration utilities.
// Validates: Requirements R6.6, R13.4
#pragma once

#include <vector>

#include "split/SplitConfig.h"
#include "velox/vector/ComplexVector.h"

namespace pangrova::velox {

// Apply column mapping based on field IDs: reorder columns to match
// output_type, and fill missing columns with null constants.
// When column_mappings is empty, returns input unchanged (zero overhead).
facebook::velox::RowVectorPtr applyColumnMapping(
    const facebook::velox::RowVectorPtr& input,
    const std::vector<split::ColumnMappingConfig>& column_mappings,
    const facebook::velox::RowTypePtr& output_type,
    facebook::velox::memory::MemoryPool* pool);

// Apply type promotion (widening casts) on columns that need type changes.
// When type_promotions is empty, returns input unchanged (zero overhead).
facebook::velox::RowVectorPtr applyTypePromotion(
    const facebook::velox::RowVectorPtr& input,
    const std::vector<split::TypePromotionConfig>& type_promotions,
    facebook::velox::memory::MemoryPool* pool);

}  // namespace pangrova::velox
