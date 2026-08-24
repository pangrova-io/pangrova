// PartitionColumnInjector: Reusable utility for injecting partition
// column values into RowVector output. Shared between Iceberg and
// Paimon partition column injection paths.
//
// Stateless free function — all memory allocated from the caller's MemoryPool.
//
// Placement: velox/ — Velox integration utilities.
// Validates: Requirements R4.5, R12.4
#pragma once

#include <string>
#include <unordered_map>

#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"

namespace pangrova::velox {

// Inject partition column values into a RowVector.
// When partition_values is empty, returns input unchanged (zero overhead).
facebook::velox::RowVectorPtr injectPartitionColumns(
    const facebook::velox::RowVectorPtr& input,
    const std::unordered_map<std::string, std::string>& partition_values,
    const facebook::velox::RowTypePtr& output_type,
    facebook::velox::memory::MemoryPool* pool);

}  // namespace pangrova::velox
