#pragma once

namespace pangrova::velox {

// Same registered SUM signatures, state and overflow; exact zero on empty final result.
void registerSumZeroAggregate();

}  // namespace pangrova::velox
