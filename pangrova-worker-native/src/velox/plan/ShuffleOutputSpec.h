#pragma once

#include <cstdint>
#include <vector>

namespace pangrova::velox {

// The one producer-output contract decoded from canonical Substrait ExchangeRel.
// Native partitions callback output directly into SerializedShuffleQueue;
// producer plans must not also contain a Velox PartitionedOutput sink.
struct ShuffleOutputSpec {
  enum class Distribution { HASH, BROADCAST, ROUND_ROBIN };

  Distribution distribution = Distribution::ROUND_ROBIN;
  int32_t partition_count = 0;
  std::vector<int32_t> key_indices;
};

}  // namespace pangrova::velox
