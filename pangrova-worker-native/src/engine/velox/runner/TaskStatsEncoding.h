#pragma once

#include <cstdint>
#include <vector>

namespace facebook::velox::exec {
struct TaskStats;
}

namespace pangrova::velox {

// Serialize exec::TaskStats directly to proto wire format.
// Used by VeloxTaskRunner::collectTaskStats() for direct serialization.
std::vector<uint8_t> serializeNativeTaskStats(
    const facebook::velox::exec::TaskStats& task_stats,
    uint64_t index_splits_evaluated = 0,
    uint64_t index_splits_skipped = 0);

}  // namespace pangrova::velox
