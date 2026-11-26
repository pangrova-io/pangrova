#pragma once

#include <cstdint>
#include <vector>

#include "pangrova/protocol/v2/result.pb.h"

namespace facebook::velox::exec {
struct TaskStats;
}

namespace pangrova::velox {

// Project one Velox snapshot into the canonical terminal report model.
protocol::v2::NativeTaskStats encodeNativeTaskStats(
    const facebook::velox::exec::TaskStats& task_stats,
    uint64_t index_splits_evaluated = 0,
    uint64_t index_splits_skipped = 0);

// Standalone statistics wire projection.
std::vector<uint8_t> serializeNativeTaskStats(
    const facebook::velox::exec::TaskStats& task_stats,
    uint64_t index_splits_evaluated = 0,
    uint64_t index_splits_skipped = 0);

std::vector<uint8_t> serializeNativeTaskStats(
    const protocol::v2::NativeTaskStats& stats);

}  // namespace pangrova::velox
