#pragma once

#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "velox/exec/OperatorType.h"
#include "velox/exec/TaskStats.h"
#include "velox/runner/SingleMatchJoinRequirement.h"

namespace pangrova::velox {

// A SINGLE outer join emits one row per preserved input row, whether matched
// once or not matched. More than one match increases only its output count.
// Validate the exact join operator, not task-wide rows or downstream output.
inline void validateSingleMatchJoinStats(
    const facebook::velox::exec::TaskStats& stats,
    const std::vector<SingleMatchJoinRequirement>& requirements) {
  struct Counts {
    uint64_t input = 0;
    uint64_t output = 0;
    bool inputIsHashBuild = false;
    bool seenInput = false;
    bool seenOutput = false;
  };
  std::unordered_map<std::string, Counts> counts;
  for (const auto& requirement : requirements) {
    if (requirement.nodeId.empty() || !counts.emplace(requirement.nodeId,
            Counts{0, 0, requirement.inputIsHashBuild, false, false}).second) {
      throw std::runtime_error("Invalid LEFT_SINGLE join plan node identity");
    }
  }
  for (const auto& pipeline : stats.pipelineStats) {
    for (const auto& op : pipeline.operatorStats) {
      auto it = counts.find(op.planNodeId);
      if (it == counts.end()) {
        continue;
      }
      auto& count = it->second;
      const auto& type = op.operatorType;
      const bool probe = type == facebook::velox::exec::OperatorType::kHashProbe ||
          type == facebook::velox::exec::OperatorType::kMergeJoin ||
          type == facebook::velox::exec::OperatorType::kNestedLoopJoinProbe;
      const bool input = count.inputIsHashBuild
          ? type == facebook::velox::exec::OperatorType::kHashBuild : probe;
      if (input) {
        if (count.input > std::numeric_limits<uint64_t>::max() - op.inputPositions) {
          throw std::runtime_error("LEFT_SINGLE join row count overflow");
        }
        count.input += op.inputPositions;
        count.seenInput = true;
      }
      if (probe) {
        if (count.output > std::numeric_limits<uint64_t>::max() - op.outputPositions) {
          throw std::runtime_error("LEFT_SINGLE join row count overflow");
        }
        count.output += op.outputPositions;
        count.seenOutput = true;
      }
    }
  }
  for (const auto& [id, count] : counts) {
    if (!count.seenInput || !count.seenOutput) {
      throw std::runtime_error(
          "LEFT_SINGLE join probe statistics are missing: plan_node_id=" + id);
    }
    if (count.output > count.input) {
      throw std::runtime_error(
          "MERGE target row matched more than one source row: plan_node_id=" + id);
    }
    if (count.output < count.input) {
      throw std::runtime_error(
          "LEFT_SINGLE join omitted target rows: plan_node_id=" + id);
    }
  }
}

}  // namespace pangrova::velox
