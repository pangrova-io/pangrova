#include <gtest/gtest.h>

#include <limits>

#include "engine/velox/runner/SingleMatchJoinGuard.h"

namespace pangrova::velox::test {
namespace {

facebook::velox::exec::TaskStats statsWithProbeRows(
    uint64_t input_rows, uint64_t output_rows,
    const std::string& operator_type = "HashProbe") {
  facebook::velox::exec::TaskStats stats;
  facebook::velox::exec::PipelineStats pipeline(true, true);
  facebook::velox::exec::OperatorStats build;
  build.planNodeId = "join-1";
  build.operatorType = "HashBuild";
  build.inputPositions = 9;
  build.outputPositions = 0;
  pipeline.operatorStats.push_back(build);
  facebook::velox::exec::OperatorStats probe;
  probe.planNodeId = "join-1";
  probe.operatorType = operator_type;
  probe.inputPositions = input_rows;
  probe.outputPositions = output_rows;
  pipeline.operatorStats.push_back(probe);
  stats.pipelineStats.push_back(std::move(pipeline));
  return stats;
}

TEST(SingleMatchJoinGuardTest, AllowsIdenticalTargetValuesWithOneMatchEach) {
  auto stats = statsWithProbeRows(2, 2);
  EXPECT_NO_THROW(validateSingleMatchJoinStats(stats, {{"join-1", false}}));
}

TEST(SingleMatchJoinGuardTest, AllowsEmptyTargetPartition) {
  auto stats = statsWithProbeRows(0, 0);
  EXPECT_NO_THROW(validateSingleMatchJoinStats(stats, {{"join-1", false}}));
}

TEST(SingleMatchJoinGuardTest, RejectsDuplicateSourceMatches) {
  auto stats = statsWithProbeRows(2, 4);
  EXPECT_THROW(validateSingleMatchJoinStats(stats, {{"join-1", false}}),
               std::runtime_error);
}

TEST(SingleMatchJoinGuardTest, CombinesDriversForOneJoin) {
  auto stats = statsWithProbeRows(2, 2);
  auto second = statsWithProbeRows(1, 2);
  stats.pipelineStats.push_back(std::move(second.pipelineStats.front()));
  EXPECT_THROW(validateSingleMatchJoinStats(stats, {{"join-1", false}}),
               std::runtime_error);
}

TEST(SingleMatchJoinGuardTest, RejectsMissingProbeStatistics) {
  auto stats = statsWithProbeRows(1, 1);
  EXPECT_THROW(validateSingleMatchJoinStats(stats, {{"unknown-join", false}}),
               std::runtime_error);
}

TEST(SingleMatchJoinGuardTest, RejectsCountOverflowAcrossDrivers) {
  auto stats = statsWithProbeRows(std::numeric_limits<uint64_t>::max(),
                                 std::numeric_limits<uint64_t>::max());
  auto second = statsWithProbeRows(1, 1);
  stats.pipelineStats.push_back(std::move(second.pipelineStats.front()));
  EXPECT_THROW(validateSingleMatchJoinStats(stats, {{"join-1", false}}),
               std::runtime_error);
}

TEST(SingleMatchJoinGuardTest, RejectsMissingTargetOutput) {
  auto stats = statsWithProbeRows(2, 1);
  EXPECT_THROW(validateSingleMatchJoinStats(stats, {{"join-1", false}}),
               std::runtime_error);
}

TEST(SingleMatchJoinGuardTest, SupportsNestedLoopAndMergeProbeTypes) {
  for (const char* type : {"NestedLoopJoinProbe", "MergeJoin"}) {
    auto stats = statsWithProbeRows(1, 1, type);
    EXPECT_NO_THROW(validateSingleMatchJoinStats(stats, {{"join-1", false}}));
  }
}

}  // namespace
}  // namespace pangrova::velox::test
