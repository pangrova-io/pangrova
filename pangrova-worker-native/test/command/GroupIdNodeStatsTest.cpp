// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

// Verifies that GroupIdNode operator statistics are correctly captured
// and serialized through the TaskStats → PipelineStats → OperatorStats
// hierarchy. GroupIdNode uses Velox's built-in OperatorStats framework
// (inputRows, outputRows, addInputTiming, getOutputTiming) which is
// automatically collected by exec::Task::taskStats() and serialized by
// serializeNativeTaskStats().
//
// GroupIdNode replicates each input row N times (one per grouping set),
// so outputRows = inputRows × N. This test verifies that relationship
// is correctly captured in the serialized statistics.

#include <gtest/gtest.h>

#include "command/StatusSerializer.h"

#include "pangrova/protocol/v2/result.pb.h"

#if __has_include("velox/exec/TaskStats.h")
#include "velox/exec/TaskStats.h"
#define PANGROVA_HAS_VELOX_TASK_STATS 1
#else
#define PANGROVA_HAS_VELOX_TASK_STATS 0
#endif

namespace pangrova::jni::test {
namespace {

#if PANGROVA_HAS_VELOX_TASK_STATS

class GroupIdNodeStatsTest : public ::testing::Test {};

// Helper: create a Velox OperatorStats with specified values.
facebook::velox::exec::OperatorStats makeOpStats(
    const std::string& op_type,
    const std::string& plan_node_id,
    uint64_t input_rows,
    uint64_t output_rows,
    uint64_t input_bytes,
    uint64_t output_bytes,
    uint64_t add_input_wall_nanos,
    uint64_t add_input_cpu_nanos,
    uint64_t get_output_wall_nanos,
    uint64_t get_output_cpu_nanos,
    uint64_t memory_bytes,
    uint64_t peak_memory_bytes) {
  facebook::velox::exec::OperatorStats stats;
  stats.operatorType = op_type;
  stats.planNodeId = plan_node_id;
  stats.inputPositions = input_rows;
  stats.outputPositions = output_rows;
  stats.inputBytes = input_bytes;
  stats.outputBytes = output_bytes;
  stats.addInputTiming.wallNanos = add_input_wall_nanos;
  stats.addInputTiming.cpuNanos = add_input_cpu_nanos;
  stats.getOutputTiming.wallNanos = get_output_wall_nanos;
  stats.getOutputTiming.cpuNanos = get_output_cpu_nanos;
  stats.memoryStats.userMemoryReservation = memory_bytes;
  stats.memoryStats.peakUserMemoryReservation = peak_memory_bytes;
  stats.spilledBytes = 0;
  return stats;
}

// Verifies that GroupIdNode statistics propagate through the
// TaskStats → PipelineStats → OperatorStats → proto serialization path.
// GroupIdNode replicates rows: outputRows = inputRows × numGroupingSets.
TEST_F(GroupIdNodeStatsTest, GroupIdNodeStatsPropagateToProto) {
  facebook::velox::exec::TaskStats taskStats;
  taskStats.executionStartTimeMs = 1000;
  taskStats.endTimeMs = 3000;

  // Simulate a GROUPING SETS pipeline:
  // TableScan → GroupIdNode → Aggregation → PartitionedOutput
  facebook::velox::exec::PipelineStats pipeline(true, true);

  // Input scan
  pipeline.operatorStats.push_back(makeOpStats(
      "TableScan", "0",
      /*input_rows=*/2000, /*output_rows=*/2000,
      /*input_bytes=*/160000, /*output_bytes=*/160000,
      /*add_input_wall=*/1'000'000, /*add_input_cpu=*/800'000,
      /*get_output_wall=*/500'000, /*get_output_cpu=*/400'000,
      /*memory=*/8192, /*peak_memory=*/16384));

  // GroupIdNode: replicates each row 3 times (3 grouping sets)
  // outputRows = 2000 × 3 = 6000
  pipeline.operatorStats.push_back(makeOpStats(
      "GroupId", "1",
      /*input_rows=*/2000, /*output_rows=*/6000,
      /*input_bytes=*/160000, /*output_bytes=*/480000,
      /*add_input_wall=*/3'000'000, /*add_input_cpu=*/2'500'000,
      /*get_output_wall=*/1'500'000, /*get_output_cpu=*/1'200'000,
      /*memory=*/16384, /*peak_memory=*/32768));

  // Aggregation over expanded rows
  pipeline.operatorStats.push_back(makeOpStats(
      "Aggregation", "2",
      /*input_rows=*/6000, /*output_rows=*/150,
      /*input_bytes=*/480000, /*output_bytes=*/12000,
      /*add_input_wall=*/2'000'000, /*add_input_cpu=*/1'500'000,
      /*get_output_wall=*/500'000, /*get_output_cpu=*/400'000,
      /*memory=*/32768, /*peak_memory=*/65536));

  // Output
  pipeline.operatorStats.push_back(makeOpStats(
      "PartitionedOutput", "3",
      /*input_rows=*/150, /*output_rows=*/150,
      /*input_bytes=*/12000, /*output_bytes=*/12000,
      /*add_input_wall=*/200'000, /*add_input_cpu=*/150'000,
      /*get_output_wall=*/50'000, /*get_output_cpu=*/40'000,
      /*memory=*/2048, /*peak_memory=*/4096));

  taskStats.pipelineStats.push_back(std::move(pipeline));

  auto bytes = serializeNativeTaskStats(taskStats);

  pangrova::protocol::v2::NativeTaskStats proto;
  ASSERT_TRUE(proto.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));

  // Verify task-level metrics.
  EXPECT_EQ(proto.elapsed_time_ms(), 2000);
  ASSERT_EQ(proto.operator_stats_size(), 4);

  // Verify GroupIdNode operator stats are captured correctly.
  const auto& groupIdOp = proto.operator_stats(1);
  EXPECT_EQ(groupIdOp.operator_type(), "GroupId");
  EXPECT_EQ(groupIdOp.plan_node_id(), "1");
  EXPECT_EQ(groupIdOp.input_rows(), 2000);
  EXPECT_EQ(groupIdOp.output_rows(), 6000);
  EXPECT_EQ(groupIdOp.input_bytes(), 160000);
  EXPECT_EQ(groupIdOp.output_bytes(), 480000);
  // wall_nanos = addInputTiming.wallNanos + getOutputTiming.wallNanos
  EXPECT_EQ(groupIdOp.wall_nanos(), 3'000'000 + 1'500'000);
  // cpu_nanos = addInputTiming.cpuNanos + getOutputTiming.cpuNanos
  EXPECT_EQ(groupIdOp.cpu_nanos(), 2'500'000 + 1'200'000);
  EXPECT_EQ(groupIdOp.memory_bytes(), 16384);
  EXPECT_EQ(groupIdOp.peak_memory_bytes(), 32768);
  EXPECT_EQ(groupIdOp.spill_bytes(), 0);
}

// Verifies that GroupIdNode in a multi-pipeline task has its stats captured
// alongside other operators (e.g., separate scan pipeline feeding GroupIdNode).
TEST_F(GroupIdNodeStatsTest, GroupIdNodeInMultiPipelineTask) {
  facebook::velox::exec::TaskStats taskStats;
  taskStats.executionStartTimeMs = 0;
  taskStats.endTimeMs = 4000;

  // Pipeline 0: TableScan (feeds into GroupIdNode pipeline)
  facebook::velox::exec::PipelineStats pipeline0(true, false);
  pipeline0.operatorStats.push_back(makeOpStats(
      "TableScan", "scan_node",
      10000, 10000, 800000, 800000,
      3'000'000, 2'000'000, 1'000'000, 800'000,
      16384, 32768));
  taskStats.pipelineStats.push_back(std::move(pipeline0));

  // Pipeline 1: GroupIdNode → Aggregation → Output
  // 5 grouping sets: outputRows = 10000 × 5 = 50000
  facebook::velox::exec::PipelineStats pipeline1(false, true);
  pipeline1.operatorStats.push_back(makeOpStats(
      "GroupId", "gid_node",
      10000, 50000, 800000, 4000000,
      5'000'000, 4'000'000, 3'000'000, 2'500'000,
      65536, 131072));
  pipeline1.operatorStats.push_back(makeOpStats(
      "Aggregation", "agg_node",
      50000, 500, 4000000, 40000,
      8'000'000, 6'000'000, 1'000'000, 800'000,
      131072, 262144));
  pipeline1.operatorStats.push_back(makeOpStats(
      "PartitionedOutput", "output_node",
      500, 500, 40000, 40000,
      300'000, 200'000, 100'000, 80'000,
      4096, 8192));
  taskStats.pipelineStats.push_back(std::move(pipeline1));

  auto bytes = serializeNativeTaskStats(taskStats);

  pangrova::protocol::v2::NativeTaskStats proto;
  ASSERT_TRUE(proto.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));

  // All 4 operators across 2 pipelines should be captured.
  ASSERT_EQ(proto.operator_stats_size(), 4);

  // Find the GroupIdNode operator in the serialized stats.
  bool found_group_id = false;
  for (int i = 0; i < proto.operator_stats_size(); ++i) {
    const auto& op = proto.operator_stats(i);
    if (op.operator_type() == "GroupId") {
      found_group_id = true;
      EXPECT_EQ(op.plan_node_id(), "gid_node");
      EXPECT_EQ(op.input_rows(), 10000);
      EXPECT_EQ(op.output_rows(), 50000);
      EXPECT_EQ(op.input_bytes(), 800000);
      EXPECT_EQ(op.output_bytes(), 4000000);
      // Timing: addInput + getOutput
      EXPECT_EQ(op.wall_nanos(), 5'000'000 + 3'000'000);
      EXPECT_EQ(op.cpu_nanos(), 4'000'000 + 2'500'000);
      EXPECT_EQ(op.memory_bytes(), 65536);
      EXPECT_EQ(op.peak_memory_bytes(), 131072);
      break;
    }
  }
  EXPECT_TRUE(found_group_id)
      << "GroupId operator stats not found in serialized TaskStats";
}

// Verifies that GroupIdNode stats contribute to task-level aggregates.
TEST_F(GroupIdNodeStatsTest, GroupIdNodeStatsContributeToAggregates) {
  facebook::velox::exec::TaskStats taskStats;
  taskStats.executionStartTimeMs = 100;
  taskStats.endTimeMs = 200;

  facebook::velox::exec::PipelineStats pipeline(true, true);
  pipeline.operatorStats.push_back(makeOpStats(
      "GroupId", "gid_0",
      1000, 3000, 80000, 240000,
      6'000'000, 4'000'000, 2'000'000, 1'500'000,
      8192, 16384));
  taskStats.pipelineStats.push_back(std::move(pipeline));

  auto bytes = serializeNativeTaskStats(taskStats);

  pangrova::protocol::v2::NativeTaskStats proto;
  ASSERT_TRUE(proto.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));

  // Task-level aggregates should include GroupIdNode's contribution.
  EXPECT_EQ(proto.rows_read(), 1000);
  EXPECT_EQ(proto.bytes_read(), 80000);
  // cpu_time_ms = total cpu nanos / 1'000'000
  EXPECT_EQ(proto.cpu_time_ms(), (4'000'000 + 1'500'000) / 1'000'000);
  EXPECT_EQ(proto.memory_bytes(), 8192);
  EXPECT_EQ(proto.peak_memory_bytes(), 16384);
}

#endif  // PANGROVA_HAS_VELOX_TASK_STATS

}  // namespace
}  // namespace pangrova::jni::test
