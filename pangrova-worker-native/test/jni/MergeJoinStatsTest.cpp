// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

// Verifies that MergeJoinNode operator statistics are correctly captured
// and serialized through the TaskStats → PipelineStats → OperatorStats
// hierarchy. MergeJoinNode uses Velox's built-in OperatorStats framework
// (inputRows, outputRows, addInputTiming, getOutputTiming) which is
// automatically collected by exec::Task::taskStats() and serialized by
// serializeNativeTaskStats().

#include <gtest/gtest.h>

#include "jni/command/StatusSerializer.h"

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

class MergeJoinStatsTest : public ::testing::Test {};

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

// Verifies that MergeJoinNode statistics propagate through the
// TaskStats → PipelineStats → OperatorStats → proto serialization path.
TEST_F(MergeJoinStatsTest, MergeJoinStatsPropagateToProto) {
  facebook::velox::exec::TaskStats taskStats;
  taskStats.executionStartTimeMs = 1000;
  taskStats.endTimeMs = 3000;

  // Simulate a merge join pipeline: TableScan → MergeJoin → PartitionedOutput
  facebook::velox::exec::PipelineStats pipeline(true, true);

  // Left input scan
  pipeline.operatorStats.push_back(makeOpStats(
      "TableScan", "0",
      /*input_rows=*/5000, /*output_rows=*/5000,
      /*input_bytes=*/400000, /*output_bytes=*/400000,
      /*add_input_wall=*/2'000'000, /*add_input_cpu=*/1'500'000,
      /*get_output_wall=*/500'000, /*get_output_cpu=*/400'000,
      /*memory=*/8192, /*peak_memory=*/16384));

  // MergeJoin operator
  pipeline.operatorStats.push_back(makeOpStats(
      "MergeJoin", "1",
      /*input_rows=*/5000, /*output_rows=*/3500,
      /*input_bytes=*/400000, /*output_bytes=*/280000,
      /*add_input_wall=*/4'000'000, /*add_input_cpu=*/3'000'000,
      /*get_output_wall=*/1'000'000, /*get_output_cpu=*/800'000,
      /*memory=*/4096, /*peak_memory=*/8192));

  // Output
  pipeline.operatorStats.push_back(makeOpStats(
      "PartitionedOutput", "2",
      /*input_rows=*/3500, /*output_rows=*/3500,
      /*input_bytes=*/280000, /*output_bytes=*/280000,
      /*add_input_wall=*/500'000, /*add_input_cpu=*/400'000,
      /*get_output_wall=*/100'000, /*get_output_cpu=*/80'000,
      /*memory=*/2048, /*peak_memory=*/4096));

  taskStats.pipelineStats.push_back(std::move(pipeline));

  auto bytes = serializeNativeTaskStats(taskStats);

  pangrova::protocol::v2::NativeTaskStats proto;
  ASSERT_TRUE(proto.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));

  // Verify task-level metrics.
  EXPECT_EQ(proto.elapsed_time_ms(), 2000);
  ASSERT_EQ(proto.operator_stats_size(), 3);

  // Verify MergeJoin operator stats are captured correctly.
  const auto& mergeJoinOp = proto.operator_stats(1);
  EXPECT_EQ(mergeJoinOp.operator_type(), "MergeJoin");
  EXPECT_EQ(mergeJoinOp.plan_node_id(), "1");
  EXPECT_EQ(mergeJoinOp.input_rows(), 5000);
  EXPECT_EQ(mergeJoinOp.output_rows(), 3500);
  EXPECT_EQ(mergeJoinOp.input_bytes(), 400000);
  EXPECT_EQ(mergeJoinOp.output_bytes(), 280000);
  // wall_nanos = addInputTiming.wallNanos + getOutputTiming.wallNanos + finishTiming.wallNanos
  EXPECT_EQ(mergeJoinOp.wall_nanos(), 4'000'000 + 1'000'000);
  // cpu_nanos = addInputTiming.cpuNanos + getOutputTiming.cpuNanos + finishTiming.cpuNanos
  EXPECT_EQ(mergeJoinOp.cpu_nanos(), 3'000'000 + 800'000);
  EXPECT_EQ(mergeJoinOp.memory_bytes(), 4096);
  EXPECT_EQ(mergeJoinOp.peak_memory_bytes(), 8192);
  EXPECT_EQ(mergeJoinOp.spill_bytes(), 0);
}

// Verifies that MergeJoin in a multi-pipeline task (separate build/probe
// pipelines) has its stats captured alongside other operators.
TEST_F(MergeJoinStatsTest, MergeJoinInMultiPipelineTask) {
  facebook::velox::exec::TaskStats taskStats;
  taskStats.executionStartTimeMs = 0;
  taskStats.endTimeMs = 5000;

  // Pipeline 0: Left side scan (feeds into MergeJoin)
  facebook::velox::exec::PipelineStats pipeline0(true, false);
  pipeline0.operatorStats.push_back(makeOpStats(
      "TableScan", "left_scan",
      10000, 10000, 800000, 800000,
      3'000'000, 2'000'000, 1'000'000, 800'000,
      16384, 32768));
  taskStats.pipelineStats.push_back(std::move(pipeline0));

  // Pipeline 1: Right side scan (feeds into MergeJoin)
  facebook::velox::exec::PipelineStats pipeline1(true, false);
  pipeline1.operatorStats.push_back(makeOpStats(
      "TableScan", "right_scan",
      8000, 8000, 640000, 640000,
      2'500'000, 1'800'000, 800'000, 600'000,
      12288, 24576));
  taskStats.pipelineStats.push_back(std::move(pipeline1));

  // Pipeline 2: MergeJoin → Output
  facebook::velox::exec::PipelineStats pipeline2(false, true);
  pipeline2.operatorStats.push_back(makeOpStats(
      "MergeJoin", "merge_node",
      18000, 7200, 1440000, 576000,
      6'000'000, 4'500'000, 2'000'000, 1'500'000,
      32768, 65536));
  pipeline2.operatorStats.push_back(makeOpStats(
      "PartitionedOutput", "output_node",
      7200, 7200, 576000, 576000,
      800'000, 600'000, 200'000, 150'000,
      4096, 8192));
  taskStats.pipelineStats.push_back(std::move(pipeline2));

  auto bytes = serializeNativeTaskStats(taskStats);

  pangrova::protocol::v2::NativeTaskStats proto;
  ASSERT_TRUE(proto.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));

  // All 4 operators across 3 pipelines should be captured.
  ASSERT_EQ(proto.operator_stats_size(), 4);

  // Find the MergeJoin operator in the serialized stats.
  bool found_merge_join = false;
  for (int i = 0; i < proto.operator_stats_size(); ++i) {
    const auto& op = proto.operator_stats(i);
    if (op.operator_type() == "MergeJoin") {
      found_merge_join = true;
      EXPECT_EQ(op.plan_node_id(), "merge_node");
      EXPECT_EQ(op.input_rows(), 18000);
      EXPECT_EQ(op.output_rows(), 7200);
      EXPECT_EQ(op.input_bytes(), 1440000);
      EXPECT_EQ(op.output_bytes(), 576000);
      // Timing: addInput + getOutput (no finish timing set)
      EXPECT_EQ(op.wall_nanos(), 6'000'000 + 2'000'000);
      EXPECT_EQ(op.cpu_nanos(), 4'500'000 + 1'500'000);
      EXPECT_EQ(op.memory_bytes(), 32768);
      EXPECT_EQ(op.peak_memory_bytes(), 65536);
      break;
    }
  }
  EXPECT_TRUE(found_merge_join)
      << "MergeJoin operator stats not found in serialized TaskStats";
}

// Verifies that MergeJoin stats contribute to task-level aggregates.
TEST_F(MergeJoinStatsTest, MergeJoinStatsContributeToAggregates) {
  facebook::velox::exec::TaskStats taskStats;
  taskStats.executionStartTimeMs = 100;
  taskStats.endTimeMs = 200;

  facebook::velox::exec::PipelineStats pipeline(true, true);
  pipeline.operatorStats.push_back(makeOpStats(
      "MergeJoin", "mj_0",
      1000, 500, 80000, 40000,
      10'000'000, 7'000'000, 3'000'000, 2'000'000,
      2048, 4096));
  taskStats.pipelineStats.push_back(std::move(pipeline));

  auto bytes = serializeNativeTaskStats(taskStats);

  pangrova::protocol::v2::NativeTaskStats proto;
  ASSERT_TRUE(proto.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));

  // Task-level aggregates should include MergeJoin's contribution.
  EXPECT_EQ(proto.rows_read(), 1000);
  EXPECT_EQ(proto.bytes_read(), 80000);
  // cpu_time_ms = total cpu nanos / 1'000'000
  EXPECT_EQ(proto.cpu_time_ms(), (7'000'000 + 2'000'000) / 1'000'000);
  EXPECT_EQ(proto.memory_bytes(), 2048);
  EXPECT_EQ(proto.peak_memory_bytes(), 4096);
}

#endif  // PANGROVA_HAS_VELOX_TASK_STATS

}  // namespace
}  // namespace pangrova::jni::test
