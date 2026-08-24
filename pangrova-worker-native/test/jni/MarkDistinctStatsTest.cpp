// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

// Verifies that MarkDistinctNode operator statistics are correctly captured
// and serialized through the TaskStats → PipelineStats → OperatorStats
// hierarchy. MarkDistinctNode uses Velox's built-in OperatorStats framework
// (inputRows, outputRows, addInputTiming, getOutputTiming) which is
// automatically collected by exec::Task::taskStats() and serialized by
// serializeNativeTaskStats().

#include <gtest/gtest.h>

#include "jni/command/StatusSerializer.h"

#include "pangrova/native/protocol/v1/task.pb.h"

#if __has_include("velox/exec/TaskStats.h")
#include "velox/exec/TaskStats.h"
#define PANGROVA_HAS_VELOX_TASK_STATS 1
#else
#define PANGROVA_HAS_VELOX_TASK_STATS 0
#endif

namespace pangrova::jni::test {
namespace {

#if PANGROVA_HAS_VELOX_TASK_STATS

class MarkDistinctStatsTest : public ::testing::Test {};

// Helper: create a Velox OperatorStats with specified values.
facebook::velox::exec::OperatorStats makeMarkDistinctOpStats(
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

// Verifies that MarkDistinctNode statistics propagate through the
// TaskStats → PipelineStats → OperatorStats → proto serialization path.
TEST_F(MarkDistinctStatsTest, MarkDistinctStatsPropagateToProto) {
  facebook::velox::exec::TaskStats taskStats;
  taskStats.executionStartTimeMs = 200;
  taskStats.endTimeMs = 1200;

  // Simulate a multi-distinct aggregation pipeline:
  // TableScan → MarkDistinct → Aggregation → PartitionedOutput
  facebook::velox::exec::PipelineStats pipeline(true, true);

  // Input scan
  pipeline.operatorStats.push_back(makeMarkDistinctOpStats(
      "TableScan", "0",
      /*input_rows=*/10000, /*output_rows=*/10000,
      /*input_bytes=*/800000, /*output_bytes=*/800000,
      /*add_input_wall=*/3'000'000, /*add_input_cpu=*/2'000'000,
      /*get_output_wall=*/1'000'000, /*get_output_cpu=*/800'000,
      /*memory=*/16384, /*peak_memory=*/32768));

  // MarkDistinct operator — passes through all rows, adds boolean marker column
  pipeline.operatorStats.push_back(makeMarkDistinctOpStats(
      "MarkDistinct", "1",
      /*input_rows=*/10000, /*output_rows=*/10000,
      /*input_bytes=*/800000, /*output_bytes=*/810000,
      /*add_input_wall=*/2'000'000, /*add_input_cpu=*/1'500'000,
      /*get_output_wall=*/800'000, /*get_output_cpu=*/600'000,
      /*memory=*/65536, /*peak_memory=*/131072));

  // Aggregation
  pipeline.operatorStats.push_back(makeMarkDistinctOpStats(
      "Aggregation", "2",
      /*input_rows=*/10000, /*output_rows=*/500,
      /*input_bytes=*/810000, /*output_bytes=*/40000,
      /*add_input_wall=*/4'000'000, /*add_input_cpu=*/3'000'000,
      /*get_output_wall=*/500'000, /*get_output_cpu=*/400'000,
      /*memory=*/32768, /*peak_memory=*/65536));

  // Output
  pipeline.operatorStats.push_back(makeMarkDistinctOpStats(
      "PartitionedOutput", "3",
      /*input_rows=*/500, /*output_rows=*/500,
      /*input_bytes=*/40000, /*output_bytes=*/40000,
      /*add_input_wall=*/200'000, /*add_input_cpu=*/150'000,
      /*get_output_wall=*/50'000, /*get_output_cpu=*/40'000,
      /*memory=*/2048, /*peak_memory=*/4096));

  taskStats.pipelineStats.push_back(std::move(pipeline));

  auto bytes = serializeNativeTaskStats(taskStats);

  pangrova::native::protocol::v1::TaskStats proto;
  ASSERT_TRUE(proto.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));

  // Verify task-level metrics.
  EXPECT_EQ(proto.elapsed_time_ms(), 1000);
  ASSERT_EQ(proto.operator_stats_size(), 4);

  // Verify MarkDistinct operator stats are captured correctly.
  const auto& markDistinctOp = proto.operator_stats(1);
  EXPECT_EQ(markDistinctOp.operator_type(), "MarkDistinct");
  EXPECT_EQ(markDistinctOp.plan_node_id(), "1");
  EXPECT_EQ(markDistinctOp.input_rows(), 10000);
  EXPECT_EQ(markDistinctOp.output_rows(), 10000);
  EXPECT_EQ(markDistinctOp.input_bytes(), 800000);
  EXPECT_EQ(markDistinctOp.output_bytes(), 810000);
  // wall_nanos = addInputTiming.wallNanos + getOutputTiming.wallNanos
  EXPECT_EQ(markDistinctOp.wall_nanos(), 2'000'000 + 800'000);
  // cpu_nanos = addInputTiming.cpuNanos + getOutputTiming.cpuNanos
  EXPECT_EQ(markDistinctOp.cpu_nanos(), 1'500'000 + 600'000);
  EXPECT_EQ(markDistinctOp.memory_bytes(), 65536);
  EXPECT_EQ(markDistinctOp.peak_memory_bytes(), 131072);
  EXPECT_EQ(markDistinctOp.spill_bytes(), 0);
}

// Verifies that chained MarkDistinctNodes (one per distinct column) have
// their stats captured independently in a multi-pipeline task.
TEST_F(MarkDistinctStatsTest, ChainedMarkDistinctNodesStats) {
  facebook::velox::exec::TaskStats taskStats;
  taskStats.executionStartTimeMs = 0;
  taskStats.endTimeMs = 4000;

  // Pipeline: TableScan → MarkDistinct(col_a) → MarkDistinct(col_b) →
  //           Aggregation → PartitionedOutput
  facebook::velox::exec::PipelineStats pipeline(true, true);

  // Input scan
  pipeline.operatorStats.push_back(makeMarkDistinctOpStats(
      "TableScan", "scan_0",
      20000, 20000, 1600000, 1600000,
      4'000'000, 3'000'000, 1'500'000, 1'200'000,
      32768, 65536));

  // First MarkDistinct (for COUNT(DISTINCT col_a))
  pipeline.operatorStats.push_back(makeMarkDistinctOpStats(
      "MarkDistinct", "mark_a",
      20000, 20000, 1600000, 1620000,
      3'000'000, 2'200'000, 1'000'000, 800'000,
      131072, 262144));

  // Second MarkDistinct (for COUNT(DISTINCT col_b))
  pipeline.operatorStats.push_back(makeMarkDistinctOpStats(
      "MarkDistinct", "mark_b",
      20000, 20000, 1620000, 1640000,
      2'500'000, 1'800'000, 900'000, 700'000,
      98304, 196608));

  // Aggregation with marker-masked aggregates
  pipeline.operatorStats.push_back(makeMarkDistinctOpStats(
      "Aggregation", "agg_0",
      20000, 1000, 1640000, 80000,
      5'000'000, 4'000'000, 800'000, 600'000,
      65536, 131072));

  // Output
  pipeline.operatorStats.push_back(makeMarkDistinctOpStats(
      "PartitionedOutput", "output_0",
      1000, 1000, 80000, 80000,
      300'000, 200'000, 100'000, 80'000,
      4096, 8192));

  taskStats.pipelineStats.push_back(std::move(pipeline));

  auto bytes = serializeNativeTaskStats(taskStats);

  pangrova::native::protocol::v1::TaskStats proto;
  ASSERT_TRUE(proto.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));

  // All 5 operators should be captured.
  ASSERT_EQ(proto.operator_stats_size(), 5);

  // Find both MarkDistinct operators in the serialized stats.
  int mark_distinct_count = 0;
  for (int i = 0; i < proto.operator_stats_size(); ++i) {
    const auto& op = proto.operator_stats(i);
    if (op.operator_type() == "MarkDistinct") {
      mark_distinct_count++;
      if (op.plan_node_id() == "mark_a") {
        EXPECT_EQ(op.input_rows(), 20000);
        EXPECT_EQ(op.output_rows(), 20000);
        EXPECT_EQ(op.input_bytes(), 1600000);
        EXPECT_EQ(op.output_bytes(), 1620000);
        EXPECT_EQ(op.wall_nanos(), 3'000'000 + 1'000'000);
        EXPECT_EQ(op.cpu_nanos(), 2'200'000 + 800'000);
        EXPECT_EQ(op.memory_bytes(), 131072);
        EXPECT_EQ(op.peak_memory_bytes(), 262144);
      } else if (op.plan_node_id() == "mark_b") {
        EXPECT_EQ(op.input_rows(), 20000);
        EXPECT_EQ(op.output_rows(), 20000);
        EXPECT_EQ(op.input_bytes(), 1620000);
        EXPECT_EQ(op.output_bytes(), 1640000);
        EXPECT_EQ(op.wall_nanos(), 2'500'000 + 900'000);
        EXPECT_EQ(op.cpu_nanos(), 1'800'000 + 700'000);
        EXPECT_EQ(op.memory_bytes(), 98304);
        EXPECT_EQ(op.peak_memory_bytes(), 196608);
      }
    }
  }
  EXPECT_EQ(mark_distinct_count, 2)
      << "Expected 2 MarkDistinct operators in serialized TaskStats";
}

// Verifies that MarkDistinct stats contribute to task-level aggregates.
TEST_F(MarkDistinctStatsTest, MarkDistinctStatsContributeToAggregates) {
  facebook::velox::exec::TaskStats taskStats;
  taskStats.executionStartTimeMs = 50;
  taskStats.endTimeMs = 150;

  facebook::velox::exec::PipelineStats pipeline(true, true);
  pipeline.operatorStats.push_back(makeMarkDistinctOpStats(
      "MarkDistinct", "md_0",
      8000, 8000, 640000, 648000,
      6'000'000, 4'500'000, 2'000'000, 1'500'000,
      49152, 98304));
  taskStats.pipelineStats.push_back(std::move(pipeline));

  auto bytes = serializeNativeTaskStats(taskStats);

  pangrova::native::protocol::v1::TaskStats proto;
  ASSERT_TRUE(proto.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));

  // Task-level aggregates should include MarkDistinct's contribution.
  EXPECT_EQ(proto.rows_read(), 8000);
  EXPECT_EQ(proto.bytes_read(), 640000);
  // cpu_time_ms = total cpu nanos / 1'000'000
  EXPECT_EQ(proto.cpu_time_ms(), (4'500'000 + 1'500'000) / 1'000'000);
  EXPECT_EQ(proto.memory_bytes(), 49152);
  EXPECT_EQ(proto.peak_memory_bytes(), 98304);
}

#endif  // PANGROVA_HAS_VELOX_TASK_STATS

}  // namespace
}  // namespace pangrova::jni::test
