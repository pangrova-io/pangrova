// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

// Verifies that TableFunctionNode (GenerateSeries) operator statistics are
// correctly captured and serialized through the TaskStats → PipelineStats →
// OperatorStats hierarchy. GenerateSeriesOperator extends Velox's Operator
// base class which automatically tracks OperatorStats (inputRows, outputRows,
// addInputTiming, getOutputTiming). These are collected by
// exec::Task::taskStats() and serialized by serializeNativeTaskStats().

#include <gtest/gtest.h>

#include "bridge/command/StatusSerializer.h"

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

class TableFunctionStatsTest : public ::testing::Test {};

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

// Verifies that GenerateSeries (TableFunctionNode) statistics propagate
// through the TaskStats → PipelineStats → OperatorStats → proto path.
TEST_F(TableFunctionStatsTest, GenerateSeriesStatsPropagateToProto) {
  facebook::velox::exec::TaskStats taskStats;
  taskStats.executionStartTimeMs = 500;
  taskStats.endTimeMs = 1500;

  // Simulate a table function pipeline: GenerateSeries → PartitionedOutput
  facebook::velox::exec::PipelineStats pipeline(true, true);

  // GenerateSeries source operator (no input rows — it's a source)
  pipeline.operatorStats.push_back(makeOpStats(
      "GenerateSeries", "0",
      /*input_rows=*/0, /*output_rows=*/1000,
      /*input_bytes=*/0, /*output_bytes=*/8000,
      /*add_input_wall=*/0, /*add_input_cpu=*/0,
      /*get_output_wall=*/2'000'000, /*get_output_cpu=*/1'500'000,
      /*memory=*/4096, /*peak_memory=*/8192));

  // Output
  pipeline.operatorStats.push_back(makeOpStats(
      "PartitionedOutput", "1",
      /*input_rows=*/1000, /*output_rows=*/1000,
      /*input_bytes=*/8000, /*output_bytes=*/8000,
      /*add_input_wall=*/500'000, /*add_input_cpu=*/400'000,
      /*get_output_wall=*/100'000, /*get_output_cpu=*/80'000,
      /*memory=*/2048, /*peak_memory=*/4096));

  taskStats.pipelineStats.push_back(std::move(pipeline));

  auto bytes = serializeNativeTaskStats(taskStats);

  pangrova::protocol::v2::NativeTaskStats proto;
  ASSERT_TRUE(proto.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));

  // Verify task-level metrics.
  EXPECT_EQ(proto.elapsed_time_ms(), 1000);
  ASSERT_EQ(proto.operator_stats_size(), 2);

  // Verify GenerateSeries operator stats are captured correctly.
  const auto& genSeriesOp = proto.operator_stats(0);
  EXPECT_EQ(genSeriesOp.operator_type(), "GenerateSeries");
  EXPECT_EQ(genSeriesOp.plan_node_id(), "0");
  EXPECT_EQ(genSeriesOp.input_rows(), 0);
  EXPECT_EQ(genSeriesOp.output_rows(), 1000);
  EXPECT_EQ(genSeriesOp.input_bytes(), 0);
  EXPECT_EQ(genSeriesOp.output_bytes(), 8000);
  // Source operator: only getOutputTiming contributes (no addInput calls).
  EXPECT_EQ(genSeriesOp.wall_nanos(), 2'000'000);
  EXPECT_EQ(genSeriesOp.cpu_nanos(), 1'500'000);
  EXPECT_EQ(genSeriesOp.memory_bytes(), 4096);
  EXPECT_EQ(genSeriesOp.peak_memory_bytes(), 8192);
  EXPECT_EQ(genSeriesOp.spill_bytes(), 0);
}

// Verifies that GenerateSeries in a lateral join pipeline (with input rows)
// has its stats captured alongside other operators.
TEST_F(TableFunctionStatsTest, TableFunctionWithLateralJoinStats) {
  facebook::velox::exec::TaskStats taskStats;
  taskStats.executionStartTimeMs = 0;
  taskStats.endTimeMs = 3000;

  // Pipeline 0: TableScan (input side of lateral join)
  facebook::velox::exec::PipelineStats pipeline0(true, false);
  pipeline0.operatorStats.push_back(makeOpStats(
      "TableScan", "scan_0",
      5000, 5000, 400000, 400000,
      2'000'000, 1'500'000, 800'000, 600'000,
      8192, 16384));
  taskStats.pipelineStats.push_back(std::move(pipeline0));

  // Pipeline 1: GenerateSeries (table function) → Output
  // In lateral join pattern, the table function receives input rows and
  // produces output_rows = input_rows × function_output_per_row.
  facebook::velox::exec::PipelineStats pipeline1(false, true);
  pipeline1.operatorStats.push_back(makeOpStats(
      "GenerateSeries", "gen_series_node",
      5000, 50000, 400000, 400000,
      3'000'000, 2'200'000, 5'000'000, 4'000'000,
      16384, 32768));
  pipeline1.operatorStats.push_back(makeOpStats(
      "PartitionedOutput", "output_node",
      50000, 50000, 400000, 400000,
      1'000'000, 800'000, 200'000, 150'000,
      4096, 8192));
  taskStats.pipelineStats.push_back(std::move(pipeline1));

  auto bytes = serializeNativeTaskStats(taskStats);

  pangrova::protocol::v2::NativeTaskStats proto;
  ASSERT_TRUE(proto.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));

  // All 3 operators across 2 pipelines should be captured.
  ASSERT_EQ(proto.operator_stats_size(), 3);

  // Find the GenerateSeries operator in the serialized stats.
  bool found_gen_series = false;
  for (int i = 0; i < proto.operator_stats_size(); ++i) {
    const auto& op = proto.operator_stats(i);
    if (op.operator_type() == "GenerateSeries") {
      found_gen_series = true;
      EXPECT_EQ(op.plan_node_id(), "gen_series_node");
      EXPECT_EQ(op.input_rows(), 5000);
      EXPECT_EQ(op.output_rows(), 50000);
      EXPECT_EQ(op.input_bytes(), 400000);
      EXPECT_EQ(op.output_bytes(), 400000);
      // Timing: addInput + getOutput (no finish timing set)
      EXPECT_EQ(op.wall_nanos(), 3'000'000 + 5'000'000);
      EXPECT_EQ(op.cpu_nanos(), 2'200'000 + 4'000'000);
      EXPECT_EQ(op.memory_bytes(), 16384);
      EXPECT_EQ(op.peak_memory_bytes(), 32768);
      break;
    }
  }
  EXPECT_TRUE(found_gen_series)
      << "GenerateSeries operator stats not found in serialized TaskStats";
}

// Verifies that GenerateSeries stats contribute to task-level aggregates.
TEST_F(TableFunctionStatsTest, GenerateSeriesStatsContributeToAggregates) {
  facebook::velox::exec::TaskStats taskStats;
  taskStats.executionStartTimeMs = 0;
  taskStats.endTimeMs = 100;

  facebook::velox::exec::PipelineStats pipeline(true, true);
  pipeline.operatorStats.push_back(makeOpStats(
      "GenerateSeries", "tf_0",
      0, 10000, 0, 80000,
      0, 0, 8'000'000, 6'000'000,
      32768, 65536));
  taskStats.pipelineStats.push_back(std::move(pipeline));

  auto bytes = serializeNativeTaskStats(taskStats);

  pangrova::protocol::v2::NativeTaskStats proto;
  ASSERT_TRUE(proto.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));

  // Task-level aggregates should include GenerateSeries contribution.
  EXPECT_EQ(proto.rows_read(), 0);  // Source operator: no input rows
  EXPECT_EQ(proto.bytes_read(), 0);
  // cpu_time_ms = total cpu nanos / 1'000'000
  EXPECT_EQ(proto.cpu_time_ms(), 6'000'000 / 1'000'000);
  EXPECT_EQ(proto.memory_bytes(), 32768);
  EXPECT_EQ(proto.peak_memory_bytes(), 65536);
}

#endif  // PANGROVA_HAS_VELOX_TASK_STATS

}  // namespace
}  // namespace pangrova::jni::test
