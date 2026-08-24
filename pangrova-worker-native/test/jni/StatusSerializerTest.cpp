// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

// Tests for StatusSerializer: validates serialization of native TaskStats
// to proto wire format, including per-operator metrics extraction.

#include <gtest/gtest.h>

#include "jni/command/StatusSerializer.h"

#include "pangrova/native/protocol/v1/task.pb.h"

// Velox TaskStats for constructing test data.
#if __has_include("velox/exec/TaskStats.h")
#include "velox/exec/TaskStats.h"
#define PANGROVA_HAS_VELOX_TASK_STATS 1
#else
#define PANGROVA_HAS_VELOX_TASK_STATS 0
#endif

namespace pangrova::jni::test {
namespace {

#if PANGROVA_HAS_VELOX_TASK_STATS

class StatusSerializerTest : public ::testing::Test {};

// Helper: create a Velox OperatorStats with specified values.
facebook::velox::exec::OperatorStats makeOpStats(
    const std::string& op_type,
    const std::string& plan_node_id,
    uint64_t input_rows,
    uint64_t output_rows,
    uint64_t input_bytes,
    uint64_t output_bytes,
    uint64_t wall_nanos,
    uint64_t cpu_nanos,
    uint64_t memory_bytes,
    uint64_t peak_memory_bytes,
    uint64_t spill_bytes) {
  facebook::velox::exec::OperatorStats stats;
  stats.operatorType = op_type;
  stats.planNodeId = plan_node_id;
  stats.inputPositions = input_rows;
  stats.outputPositions = output_rows;
  stats.inputBytes = input_bytes;
  stats.outputBytes = output_bytes;
  stats.addInputTiming.wallNanos = wall_nanos;
  stats.addInputTiming.cpuNanos = cpu_nanos;
  stats.memoryStats.userMemoryReservation = memory_bytes;
  stats.memoryStats.peakUserMemoryReservation = peak_memory_bytes;
  stats.spilledBytes = spill_bytes;
  return stats;
}

TEST_F(StatusSerializerTest, SerializeEmptyTaskStats) {
  facebook::velox::exec::TaskStats taskStats;
  auto bytes = serializeNativeTaskStats(taskStats);

  pangrova::native::protocol::v1::TaskStats proto;
  ASSERT_TRUE(proto.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));

  EXPECT_EQ(proto.elapsed_time_ms(), 0);
  EXPECT_EQ(proto.rows_read(), 0);
  EXPECT_EQ(proto.bytes_read(), 0);
  EXPECT_EQ(proto.cpu_time_ms(), 0);
  EXPECT_EQ(proto.memory_bytes(), 0);
  EXPECT_EQ(proto.peak_memory_bytes(), 0);
  EXPECT_EQ(proto.spill_bytes(), 0);
  EXPECT_EQ(proto.operator_stats_size(), 0);
}

TEST_F(StatusSerializerTest, SerializeSingleOperator) {
  facebook::velox::exec::TaskStats taskStats;
  taskStats.executionStartTimeMs = 1000;
  taskStats.endTimeMs = 2500;

  facebook::velox::exec::PipelineStats pipeline(true, true);
  pipeline.operatorStats.push_back(makeOpStats(
      "TableScan", "0",
      /*input_rows=*/1000, /*output_rows=*/800,
      /*input_bytes=*/65536, /*output_bytes=*/51200,
      /*wall_nanos=*/5'000'000, /*cpu_nanos=*/3'000'000,
      /*memory_bytes=*/1024, /*peak_memory_bytes=*/2048,
      /*spill_bytes=*/0));
  taskStats.pipelineStats.push_back(std::move(pipeline));

  auto bytes = serializeNativeTaskStats(taskStats);

  pangrova::native::protocol::v1::TaskStats proto;
  ASSERT_TRUE(proto.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));

  EXPECT_EQ(proto.elapsed_time_ms(), 1500);
  EXPECT_EQ(proto.rows_read(), 1000);
  EXPECT_EQ(proto.bytes_read(), 65536);
  EXPECT_EQ(proto.cpu_time_ms(), 3);  // 3'000'000 ns = 3 ms
  EXPECT_EQ(proto.memory_bytes(), 1024);
  EXPECT_EQ(proto.peak_memory_bytes(), 2048);
  EXPECT_EQ(proto.spill_bytes(), 0);

  ASSERT_EQ(proto.operator_stats_size(), 1);
  const auto& op = proto.operator_stats(0);
  EXPECT_EQ(op.operator_type(), "TableScan");
  EXPECT_EQ(op.plan_node_id(), "0");
  EXPECT_EQ(op.input_rows(), 1000);
  EXPECT_EQ(op.output_rows(), 800);
  EXPECT_EQ(op.input_bytes(), 65536);
  EXPECT_EQ(op.output_bytes(), 51200);
  EXPECT_EQ(op.wall_nanos(), 5'000'000);
  EXPECT_EQ(op.cpu_nanos(), 3'000'000);
  EXPECT_EQ(op.memory_bytes(), 1024);
  EXPECT_EQ(op.peak_memory_bytes(), 2048);
  EXPECT_EQ(op.spill_bytes(), 0);
}

TEST_F(StatusSerializerTest, SerializeMultiplePipelinesAndOperators) {
  facebook::velox::exec::TaskStats taskStats;
  taskStats.executionStartTimeMs = 100;
  taskStats.endTimeMs = 600;

  // Pipeline 0: TableScan → Filter
  facebook::velox::exec::PipelineStats pipeline0(true, false);
  pipeline0.operatorStats.push_back(makeOpStats(
      "TableScan", "0",
      100, 100, 8000, 8000,
      1'000'000, 500'000, 512, 1024, 0));
  pipeline0.operatorStats.push_back(makeOpStats(
      "FilterProject", "1",
      100, 50, 8000, 4000,
      2'000'000, 1'000'000, 256, 512, 0));
  taskStats.pipelineStats.push_back(std::move(pipeline0));

  // Pipeline 1: HashAggregation (with spill)
  facebook::velox::exec::PipelineStats pipeline1(false, true);
  pipeline1.operatorStats.push_back(makeOpStats(
      "HashAggregation", "2",
      50, 10, 4000, 800,
      3'000'000, 2'000'000, 4096, 8192, 16384));
  taskStats.pipelineStats.push_back(std::move(pipeline1));

  auto bytes = serializeNativeTaskStats(taskStats);

  pangrova::native::protocol::v1::TaskStats proto;
  ASSERT_TRUE(proto.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));

  EXPECT_EQ(proto.elapsed_time_ms(), 500);
  ASSERT_EQ(proto.operator_stats_size(), 3);

  // Verify aggregated totals.
  EXPECT_EQ(proto.rows_read(), 250);   // 100 + 100 + 50
  EXPECT_EQ(proto.bytes_read(), 20000); // 8000 + 8000 + 4000
  EXPECT_EQ(proto.cpu_time_ms(), 3);    // (500000 + 1000000 + 2000000) / 1M = 3
  EXPECT_EQ(proto.peak_memory_bytes(), 8192);  // max of all operators
  EXPECT_EQ(proto.spill_bytes(), 16384);

  // Verify individual operator stats.
  EXPECT_EQ(proto.operator_stats(0).operator_type(), "TableScan");
  EXPECT_EQ(proto.operator_stats(1).operator_type(), "FilterProject");
  EXPECT_EQ(proto.operator_stats(2).operator_type(), "HashAggregation");
  EXPECT_EQ(proto.operator_stats(2).spill_bytes(), 16384);
}

TEST_F(StatusSerializerTest, SerializeRoundTrip) {
  // Verify that serialized proto can be deserialized back correctly.
  facebook::velox::exec::TaskStats taskStats;
  taskStats.executionStartTimeMs = 0;
  taskStats.endTimeMs = 1000;

  facebook::velox::exec::PipelineStats pipeline(true, true);
  pipeline.operatorStats.push_back(makeOpStats(
      "OrderBy", "5",
      500, 500, 40000, 40000,
      10'000'000, 8'000'000, 2048, 4096, 1024));
  taskStats.pipelineStats.push_back(std::move(pipeline));

  auto bytes = serializeNativeTaskStats(taskStats);

  // First deserialization.
  pangrova::native::protocol::v1::TaskStats proto1;
  ASSERT_TRUE(proto1.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));

  // Re-serialize and deserialize again.
  std::vector<uint8_t> bytes2(proto1.ByteSizeLong());
  proto1.SerializeToArray(bytes2.data(), static_cast<int>(bytes2.size()));

  pangrova::native::protocol::v1::TaskStats proto2;
  ASSERT_TRUE(proto2.ParseFromArray(bytes2.data(), static_cast<int>(bytes2.size())));

  // Verify round-trip equivalence.
  EXPECT_EQ(proto1.elapsed_time_ms(), proto2.elapsed_time_ms());
  EXPECT_EQ(proto1.rows_read(), proto2.rows_read());
  EXPECT_EQ(proto1.spill_bytes(), proto2.spill_bytes());
  ASSERT_EQ(proto1.operator_stats_size(), proto2.operator_stats_size());
  EXPECT_EQ(proto1.operator_stats(0).operator_type(),
            proto2.operator_stats(0).operator_type());
  EXPECT_EQ(proto1.operator_stats(0).spill_bytes(),
            proto2.operator_stats(0).spill_bytes());
}

TEST_F(StatusSerializerTest, AllMetricsNonNegative) {
  facebook::velox::exec::TaskStats taskStats;
  taskStats.executionStartTimeMs = 10;
  taskStats.endTimeMs = 20;

  facebook::velox::exec::PipelineStats pipeline(true, true);
  pipeline.operatorStats.push_back(makeOpStats(
      "TableScan", "0", 0, 0, 0, 0, 0, 0, 0, 0, 0));
  taskStats.pipelineStats.push_back(std::move(pipeline));

  auto bytes = serializeNativeTaskStats(taskStats);

  pangrova::native::protocol::v1::TaskStats proto;
  ASSERT_TRUE(proto.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));

  EXPECT_GE(proto.elapsed_time_ms(), 0);
  EXPECT_GE(proto.rows_read(), 0);
  EXPECT_GE(proto.bytes_read(), 0);
  EXPECT_GE(proto.cpu_time_ms(), 0);
  EXPECT_GE(proto.memory_bytes(), 0);
  EXPECT_GE(proto.peak_memory_bytes(), 0);
  EXPECT_GE(proto.spill_bytes(), 0);

  for (int i = 0; i < proto.operator_stats_size(); ++i) {
    const auto& op = proto.operator_stats(i);
    EXPECT_GE(op.input_rows(), 0);
    EXPECT_GE(op.output_rows(), 0);
    EXPECT_GE(op.input_bytes(), 0);
    EXPECT_GE(op.output_bytes(), 0);
    EXPECT_GE(op.wall_nanos(), 0);
    EXPECT_GE(op.cpu_nanos(), 0);
    EXPECT_GE(op.memory_bytes(), 0);
    EXPECT_GE(op.peak_memory_bytes(), 0);
    EXPECT_GE(op.spill_bytes(), 0);
  }
}

#endif  // PANGROVA_HAS_VELOX_TASK_STATS

}  // namespace
}  // namespace pangrova::jni::test
