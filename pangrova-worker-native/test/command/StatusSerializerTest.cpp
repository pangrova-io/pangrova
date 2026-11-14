// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

// Tests for StatusSerializer: validates serialization of native TaskStats
// to proto wire format, including per-operator metrics extraction.

#include "CanonicalTestAttempt.h"
#include <gtest/gtest.h>
#include "velox/expression/FunctionRegistrySnapshot.h"

#include <openssl/evp.h>

#include <array>

#include "command/StatusSerializer.h"
#include "engine/velox/runner/TaskStatsEncoding.h"

#include "pangrova/protocol/v2/result.pb.h"
#include "pangrova/protocol/v2/runtime.pb.h"

// Velox TaskStats for constructing test data.
#if __has_include("velox/exec/TaskStats.h")
#include "velox/exec/BlockingReason.h"
#include "velox/exec/TaskStats.h"
#define PANGROVA_HAS_VELOX_TASK_STATS 1
#else
#define PANGROVA_HAS_VELOX_TASK_STATS 0
#endif

namespace pangrova::jni::test {
namespace {

std::string reportSha256(std::string_view input) {
  std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
  unsigned int size = 0;
  EXPECT_EQ(EVP_Digest(input.data(), input.size(), digest.data(), &size,
                       EVP_sha256(), nullptr), 1);
  return {reinterpret_cast<const char*>(digest.data()), size};
}

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
  auto bytes = pangrova::velox::serializeNativeTaskStats(taskStats);

  pangrova::protocol::v2::NativeTaskStats proto;
  ASSERT_TRUE(proto.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));

  EXPECT_EQ(proto.elapsed_time_ms(), 0);
  EXPECT_EQ(proto.rows_read(), 0);
  EXPECT_EQ(proto.bytes_read(), 0);
  EXPECT_EQ(proto.cpu_time_ms(), 0);
  EXPECT_EQ(proto.memory_bytes(), 0);
  EXPECT_EQ(proto.peak_memory_bytes(), 0);
  EXPECT_EQ(proto.spill_bytes(), 0);
  EXPECT_EQ(proto.operator_stats_size(), 0);
  EXPECT_EQ(proto.consumer_blocked_drivers(), 0);
}

TEST_F(StatusSerializerTest, SerializesActualPreScanIndexSplitDecisions) {
  facebook::velox::exec::TaskStats taskStats;
  auto bytes = pangrova::velox::serializeNativeTaskStats(taskStats, 8, 7);

  pangrova::protocol::v2::NativeTaskStats proto;
  ASSERT_TRUE(proto.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));
  EXPECT_EQ(proto.index_splits_evaluated(), 8);
  EXPECT_EQ(proto.index_splits_skipped(), 7);
}

TEST_F(StatusSerializerTest, LiveConsumerDriverCountIsDistinctFromCompletedWaitMetrics) {
  facebook::velox::exec::TaskStats taskStats;
  taskStats.numBlockedDrivers.emplace(
      facebook::velox::exec::BlockingReason::kWaitForConsumer, 2);
  taskStats.numBlockedDrivers.emplace(
      facebook::velox::exec::BlockingReason::kWaitForConnector, 7);
  auto bytes = pangrova::velox::serializeNativeTaskStats(taskStats);
  pangrova::protocol::v2::NativeTaskStats decoded;
  ASSERT_TRUE(decoded.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));
  EXPECT_EQ(decoded.consumer_blocked_drivers(), 2);
  EXPECT_EQ(decoded.backpressure_wait_ms(), 0);
  taskStats.numBlockedDrivers.clear();
  bytes = pangrova::velox::serializeNativeTaskStats(taskStats);
  ASSERT_TRUE(decoded.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));
  EXPECT_EQ(decoded.consumer_blocked_drivers(), 0);
}

TEST_F(StatusSerializerTest, ConsumerWaitMetricDoesNotIncludeIoOrOtherBlockingReasons) {
  facebook::velox::exec::TaskStats taskStats;
  facebook::velox::exec::PipelineStats pipeline(true, true);
  facebook::velox::exec::OperatorStats op;
  op.blockedWallNanos = 999'000'000;
  op.runtimeStats.emplace("blockedWaitForConsumerWallNanos",
      facebook::velox::RuntimeMetric(7'500'000, facebook::velox::RuntimeCounter::Unit::kNanos));
  op.runtimeStats.emplace("blockedWaitForConnectorWallNanos",
      facebook::velox::RuntimeMetric(990'000'000, facebook::velox::RuntimeCounter::Unit::kNanos));
  pipeline.operatorStats.push_back(std::move(op));
  taskStats.pipelineStats.push_back(std::move(pipeline));
  auto bytes = pangrova::velox::serializeNativeTaskStats(taskStats);
  pangrova::protocol::v2::NativeTaskStats decoded;
  ASSERT_TRUE(decoded.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));
  EXPECT_EQ(decoded.backpressure_wait_ms(), 7);
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

  auto bytes = pangrova::velox::serializeNativeTaskStats(taskStats);

  pangrova::protocol::v2::NativeTaskStats proto;
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

  auto bytes = pangrova::velox::serializeNativeTaskStats(taskStats);

  pangrova::protocol::v2::NativeTaskStats proto;
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

  auto bytes = pangrova::velox::serializeNativeTaskStats(taskStats);

  // First deserialization.
  pangrova::protocol::v2::NativeTaskStats proto1;
  ASSERT_TRUE(proto1.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));

  // Re-serialize and deserialize again.
  std::vector<uint8_t> bytes2(proto1.ByteSizeLong());
  proto1.SerializeToArray(bytes2.data(), static_cast<int>(bytes2.size()));

  pangrova::protocol::v2::NativeTaskStats proto2;
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

  auto bytes = pangrova::velox::serializeNativeTaskStats(taskStats);

  pangrova::protocol::v2::NativeTaskStats proto;
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

TEST_F(StatusSerializerTest, TaskReportPreservesExactAdmittedWorkerIncarnation) {
  executor::TaskExecutorConfig config;
  config.function_registry = facebook::velox::exec::FunctionRegistrySnapshot::capture();
  config.task_id = "incarnation-task";
  config.query_id = "incarnation-query";
  pangrova::test::bindTestAttempt(config);
  protocol::v2::ExecutionCallbackContext context;
  ASSERT_TRUE(context.ParseFromString(config.callback_context));
  auto* worker = context.mutable_attempt()->mutable_worker_execution_identity();
  worker->set_admission_domain_id("00000000-0000-0000-0000-000000000001");
  worker->set_incarnation_id("00000000-0000-0000-0000-000000000002");
  config.callback_context = context.SerializeAsString();
  executor::TaskExecutor executor(config);
  const auto bytes = serializeTaskStatus(executor);
  protocol::v2::TaskReport status;
  ASSERT_TRUE(status.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));
  EXPECT_EQ(status.attempt().SerializeAsString(), context.attempt().SerializeAsString());
}

TEST_F(StatusSerializerTest, TaskReportCarriesCanonicalArtifactWorkReport) {
  executor::TaskExecutorConfig config;
  config.function_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  config.task_id = "task-1";
  config.query_id = "job-1";
  config.stage_id = 3;
  config.partition_id = 7;
  config.plan_epoch = 11;
  config.attempt_id = 2;
  config.fencing_token = 47;
  config.control_epoch = 13;
  config.owner_id = "coordinator-a";
  config.owner_incarnation = "process-a";
  config.owner_fencing_token = 19;
  pangrova::test::bindTestAttempt(config);
  executor::TaskExecutor executor(config);

  protocol::v2::ArtifactWorkReport report;
  report.set_job_id("job-1");
  report.set_page_id("page-1");
  report.set_fencing_token(5);
  report.set_source_id("catalog.schema.items");
  report.set_source_snapshot("snapshot-1");
  report.set_staging_generation("generation-1");
  auto* output = report.add_outputs();
  output->set_object_id("object-1");
  output->set_opaque_uri("s3://bucket/object-1");
  output->set_size_bytes(10);
  output->set_object_role("GRAPH");
  output->set_source_object_id("source-1");
  output->set_scope(
      protocol::v2::ARTIFACT_OBJECT_SCOPE_SOURCE_OBJECT);
  output->set_row_count(1);
  output->mutable_checksum()->set_algorithm(
      protocol::v2::Checksum::ALGORITHM_SHA256);
  output->mutable_checksum()->set_digest(std::string(32, 'a'));
  report.mutable_report_checksum()->set_algorithm(
      protocol::v2::Checksum::ALGORITHM_SHA256);
  auto unsigned_report = report;
  unsigned_report.clear_report_checksum();
  report.mutable_report_checksum()->set_digest(
      reportSha256(unsigned_report.SerializeAsString()));
  config.artifact_report_state->publish(report);

  auto bytes = serializeTaskStatus(executor);
  protocol::v2::TaskReport status;
  ASSERT_TRUE(status.ParseFromArray(bytes.data(), bytes.size()));
  ASSERT_TRUE(status.has_attempt());
  EXPECT_EQ(status.report_origin(),
            protocol::v2::TASK_REPORT_ORIGIN_NATIVE);
  EXPECT_EQ(status.attempt().query_or_job_id(), "job-1");
  EXPECT_EQ(status.attempt().task_id(), "task-1");
  EXPECT_EQ(status.attempt().stage_id(), 3);
  EXPECT_EQ(status.attempt().partition_id(), 7);
  EXPECT_EQ(status.attempt().plan_epoch(), 11);
  EXPECT_EQ(status.attempt().attempt_id(), 2);
  EXPECT_EQ(status.attempt().fencing_token(), 47);
  EXPECT_EQ(status.attempt().control_epoch(), 13);
  ASSERT_TRUE(status.attempt().has_execution_owner());
  EXPECT_EQ(status.attempt().execution_owner().owner_id(), "coordinator-a");
  EXPECT_EQ(status.attempt().execution_owner().owner_incarnation(), "process-a");
  EXPECT_EQ(status.attempt().execution_owner().fencing_token(), 19);
  ASSERT_TRUE(status.has_artifact_work());
  EXPECT_EQ(status.artifact_work().SerializeAsString(), report.SerializeAsString());
}

TEST_F(StatusSerializerTest, FailedTaskReportsPinnedSourceCoverage) {
  executor::TaskExecutorConfig config;
  config.function_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  config.task_id = "task-coverage";
  config.query_id = "query-coverage";
  config.stage_id = 4;
  config.partition_id = 8;
  config.plan_epoch = 3;
  config.attempt_id = 1;
  config.fencing_token = 33;
  config.control_epoch = 7;
  config.owner_id = "coordinator-b";
  config.owner_incarnation = "process-b";
  config.owner_fencing_token = 12;
  for (int index = 0; index < 2; ++index) {
    split::SplitConfig split;
    split.split_id = "split-" + std::to_string(index);
    split.table_name = "catalog.schema.events";
    split.source_snapshot = "snapshot-42";
    split.data_object_id = "object-" + std::to_string(index);
    config.splits.push_back(std::move(split));
  }
  pangrova::test::bindTestAttempt(config);
  executor::TaskExecutor executor(config);
  executor.notifyTaskFailed("source read failed");

  auto bytes = serializeTaskStatus(executor);
  protocol::v2::TaskReport status;
  ASSERT_TRUE(status.ParseFromArray(bytes.data(), bytes.size()));
  EXPECT_EQ(status.state(), protocol::v2::TASK_STATE_FAILED);
  EXPECT_EQ(status.report_origin(),
            protocol::v2::TASK_REPORT_ORIGIN_NATIVE);
  ASSERT_TRUE(status.has_error());
  ASSERT_TRUE(status.has_coverage());
  const auto& coverage = status.coverage();
  EXPECT_EQ(coverage.completion_state(),
            protocol::v2::COMPLETION_STATE_PARTIAL);
  EXPECT_EQ(coverage.expected_work_units(), 2);
  EXPECT_EQ(coverage.completed_work_units(), 0);
  EXPECT_DOUBLE_EQ(coverage.coverage_ratio(), 0.0);
  ASSERT_EQ(coverage.failed_work_unit_ids_size(), 2);
  EXPECT_EQ(coverage.failed_work_unit_ids(0), "split-0");
  EXPECT_EQ(coverage.failed_work_unit_ids(1), "split-1");
  ASSERT_EQ(coverage.source_snapshots_size(), 1);
  EXPECT_EQ(coverage.source_snapshots(0).source_id(),
            "catalog.schema.events");
  EXPECT_EQ(coverage.source_snapshots(0).snapshot_id(), "snapshot-42");
  EXPECT_EQ(coverage.source_snapshots(0).expected_work_units(), 2);
  EXPECT_EQ(coverage.source_snapshots(0).completed_work_units(), 0);
}

TEST_F(StatusSerializerTest, FailedTaskReportsBoundLargeSplitCoverage) {
  executor::TaskExecutorConfig config;
  config.function_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  config.task_id = "task-large-coverage";
  config.query_id = "query-large-coverage";
  for (int index = 0; index < 4097; ++index) {
    split::SplitConfig split;
    split.split_id = "split-" + std::to_string(index);
    split.table_name = "catalog.schema.events";
    split.source_snapshot = "snapshot-42";
    config.splits.push_back(std::move(split));
  }
  pangrova::test::bindTestAttempt(config);
  executor::TaskExecutor executor(config);
  executor.notifyTaskFailed("source read failed");

  auto bytes = serializeTaskStatus(executor);
  protocol::v2::TaskReport status;
  ASSERT_TRUE(status.ParseFromArray(bytes.data(), bytes.size()));
  ASSERT_TRUE(status.has_coverage());
  const auto& coverage = status.coverage();
  EXPECT_EQ(status.report_origin(), protocol::v2::TASK_REPORT_ORIGIN_NATIVE);
  EXPECT_TRUE(coverage.all_work_units_failed());
  EXPECT_EQ(coverage.failed_work_unit_ids_size(), 0);
  EXPECT_EQ(coverage.expected_work_units(), 4097);
  EXPECT_EQ(coverage.completed_work_units(), 0);
  ASSERT_EQ(coverage.source_snapshots_size(), 1);
  EXPECT_EQ(coverage.source_snapshots(0).expected_work_units(), 4097);
}

TEST_F(StatusSerializerTest, FailedTaskRetainsTaskOwnedColumnarCopyObservation) {
  executor::TaskExecutorConfig config;
  config.function_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  config.task_id = "copy-observation-owner";
  config.query_id = "copy-observation-query";
  pangrova::test::bindTestAttempt(config);
  executor::TaskExecutor executor(config);
  executor.metricsCollector().observeColumnarConversionCopiedBytes(40);
  executor.metricsCollector().observeColumnarConversionCopiedBytes(40);
  executor.notifyTaskFailed("physical writer failed after conversion");
  const auto encoded = serializeTaskStatus(executor);
  protocol::v2::TaskReport report;
  ASSERT_TRUE(report.ParseFromArray(encoded.data(), static_cast<int>(encoded.size())));
  EXPECT_EQ(report.state(), protocol::v2::TASK_STATE_FAILED);
  EXPECT_EQ(report.attempt().task_id(), config.task_id);
  EXPECT_EQ(report.usage().columnar_conversion_copied_bytes(), 40);
  EXPECT_FALSE(report.has_artifact_work());
}

#endif  // PANGROVA_HAS_VELOX_TASK_STATS

}  // namespace
}  // namespace pangrova::jni::test
