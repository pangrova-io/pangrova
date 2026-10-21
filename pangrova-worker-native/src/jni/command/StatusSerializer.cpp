// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

// Serializes TaskExecutor status, Velox TaskStats, and Velox CacheStats to
// proto wire format. Migrated from JniBridge.cpp to isolate serialization
// concerns.
//
// Task 5.2: serializeTaskStats now extracts per-operator metrics from Velox
// exec::TaskStats via VeloxTaskRunner, replacing the legacy OperatorStats path.

#include "jni/command/StatusSerializer.h"

#include <algorithm>
#include <map>
#include <stdexcept>
#include <string>

// Velox TaskStats for per-operator metrics.
#if __has_include("velox/exec/TaskStats.h")
#include "velox/exec/BlockingReason.h"
#include "velox/exec/TaskStats.h"
#define PANGROVA_HAS_VELOX_TASK_STATS 1
#else
#define PANGROVA_HAS_VELOX_TASK_STATS 0
#endif

// Velox AsyncDataCache stats.
#include "compat/NativeFeatureDetection.h"

// Access VeloxTaskRunner through PlanExecutionDriver.
#include "executor/driver/PlanExecutionDriver.h"
#include "velox/runner/VeloxTaskRunner.h"

#include "pangrova/protocol/v2/result.pb.h"
#include "pangrova/protocol/v2/runtime.pb.h"

namespace pangrova::jni {
namespace {

// Maps internal ExecutorState to proto TaskState enum.
pangrova::protocol::v2::TaskState toProtoState(
    executor::ExecutorState state) {
  switch (state) {
    case executor::ExecutorState::PENDING:
      return pangrova::protocol::v2::TASK_STATE_PENDING;
    case executor::ExecutorState::RUNNING:
      return pangrova::protocol::v2::TASK_STATE_RUNNING;
    case executor::ExecutorState::FLUSHING:
      return pangrova::protocol::v2::TASK_STATE_FLUSHING;
    case executor::ExecutorState::FINISHED:
      return pangrova::protocol::v2::TASK_STATE_FINISHED;
    case executor::ExecutorState::FAILED:
      return pangrova::protocol::v2::TASK_STATE_FAILED;
    case executor::ExecutorState::CANCELLED:
      return pangrova::protocol::v2::TASK_STATE_CANCELLED;
  }
  return pangrova::protocol::v2::TASK_STATE_PENDING;
}

}  // namespace

std::vector<uint8_t> serializeTaskStatus(
    executor::TaskExecutor& executor) {
  pangrova::protocol::v2::TaskReport status;
  const auto lifecycle = executor.lifecycleSnapshot();
  auto* attempt = status.mutable_attempt();
  attempt->set_query_or_job_id(executor.queryId());
  attempt->set_stage_id(executor.stageId());
  attempt->set_partition_id(executor.partitionId());
  attempt->set_plan_epoch(executor.planEpoch());
  attempt->set_attempt_id(executor.attemptId());
  attempt->set_fencing_token(executor.fencingToken());
  attempt->set_task_id(executor.taskId());
  attempt->set_control_epoch(executor.controlEpoch());
  auto* owner = attempt->mutable_execution_owner();
  owner->set_owner_id(executor.ownerId());
  owner->set_owner_incarnation(executor.ownerIncarnation());
  owner->set_fencing_token(executor.ownerFencingToken());
  status.set_state(toProtoState(lifecycle.state));
  status.set_report_origin(
      pangrova::protocol::v2::TASK_REPORT_ORIGIN_NATIVE);
  if (executor.isExecutionReleased()) {
    status.set_execution_drain_state(
        pangrova::protocol::v2::EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED);
  }
  // Refresh actual operator copies before sampling task-owned usage. Final
  // stats survive reader/task deletion; live stats cover failed/cancelled scans.
  const auto actual_stats = executor.collectTaskStats();
  if (!actual_stats.empty()) {
    if (!status.mutable_native_task_stats()->ParseFromArray(
            actual_stats.data(), static_cast<int>(actual_stats.size()))) {
      throw std::runtime_error("Actual native task statistics are malformed");
    }
  }
  // The task-owned accumulator survives writer errors and destruction. No
  // commit fragment is required to observe an allocation that already happened.
  status.mutable_usage()->set_object_store_read_bytes(executor.requestedObjectReadBytes());
  status.mutable_usage()->set_columnar_conversion_copied_bytes(
      executor.metricsCollector().snapshot().columnar_conversion_copied_bytes);
  auto* coverage = status.mutable_coverage();
  const auto state = lifecycle.state;
  const uint64_t expected = std::max<uint64_t>(1, executor.splits().size());
  const bool finished = state == executor::ExecutorState::FINISHED;
  const bool unsuccessful = state == executor::ExecutorState::FAILED ||
      state == executor::ExecutorState::CANCELLED;
  coverage->set_expected_work_units(expected);
  coverage->set_completed_work_units(finished ? expected : 0);
  coverage->set_coverage_ratio(finished ? 1.0 : 0.0);
  coverage->set_completion_state(
      finished
          ? pangrova::protocol::v2::COMPLETION_STATE_COMPLETE
          : unsuccessful
              ? pangrova::protocol::v2::COMPLETION_STATE_PARTIAL
              : pangrova::protocol::v2::COMPLETION_STATE_UNSPECIFIED);
  if (unsuccessful) {
    if (executor.splits().empty()) {
      coverage->add_failed_work_unit_ids(executor.taskId());
    } else if (executor.splits().size() > 4096) {
      coverage->set_all_work_units_failed(true);
    } else {
      for (const auto& split : executor.splits()) {
        coverage->add_failed_work_unit_ids(split.split_id);
      }
    }
  }

  // A task may read several tables. Group by logical source and snapshot; never
  // collapse this to one vector/index-specific freshness field.
  std::map<std::pair<std::string, std::string>, uint64_t> source_counts;
  for (const auto& split : executor.splits()) {
    source_counts[{split.table_name, split.source_snapshot}]++;
  }
  for (const auto& [source, count] : source_counts) {
    auto* report = coverage->add_source_snapshots();
    report->set_source_id(source.first);
    report->set_snapshot_id(source.second);
    report->set_expected_work_units(count);
    report->set_completed_work_units(finished ? count : 0);
  }

  // Read bindings are immutable generation pins. Their timestamps are optional
  // until supplied by the manifest, but identity/snapshot are never fabricated.
  for (const auto& [plan_node_id, binding] : executor.operatorBindings()) {
    (void)plan_node_id;
    for (const auto& artifact : binding.artifacts()) {
      auto* report = coverage->add_artifacts();
      report->set_artifact_id(artifact.artifact_id());
      report->set_artifact_type(artifact.artifact_type());
      report->set_generation(artifact.generation());
      report->set_source_id(artifact.source_id());
      report->set_source_snapshot(artifact.source_snapshot());
      report->set_source_data_timestamp_ms(
          artifact.source_data_timestamp_ms());
      report->set_artifact_created_at_epoch_ms(
          artifact.artifact_created_at_epoch_ms());
    }
  }
  if (unsuccessful && lifecycle.error.has_value()) {
    *status.mutable_error() = *lifecycle.error;
  }
  if (auto artifact_report = executor.artifactWorkReport();
      artifact_report.has_value()) {
    *status.mutable_artifact_work() = std::move(*artifact_report);
  }
  std::vector<uint8_t> buf(status.ByteSizeLong());
  if (!status.SerializeToArray(buf.data(), static_cast<int>(buf.size()))) {
    throw std::runtime_error("Canonical task report serialization failed");
  }
  return buf;
}

std::vector<uint8_t> serializeNativeTaskStats(
    const facebook::velox::exec::TaskStats& task_stats,
    uint64_t index_splits_evaluated,
    uint64_t index_splits_skipped) {
  pangrova::protocol::v2::NativeTaskStats proto;
  proto.set_index_splits_evaluated(index_splits_evaluated);
  proto.set_index_splits_skipped(index_splits_skipped);

#if PANGROVA_HAS_VELOX_TASK_STATS
  const auto consumer_blocked = task_stats.numBlockedDrivers.find(
      facebook::velox::exec::BlockingReason::kWaitForConsumer);
  if (consumer_blocked != task_stats.numBlockedDrivers.end()) {
    proto.set_consumer_blocked_drivers(consumer_blocked->second);
  }
  // Total wall time from Velox task lifecycle.
  int64_t totalWallMs = 0;
  if (task_stats.endTimeMs > 0 && task_stats.executionStartTimeMs > 0) {
    totalWallMs = task_stats.endTimeMs - task_stats.executionStartTimeMs;
  }
  proto.set_elapsed_time_ms(totalWallMs);

  // Aggregate across all pipelines and operators.
  int64_t totalRowsRead = 0;
  int64_t totalBytesRead = 0;
  int64_t totalCpuNs = 0;
  int64_t totalMemory = 0;
  int64_t peakMemory = 0;
  int64_t totalSpill = 0;
  int64_t consumerWaitNanos = 0;

  for (const auto& pipelineStats : task_stats.pipelineStats) {
    for (const auto& opStats : pipelineStats.operatorStats) {
      auto* entry = proto.add_operator_stats();
      entry->set_operator_type(opStats.operatorType);
      entry->set_plan_node_id(opStats.planNodeId);
      entry->set_input_rows(opStats.inputPositions);
      entry->set_output_rows(opStats.outputPositions);
      entry->set_input_bytes(opStats.inputBytes);
      entry->set_output_bytes(opStats.outputBytes);
      entry->set_wall_nanos(
          opStats.addInputTiming.wallNanos
          + opStats.getOutputTiming.wallNanos
          + opStats.finishTiming.wallNanos);
      entry->set_cpu_nanos(
          opStats.addInputTiming.cpuNanos
          + opStats.getOutputTiming.cpuNanos
          + opStats.finishTiming.cpuNanos);
      entry->set_memory_bytes(opStats.memoryStats.userMemoryReservation);
      entry->set_peak_memory_bytes(
          opStats.memoryStats.peakUserMemoryReservation);
      entry->set_spill_bytes(opStats.spilledBytes);

      totalRowsRead += opStats.inputPositions;
      totalBytesRead += opStats.inputBytes;
      totalCpuNs += opStats.addInputTiming.cpuNanos
          + opStats.getOutputTiming.cpuNanos
          + opStats.finishTiming.cpuNanos;
      totalMemory += opStats.memoryStats.userMemoryReservation;
      peakMemory = std::max(
          peakMemory,
          static_cast<int64_t>(
              opStats.memoryStats.peakUserMemoryReservation));
      totalSpill += opStats.spilledBytes;
      const auto consumerWait = opStats.runtimeStats.find("blockedWaitForConsumerWallNanos");
      if (consumerWait != opStats.runtimeStats.end()) {
        consumerWaitNanos += consumerWait->second.sum;
      }
    }
  }

  proto.set_rows_read(totalRowsRead);
  proto.set_bytes_read(totalBytesRead);
  proto.set_cpu_time_ms(totalCpuNs / 1'000'000);
  proto.set_memory_bytes(totalMemory);
  proto.set_peak_memory_bytes(peakMemory);
  proto.set_spill_bytes(totalSpill);
  proto.set_backpressure_wait_ms(consumerWaitNanos / 1'000'000);
#else
  (void)task_stats;
#endif

  std::vector<uint8_t> buf(proto.ByteSizeLong());
  proto.SerializeToArray(buf.data(), static_cast<int>(buf.size()));
  return buf;
}

std::vector<uint8_t> serializeTaskStats(
    executor::TaskExecutor& executor) {
  // P1-STAT-3: Unified stats serialization.
  // Primary path: collect from Velox TaskStats via VeloxTaskRunner.
  // This provides per-operator metrics (wall time, CPU time, rows, memory,
  // spill) directly from the Velox execution engine.
  // Both paths serialize to the same canonical TaskStats proto.
  auto* runner = executor.veloxTaskRunner();
  if (runner) {
    auto veloxStats = runner->collectTaskStats();
    if (!veloxStats.empty()) {
      return veloxStats;
    }
  }

  // Fallback: use NativeMetricsCollector snapshot for top-level aggregates.
  // This path is used when VeloxTaskRunner is not yet started or has no stats.
  // Uses the same TaskStats proto schema as the primary path for consistency.
  auto snap = executor.metricsCollector().snapshot();
  pangrova::protocol::v2::NativeTaskStats stats;
  stats.set_rows_read(snap.rows_read);
  stats.set_bytes_read(snap.bytes_read);
  stats.set_cpu_time_ms(snap.cpu_time_ns / 1'000'000);
  stats.set_memory_bytes(0);
  stats.set_peak_memory_bytes(snap.peak_memory_bytes);
  stats.set_elapsed_time_ms(snap.wall_time_ns / 1'000'000);

  std::vector<uint8_t> buf(stats.ByteSizeLong());
  stats.SerializeToArray(buf.data(), static_cast<int>(buf.size()));
  return buf;
}

std::vector<uint8_t> serializeNativeCacheStats(
    const facebook::velox::cache::CacheStats& stats) {
  pangrova::protocol::v2::CacheMetrics proto;
#if PANGROVA_HAS_VELOX_ASYNC_CACHE
  proto.set_ram_hits(stats.numHit);
  proto.set_ram_misses(stats.numNew);
  proto.set_ssd_hits(stats.ssdStats ? stats.ssdStats->entriesRead : 0);
  proto.set_ssd_misses(stats.ssdStats ? stats.ssdStats->entriesWritten : 0);
  proto.set_promotions(stats.ssdStats ? stats.ssdStats->entriesRead : 0);
  proto.set_demotions(stats.numEvict);
  proto.set_evictions(stats.numEvict);
  // P1-CACHE-3: Use actual cache size fields instead of heuristic estimate.
  proto.set_ram_used_bytes(stats.tinySize + stats.largeSize);
  proto.set_ssd_used_bytes(
      stats.ssdStats ? stats.ssdStats->bytesWritten : 0);
  auto total = stats.numHit + stats.numNew;
  proto.set_prefetch_hit_rate(
      total > 0
          ? static_cast<double>(stats.numHit) / static_cast<double>(total)
          : 0.0);
#else
  (void)stats;
#endif
  std::vector<uint8_t> buf(proto.ByteSizeLong());
  proto.SerializeToArray(buf.data(), static_cast<int>(buf.size()));
  return buf;
}

}  // namespace pangrova::jni
