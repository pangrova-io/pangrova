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
#include <string>

// Velox TaskStats for per-operator metrics.
#if __has_include("velox/exec/TaskStats.h")
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

#include "pangrova/native/protocol/v1/task.pb.h"
#include "pangrova/native/protocol/v1/cache.pb.h"

namespace pangrova::jni {
namespace {

// Maps internal ExecutorState to proto TaskState enum.
pangrova::native::protocol::v1::TaskState toProtoState(
    executor::ExecutorState state) {
  switch (state) {
    case executor::ExecutorState::PENDING:
      return pangrova::native::protocol::v1::TASK_STATE_PENDING;
    case executor::ExecutorState::RUNNING:
      return pangrova::native::protocol::v1::TASK_STATE_RUNNING;
    case executor::ExecutorState::FLUSHING:
      return pangrova::native::protocol::v1::TASK_STATE_FLUSHING;
    case executor::ExecutorState::FINISHED:
      return pangrova::native::protocol::v1::TASK_STATE_FINISHED;
    case executor::ExecutorState::FAILED:
      return pangrova::native::protocol::v1::TASK_STATE_FAILED;
    case executor::ExecutorState::CANCELLED:
      return pangrova::native::protocol::v1::TASK_STATE_CANCELLED;
  }
  return pangrova::native::protocol::v1::TASK_STATE_PENDING;
}

}  // namespace

std::vector<uint8_t> serializeTaskStatus(
    executor::TaskExecutor& executor) {
  pangrova::native::protocol::v1::TaskStatus status;
  status.set_task_id(executor.taskId());
  status.set_state(toProtoState(executor.state()));
  status.set_error_message(executor.errorMessage());
  auto stack_trace = executor.nativeStackTrace();
  if (!stack_trace.empty()) {
    status.set_native_stack_trace(stack_trace);
  }
  std::vector<uint8_t> buf(status.ByteSizeLong());
  status.SerializeToArray(buf.data(), static_cast<int>(buf.size()));
  return buf;
}

std::vector<uint8_t> serializeNativeTaskStats(
    const facebook::velox::exec::TaskStats& task_stats) {
  pangrova::native::protocol::v1::TaskStats proto;

#if PANGROVA_HAS_VELOX_TASK_STATS
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
    }
  }

  proto.set_rows_read(totalRowsRead);
  proto.set_bytes_read(totalBytesRead);
  proto.set_cpu_time_ms(totalCpuNs / 1'000'000);
  proto.set_memory_bytes(totalMemory);
  proto.set_peak_memory_bytes(peakMemory);
  proto.set_spill_bytes(totalSpill);
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
  // Both paths serialize to the same pangrova.native.protocol.v1.TaskStats proto.
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
  pangrova::native::protocol::v1::TaskStats stats;
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
  pangrova::native::protocol::v1::CacheStats proto;
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
