#include "engine/velox/runner/TaskStatsEncoding.h"

#include <algorithm>

// Velox TaskStats for per-operator metrics.
#if __has_include("velox/exec/TaskStats.h")
#include "velox/exec/BlockingReason.h"
#include "velox/exec/TaskStats.h"
#define PANGROVA_HAS_VELOX_TASK_STATS 1
#else
#define PANGROVA_HAS_VELOX_TASK_STATS 0
#endif

#include "pangrova/protocol/v2/result.pb.h"

namespace pangrova::velox {

protocol::v2::NativeTaskStats encodeNativeTaskStats(
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

  return proto;
}

std::vector<uint8_t> serializeNativeTaskStats(
    const facebook::velox::exec::TaskStats& task_stats,
    uint64_t index_splits_evaluated,
    uint64_t index_splits_skipped) {
  return serializeNativeTaskStats(
      encodeNativeTaskStats(task_stats, index_splits_evaluated, index_splits_skipped));
}

std::vector<uint8_t> serializeNativeTaskStats(
    const protocol::v2::NativeTaskStats& stats) {
  std::vector<uint8_t> buf(stats.ByteSizeLong());
  stats.SerializeToArray(buf.data(), static_cast<int>(buf.size()));
  return buf;
}

}  // namespace pangrova::velox
