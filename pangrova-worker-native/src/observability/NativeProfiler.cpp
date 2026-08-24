#include "observability/NativeProfiler.h"

#include <chrono>
#include <sstream>

#include "velox/exec/Task.h"
#include "velox/exec/TaskStats.h"
#include "velox/exec/OperatorStats.h"

namespace pangrova::observability {

namespace {

// P1-OBS-5: Escape special characters for JSON string values.
std::string jsonEscape(const std::string& s) {
  std::string result;
  result.reserve(s.size());
  for (char c : s) {
    switch (c) {
      case '"': result += "\\\""; break;
      case '\\': result += "\\\\"; break;
      case '\n': result += "\\n"; break;
      case '\r': result += "\\r"; break;
      case '\t': result += "\\t"; break;
      default: result += c;
    }
  }
  return result;
}

// Format a CpuWallTiming as JSON.
std::string formatTiming(const facebook::velox::CpuWallTiming& t) {
  std::ostringstream oss;
  oss << "{\"count\":" << t.count
      << ",\"wall_nanos\":" << t.wallNanos
      << ",\"cpu_nanos\":" << t.cpuNanos << "}";
  return oss.str();
}

// Format operator stats as JSON.
std::string formatOperatorStats(const facebook::velox::exec::OperatorStats& os) {
  std::ostringstream oss;
  oss << "{\"op_id\":" << os.operatorId
      << ",\"op_type\":\"" << jsonEscape(os.operatorType) << "\""
      << ",\"input_rows\":" << os.inputPositions
      << ",\"input_bytes\":" << os.inputBytes
      << ",\"output_rows\":" << os.outputPositions
      << ",\"output_bytes\":" << os.outputBytes
      << ",\"add_input\":" << formatTiming(os.addInputTiming)
      << ",\"get_output\":" << formatTiming(os.getOutputTiming)
      << ",\"finish\":" << formatTiming(os.finishTiming)
      << ",\"blocked_wall_nanos\":" << os.blockedWallNanos
      << ",\"spilled_bytes\":" << os.spilledBytes
      << ",\"spilled_rows\":" << os.spilledRows
      << ",\"spilled_partitions\":" << os.spilledPartitions
      << ",\"peak_memory\":" << os.memoryStats.peakTotalMemoryReservation
      << ",\"memory\":" << os.memoryStats.userMemoryReservation
      << ",\"num_drivers\":" << os.numDrivers
      << "}";
  return oss.str();
}

// P1-OBS-7: Format task stats as JSON — shared helper for captureSnapshot() and getOutput().
std::string formatTaskStatsJson(
    const std::string& taskId,
    const facebook::velox::exec::TaskStats& stats) {
  std::ostringstream oss;
  oss << "{\"task\":\"" << jsonEscape(taskId) << "\""
      << ",\"splits\":" << stats.numTotalSplits
      << ",\"finished_splits\":" << stats.numFinishedSplits
      << ",\"running_splits\":" << stats.numRunningSplits
      << ",\"queued_splits\":" << stats.numQueuedSplits
      << ",\"pipelines\":" << stats.pipelineStats.size();

  oss << ",\"operators\":[";
  bool firstOp = true;
  int64_t total_input_rows = 0;
  int64_t total_output_rows = 0;
  int64_t total_spill_bytes = 0;
  uint64_t total_wall_nanos = 0;
  for (const auto& ps : stats.pipelineStats) {
    for (const auto& os : ps.operatorStats) {
      if (!firstOp) oss << ",";
      firstOp = false;
      oss << formatOperatorStats(os);
      total_input_rows += os.inputPositions;
      total_output_rows += os.outputPositions;
      total_spill_bytes += os.spilledBytes;
      total_wall_nanos += os.addInputTiming.wallNanos
          + os.getOutputTiming.wallNanos
          + os.finishTiming.wallNanos;
    }
  }
  oss << "]"
      << ",\"total_input_rows\":" << total_input_rows
      << ",\"total_output_rows\":" << total_output_rows
      << ",\"total_spill_bytes\":" << total_spill_bytes
      << ",\"total_wall_nanos\":" << total_wall_nanos
      << "}";
  return oss.str();
}

}  // namespace

NativeProfiler& NativeProfiler::instance() {
  static NativeProfiler inst;
  return inst;
}

std::string NativeProfiler::start(ProfilingMode mode,
                                  int durationSeconds,
                                  int samplingFrequencyHz) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto id = "profile-" + std::to_string(next_session_id_.fetch_add(1));
  ProfilingSession session;
  session.session_id = id;
  session.mode = mode;
  session.active = true;
  session.duration_seconds = durationSeconds;
  session.sampling_frequency_hz = samplingFrequencyHz;
  session.start_time_ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch()).count();
  sessions_[id] = std::move(session);
  return id;
}

void NativeProfiler::stop(const std::string& sessionId) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = sessions_.find(sessionId);
  if (it != sessions_.end()) {
    it->second.active = false;
  }
}

std::vector<uint8_t> NativeProfiler::getOutput(const std::string& sessionId,
                                                OutputFormat format) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = sessions_.find(sessionId);
  if (it == sessions_.end()) {
    return {};
  }

  // Only TRACE_EVENT format is currently supported.
  if (format != OutputFormat::TRACE_EVENT) {
    throw std::runtime_error(
        "NativeProfiler: unsupported output format. "
        "Only TRACE_EVENT is currently supported.");
  }

  // Capture a final snapshot if session is still active.
  if (it->second.active) {
    // P1-OBS-1: Prune expired weak_ptr entries.
    for (auto taskIt = tasks_.begin(); taskIt != tasks_.end(); ) {
      if (taskIt->second.expired()) {
        taskIt = tasks_.erase(taskIt);
      } else {
        ++taskIt;
      }
    }
    for (auto& [taskId, weakTask] : tasks_) {
      if (auto task = weakTask.lock()) {
        auto stats = task->taskStats();
        it->second.snapshots.push_back(formatTaskStatsJson(taskId, stats));
      }
    }
    it->second.active = false;
  }

  // Format output as JSON array (TRACE_EVENT format).
  std::ostringstream result;
  result << "[";
  for (size_t i = 0; i < it->second.snapshots.size(); ++i) {
    if (i > 0) result << ",";
    result << it->second.snapshots[i];
  }
  result << "]";
  auto str = result.str();

  // P3-BUG-22: Remove completed session from map to prevent memory leak.
  // Session data has been consumed; keeping it would cause sessions_ to
  // grow unboundedly over the process lifetime.
  sessions_.erase(it);

  return std::vector<uint8_t>(str.begin(), str.end());
}

void NativeProfiler::registerTask(
    const std::string& taskId,
    std::shared_ptr<facebook::velox::exec::Task> task) {
  std::lock_guard<std::mutex> lock(mutex_);
  tasks_[taskId] = task;
}

void NativeProfiler::unregisterTask(const std::string& taskId) {
  std::lock_guard<std::mutex> lock(mutex_);
  tasks_.erase(taskId);
}

std::string NativeProfiler::captureSnapshot() {
  std::lock_guard<std::mutex> lock(mutex_);
  // P1-OBS-1: Prune expired weak_ptr entries before iterating.
  for (auto it = tasks_.begin(); it != tasks_.end(); ) {
    if (it->second.expired()) {
      it = tasks_.erase(it);
    } else {
      ++it;
    }
  }
  std::ostringstream oss;
  oss << "[";
  bool first = true;
  for (auto& [taskId, weakTask] : tasks_) {
    if (auto task = weakTask.lock()) {
      if (!first) oss << ",";
      first = false;
      auto stats = task->taskStats();
      oss << formatTaskStatsJson(taskId, stats);
    }
  }
  oss << "]";
  return oss.str();
}

} // namespace pangrova::observability
