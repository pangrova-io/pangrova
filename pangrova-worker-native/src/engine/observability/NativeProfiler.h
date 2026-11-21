#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace facebook::velox::exec { class Task; }

namespace pangrova::observability {

enum class ProfilingMode {
    ON_DEMAND,
    CONTINUOUS,
    QUERY_LEVEL
};

enum class OutputFormat {
    FLAME_GRAPH_SVG,
    PPROF,
    TRACE_EVENT
};

// Profiling session that captures Velox TaskStats snapshots.
struct ProfilingSession {
  std::string session_id;
  ProfilingMode mode;
  bool active = false;
  int64_t start_time_ns = 0;
  int duration_seconds = 0;
  int sampling_frequency_hz = 0;
  // Captured task stats JSON snapshots.
  std::vector<std::string> snapshots;
};

/**
 * Native CPU/memory/lock/IO profiler integrated with Velox TaskStats.
 * Captures operator-level timing, memory, and spill statistics from
 * running Velox Tasks for observability.
 */
class NativeProfiler {
public:
    std::string start(ProfilingMode mode, int durationSeconds, int samplingFrequencyHz);
    void stop(const std::string& sessionId);
    std::vector<uint8_t> getOutput(const std::string& sessionId, OutputFormat format);

    // Register a running Velox task for profiling.
    void registerTask(const std::string& taskId,
                      std::shared_ptr<facebook::velox::exec::Task> task);
    void unregisterTask(const std::string& taskId);

    // Take a snapshot of all registered tasks' stats.
    std::string captureSnapshot();

    // Singleton accessor.
    static NativeProfiler& instance();

private:
    std::mutex mutex_;
    std::unordered_map<std::string, ProfilingSession> sessions_;
    std::unordered_map<std::string, std::weak_ptr<facebook::velox::exec::Task>> tasks_;
    std::atomic<int> next_session_id_{0};
};

} // namespace pangrova::observability
