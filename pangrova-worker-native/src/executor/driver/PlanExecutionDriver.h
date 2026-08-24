// Drives Velox plan execution on a background thread.
// Converts Substrait plan → Velox PlanNode → Velox Task → OutputQueue.
// Extracted from TaskExecutor to enforce single-responsibility.
#pragma once

#include <atomic>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>

#include "executor/driver/TaskLifecycle.h"
#include "executor/queue/OutputQueue.h"
#include "observability/NativeMetricsCollector.h"
#include "shuffle/SerializedShuffleQueue.h"
#include "velox/type/Type.h"
#include "velox/common/memory/Memory.h"

namespace pangrova::velox {
class VeloxTaskRunner;  // forward declaration
}  // namespace pangrova::velox

namespace pangrova::executor {

struct TaskExecutorConfig;  // forward declaration

// Drives Velox plan execution on a background thread.
// Owns the execution thread and delegates plan conversion + execution
// to VeloxPlanConverter and VeloxTaskRunner.
//
// Thread model: executeAsync() spawns a background thread that runs
// executeVeloxPlan(). join() blocks until that thread completes.
// cancel() propagates cancellation to the active VeloxTaskRunner
// and transitions the lifecycle to CANCELLED.
//
// @ThreadSafe — cancel() may be called from any thread while
// executeVeloxPlan() runs on the background thread. Access to
// active_runner_ is protected by runner_mutex_.
class PlanExecutionDriver {
 public:
  PlanExecutionDriver(const TaskExecutorConfig& config,
                      TaskLifecycle& lifecycle,
                      OutputQueue* output_queue,
                      pangrova::shuffle::SerializedShuffleQueue* shuffle_queue,
                      observability::NativeMetricsCollector& metrics);
  ~PlanExecutionDriver();

  // Non-copyable, non-movable.
  PlanExecutionDriver(const PlanExecutionDriver&) = delete;
  PlanExecutionDriver& operator=(const PlanExecutionDriver&) = delete;

  // Start execution on a background thread.
  void executeAsync();

  // Join the execution thread (blocking).
  void join();

  // Cancel execution (best-effort, thread-safe).
  // Propagates cancel to VeloxTaskRunner if active, closes output
  // queue to unblock consumers, and transitions lifecycle to CANCELLED.
  void cancel();

  // Whether cancel has been requested (atomic, lock-free).
  bool isCancelled() const;

  // Access the active VeloxTaskRunner (may be null if not yet started).
  // Thread-safe: protected by runner_mutex_.
  velox::VeloxTaskRunner* veloxTaskRunner() const;

  // Look up the output RowType for a TableScan node by plan_node_id.
  // Returns the RowType if found, or std::nullopt if unknown.
  // Thread-safe: protected by runner_mutex_.
  std::optional<facebook::velox::RowTypePtr> getScanNodeOutputType(
      const std::string& plan_node_id) const;

 private:
  // Core Velox execution logic — runs on the background thread.
  void executeVeloxPlan();

  const TaskExecutorConfig& config_;
  TaskLifecycle& lifecycle_;
  OutputQueue* output_queue_;
  pangrova::shuffle::SerializedShuffleQueue* shuffle_queue_;
  observability::NativeMetricsCollector& metrics_;

  // Promise/future pair for async completion signaling.
  // completion_promise_ is set by either:
  //   - RAII exit_guard in executeVeloxPlan() on early exit (before async armed)
  //   - The async continuation chain after Velox task completes
  // join() and ~PlanExecutionDriver() both wait on completion_future_.
  // Declared in this order so completion_future_ initializes after the promise.
  std::promise<void> completion_promise_;
  std::future<void> completion_future_;
  std::unique_ptr<std::thread> fallback_thread_;

  // Cancel flag — set by cancel(), checked by executeVeloxPlan().
  // @GuardedBy: atomic, no lock needed.
  std::atomic<bool> cancelled_{false};

  // Active VeloxTaskRunner — set on execution thread, read on cancel thread.
  // @GuardedBy: runner_mutex_
  mutable std::mutex runner_mutex_;

  // Memory pools created during plan conversion (e.g., for ValuesNode vectors).
  // Must be kept alive as long as the VeloxTaskRunner holds the plan tree,
  // because RowVector/FlatVector buffers reference the pool for deallocation.
  // IMPORTANT: Declared BEFORE active_runner_ so that C++ reverse-order
  // destruction destroys the runner (and its PlanNode tree / ArrayVectors)
  // BEFORE the pools they reference are freed.
  // @GuardedBy: runner_mutex_
  std::vector<std::shared_ptr<facebook::velox::memory::MemoryPool>>
      values_pool_holders_;

  std::shared_ptr<velox::VeloxTaskRunner> active_runner_;

  // TableScan planNodeId → output RowType mapping.
  // Copied from VeloxPlanConverter after plan conversion.
  // @GuardedBy: runner_mutex_
  std::unordered_map<std::string, facebook::velox::RowTypePtr>
      scan_node_output_types_;
};

}  // namespace pangrova::executor
