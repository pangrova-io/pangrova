// Drives Velox plan execution on a background thread.
// Converts Substrait plan → Velox PlanNode → Velox Task → OutputQueue.
// Extracted from TaskExecutor to enforce single-responsibility.
#pragma once

#include <atomic>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>

#include "engine/executor/driver/TaskLifecycle.h"
#include "engine/executor/driver/WriteOutputDrain.h"
#include "engine/executor/queue/OutputQueue.h"
#include "engine/observability/NativeMetricsCollector.h"
#include "engine/shuffle/SerializedShuffleQueue.h"
#include "pangrova/protocol/v2/result.pb.h"
#include "pangrova/protocol/v2/runtime.pb.h"
#include "velox/type/Type.h"
#include "velox/type/Filter.h"
#include "velox/common/memory/Memory.h"

namespace pangrova::resource { class TaskObjectReadBudget; }

namespace pangrova::velox {
class VeloxTaskRunner;  // forward declaration
class VeloxMemoryGovernor;
}  // namespace pangrova::velox

namespace pangrova::lance {
class LanceBatchWriter;
class LancePhysicalBatchWriter;
}
namespace pangrova::observability { class SpanScope; }

namespace pangrova::executor {

struct TaskExecutorConfig;  // forward declaration

// Effective per-task spill policy after intersecting a Coordinator-issued byte
// grant with Worker-local runtime policy. Filesystem paths never cross the
// control/data-plane boundary.
struct WorkerLocalSpillPolicy {
  bool enabled = false;
  std::string directory;
  int64_t max_bytes = 0;
};

WorkerLocalSpillPolicy resolveWorkerLocalSpillPolicy(
    int64_t task_spill_grant_bytes,
    const velox::VeloxMemoryGovernor* memory_governor);

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
  void executeAsync(std::function<void()> on_result_ready = {});

  // Join the execution thread (blocking).
  void join();

  // Explicit release can fail retryably while RPC buffers retain task grants.
  void release();

  // Final actual producer statistics survive Task deletion until receipt handoff.
  std::optional<protocol::v2::NativeTaskStats> collectTaskStats() const;
  bool hasFinalTaskStats() const;
  std::optional<protocol::v2::NativeObjectReadStats> objectReadStats() const;

  // Cancel execution (best-effort, thread-safe).
  // Propagates cancel to VeloxTaskRunner if active, closes output
  // queue to unblock consumers, and transitions lifecycle to CANCELLED.
  void cancel();

  // Whether cancel has been requested (atomic, lock-free).
  bool isCancelled() const;

  // Access the active VeloxTaskRunner (may be null if not yet started).
  // Thread-safe: protected by runner_mutex_.
  velox::VeloxTaskRunner* veloxTaskRunner() const;
  void watchResultDrain(std::function<void()> listener) const;

  // Look up the output RowType for a TableScan node by plan_node_id.
  // Returns the RowType if found, or std::nullopt if unknown.
  // Thread-safe: protected by runner_mutex_.
  std::optional<facebook::velox::RowTypePtr> getScanNodeOutputType(
      const std::string& plan_node_id) const;

  // Resolve a runtime-filter target to an exact scan node. Exact IDs win. A
  // legacy/logical selector may be resolved by column only when exactly one
  // scan exposes that column; zero or multiple matches fail closed.
  std::string resolveScanNodeId(
      const std::string& requested_plan_node_id,
      const std::string& column_name) const;

  // Validate and enqueue an external runtime filter. Calls racing task startup
  // are retained and applied before the first Velox driver is started.
  void injectDynamicFilter(
      const std::string& plan_node_id,
      const google::protobuf::RepeatedPtrField<protocol::v2::RuntimeFilter>& filters);

 private:
  struct Execution;
  struct PendingDynamicFilter {
    std::string requested_plan_node_id;
    std::string column_name;
    std::unique_ptr<facebook::velox::common::Filter> filter;
  };

  void applyDynamicFilter(
      const std::shared_ptr<velox::VeloxTaskRunner>& runner,
      PendingDynamicFilter filter);

  // Core Velox execution logic — runs on the background thread.
  void executeVeloxPlan(const std::function<void()>& on_result_ready);
  std::shared_ptr<Execution> prepareExecution(observability::SpanScope* span);
  bool publishRunner(const std::shared_ptr<Execution>& execution);
  void startOutputDrains(const std::shared_ptr<Execution>& execution);
  void drainWriteOutput(const std::shared_ptr<Execution>& execution);
  void completeExecution(const std::shared_ptr<Execution>& execution, bool success);
  std::string validateProducer(const std::shared_ptr<Execution>& execution, bool& success);
  void completeShuffleDrain(const std::shared_ptr<Execution>& execution, bool success);
  bool completeWriteDrain(const std::shared_ptr<Execution>& execution, bool success);
  void finishSuccessfulExecution(const std::shared_ptr<Execution>& execution);
  std::string stopExecution(const std::shared_ptr<Execution>& execution);
  void reportExecutionFailure(std::exception_ptr failure, const char* context,
                              const std::string& cleanupErrors);
  bool dispatchExecution(const std::shared_ptr<Execution>& execution,
                         const std::function<void()>& on_result_ready);
  void signalCompletion();

  // Close failure channels before publishing FAILED to terminal observers.
  void notifyFailure(const std::string& message);

  void closePhysicalWriter();
  void closeDatasetWriter();

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
  std::atomic<bool> completion_signalled_{false};
  std::future<void> completion_future_;
  std::unique_ptr<std::thread> fallback_thread_;

  // Cancel flag — set by cancel(), checked by executeVeloxPlan().
  // @GuardedBy: atomic, no lock needed.
  std::atomic<bool> cancelled_{false};

  // Separates producer success from queue closure. cancel() participates in
  // the same CAS state machine as the write drain, so it cannot race a stale
  // SUCCEEDED store into publishing failed/cancelled task output.
  WriteProducerOutcomeRef write_producer_outcome_ =
      std::make_shared<std::atomic<WriteProducerOutcome>>(
          WriteProducerOutcome::RUNNING);

  // Active VeloxTaskRunner — set on execution thread, read on cancel thread.
  // @GuardedBy: runner_mutex_
  mutable std::mutex runner_mutex_;

  std::shared_ptr<velox::VeloxTaskRunner> active_runner_;
  std::shared_ptr<resource::TaskObjectReadBudget> task_read_budget_;

  // Written only by the sink; release reads it after the producer join barrier.
  // This owner must outlive failed SDK drain/private-stage cleanup for retry.
  std::shared_ptr<lance::LancePhysicalBatchWriter> physical_writer_;
  std::shared_ptr<lance::LanceBatchWriter> dataset_writer_;
  // @GuardedBy: runner_mutex_; snapshot before actual Task deletion.
  std::optional<protocol::v2::NativeTaskStats> final_task_stats_;

  // TableScan planNodeId → output RowType mapping.
  // Copied from VeloxPlanConverter after plan conversion.
  // @GuardedBy: runner_mutex_
  std::unordered_map<std::string, facebook::velox::RowTypePtr>
      scan_node_output_types_;

  // Runtime commands may arrive after executor creation but before plan
  // conversion publishes active_runner_. The same lock protects this queue and
  // the runner publication, so no filter can fall through that hand-off race.
  // @GuardedBy: runner_mutex_
  std::vector<PendingDynamicFilter> pending_dynamic_filters_;
};

}  // namespace pangrova::executor
