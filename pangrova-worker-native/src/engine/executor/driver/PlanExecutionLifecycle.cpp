#include "engine/executor/driver/PlanExecutionDriver.h"

#include <algorithm>
#include <stdexcept>
#include <folly/executors/CPUThreadPoolExecutor.h>

#include "engine/executor/driver/TaskExecutor.h"
#include "engine/resource/TaskObjectReadBudget.h"
#include "jni/RuntimeInitializer.h"
#include "lance/LanceBatchWriter.h"
#include "lance/LancePhysicalBatchWriter.h"
#include "pangrova/protocol/v2/runtime.pb.h"
#include "engine/velox/filter/DynamicFilterConverter.h"
#include "velox/exec/Task.h"
#include "engine/velox/memory/VeloxMemoryGovernor.h"
#include "engine/velox/runner/VeloxTaskRunner.h"
#include "engine/velox/runtime/NativeEngineLifecycle.h"

namespace pangrova::executor {
WorkerLocalSpillPolicy resolveWorkerLocalSpillPolicy(
    int64_t task_spill_grant_bytes,
    const velox::VeloxMemoryGovernor* memory_governor) {
  WorkerLocalSpillPolicy policy;
  if (memory_governor == nullptr || !memory_governor->spillEnabled() ||
      memory_governor->spillDirectory().empty() ||
      task_spill_grant_bytes <= 0) {
    return policy;
  }

  policy.enabled = true;
  policy.directory = memory_governor->spillDirectory();
  const int64_t node_spill_limit = memory_governor->maxSpillBytes();
  policy.max_bytes = node_spill_limit > 0
      ? std::min(task_spill_grant_bytes, node_spill_limit)
      : task_spill_grant_bytes;
  return policy;
}

PlanExecutionDriver::PlanExecutionDriver(
    const TaskExecutorConfig& config,
    TaskLifecycle& lifecycle,
    OutputQueue* output_queue,
    pangrova::shuffle::SerializedShuffleQueue* shuffle_queue,
    observability::NativeMetricsCollector& metrics)
    : config_(config),
      lifecycle_(lifecycle),
      output_queue_(output_queue),
      shuffle_queue_(shuffle_queue),
      metrics_(metrics) {}

PlanExecutionDriver::~PlanExecutionDriver() {
  // Ensure async execution finishes before destruction (RAII safety).
  if (completion_future_.valid()) {
    completion_future_.wait();
  }
  if (fallback_thread_ && fallback_thread_->joinable()) {
    fallback_thread_->join();
  }

  // Completion can precede the final Driver's Task release. Join that actual
  // destruction before tearing down this driver's execution resources.
  std::shared_ptr<velox::VeloxTaskRunner> runner;
  std::shared_ptr<resource::TaskObjectReadBudget> read_budget;
  {
    std::lock_guard<std::mutex> lock(runner_mutex_);
    runner = active_runner_;
    read_budget = task_read_budget_;
  }
  if (read_budget) read_budget->stopAndDrain();
  if (runner) runner->releaseTaskAndWaitForDeletion();
}

void PlanExecutionDriver::executeAsync(std::function<void()> on_result_ready) {
  // Lazy-init: completion_future_ is only valid after executeAsync() is called.
  // Must not be in constructor — tests that skip start() would hang in join().
  completion_future_ = completion_promise_.get_future();

  // Prefer the process-global folly CPUThreadPoolExecutor (owned by
  // NativeEngineLifecycle). Pool threads return immediately after submitting
  // the Velox task — completion is signaled via completion_promise_ from
  // the async continuation chain (Prestissimo pattern).
  auto exec = pangrova::jni::NativeEngineLifecycle::instance().executor();
  auto execute = [this, on_result_ready = std::move(on_result_ready)] {
    executeVeloxPlan(on_result_ready);
  };
  if (exec) {
    exec->add(std::move(execute));
    return;
  }
  // Fallback for unit tests that don't initialize the global lifecycle.
  fallback_thread_ = std::make_unique<std::thread>(std::move(execute));
}

void PlanExecutionDriver::join() {
  if (completion_future_.valid()) {
    completion_future_.wait();
  }
  if (fallback_thread_ && fallback_thread_->joinable()) {
    fallback_thread_->join();
  }
}

void PlanExecutionDriver::release() {
  join();
  closePhysicalWriter();
  closeDatasetWriter();
  std::shared_ptr<velox::VeloxTaskRunner> runner;
  {
    std::lock_guard<std::mutex> lock(runner_mutex_);
    runner = active_runner_;
  }
  std::shared_ptr<resource::TaskObjectReadBudget> read_budget;
  {
    std::lock_guard<std::mutex> lock(runner_mutex_);
    read_budget = task_read_budget_;
  }
  if (read_budget) read_budget->requestCancel();
  if (!runner) {
    if (read_budget) read_budget->stopAndDrain();
    return;
  }
  runner->cancel();
  if (runner->hasResultOutput() && !runner->resultDrainEvidence().drained()) {
    throw std::runtime_error("Result RPC readers or exported buffers still own task resources");
  }
  auto stats = collectTaskStats();
  {
    std::lock_guard<std::mutex> lock(runner_mutex_);
    if (stats) final_task_stats_ = std::move(stats);
  }
  if (read_budget) read_budget->stopAndDrain();
  runner->releaseTaskAndWaitForDeletion();
}

void PlanExecutionDriver::closePhysicalWriter() {
  if (!physical_writer_) return;
  try {
    physical_writer_->close();
  } catch (const std::exception& failure) {
    lifecycle_.appendError(std::string("Physical writer release failed: ") + failure.what());
    if (!physical_writer_->isDrainedAndCleaned()) throw;
    physical_writer_->releaseDrained();
  }
  physical_writer_.reset();
}

void PlanExecutionDriver::closeDatasetWriter() {
  if (!dataset_writer_) return;
  try {
    dataset_writer_->close();
  } catch (const std::exception& failure) {
    lifecycle_.appendError(std::string("Dataset writer release failed: ") + failure.what());
    if (!dataset_writer_->isDrainedAndCleaned()) throw;
    dataset_writer_->releaseDrained();
  }
  dataset_writer_.reset();
}

std::optional<protocol::v2::NativeTaskStats> PlanExecutionDriver::collectTaskStats() const {
  // The runner fence also protects the final snapshot published before deletion.
  std::lock_guard<std::mutex> lock(runner_mutex_);
  if (final_task_stats_) return final_task_stats_;
  return active_runner_ ? active_runner_->collectTaskStats(&metrics_) : std::nullopt;
}

std::optional<protocol::v2::NativeObjectReadStats> PlanExecutionDriver::objectReadStats() const {
  std::shared_ptr<resource::TaskObjectReadBudget> owner;
  {
    std::lock_guard<std::mutex> lock(runner_mutex_);
    owner = task_read_budget_;
  }
  if (!owner) return std::nullopt;
  const auto sample = owner->snapshot();
  protocol::v2::NativeObjectReadStats stats;
  stats.set_requested_bytes(sample.requested_bytes);
  stats.set_returned_bytes(sample.returned_bytes);
  stats.set_payload_calls(sample.payload_calls);
  stats.set_metadata_calls(sample.metadata_calls);
  stats.set_failed_calls(sample.failed_calls);
  return stats;
}

bool PlanExecutionDriver::hasFinalTaskStats() const {
  std::lock_guard<std::mutex> lock(runner_mutex_);
  return final_task_stats_.has_value();
}

void PlanExecutionDriver::cancel() {
  if (shuffle_queue_ && !shuffle_queue_->abort()) {
    return;  // direct Flight commit already won
  }
  cancelled_.store(true);
  markWriteProducerCancelled(write_producer_outcome_);

  // Propagate cancel to active VeloxTaskRunner if present.
  {
    std::lock_guard<std::mutex> lock(runner_mutex_);
    if (task_read_budget_) task_read_budget_->requestCancel();
    if (active_runner_) {
      active_runner_->cancel();
    }
  }

  // Close output queue to unblock any waiting consumers.
  if (output_queue_) {
    output_queue_->close();
  }
}

bool PlanExecutionDriver::isCancelled() const {
  return cancelled_.load();
}

void PlanExecutionDriver::notifyFailure(const std::string& message) {
  markWriteProducerFailed(write_producer_outcome_);
  if (shuffle_queue_) shuffle_queue_->abort();
  if (output_queue_) output_queue_->close();
  lifecycle_.notifyTaskFailed(message);
}

velox::VeloxTaskRunner* PlanExecutionDriver::veloxTaskRunner() const {
  std::lock_guard<std::mutex> lock(runner_mutex_);
  return active_runner_.get();
}

void PlanExecutionDriver::watchResultDrain(std::function<void()> listener) const {
  std::shared_ptr<velox::VeloxTaskRunner> runner;
  {
    std::lock_guard<std::mutex> lock(runner_mutex_);
    runner = active_runner_;
  }
  if (!runner) throw std::logic_error("Finished result task has no active runner");
  runner->watchResultDrain(std::move(listener));
}

std::optional<facebook::velox::RowTypePtr>
PlanExecutionDriver::getScanNodeOutputType(
    const std::string& plan_node_id) const {
  std::lock_guard<std::mutex> lock(runner_mutex_);
  auto it = scan_node_output_types_.find(plan_node_id);
  if (it != scan_node_output_types_.end()) {
    return it->second;
  }
  return std::nullopt;
}

std::string PlanExecutionDriver::resolveScanNodeId(
    const std::string& requested_plan_node_id) const {
  std::lock_guard<std::mutex> lock(runner_mutex_);
  if (requested_plan_node_id.empty()) {
    if (scan_node_output_types_.size() == 1) return scan_node_output_types_.begin()->first;
    throw std::runtime_error("Runtime filter requires exactly one Native table scan");
  }
  if (scan_node_output_types_.contains(requested_plan_node_id)) return requested_plan_node_id;
  throw std::runtime_error("Runtime filter references unknown Native scan node " + requested_plan_node_id);
}

void PlanExecutionDriver::injectDynamicFilter(const protocol::v2::RuntimeFilterBinding& binding) {
  const auto& plan_node_id = binding.plan_node_id();
  if ((!binding.sole_table_scan() && plan_node_id.empty()) || binding.filters().empty()) {
    throw std::invalid_argument("Native runtime filters require a plan_node_id or sole table scan and predicates");
  }

  auto converted = pangrova::velox::convertDynamicFilters(binding.filters());
  std::shared_ptr<velox::VeloxTaskRunner> runner;
  {
    std::lock_guard<std::mutex> lock(runner_mutex_);
    if (!active_runner_) {
      for (auto& converted_filter : converted) {
        pending_dynamic_filters_.push_back(PendingDynamicFilter{
            plan_node_id,
            std::move(converted_filter.column_name),
            std::move(converted_filter.filter)});
      }
      return;
    }
    runner = active_runner_;
  }

  for (auto& converted_filter : converted) {
    applyDynamicFilter(
        runner,
        PendingDynamicFilter{
            plan_node_id,
            std::move(converted_filter.column_name),
            std::move(converted_filter.filter)});
  }
}

void PlanExecutionDriver::applyDynamicFilter(
    const std::shared_ptr<velox::VeloxTaskRunner>& runner,
    PendingDynamicFilter filter) {
  const std::string resolved_plan_node_id = resolveScanNodeId(filter.requested_plan_node_id);
  auto output_type = getScanNodeOutputType(resolved_plan_node_id);
  if (!output_type.has_value()) {
    throw std::runtime_error(
        "No output type for Native runtime-filter scan node " +
        resolved_plan_node_id);
  }
  const int resolved_column = pangrova::velox::resolveColumnIndex(
      filter.column_name, output_type.value());
  if (resolved_column < 0) {
    throw std::runtime_error(
        "Cannot resolve Native runtime-filter column " + filter.column_name +
        " for plan node " + resolved_plan_node_id);
  }
  runner->injectDynamicFilter(
      resolved_plan_node_id,
      static_cast<facebook::velox::column_index_t>(resolved_column),
      std::move(filter.filter));
}


}  // namespace pangrova::executor
