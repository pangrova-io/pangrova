#include "executor/driver/PlanExecutionDriver.h"

#include <algorithm>
#include <stdexcept>
#include <folly/executors/CPUThreadPoolExecutor.h>

#include "executor/driver/TaskExecutor.h"
#include "resource/TaskObjectReadBudget.h"
#include "jni/RuntimeInitializer.h"
#include "jni/command/StatusSerializer.h"
#include "lance/LanceBatchWriter.h"
#include "lance/LancePhysicalBatchWriter.h"
#include "pangrova/protocol/v2/runtime.pb.h"
#include "velox/filter/DynamicFilterConverter.h"
#include "velox/exec/Task.h"
#include "velox/memory/VeloxMemoryGovernor.h"
#include "velox/runner/VeloxTaskRunner.h"
#include "velox/runtime/NativeEngineLifecycle.h"

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

void PlanExecutionDriver::executeAsync() {
  // Lazy-init: completion_future_ is only valid after executeAsync() is called.
  // Must not be in constructor — tests that skip start() would hang in join().
  completion_future_ = completion_promise_.get_future();

  // Prefer the process-global folly CPUThreadPoolExecutor (owned by
  // NativeEngineLifecycle). Pool threads return immediately after submitting
  // the Velox task — completion is signaled via completion_promise_ from
  // the async continuation chain (Prestissimo pattern).
  auto exec = pangrova::jni::NativeEngineLifecycle::instance().executor();
  if (exec) {
    exec->add([this]() { executeVeloxPlan(); });
    return;
  }
  // Fallback for unit tests that don't initialize the global lifecycle.
  fallback_thread_ = std::make_unique<std::thread>(
      [this]() { executeVeloxPlan(); });
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
    if (!stats.empty()) final_task_stats_ = std::move(stats);
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

std::vector<uint8_t> PlanExecutionDriver::collectTaskStats() const {
  // Hold the runner fence until this actual snapshot is observed/serialized;
  // release publishes final_task_stats_ under the same fence before deletion.
  std::lock_guard<std::mutex> lock(runner_mutex_);
  if (!final_task_stats_.empty()) return final_task_stats_;
  auto* task = active_runner_ ? active_runner_->veloxTask() : nullptr;
  if (!task) return {};
  const auto stats = task->taskStats();
  metrics_.observeColumnarReadTaskStats(stats);
  return pangrova::jni::serializeNativeTaskStats(stats);
}

bool PlanExecutionDriver::hasFinalTaskStats() const {
  std::lock_guard<std::mutex> lock(runner_mutex_);
  return !final_task_stats_.empty();
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
    const std::string& requested_plan_node_id,
    const std::string& column_name) const {
  std::lock_guard<std::mutex> lock(runner_mutex_);
  auto exact = scan_node_output_types_.find(requested_plan_node_id);
  if (exact != scan_node_output_types_.end()) {
    return exact->first;
  }

  std::string match;
  size_t match_count = 0;
  for (const auto& [node_id, output_type] : scan_node_output_types_) {
    if (!output_type || !output_type->containsChild(column_name)) {
      continue;
    }
    match = node_id;
    ++match_count;
  }
  if (match_count == 1) {
    return match;
  }
  if (match_count == 0) {
    throw std::runtime_error(
        "No scan node exposes runtime-filter column " + column_name +
        " (requested plan node " + requested_plan_node_id + ")");
  }
  throw std::runtime_error(
      "Ambiguous runtime-filter target for column " + column_name +
      ": " + std::to_string(match_count) +
      " scan nodes match; an exact plan_node_id is required");
}

void PlanExecutionDriver::injectDynamicFilter(
    const std::string& plan_node_id,
    const std::string& filter_proto) {
  pangrova::protocol::v2::InjectRuntimeFilterRequest payload;
  if (!payload.ParseFromString(filter_proto) || payload.plan_node_id().empty() ||
      payload.plan_node_id() != plan_node_id) {
    throw std::invalid_argument(
        "Invalid Native runtime-filter command payload or plan_node_id");
  }

  auto converted = pangrova::velox::convertDynamicFilters(payload);
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
  const std::string resolved_plan_node_id = resolveScanNodeId(
      filter.requested_plan_node_id, filter.column_name);
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
