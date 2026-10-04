#include "executor/driver/PlanExecutionDriver.h"

#include <algorithm>
#include <stdexcept>
#include <folly/ScopeGuard.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/Future.h>

#include "executor/driver/PlanExecutionConfiguration.h"
#include "executor/driver/PlanExecutionState.h"
#include "executor/driver/PlanExecutionWriteSinks.h"
#include "executor/driver/TaskExecutor.h"
#include "executor/NativeUnsupportedFeatureException.h"
#include "jni/RuntimeInitializer.h"
#include "observability/NativeTracer.h"
#include "observability/PangrovaLog.h"
#include "observability/SpanScope.h"
#include "pangrova/protocol/v2/runtime.pb.h"
#include "shuffle/ShuffleEgressExecutor.h"
#include "velox/exec/Task.h"
#include "velox/memory/VeloxMemoryGovernor.h"
#include "velox/plan/VeloxPlanConverter.h"
#include "velox/runtime/NativeEngineLifecycle.h"

namespace pangrova::executor {

void PlanExecutionDriver::signalCompletion() {
  if (!completion_signalled_.exchange(true)) completion_promise_.set_value();
}

void PlanExecutionDriver::executeVeloxPlan() {
  auto completion = folly::makeGuard([this] { signalCompletion(); });
  if (cancelled_.load()) return;
  std::shared_ptr<Execution> execution;
  try {
    std::unique_ptr<observability::SpanScope> span;
    if (!config_.trace_id.empty() && observability::NativeTracer::isInitialized()) {
      span = observability::NativeTracer::startSpanWithParent(
          config_.trace_id, config_.span_id, "native.task.execute");
    }
    execution = prepareExecution(span.get());
    if (!execution || !publishRunner(execution)) return;
    execution->runner->start();
    startOutputDrains(execution);
    if (dispatchExecution(execution)) completion.dismiss();
  } catch (...) {
    auto failure = std::current_exception();
    const auto cleanupErrors = stopExecution(execution);
    reportExecutionFailure(failure, "Velox execution failed", cleanupErrors);
  }
}

std::shared_ptr<PlanExecutionDriver::Execution>
PlanExecutionDriver::prepareExecution(observability::SpanScope* span) {
  std::unique_ptr<observability::SpanScope> conversionSpan;
  if (span && span->isValid()) {
    conversionSpan = observability::NativeTracer::startSpanWithParent(
        span->traceIdHex(), span->spanIdHex(), "native.plan.convert");
  }
  auto execution = std::make_shared<Execution>();
  execution->spillPolicy = resolveWorkerLocalSpillPolicy(
      config_.resource_limit.max_spill_bytes, jni::globalVeloxMemoryGovernor());
  auto context = std::make_shared<velox::ArtifactBuildTaskContext>();
  context->job_id = config_.query_id;
  context->page_id = config_.task_id;
  context->fencing_token = config_.fencing_token;
  context->scratch_directory = execution->spillPolicy.directory;
  context->max_input_bytes = static_cast<uint64_t>(
      std::max<int64_t>(0, config_.resource_limit.max_memory_bytes));
  context->max_spill_bytes = static_cast<uint64_t>(
      std::max<int64_t>(0, execution->spillPolicy.max_bytes));
  context->max_object_store_read_bytes = static_cast<uint64_t>(
      std::max<int64_t>(0, config_.resource_limit.max_object_store_read_bytes));
  context->max_object_store_write_bytes = static_cast<uint64_t>(
      std::max<int64_t>(0, config_.resource_limit.max_object_store_write_bytes));
  context->report_state = config_.artifact_report_state;
  execution->artifactContext = std::move(context);
  execution->retrieval = artifact::ArtifactRetrievalPlanner::prepare(
      config_.substrait_plan, config_.operator_bindings, config_.splits,
      static_cast<uint64_t>(
          std::max<int64_t>(0, config_.resource_limit.max_object_store_read_bytes)));
  velox::VeloxPlanConverter converter(config_.function_registry);
  auto plan = converter.convert(config_.substrait_plan, execution->retrieval->splits,
      config_.is_result_task, "ArrowBatch", config_.operator_bindings,
      execution->artifactContext, execution->retrieval->split_overrides,
      config_.num_shuffle_partitions);
  if (!plan) throw std::runtime_error("Failed to convert Substrait plan to Velox PlanNode");
  execution->writeOutputType = plan->outputType();
  execution->singleMatchRequirements = converter.singleMatchJoinRequirements();
  {
    std::lock_guard<std::mutex> lock(runner_mutex_);
    scan_node_output_types_ = converter.scanNodeOutputTypes();
  }
  if (cancelled_.load()) return nullptr;
  execution->runner = std::make_shared<velox::VeloxTaskRunner>(
      makeRunnerConfig(config_, converter, execution->spillPolicy), plan,
      execution->retrieval->splits, output_queue_, shuffle_queue_,
      converter.scanNodeSplitOverrides());
  return execution;
}

bool PlanExecutionDriver::publishRunner(const std::shared_ptr<Execution>& execution) {
  for (const auto& binding : config_.initial_runtime_filters) {
    protocol::v2::InjectRuntimeFilterRequest payload;
    payload.set_plan_node_id(binding.plan_node_id());
    payload.mutable_filters()->CopyFrom(binding.filters());
    injectDynamicFilter(binding.plan_node_id(), payload.SerializeAsString());
  }
  std::vector<PendingDynamicFilter> filters;
  {
    std::lock_guard<std::mutex> lock(runner_mutex_);
    active_runner_ = execution->runner;
    filters.swap(pending_dynamic_filters_);
  }
  for (auto& filter : filters) applyDynamicFilter(execution->runner, std::move(filter));
  if (!cancelled_.load()) return true;
  execution->runner->cancel();
  std::lock_guard<std::mutex> lock(runner_mutex_);
  active_runner_.reset();
  return false;
}

void PlanExecutionDriver::startOutputDrains(const std::shared_ptr<Execution>& execution) {
  if (config_.shuffle_egress) {
    execution->shuffleDrain = std::make_unique<std::thread>([this, execution] {
      try {
        drainShuffleToEgress(shuffle_queue_, config_);
      } catch (...) {
        execution->shuffleFailure = std::current_exception();
        config_.shuffle_egress->onTaskAborted(
            config_.query_id, config_.task_id, "Native external shuffle drain failed");
        execution->runner->cancel();
      }
    });
  }
  if (!config_.write_config.target_table.empty()) {
    execution->writeDrain = std::make_unique<std::thread>([this, execution] {
      try {
        drainWriteOutput(execution);
      } catch (...) {
        execution->writeFailure = std::current_exception();
        execution->runner->cancel();
        if (output_queue_) output_queue_->close();
      }
    });
  }
}

void PlanExecutionDriver::drainWriteOutput(const std::shared_ptr<Execution>& execution) {
  const auto& runner = execution->runner;
  if (!runner->veloxTask()) throw std::runtime_error("Write sink has no actual producer Task");
  auto pool = runner->veloxTask()->pool()->addLeafChild("write-drain-" + config_.task_id);
  if (config_.write_config.physical_output) {
    drainOutputToPhysicalWriter(output_queue_, config_, execution->writeOutputType,
        pool, execution->spillPolicy, write_producer_outcome_,
        PhysicalWriterOwner{metrics_, physical_writer_});
  } else if (config_.write_config.file_format == split::FileFormatType::LANCE) {
    drainOutputToLance(output_queue_, config_.write_config, config_.task_id,
        config_.tenant_id, execution->writeOutputType, pool,
        write_producer_outcome_, dataset_writer_);
  } else {
    drainOutputToWriter(output_queue_, config_.write_config, config_.task_id,
        pool, write_producer_outcome_);
  }
}

bool PlanExecutionDriver::dispatchExecution(const std::shared_ptr<Execution>& execution) {
  auto executor = jni::NativeEngineLifecycle::instance().executor();
  if (!executor) {
    completeExecution(execution, execution->runner->waitForCompletion());
    return false;
  }
  execution->runner->startAsync().via(executor.get()).thenTry(
      [this, execution](folly::Try<bool>&& outcome) {
        auto completion = folly::makeGuard([this] { signalCompletion(); });
        try {
          completeExecution(execution, outcome.value());
        } catch (...) {
          auto failure = std::current_exception();
          const auto cleanupErrors = stopExecution(execution);
          reportExecutionFailure(failure, "Post-completion failed", cleanupErrors);
        }
      });
  return true;
}

std::string PlanExecutionDriver::stopExecution(const std::shared_ptr<Execution>& execution) {
  std::string errors;
  const auto attempt = [&errors](auto&& cleanup) {
    try {
      cleanup();
    } catch (const std::exception& failure) {
      errors += "; cleanup: " + std::string(failure.what());
    } catch (...) {
      errors += "; cleanup: unknown error";
    }
  };
  if (cancelled_.load()) markWriteProducerCancelled(write_producer_outcome_);
  else markWriteProducerFailed(write_producer_outcome_);
  if (shuffle_queue_) attempt([this] { shuffle_queue_->abort(); });
  if (output_queue_) attempt([this] { output_queue_->close(); });
  if (!execution) return errors;
  if (execution->runner) attempt([&] { execution->runner->cancel(); });
  if (execution->shuffleDrain && execution->shuffleDrain->joinable()) {
    attempt([&] { execution->shuffleDrain->join(); });
  }
  if (execution->writeDrain && execution->writeDrain->joinable()) {
    attempt([&] { execution->writeDrain->join(); });
  }
  return errors;
}

void PlanExecutionDriver::reportExecutionFailure(std::exception_ptr failure,
                                                const char* context,
                                                const std::string& cleanupErrors) {
  std::string detail;
  try {
    std::rethrow_exception(failure);
  } catch (const NativeUnsupportedFeatureException& unsupported) {
    detail = unsupported.what();
    PANGROVA_LOG_ERROR << "native_unsupported_feature trace_id=" << config_.trace_id
                      << " query_id=" << config_.query_id << " task_id=" << config_.task_id
                      << " feature_name=" << unsupported.featureName()
                      << " connector_type=" << unsupported.connectorType()
                      << " split_id=" << unsupported.splitId();
  } catch (const std::exception& error) {
    detail = error.what();
  } catch (...) {
    detail = "unknown error";
  }
  const std::string message = std::string(context) + ": " + detail + cleanupErrors;
  PANGROVA_LOG_ERROR << "native_execution_failure trace_id=" << config_.trace_id
                    << " query_id=" << config_.query_id << " task_id=" << config_.task_id
                    << " error_detail=" << message;
  if (cancelled_.load()) lifecycle_.appendError(message);
  else notifyFailure(message);
}

}  // namespace pangrova::executor
