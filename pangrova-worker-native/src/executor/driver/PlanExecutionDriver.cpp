#include "executor/driver/PlanExecutionDriver.h"

#include <algorithm>
#include <future>
#include <stdexcept>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/Future.h>

#include "artifact/ArtifactRetrievalPlanner.h"
#include "executor/driver/PlanExecutionWriteSinks.h"
#include "executor/driver/PlanExecutionConfiguration.h"
#include "executor/driver/TaskExecutor.h"
#include "executor/NativeUnsupportedFeatureException.h"
#include "jni/RuntimeInitializer.h"
#include "observability/PangrovaLog.h"
#include "observability/NativeTracer.h"
#include "observability/SpanScope.h"
#include "shuffle/ShuffleEgressExecutor.h"
#include "velox/exec/Task.h"
#include "velox/memory/VeloxMemoryGovernor.h"
#include "velox/plan/ArtifactBuildOperator.h"
#include "velox/plan/VeloxPlanConverter.h"
#include "velox/runner/SingleMatchJoinGuard.h"
#include "velox/runner/VeloxTaskRunner.h"
#include "velox/runtime/NativeEngineLifecycle.h"

#if __has_include(<glog/logging.h>)
#include <glog/logging.h>
#define PANGROVA_EXEC_LOG_ERROR LOG(ERROR)
#else
#include <iostream>
#define PANGROVA_EXEC_LOG_ERROR std::cerr << "[ERROR] "
#endif

namespace pangrova::executor {
void PlanExecutionDriver::executeVeloxPlan() {
  // RAII guard: sets completion_promise_ when this function exits without
  // arming the async continuation (i.e., on all early-return paths and in
  // the blocking unit-test fallback). Prevents join() from hanging.
  bool armed = false;
  struct ExitGuard {
    std::promise<void>& p;
    bool& armed;
    ~ExitGuard() {
      if (!armed) {
        try { p.set_value(); } catch (...) {}
      }
    }
  } exit_guard{completion_promise_, armed};

  // Early exit if already cancelled before execution starts.
  if (cancelled_.load()) {
    return;
  }

  // Create tracing span when trace_id is available and tracer is initialized.
  // SpanScope is RAII — span ends automatically when scope exits.
  std::unique_ptr<observability::SpanScope> span;
  if (!config_.trace_id.empty() && observability::NativeTracer::isInitialized()) {
    span = observability::NativeTracer::startSpanWithParent(
        config_.trace_id, config_.span_id, "native.task.execute");
  }

  try {
    // Convert Substrait plan to Velox PlanNode.
    // Sub-span for plan conversion phase.
    std::unique_ptr<observability::SpanScope> convertSpan;
    if (span && span->isValid()) {
      convertSpan = observability::NativeTracer::startSpanWithParent(
          span->traceIdHex(), span->spanIdHex(), "native.plan.convert");
    }
    const auto spill_policy = resolveWorkerLocalSpillPolicy(
        config_.resource_limit.max_spill_bytes,
        pangrova::jni::globalVeloxMemoryGovernor());
    auto artifact_context = std::make_shared<velox::ArtifactBuildTaskContext>();
    artifact_context->job_id = config_.query_id;
    artifact_context->page_id = config_.task_id;
    artifact_context->fencing_token = config_.fencing_token;
    artifact_context->scratch_directory = spill_policy.directory;
    artifact_context->max_input_bytes =
        static_cast<uint64_t>(std::max<int64_t>(
            0, config_.resource_limit.max_memory_bytes));
    artifact_context->max_spill_bytes =
        static_cast<uint64_t>(std::max<int64_t>(
            0, spill_policy.max_bytes));
    artifact_context->max_object_store_read_bytes =
        static_cast<uint64_t>(std::max<int64_t>(
            0, config_.resource_limit.max_object_store_read_bytes));
    artifact_context->max_object_store_write_bytes =
        static_cast<uint64_t>(std::max<int64_t>(
            0, config_.resource_limit.max_object_store_write_bytes));
    artifact_context->report_state = config_.artifact_report_state;

    auto retrieval_preparation = artifact::ArtifactRetrievalPlanner::prepare(
        config_.substrait_plan,
        config_.operator_bindings,
        config_.splits,
        static_cast<uint64_t>(std::max<int64_t>(
            0, config_.resource_limit.max_object_store_read_bytes)));
    const auto& execution_splits = retrieval_preparation->splits;

    velox::VeloxPlanConverter converter;
    auto velox_plan = converter.convert(
        config_.substrait_plan, execution_splits, config_.is_result_task,
        "ArrowBatch", config_.operator_bindings, artifact_context,
        retrieval_preparation->split_overrides,
        config_.num_shuffle_partitions);
    auto scan_split_overrides = converter.scanNodeSplitOverrides();
    auto single_match_join_requirements = converter.singleMatchJoinRequirements();
    convertSpan.reset();  // End plan conversion span.
    if (!velox_plan) {
      if (shuffle_queue_) shuffle_queue_->abort();
      notifyFailure(
          "Failed to convert Substrait plan to Velox PlanNode");
      return;
    }
    const auto write_output_type = velox_plan->outputType();

    // Persist scan node output types for dynamic filter column resolution.
    // VeloxPlanConverter is local — copy the map before it goes out of scope.
    {
      std::lock_guard<std::mutex> lock(runner_mutex_);
      scan_node_output_types_ = converter.scanNodeOutputTypes();
    }

    // Check cancel after plan conversion (before expensive execution).
    if (cancelled_.load()) {
      return;
    }

    auto runner_config = makeRunnerConfig(config_, converter, spill_policy);

    auto runner = std::make_shared<velox::VeloxTaskRunner>(
        runner_config, velox_plan, execution_splits, output_queue_,
        shuffle_queue_, std::move(scan_split_overrides));

    // Submission-time filters are carried by the canonical TaskRequest. Queue
    // them through the same validated Native path before publishing the runner.
    for (const auto& binding : config_.initial_runtime_filters) {
      pangrova::protocol::v2::InjectRuntimeFilterRequest payload;
      payload.set_plan_node_id(binding.plan_node_id());
      payload.mutable_filters()->CopyFrom(binding.filters());
      injectDynamicFilter(binding.plan_node_id(), payload.SerializeAsString());
    }

    // Publish runner under lock so cancel() can reach it.
    std::vector<PendingDynamicFilter> startup_filters;
    {
      std::lock_guard<std::mutex> lock(runner_mutex_);
      active_runner_ = runner;
      startup_filters.swap(pending_dynamic_filters_);
    }
    for (auto& filter : startup_filters) {
      applyDynamicFilter(runner, std::move(filter));
    }

    // If cancel arrived between creation and publish, propagate immediately.
    if (cancelled_.load()) {
      runner->cancel();
      // Clear runner reference under lock.
      std::lock_guard<std::mutex> lock(runner_mutex_);
      active_runner_.reset();
      return;
    }

    runner->start();

    auto shuffle_drain_exc = std::make_shared<std::exception_ptr>(nullptr);
    std::shared_ptr<std::thread> shuffle_drain_thread;
    if (config_.shuffle_egress) {
      shuffle_drain_thread = std::make_shared<std::thread>(
          [this, runner, shuffle_drain_exc]() {
            try {
              drainShuffleToEgress(shuffle_queue_, config_);
            } catch (...) {
              *shuffle_drain_exc = std::current_exception();
              config_.shuffle_egress->onTaskAborted(
                  config_.query_id, config_.task_id,
                  "Native external shuffle drain failed");
              runner->cancel();
            }
          });
    }

    // WRITE task deadlock prevention: For write tasks, Velox's consumer
    // callback pushes RowVectors to OutputQueue. If OutputQueue fills up,
    // the Velox Driver suspends (kWaitForConsumer) and taskCompletionFuture
    // never resolves — because drainOutputToWriter (the consumer) only
    // starts AFTER task completion. This creates a deadlock:
    //   Velox Driver → OutputQueue full → waits for space
    //   taskCompletionFuture → waits for task to finish → never resolves
    //   drainOutputToWriter → never starts → queue never drained
    //
    // Fix: Start drainOutputToWriter on a separate thread BEFORE submitting
    // startAsync(), so the queue is actively consumed while Velox produces
    // data. drain_thread and drain_exc use shared_ptr to safely cross the
    // async continuation boundary.
    auto drain_exc = std::make_shared<std::exception_ptr>(nullptr);
    const auto write_producer_outcome = write_producer_outcome_;
    std::shared_ptr<std::thread> drain_thread;
    if (!config_.write_config.target_table.empty()) {
      drain_thread = std::make_shared<std::thread>(
          [this, runner, drain_exc, write_output_type, spill_policy,
           write_producer_outcome]() {
            try {
              if (!runner->veloxTask()) throw std::runtime_error("Write sink has no actual producer Task");
              auto pool_holder = runner->veloxTask()->pool()->addLeafChild("write-drain-" + config_.task_id);
              auto* write_pool = pool_holder.get();
              if (config_.write_config.physical_output) {
                drainOutputToPhysicalWriter(output_queue_, config_, write_output_type,
                    pool_holder, spill_policy, write_producer_outcome,
                    PhysicalWriterOwner{metrics_, physical_writer_});
              } else if (config_.write_config.file_format ==
                  split::FileFormatType::LANCE) {
                drainOutputToLance(
                    output_queue_, config_.write_config, config_.task_id,
                    config_.tenant_id, write_output_type, pool_holder,
                    write_producer_outcome, dataset_writer_);
              } else {
                drainOutputToWriter(
                    output_queue_, config_.write_config, config_.task_id,
                    write_pool, write_producer_outcome);
              }
            } catch (...) {
              *drain_exc = std::current_exception();
              // A failed sink cannot keep consuming. Cancel the producer and
              // close the queue so a backpressured Velox driver cannot hang.
              runner->cancel();
              if (output_queue_) output_queue_->close();
            }
          });
    }

    // Post-completion handler: runs after Velox task finishes (in the async
    // continuation or the blocking unit-test fallback). All state captured by
    // value / shared_ptr for async-boundary safety.
    //
    // Lifetime safety: this runs before completion_promise_.set_value().
    // join() / ~PlanExecutionDriver() wait on completion_future_, so `this`
    // is guaranteed valid for the entire duration of on_complete.
    auto on_complete = [this, runner, runner_config, drain_thread, drain_exc,
                        write_producer_outcome,
                        shuffle_drain_thread, shuffle_drain_exc,
                        artifact_context,
                        retrieval_preparation,
                        single_match_join_requirements](bool success) mutable {
      std::string single_match_failure;
      if (success && !cancelled_.load() &&
          !single_match_join_requirements.empty()) {
        try {
          if (!runner->veloxTask()) {
            throw std::runtime_error("LEFT_SINGLE join task has no Native statistics");
          }
          velox::validateSingleMatchJoinStats(
              runner->veloxTask()->taskStats(), single_match_join_requirements);
        } catch (const std::exception& e) {
          single_match_failure = e.what();
          success = false;
        }
      }
      std::string shuffle_flush_failure;
      if (success && !cancelled_.load() && shuffle_queue_) {
        try {
          runner->finalizeShuffleOutput();
        } catch (const std::exception& e) {
          shuffle_flush_failure = e.what();
          success = false;
        }
      }
      if ((!success || cancelled_.load()) && shuffle_queue_) {
        // Direct Flight consumers must observe an error, not a clean EOF.
        // A failed runner cannot retry on the same published shuffle stream.
        shuffle_queue_->abort();
      }
      if (shuffle_drain_thread) {
        if (shuffle_queue_) shuffle_queue_->close();
        shuffle_drain_thread->join();
        if (*shuffle_drain_exc) {
          if (cancelled_.load()) {
            markWriteProducerCancelled(write_producer_outcome);
          } else {
            markWriteProducerFailed(write_producer_outcome);
          }
          if (output_queue_) output_queue_->close();
          if (drain_thread && drain_thread->joinable()) drain_thread->join();
          std::rethrow_exception(*shuffle_drain_exc);
        }
        if (success && !cancelled_.load()) {
          config_.shuffle_egress->onTaskCompleted(
              config_.query_id, config_.task_id);
        } else {
          config_.shuffle_egress->onTaskAborted(
              config_.query_id, config_.task_id,
              cancelled_.load() ? "Native task cancelled" :
                                  "Native task execution failed");
        }
      }
      if (!success) {
        if (cancelled_.load()) {
          markWriteProducerCancelled(write_producer_outcome);
        } else {
          markWriteProducerFailed(write_producer_outcome);
          const auto error = !single_match_failure.empty() ? single_match_failure :
              (!shuffle_flush_failure.empty() ? shuffle_flush_failure : runner->errorMessage());
          PANGROVA_EXEC_LOG_ERROR << "plan_execution_failed"
              << " trace_id=" << config_.trace_id << " query_id=" << config_.query_id
              << " task_id=" << config_.task_id << " error_detail=" << error;
          notifyFailure("Native task failed: " + error);
        }
        if (output_queue_) output_queue_->close();
        if (drain_thread && drain_thread->joinable()) drain_thread->join();
        if (*drain_exc) std::rethrow_exception(*drain_exc);
        return;
      }

      // DO NOT reset active_runner_ here. The runner owns the Velox Task
      // whose QueryCtx memory pool backs all RowVectors in the OutputQueue.
      // If we destroy the runner now, those RowVectors become dangling —
      // the Velox task's consumer callback pushing to the OutputQueue
      // would SIGSEGV in AlignedBuffer::freeToPool() when releasing them.
      //
      // The runner stays alive until TaskExecutor::release() drains the
      // OutputQueue (releasing all RowVectors) and then destroys the
      // PlanExecutionDriver (which destructs active_runner_).

      if (!cancelled_.load()) {
        // WRITE task: wait for drain thread to finish consuming OutputQueue.
        // The Velox task has completed (all data pushed to OutputQueue),
        // so the drain thread will see CLOSED after consuming remaining items.
        if (drain_thread) {
          markWriteProducerSucceeded(write_producer_outcome);
          if (output_queue_) {
            output_queue_->close();
          }
          drain_thread->join();
          // Propagate drain thread exceptions.
          if (*drain_exc) {
            std::rethrow_exception(*drain_exc);
          }
          if (write_producer_outcome->load(std::memory_order_acquire) !=
              WriteProducerOutcome::FINISHED) {
            // Cancellation won the success/finalize race. TaskExecutor::cancel
            // owns the lifecycle transition; no staged output was published.
            return;
          }
        }

        // Non-shuffle output is already committed when the runner finishes.
        // Shuffle completion is published only after cleanup and a clean queue
        // close, so Java cannot unregister the Flight producer while buffered
        // rows are still being flushed or queued.
        if (!shuffle_queue_) {
          lifecycle_.notifyTaskComplete();  // RUNNING → FLUSHING
          lifecycle_.transitionTo(ExecutorState::FINISHED);  // FLUSHING → FINISHED
        }

        // G.2: Report spill bytes and decrement pool count on task completion.
        {
          std::lock_guard<std::mutex> lock(runner_mutex_);
          if (active_runner_ && active_runner_->veloxTask()) {
            auto taskStats = active_runner_->veloxTask()->taskStats();
            int64_t taskSpillBytes = 0;
            for (const auto& ps : taskStats.pipelineStats) {
              for (const auto& os : ps.operatorStats) {
                taskSpillBytes += os.spilledBytes;
              }
            }
            auto* memGovernor = pangrova::jni::globalVeloxMemoryGovernor();
            if (memGovernor) {
              memGovernor->reportSpillBytes(taskSpillBytes);
            }
          }
        }

        // Cleanup runner resources (close output queue, release spill dirs)
        // after FINISHED transition. Safe because OutputQueue data has already
        // been drained to the writer above.
        {
          std::lock_guard<std::mutex> lock(runner_mutex_);
          if (active_runner_) {
            active_runner_->cleanup();
          }
        }
        if (shuffle_queue_) {
          // The external egress drain was closed and joined above. Direct
          // Flight keeps the queue open until every fallible success step has
          // completed, then commits EOF before reporting FINISHED.
          if (!shuffle_queue_->commit()) {
            // Cancellation/deadline won the queue outcome while cleanup ran.
            // Its owner publishes the failed or cancelled task state.
            return;
          }
          lifecycle_.notifyTaskComplete();
          lifecycle_.transitionTo(ExecutorState::FINISHED);
        }
      } else {
        markWriteProducerCancelled(write_producer_outcome);
        if (output_queue_) output_queue_->close();
        if (drain_thread && drain_thread->joinable()) drain_thread->join();
        if (*drain_exc) std::rethrow_exception(*drain_exc);
      }
    };

    // Dispatch: prefer non-blocking async path on the global executor.
    // When the global executor is absent (unit tests without NativeEngineLifecycle),
    // fall back to a synchronous waitForCompletion() on the current thread.
    auto exec = pangrova::jni::NativeEngineLifecycle::instance().executor();
    if (!exec) {
      // Blocking fallback for unit tests. waitForCompletion() calls
      // velox_task_->start() and blocks until the Velox task finishes.
      bool ok = runner->waitForCompletion();
      on_complete(ok);
      return;  // exit_guard fires: completion_promise_.set_value()
    }

    // Arm the Prestissimo-style async continuation.
    // startAsync() calls velox_task_->start() and returns a SemiFuture.
    // via(exec) schedules the continuation on the folly CPUThreadPoolExecutor.
    // thenValue fires when the Velox task reaches a terminal state.
    // The folly pool thread (this function's caller) returns immediately.
    //
    // P1-FIX: Only disarm ExitGuard AFTER the async chain is fully
    // constructed. If chain construction throws (e.g., OOM during
    // lambda capture or executor internal error), armed stays false
    // and ExitGuard will set completion_promise_, preventing join()
    // from deadlocking.
    runner->startAsync()
        .via(exec.get())
        .thenValue([on_complete = std::move(on_complete),
                    this](bool success) mutable {
          // RAII: always set completion_promise_ even if on_complete throws
          // (e.g., drain exception rethrown after successful Velox execution).
          struct CompGuard {
            std::promise<void>& p;
            bool done = false;
            ~CompGuard() {
              if (!done) {
                try { p.set_value(); } catch (...) {}
              }
            }
          } comp_guard{completion_promise_};
          try {
            on_complete(success);
          } catch (const std::exception& e) {
            if (shuffle_queue_) shuffle_queue_->abort();
            if (!cancelled_.load()) {
              notifyFailure(
                  std::string("Post-completion failed: ") + e.what());
            } else {
              lifecycle_.appendError(std::string("Post-cancellation cleanup failed: ") + e.what());
            }
          } catch (...) {
            if (shuffle_queue_) shuffle_queue_->abort();
            if (!cancelled_.load()) {
              notifyFailure(
                  "Post-completion failed: unknown error");
            } else {
              lifecycle_.appendError("Post-cancellation cleanup failed: unknown error");
            }
          }
          comp_guard.done = true;
          completion_promise_.set_value();
        });
    // Chain fully constructed — disarm ExitGuard.
    armed = true;

  } catch (const NativeUnsupportedFeatureException& e) {
    if (shuffle_queue_) shuffle_queue_->abort();
    // DO NOT reset active_runner_ here — same reason as the success path.
    // OutputQueue may hold RowVectors backed by the runner's memory pool.
    // TaskExecutor::release() will drain the queue before destroying us.
    if (!cancelled_.load()) {
      PANGROVA_EXEC_LOG_ERROR << "native_unsupported_feature"
                            << " trace_id=" << config_.trace_id
                            << " query_id=" << config_.query_id
                            << " task_id=" << config_.task_id
                            << " error_type=unsupported_feature"
                            << " feature_name=" << e.featureName()
                            << " connector_type=" << e.connectorType()
                            << " split_id=" << e.splitId();
      notifyFailure(
          std::string("NATIVE_FALLBACK_REQUIRED: ") + e.what());
    } else {
      lifecycle_.appendError(std::string("Cancelled execution failed: ") + e.what());
    }
    // exit_guard fires (armed=false): completion_promise_.set_value()
  } catch (const std::exception& e) {
    if (shuffle_queue_) shuffle_queue_->abort();
    // DO NOT reset active_runner_ here — same reason as above.
    if (!cancelled_.load()) {
      PANGROVA_EXEC_LOG_ERROR << "velox_execution_exception"
                            << " trace_id=" << config_.trace_id
                            << " query_id=" << config_.query_id
                            << " task_id=" << config_.task_id
                            << " error_type=runtime_exception"
                            << " error_detail=" << e.what();
      notifyFailure(
          std::string("Velox execution failed: ") + e.what());
    } else {
      lifecycle_.appendError(std::string("Cancelled execution failed: ") + e.what());
    }
    // exit_guard fires (armed=false): completion_promise_.set_value()
  }
}

}  // namespace pangrova::executor
