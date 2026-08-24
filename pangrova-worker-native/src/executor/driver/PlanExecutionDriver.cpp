// PlanExecutionDriver: Velox plan execution logic extracted from TaskExecutor.
// Converts Substrait plan → Velox PlanNode → Velox Task → OutputQueue.
// Runs on a background thread spawned by executeAsync().
//
// cancel() propagates to VeloxTaskRunner via shared_ptr under mutex,
// ensuring native-side memory and compute resources are released.
#include "executor/driver/PlanExecutionDriver.h"

#include <future>
#include <stdexcept>

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/Future.h>
#include "velox/runtime/NativeEngineLifecycle.h"

#include <arrow/buffer.h>
#include <arrow/c/bridge.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <velox/vector/LazyVector.h>
#include <velox/vector/arrow/Bridge.h>

#include "executor/driver/TaskExecutor.h"
#include "executor/NativeUnsupportedFeatureException.h"
#include "jni/bridge/ColumnarDataExporter.h"
#include "jni/bridge/GlobalWriterRegistry.h"
#include "jni/bridge/JniBridge.h"
#include "jni/bridge/JniExceptionScope.h"
#include "observability/PangrovaLog.h"
#include "jni/RuntimeInitializer.h"
#include "velox/memory/VeloxMemoryGovernor.h"
#include "write/NativeWriteExecutor.h"
#include "observability/NativeTracer.h"
#include "observability/SpanScope.h"
#include "shuffle/SerializedShuffleQueue.h"
#include "velox/plan/VeloxPlanConverter.h"
#include "velox/runner/VeloxTaskRunner.h"
#include "executor/queue/OutputQueue.h"
#include "velox/exec/Task.h"

// Structured logging for error propagation observability.
#if __has_include(<glog/logging.h>)
#include <glog/logging.h>
#define PANGROVA_EXEC_LOG_ERROR LOG(ERROR)
#else
#include <iostream>
#define PANGROVA_EXEC_LOG_ERROR std::cerr << "[ERROR] "
#endif

namespace pangrova::executor {

namespace {

// Maps write_mode string from WriteConfig to WriteDescriptor int32_t.
// 0=INSERT, 1=OVERWRITE (used for UPDATE/MERGE), 2=DELETE.
int32_t mapWriteMode(const std::string& mode) {
  if (mode == "DELETE") return 2;
  if (mode == "UPDATE" || mode == "MERGE" || mode == "OVERWRITE") return 1;
  return 0;  // INSERT or unknown defaults to INSERT
}

// Parses "catalog.schema.table" into three parts.
struct TableParts {
  std::string catalog;
  std::string schema;
  std::string table;
};

TableParts parseTableName(const std::string& fqn) {
  TableParts parts;
  size_t dot1 = fqn.find('.');
  if (dot1 == std::string::npos) {
    parts = {"default", "default", fqn};
    return parts;
  }
  size_t dot2 = fqn.find('.', dot1 + 1);
  if (dot2 == std::string::npos) {
    parts = {fqn.substr(0, dot1), "default", fqn.substr(dot1 + 1)};
    return parts;
  }
  parts = {fqn.substr(0, dot1), fqn.substr(dot1 + 1, dot2 - dot1 - 1),
            fqn.substr(dot2 + 1)};
  return parts;
}

// Acquires JNIEnv for the current thread, attaching if needed.
// Returns {env, should_detach}. Caller must detach if should_detach is true.
std::pair<JNIEnv*, bool> acquireJniEnv() {
  JavaVM* vm = pangrova::jni::globalJavaVM();
  if (!vm) {
    throw std::runtime_error("drainOutputToWriter: no JavaVM available");
  }
  JNIEnv* env = nullptr;
  bool attached = false;
  jint result = vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8);
  if (result == JNI_EDETACHED) {
    if (vm->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr) != JNI_OK) {
      throw std::runtime_error("drainOutputToWriter: AttachCurrentThread failed");
    }
    attached = true;
  }
  return {env, attached};
}

// RAII guard to detach JNI thread on scope exit.
struct JniDetachGuard {
  JavaVM* vm;
  bool should_detach;
  ~JniDetachGuard() { if (should_detach && vm) vm->DetachCurrentThread(); }
};

// Drains the OutputQueue and writes all batches to Java via NativeWriteExecutor.
// Called after Velox plan execution completes for WRITE tasks.
//
// Logging policy:
//   - Per-batch detail → DEBUG (VLOG(1)), never contains row data (PII risk)
//   - Aggregate progress → INFO every kDrainLogBatchInterval batches
//   - State changes (start/commit/finish) → INFO
constexpr int kDrainLogBatchInterval = 1000;

void drainOutputToWriter(
    OutputQueue* output_queue,
    const WriteConfigInfo& write_config,
    const std::string& task_id,
    facebook::velox::memory::MemoryPool* pool) {
  auto writer_lease = jni::GlobalWriterRegistry::instance().writerForTask(task_id);
  if (!writer_lease) {
    throw std::runtime_error(
        "Write task requires registered NativeWriter. "
        "Call NativeBridge.nativeRegisterWriter() during bootstrap. "
        "task_id=" + task_id);
  }
  jobject writer_ref = writer_lease.get();

  PANGROVA_LOG_INFO << "drain_output_start task_id=" << task_id
                  << " target=" << write_config.target_table
                  << " pool=" << (pool ? "valid" : "NULL");

  auto [env, should_detach] = acquireJniEnv();
  JavaVM* vm = nullptr;
  env->GetJavaVM(&vm);
  JniDetachGuard detach_guard{vm, should_detach};

  write::NativeWriteExecutor write_executor(env, writer_ref);

  auto tp = parseTableName(write_config.target_table);
  write::WriteDescriptor descriptor;
  descriptor.catalogName = tp.catalog;
  descriptor.schemaName = tp.schema;
  descriptor.tableName = tp.table;
  descriptor.writeMode = mapWriteMode(write_config.write_mode);
  descriptor.properties.emplace_back("taskId", task_id);

  int64_t write_handle = -1;
  facebook::velox::RowVectorPtr row_vector;
  int batch_count = 0;
  int64_t total_rows = 0;
  int64_t total_bytes = 0;

  // Cached IPC serialization state for reuse across batches (Task 27.2 /
  // Requirement I.1). The Java sink consumes one self-contained IPC stream
  // per appendSerializedBatch() call, so the BufferOutputStream cannot be
  // reused (arrow::io::BufferOutputStream::Finish() consumes the sink).
  // However the schema object and IpcWriteOptions can be shared across
  // batches — both are cheap to reference but expensive to (re)construct
  // (schema cloning + compression codec init). Caching them eliminates the
  // dominant per-batch overhead measured in profiling.
  std::shared_ptr<arrow::Schema> stream_schema;
  arrow::ipc::IpcWriteOptions ipc_write_options =
      arrow::ipc::IpcWriteOptions::Defaults();

  // F-NATIVE-EXEC-02: RAII abort guard — if an exception escapes before
  // commitWrite, abort the Java write session to release the PageSink and
  // delete partial files immediately (instead of leaking until shutdown).
  bool committed = false;
  auto abortGuard = [&]() noexcept {
    if (write_handle >= 0 && !committed) {
      PANGROVA_LOG_WARN << "drain_output_abort task_id=" << task_id
                      << " handle=" << write_handle;
      write_executor.abortWrite(write_handle);
    }
  };

  try {

  while (output_queue->pop(row_vector, std::chrono::milliseconds(100))
             == pangrova::executor::PopResult::DATA) {
    if (!row_vector || row_vector->size() == 0) {
      PANGROVA_LOG_DEBUG << "drain_output_empty_batch task_id=" << task_id;
      continue;
    }

    PANGROVA_LOG_DEBUG << "drain_output_batch task_id=" << task_id
                     << " rows=" << row_vector->size()
                     << " batch=" << batch_count;

    // Schema diagnostic at DEBUG level — column names and types only, no row data.
    PANGROVA_LOG_DEBUG << "drain_output_schema task_id=" << task_id
                     << " columns=" << row_vector->type()->size();

    if (write_handle < 0) {
      auto row_type = std::dynamic_pointer_cast<const facebook::velox::RowType>(
          row_vector->type());
      for (size_t i = 0; i < row_type->size(); ++i) {
        descriptor.columns.emplace_back(
            std::string(row_type->nameOf(i)),
            row_type->childAt(i)->toString());
      }
      PANGROVA_LOG_INFO << "drain_output_begin_write task_id=" << task_id
                      << " columns=" << row_type->size();
      write_handle = write_executor.beginWrite(descriptor);
      PANGROVA_LOG_DEBUG << "drain_output_begin_write_done task_id=" << task_id
                       << " handle=" << write_handle;
    }

    PANGROVA_LOG_DEBUG << "drain_output_export task_id=" << task_id;
    // Serialize RowVector to Arrow IPC bytes directly in C++ to avoid
    // nested JNI callback (C++ → Java appendBatch → C++ nativeSerializeBatchFromPointers)
    // which deadlocks on Velox memory pool during arrow::ImportRecordBatch release callback.
    ArrowArray arrow_array;
    ArrowSchema arrow_schema;
    // Flatten dictionary and constant encodings before Arrow IPC serialization.
    // Without this, Velox Project nodes may produce dictionary-encoded columns
    // (e.g., computed field references) that Arrow IPC preserves as dictionary
    // encoding. Java's ArrowStreamReader then returns index vectors (IntVector)
    // whose getObject() yields dictionary indices (0, 1, ...) instead of actual
    // values ("bob_updated", 999). Flattening ensures Java sees plain vectors.
    ArrowOptions arrow_opts;
    arrow_opts.flattenDictionary = true;
    arrow_opts.flattenConstant = true;
    // Load any LazyVector columns before Arrow export. The drain thread
    // consumes raw RowVectors outside the Velox operator pipeline, so
    // identity-projected columns (e.g. a FILE ROW column untouched by
    // Filter/Project in an UPDATE) may still be unloaded LazyVectors.
    // Bridge.cpp's schema export does NOT handle LAZY encoding — it
    // asUnchecked-casts to RowVector and reads garbage childrenSize(),
    // causing SIGSEGV or an unbounded allocation (OOM SIGKILL).
    facebook::velox::LazyVector::ensureLoadedRows(
        row_vector,
        facebook::velox::SelectivityVector(row_vector->size()));
    facebook::velox::exportToArrow(row_vector, arrow_array, pool, arrow_opts);
    facebook::velox::exportToArrow(row_vector, arrow_schema, arrow_opts);

    auto import_result = arrow::ImportRecordBatch(&arrow_array, &arrow_schema);
    if (!import_result.ok()) {
      throw std::runtime_error(
          "drain_output: Arrow import failed: " + import_result.status().ToString());
    }
    auto record_batch = *import_result;

    // Reuse schema across batches: cache on first batch, reuse for all
    // subsequent batches. Avoids per-batch schema/dictionary reconstruction
    // in the IPC stream writer (Requirement I.1).
    if (!stream_schema) {
      stream_schema = record_batch->schema();
    }

    auto sink_result = arrow::io::BufferOutputStream::Create();
    if (!sink_result.ok()) {
      throw std::runtime_error("drain_output: buffer creation failed");
    }
    auto sink = *sink_result;
    // Reuses cached stream_schema + ipc_write_options across batches —
    // only the sink and writer wrapper are recreated (cheap allocations).
    auto writer_result =
        arrow::ipc::MakeStreamWriter(sink, stream_schema, ipc_write_options);
    if (!writer_result.ok()) {
      throw std::runtime_error(
          "drain_output: writer creation failed: " + writer_result.status().ToString());
    }
    auto writer = *writer_result;
    auto write_status = writer->WriteRecordBatch(*record_batch);
    if (!write_status.ok()) {
      throw std::runtime_error(
          "drain_output: write failed: " + write_status.ToString());
    }
    auto close_status = writer->Close();
    if (!close_status.ok()) {
      throw std::runtime_error(
          "drain_output: close failed: " + close_status.ToString());
    }
    auto finish_result = sink->Finish();
    if (!finish_result.ok()) {
      throw std::runtime_error(
          "drain_output: finish failed: " + finish_result.status().ToString());
    }
    auto buffer = *finish_result;

    PANGROVA_LOG_DEBUG << "drain_output_append task_id=" << task_id
                     << " bytes=" << buffer->size()
                     << " rows=" << record_batch->num_rows();

    // Pass serialized IPC bytes to Java via JNI — single direction, no
    // nested callback. Task 27.8: JniExceptionScope (kRethrow) catches any
    // pending Java exception at scope exit and converts it to
    // std::runtime_error. Without this guard a Java exception in
    // appendSerializedBatch (e.g. table writer rejecting the batch) would
    // sit pending across the next NewByteArray call and crash the JVM.
    {
      pangrova::jni::JniExceptionScope jni_scope(
          env, "drainOutputToWriter.appendBatch");
      jbyteArray jbytes = env->NewByteArray(static_cast<jint>(buffer->size()));
      env->SetByteArrayRegion(jbytes, 0, static_cast<jint>(buffer->size()),
                              reinterpret_cast<const jbyte*>(buffer->data()));
      int32_t row_count = static_cast<int32_t>(record_batch->num_rows());
      int32_t col_count = static_cast<int32_t>(record_batch->num_columns());
      write_executor.appendSerializedBatch(write_handle, jbytes, row_count, col_count);
      env->DeleteLocalRef(jbytes);
    }

    total_rows += record_batch->num_rows();
    total_bytes += buffer->size();
    ++batch_count;

    // Periodic aggregate INFO log every kDrainLogBatchInterval batches.
    if (batch_count % kDrainLogBatchInterval == 0) {
      PANGROVA_LOG_INFO << "drain_output_progress task_id=" << task_id
                      << " batches=" << batch_count
                      << " total_rows=" << total_rows
                      << " total_bytes=" << total_bytes;
    }
  }

  PANGROVA_LOG_INFO << "drain_output_complete task_id=" << task_id
                  << " batches=" << batch_count
                  << " total_rows=" << total_rows
                  << " total_bytes=" << total_bytes;

  if (write_handle >= 0) {
    PANGROVA_LOG_INFO << "drain_output_commit task_id=" << task_id;
    write_executor.commitWrite(write_handle);
    committed = true;
    PANGROVA_LOG_INFO << "drain_output_commit_done task_id=" << task_id;
  }

  } catch (...) {
    // F-NATIVE-EXEC-02: Abort the write session on any failure to release
    // the Java PageSink, delete partial files, and free resources.
    abortGuard();
    throw;
  }
}

}  // namespace

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

void PlanExecutionDriver::cancel() {
  cancelled_.store(true);

  // Propagate cancel to active VeloxTaskRunner if present.
  {
    std::lock_guard<std::mutex> lock(runner_mutex_);
    if (active_runner_) {
      active_runner_->cancel();
    }
  }

  // Close output queue to unblock any waiting consumers.
  if (output_queue_) {
    output_queue_->close();
  }
  // Close shuffle queue to unblock any waiting consumers.
  if (shuffle_queue_) {
    shuffle_queue_->close();
  }
}

bool PlanExecutionDriver::isCancelled() const {
  return cancelled_.load();
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
    velox::VeloxPlanConverter converter;
    // Resolve result serde name from session properties.
    // Maps config value ("presto"/"arrow_batch") to Velox serde registry name
    // ("Presto"/"ArrowBatch"). Default is "Presto" (legacy PrestoVectorSerde).
    std::string result_serde_name = "Presto";
    auto serde_it = config_.session_properties.find("velox.result_serde_mode");
    if (serde_it != config_.session_properties.end() &&
        serde_it->second == "arrow_batch") {
      result_serde_name = "ArrowBatch";
    }
    auto velox_plan = converter.convert(
        config_.substrait_plan, config_.splits, config_.is_result_task,
        result_serde_name);
    convertSpan.reset();  // End plan conversion span.
    if (!velox_plan) {
      lifecycle_.notifyTaskFailed(
          "Failed to convert Substrait plan to Velox PlanNode");
      return;
    }

    // Persist scan node output types for dynamic filter column resolution.
    // VeloxPlanConverter is local — copy the map before it goes out of scope.
    // Also capture memory pool holders from the converter to keep ValuesNode
    // vector buffers alive for the duration of task execution.
    {
      std::lock_guard<std::mutex> lock(runner_mutex_);
      scan_node_output_types_ = converter.scanNodeOutputTypes();
      values_pool_holders_ = std::move(converter.valuesPoolHolders());
    }

    // Check cancel after plan conversion (before expensive execution).
    if (cancelled_.load()) {
      return;
    }

    // Create VeloxTaskRunner and store as shared member for cancel access.
    velox::VeloxTaskRunnerConfig runner_config;
    runner_config.task_id = config_.task_id;
    runner_config.query_id = config_.query_id;
    // Enforce min 2 drivers: Velox serial mode (max_drivers=1) uses
    // Task::next() which deadlocks on multi-pipeline plans (JOIN/agg)
    // because the single driver thread blocks waiting for the build side.
    // Parallel mode (Task::start() + taskCompletionFuture()) delegates
    // scheduling to Velox's internal executor, handling this correctly.
    // L3-4: Allow per-query override via session property "velox.max_drivers".
    if (runner_config.max_drivers_override > 0) {
      runner_config.max_drivers =
          std::max(2, runner_config.max_drivers_override);
    } else {
      runner_config.max_drivers =
          std::max(2, config_.resource_limit.max_threads);
    }
    // Propagate spill configuration from TaskExecutorConfig.
    runner_config.spill_enabled =
        !config_.resource_limit.spill_directory.empty();
    runner_config.spill_directory = config_.resource_limit.spill_directory;
    runner_config.max_spill_bytes = config_.resource_limit.max_spill_bytes;

    // Propagate shuffle configuration.
    runner_config.is_shuffle_task = (config_.num_shuffle_partitions > 0);
    runner_config.num_shuffle_partitions = config_.num_shuffle_partitions;
    runner_config.shuffle_partition_key_indices =
        config_.shuffle_partition_key_indices;

    // Propagate result task flag. When true, VeloxTaskRunner skips the
    // consumer callback — Velox's PartitionedOutputNode routes output
    // through OutputBufferManager, and FlightResultServer pulls from it.
    runner_config.is_result_task = config_.is_result_task;

    // Propagate session properties for Velox connector configuration.
    runner_config.session_properties = config_.session_properties;

    // Apply session property overrides for performance tuning knobs.
    // Session properties with prefix "velox." override VeloxTaskRunnerConfig
    // defaults (e.g., velox.hash_probe_finish_early_on_empty_build=false).
    for (const auto& [key, value] : config_.session_properties) {
      if (key == "velox.hash_probe_finish_early_on_empty_build") {
        runner_config.hash_probe_finish_early_on_empty_build = (value == "true");
      } else if (key == "velox.hash_probe_string_dynamic_filter_pushdown") {
        runner_config.hash_probe_string_dynamic_filter_pushdown = (value == "true");
      } else if (key == "velox.adaptive_filter_reordering") {
        runner_config.adaptive_filter_reordering = (value == "true");
      } else if (key == "velox.parallel_output_join_build_rows") {
        runner_config.parallel_output_join_build_rows = (value == "true");
      } else if (key == "velox.table_scan_scaled_processing") {
        runner_config.table_scan_scaled_processing = (value == "true");
      } else if (key == "velox.max_split_preload_per_driver") {
        try {
          runner_config.max_split_preload_per_driver = std::stoi(value);
        } catch (const std::exception&) {
          PANGROVA_LOG_WARN << "invalid_session_property"
                          << " key=" << key << " value=" << value
                          << " task_id=" << config_.task_id;
        }
      } else if (key == "velox.result_serde_mode") {
        runner_config.result_serde_mode = value;
      } else if (key == "velox.abandon_partial_agg_min_rows") {
        try {
          runner_config.abandon_partial_agg_min_rows = std::stoi(value);
        } catch (const std::exception&) {
          PANGROVA_LOG_WARN << "invalid_session_property"
                          << " key=" << key << " value=" << value
                          << " task_id=" << config_.task_id;
        }
      } else if (key == "velox.abandon_partial_agg_min_pct") {
        try {
          runner_config.abandon_partial_agg_min_pct = std::stoi(value);
        } catch (const std::exception&) {
          PANGROVA_LOG_WARN << "invalid_session_property"
                          << " key=" << key << " value=" << value
                          << " task_id=" << config_.task_id;
        }
      } else if (key == "velox.max_partial_agg_memory") {
        try {
          runner_config.max_partial_agg_memory = std::stol(value);
        } catch (const std::exception&) {
          PANGROVA_LOG_WARN << "invalid_session_property"
                          << " key=" << key << " value=" << value
                          << " task_id=" << config_.task_id;
        }
      } else if (key == "velox.max_extended_partial_agg_memory") {
        try {
          runner_config.max_extended_partial_agg_memory = std::stol(value);
        } catch (const std::exception&) {
          PANGROVA_LOG_WARN << "invalid_session_property"
                          << " key=" << key << " value=" << value
                          << " task_id=" << config_.task_id;
        }
      } else if (key == "velox.spill_write_buffer_size") {
        try {
          runner_config.spill_write_buffer_size = std::stol(value);
        } catch (const std::exception&) {
          PANGROVA_LOG_WARN << "invalid_session_property"
                          << " key=" << key << " value=" << value
                          << " task_id=" << config_.task_id;
        }
      } else if (key == "velox.spill_num_partition_bits") {
        try {
          runner_config.spill_num_partition_bits = std::stoi(value);
        } catch (const std::exception&) {
          PANGROVA_LOG_WARN << "invalid_session_property"
                          << " key=" << key << " value=" << value
                          << " task_id=" << config_.task_id;
        }
      } else if (key == "velox.driver_cpu_time_slice_limit_ms") {
        try {
          runner_config.driver_cpu_time_slice_limit_ms = std::stoi(value);
        } catch (const std::exception&) {
          PANGROVA_LOG_WARN << "invalid_session_property"
                          << " key=" << key << " value=" << value
                          << " task_id=" << config_.task_id;
        }
      } else if (key == "velox.min_spillable_reservation_pct") {
        try {
          runner_config.min_spillable_reservation_pct = std::stoi(value);
        } catch (const std::exception&) {
          PANGROVA_LOG_WARN << "invalid_session_property"
                          << " key=" << key << " value=" << value
                          << " task_id=" << config_.task_id;
        }
      } else if (key == "velox.spillable_reservation_growth_pct") {
        try {
          runner_config.spillable_reservation_growth_pct = std::stoi(value);
        } catch (const std::exception&) {
          PANGROVA_LOG_WARN << "invalid_session_property"
                          << " key=" << key << " value=" << value
                          << " task_id=" << config_.task_id;
        }
      } else if (key == "velox.shuffle_compression_codec") {
        runner_config.shuffle_compression_codec = value;
      } else if (key == "velox.preserve_dictionary_encoding") {
        // L2-3: Control whether Arrow export preserves dictionary encoding.
        runner_config.preserve_dictionary_encoding = (value == "true");
      } else if (key == "velox.max_drivers") {
        // L3-4: Per-query max_drivers override from Java planner.
        try {
          runner_config.max_drivers_override = std::stoi(value);
        } catch (const std::exception&) {
          PANGROVA_LOG_WARN << "invalid_session_property"
                          << " key=" << key << " value=" << value
                          << " task_id=" << config_.task_id;
        }
      } else if (key == "velox.shuffle_compression_min_bytes") {
        // L4-1B: Minimum bytes to apply shuffle compression.
        try {
          runner_config.shuffle_compression_min_bytes = std::stol(value);
        } catch (const std::exception&) {
          PANGROVA_LOG_WARN << "invalid_session_property"
                          << " key=" << key << " value=" << value
                          << " task_id=" << config_.task_id;
        }
      }
    }

    auto runner = std::make_shared<velox::VeloxTaskRunner>(
        runner_config, velox_plan, config_.splits, output_queue_,
        shuffle_queue_);

    // Attach plan conversion memory pools to the runner so they outlive
    // the PlanNode tree even during async Task destruction on Velox's
    // thread pool (where PlanExecutionDriver may already be destroyed).
    {
      std::lock_guard<std::mutex> lock(runner_mutex_);
      if (!values_pool_holders_.empty()) {
        runner->attachPoolHolders(values_pool_holders_);
      }
    }

    // Publish runner under lock so cancel() can reach it.
    {
      std::lock_guard<std::mutex> lock(runner_mutex_);
      active_runner_ = runner;
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
    std::shared_ptr<std::thread> drain_thread;
    if (!config_.write_config.target_table.empty()) {
      drain_thread = std::make_shared<std::thread>(
          [this, drain_exc]() {
            try {
              auto* mem_governor = pangrova::jni::globalVeloxMemoryGovernor();
              facebook::velox::memory::MemoryPool* write_pool = nullptr;
              std::shared_ptr<facebook::velox::memory::MemoryPool> pool_holder;
              if (mem_governor && mem_governor->veloxMemoryManager()) {
                pool_holder = mem_governor->veloxMemoryManager()->addLeafPool(
                    "write-drain-" + config_.task_id);
                write_pool = pool_holder.get();
              }
              drainOutputToWriter(
                  output_queue_, config_.write_config, config_.task_id,
                  write_pool);
            } catch (...) {
              *drain_exc = std::current_exception();
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
                        result_serde_name](bool success) mutable {
      if (!success) {
        if (!cancelled_.load()) {
          // Serde fallback: if the task failed because ArrowBatch serde
          // encountered an unsupported type, restart with Presto serde.
          // This handles Velox-specific opaque types not representable in
          // Arrow IPC format (Requirement 3.5).
          if (result_serde_name == "ArrowBatch" &&
              runner->isResultSerdeUnsupportedTypeError()) {
            auto errMsg = runner->errorMessage();
#if __has_include(<glog/logging.h>)
            LOG(WARNING) << "result_serde_fallback"
                         << " query_id=" << config_.query_id
                         << " task_id=" << config_.task_id
                         << " reason=" << errMsg
                         << " action=restart_with_presto_serde";
#endif
            // Cancel the failed runner and release it.
            runner->cancel();
            {
              std::lock_guard<std::mutex> lock(runner_mutex_);
              active_runner_.reset();
            }

            // Drain any partial output from the failed ArrowBatch run before
            // restarting with Presto serde. Mixing formats would corrupt results.
            if (output_queue_) {
              output_queue_->drain();
            }

            // Re-convert plan with Presto serde.
            velox::VeloxPlanConverter fallback_converter;
            auto fallback_plan = fallback_converter.convert(
                config_.substrait_plan, config_.splits,
                config_.is_result_task, "Presto");
            if (!fallback_plan) {
              lifecycle_.notifyTaskFailed(
                  "Failed to re-convert plan with Presto serde after "
                  "ArrowBatch fallback");
              if (output_queue_) output_queue_->close();
              if (drain_thread && drain_thread->joinable()) drain_thread->join();
              return;
            }

            // Update pool holders from fallback conversion.
            {
              std::lock_guard<std::mutex> lock(runner_mutex_);
              values_pool_holders_ =
                  std::move(fallback_converter.valuesPoolHolders());
            }

            // Create new runner with Presto serde mode.
            auto fallback_cfg = runner_config;
            fallback_cfg.result_serde_mode = "presto";
            auto fallback_runner = std::make_shared<velox::VeloxTaskRunner>(
                fallback_cfg, fallback_plan, config_.splits,
                output_queue_, shuffle_queue_);
            {
              std::lock_guard<std::mutex> lock(runner_mutex_);
              if (!values_pool_holders_.empty()) {
                fallback_runner->attachPoolHolders(values_pool_holders_);
              }
              active_runner_ = fallback_runner;
            }

            if (cancelled_.load()) {
              fallback_runner->cancel();
              std::lock_guard<std::mutex> lock(runner_mutex_);
              active_runner_.reset();
              if (output_queue_) output_queue_->close();
              if (drain_thread && drain_thread->joinable()) drain_thread->join();
              return;
            }

            fallback_runner->start();
            // Blocking wait for fallback runner (rare serde-fallback path —
            // acceptable to block the continuation thread briefly here).
            if (!fallback_runner->waitForCompletion()) {
              if (!cancelled_.load()) {
                auto fallbackErr = fallback_runner->errorMessage();
                PANGROVA_EXEC_LOG_ERROR << "plan_execution_failed"
                                      << " trace_id=" << config_.trace_id
                                      << " query_id=" << config_.query_id
                                      << " task_id=" << config_.task_id
                                      << " error_detail=" << fallbackErr
                                      << " note=after_serde_fallback";
                lifecycle_.notifyTaskFailed(
                    "Velox task failed: " + fallbackErr);
              }
              if (output_queue_) output_queue_->close();
              if (drain_thread && drain_thread->joinable()) drain_thread->join();
              return;
            }
            // Fallback runner succeeded — fall through to success path.
          } else {
            auto errMsg = runner->errorMessage();
            PANGROVA_EXEC_LOG_ERROR << "plan_execution_failed"
                                  << " trace_id=" << config_.trace_id
                                  << " query_id=" << config_.query_id
                                  << " task_id=" << config_.task_id
                                  << " error_detail=" << errMsg;
            lifecycle_.notifyTaskFailed(
                "Velox task failed: " + errMsg);
            // Close output queue to unblock drain thread if waiting on pop().
            if (output_queue_) output_queue_->close();
            if (drain_thread && drain_thread->joinable()) drain_thread->join();
            return;
          }
        } else {
          // Cancelled — close output queue and join drain thread.
          if (output_queue_) output_queue_->close();
          if (drain_thread && drain_thread->joinable()) drain_thread->join();
          return;
        }
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
          if (output_queue_) {
            output_queue_->close();
          }
          drain_thread->join();
          // Propagate drain thread exceptions.
          if (*drain_exc) {
            std::rethrow_exception(*drain_exc);
          }
        }

        // Transition to FINISHED before cleanup. The cleanup (flush shuffle
        // accumulator, remove spill dirs) can block under resource pressure,
        // causing Java-side TaskCompletionMonitor to time out. The task is
        // logically complete after drainOutputToWriter returns — cleanup is
        // best-effort housekeeping that must not delay the FINISHED signal.
        lifecycle_.notifyTaskComplete();  // RUNNING → FLUSHING
        lifecycle_.transitionTo(ExecutorState::FINISHED);  // FLUSHING → FINISHED

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
            if (!cancelled_.load()) {
              lifecycle_.notifyTaskFailed(
                  std::string("Post-completion failed: ") + e.what());
            }
          } catch (...) {
            if (!cancelled_.load()) {
              lifecycle_.notifyTaskFailed(
                  "Post-completion failed: unknown error");
            }
          }
          comp_guard.done = true;
          completion_promise_.set_value();
        });
    // Chain fully constructed — disarm ExitGuard.
    armed = true;

  } catch (const NativeUnsupportedFeatureException& e) {
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
      lifecycle_.notifyTaskFailed(
          std::string("NATIVE_FALLBACK_REQUIRED: ") + e.what());
    }
    // exit_guard fires (armed=false): completion_promise_.set_value()
  } catch (const std::exception& e) {
    // DO NOT reset active_runner_ here — same reason as above.
    if (!cancelled_.load()) {
      PANGROVA_EXEC_LOG_ERROR << "velox_execution_exception"
                            << " trace_id=" << config_.trace_id
                            << " query_id=" << config_.query_id
                            << " task_id=" << config_.task_id
                            << " error_type=runtime_exception"
                            << " error_detail=" << e.what();
      lifecycle_.notifyTaskFailed(
          std::string("Velox execution failed: ") + e.what());
    }
    // exit_guard fires (armed=false): completion_promise_.set_value()
  }
}

}  // namespace pangrova::executor
