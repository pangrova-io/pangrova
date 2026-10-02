#include "executor/driver/PlanExecutionWriteSinks.h"

#include <algorithm>
#include <stdexcept>
#include <unordered_set>
#include <arrow/c/bridge.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <velox/vector/LazyVector.h>
#include <velox/vector/arrow/Bridge.h>

#include "executor/driver/PlanExecutionDriver.h"
#include "executor/driver/TaskExecutor.h"
#include "executor/NativeUnsupportedFeatureException.h"
#include "jni/bridge/GlobalWriterRegistry.h"
#include "jni/bridge/JniBridge.h"
#include "jni/RuntimeInitializer.h"
#include "lance/LanceBatchWriter.h"
#include "lance/LancePhysicalBatchWriter.h"
#include "observability/PangrovaLog.h"
#include "shuffle/ShuffleEgressExecutor.h"
#include "shuffle/ShuffleEgressPump.h"
#include "write/NativeWriteExecutor.h"
#include "write/WriteArrowExport.h"

namespace pangrova::executor {
namespace {

// Maps write_mode string from WriteConfig to WriteDescriptor int32_t.
// 0=INSERT, 1=OVERWRITE (used for UPDATE/MERGE), 2=DELETE.
int32_t mapWriteMode(const std::string& mode) {
  if (mode == "DELETE") return 2;
  if (mode == "UPDATE" || mode == "MERGE" || mode == "OVERWRITE") return 1;
  if (mode == "INSERT" || mode == "APPEND") return 0;
  throw std::invalid_argument("Unsupported write mode: " + mode);
}

std::string writeFileFormatName(split::FileFormatType format) {
  switch (format) {
    case split::FileFormatType::PARQUET:
      return "PARQUET";
    case split::FileFormatType::ORC:
      return "ORC";
    case split::FileFormatType::LANCE:
      return "LANCE";
    case split::FileFormatType::METADATA_JSON:
      throw std::invalid_argument("Metadata JSON is read-only and cannot be a write format");
  }
  throw std::invalid_argument("Unsupported write file format");
}

void addWriteFormatProperties(
    const WriteConfigInfo& config,
    write::WriteDescriptor& descriptor) {
  if (config.target_file_size < 0) {
    throw std::invalid_argument("Write target file size must be non-negative");
  }
  const auto format = writeFileFormatName(config.file_format);
  if (config.file_format == split::FileFormatType::LANCE) {
    throw NativeUnsupportedFeatureException(
        "write_format:lance", "lance",
        "Native Lance writes require an allocated output URI and external-file "
        "commit metadata contract");
  }
  descriptor.properties.emplace_back("fileFormat", format);
  descriptor.properties.emplace_back(
      "targetFileSizeBytes", std::to_string(config.target_file_size));
}

// Parses "catalog.schema.table" into three parts.
struct TableParts {
  std::string catalog;
  std::string schema;
  std::string table;
};

TableParts parseTableName(const std::string& fqn) {
  size_t dot1 = fqn.find('.');
  size_t dot2 = fqn.find('.', dot1 + 1);
  if (dot1 == std::string::npos || dot2 == std::string::npos ||
      dot1 == 0 || dot2 == dot1 + 1 || dot2 + 1 >= fqn.size() ||
      fqn.find('.', dot2 + 1) != std::string::npos) {
    throw std::invalid_argument(
        "Write target must be exactly catalog.schema.table: " + fqn);
  }
  return {fqn.substr(0, dot1), fqn.substr(dot1 + 1, dot2 - dot1 - 1),
          fqn.substr(dot2 + 1)};
}

void addStaticPartitionProperties(
    const std::vector<std::string>& partition_spec,
    write::WriteDescriptor& descriptor) {
  std::unordered_set<std::string> names;
  for (const auto& binding : partition_spec) {
    const auto equals = binding.find('=');
    if (equals == std::string::npos || equals == 0) {
      throw std::invalid_argument(
          "Static partition binding must be name=value: " + binding);
    }
    const auto name = binding.substr(0, equals);
    if (!names.emplace(name).second) {
      throw std::invalid_argument("Duplicate static partition key: " + name);
    }
    descriptor.properties.emplace_back(
        "staticPartition." + name, binding.substr(equals + 1));
  }
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
  } else if (result != JNI_OK || env == nullptr) {
    throw std::runtime_error("Write sink could not acquire its attached JNIEnv");
  }
  return {env, attached};
}

// RAII guard to detach JNI thread on scope exit.
struct JniDetachGuard {
  JavaVM* vm;
  bool should_detach;
  ~JniDetachGuard() { if (should_detach && vm) vm->DetachCurrentThread(); }
};

// The Task keeps the writer on any failed drain. Preserve both operation and
// cleanup diagnostics, but never mistake destructor completion for SDK join.
template <typename Writer>
void closeSinkAfterOperation(Writer& writer, std::exception_ptr operationFailure,
                            const char* sinkName) {
  try {
    writer.close();
  } catch (const std::exception& cleanupFailure) {
    if (operationFailure) {
      try {
        std::rethrow_exception(operationFailure);
      } catch (const std::exception& original) {
        throw std::runtime_error(std::string(original.what()) + "; " + sinkName +
                                 " cleanup: " + cleanupFailure.what());
      }
    }
    throw;
  }
  if (operationFailure) std::rethrow_exception(operationFailure);
}

// Drains the OutputQueue and writes all batches to Java via NativeWriteExecutor.
// Called after Velox plan execution completes for WRITE tasks.
//
// Logging policy:
//   - Per-batch detail → DEBUG (VLOG(1)), never contains row data (PII risk)
//   - Aggregate progress → INFO every kDrainLogBatchInterval batches
//   - State changes (start/commit/finish) → INFO
constexpr int kDrainLogBatchInterval = 1000;

}  // namespace

void drainOutputToWriter(
    OutputQueue* output_queue,
    const WriteConfigInfo& write_config,
    const std::string& task_id,
    facebook::velox::memory::MemoryPool* pool,
    const WriteProducerOutcomeRef& producer_outcome) {
  if (task_id.empty()) {
    throw std::invalid_argument("Write task requires a non-empty canonical task_id");
  }
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
  addStaticPartitionProperties(write_config.partition_spec, descriptor);
  addWriteFormatProperties(write_config, descriptor);

  int64_t write_handle = -1;
  facebook::velox::RowVectorPtr row_vector;
  int batch_count = 0;
  int64_t total_rows = 0;
  int64_t total_bytes = 0;

  // Cached IPC serialization state for reuse across batches (Task 27.2 /
  // Requirement I.1). The Java sink consumes one self-contained IPC stream
  // per appendBatchData() call, so the BufferOutputStream cannot be
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

  const bool finalized = drainWriteOutput(
      output_queue, producer_outcome, std::chrono::milliseconds(100),
      [&](const facebook::velox::RowVectorPtr& next) {
    row_vector = next;
    if (!row_vector || row_vector->size() == 0) {
      PANGROVA_LOG_DEBUG << "drain_output_empty_batch task_id=" << task_id;
      return;
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
    auto export_vector = pangrova::write::prepareWriteArrowExport(row_vector);
    facebook::velox::exportToArrow(export_vector, arrow_array, pool, arrow_opts);
    facebook::velox::exportToArrow(export_vector, arrow_schema, arrow_opts);

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

    // The bridge borrows the Arrow buffer for this synchronous JNI call and
    // converts Java exceptions before the buffer or local JNI reference dies.
    write_executor.appendBatchData(
        write_handle, buffer->data(), buffer->size(),
        static_cast<int32_t>(record_batch->num_rows()),
        static_cast<int32_t>(record_batch->num_columns()));

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
  }, [&] {
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
  });
  if (!finalized) {
    // Queue closure after a failed or cancelled producer is not a successful
    // end-of-stream. Release the Java PageSink and its staged files now.
    abortGuard();
  }

  } catch (...) {
    // F-NATIVE-EXEC-02: Abort the write session on any failure to release
    // the Java PageSink, delete partial files, and free resources.
    abortGuard();
    throw;
  }
}

void drainOutputToLance(
    OutputQueue* output_queue,
    const WriteConfigInfo& write_config,
    const std::string& task_id,
    const std::string& tenant_id,
    const facebook::velox::RowTypePtr& output_type,
    std::shared_ptr<facebook::velox::memory::MemoryPool> pool,
    const WriteProducerOutcomeRef& producer_outcome,
    std::shared_ptr<lance::LanceBatchWriter>& task_writer) {
  if (!write_config.lance_output.has_value()) {
    throw std::invalid_argument(
        "Lance write requires a connector-owned output binding");
  }
  if (task_id.empty() || tenant_id.empty() || !output_type || !pool) {
    throw std::invalid_argument(
        "Lance write requires task identity, plan output schema, and memory pool");
  }
  auto writer_lease = jni::GlobalWriterRegistry::instance().writerForTask(task_id);
  if (!writer_lease) {
    throw std::runtime_error(
        "Lance write requires registered NativeWriter metadata channel; task_id=" +
        task_id);
  }
  auto [env, should_detach] = acquireJniEnv();
  JavaVM* vm = nullptr;
  env->GetJavaVM(&vm);
  JniDetachGuard detach_guard{vm, should_detach};
  write::NativeWriteExecutor result_channel(env, writer_lease.get());

  const auto& binding = *write_config.lance_output;
  lance::LanceWriterConfig config;
  config.outputPath = binding.dataset_uri;
  switch (binding.operation) {
    case pangrova::protocol::v2::LANCE_WRITE_OPERATION_CREATE:
      config.operation = lance::LanceWriteOperation::kCreate;
      break;
    case pangrova::protocol::v2::LANCE_WRITE_OPERATION_APPEND:
      config.operation = lance::LanceWriteOperation::kAppend;
      break;
    case pangrova::protocol::v2::LANCE_WRITE_OPERATION_OVERWRITE:
      config.operation = lance::LanceWriteOperation::kOverwrite;
      break;
    default:
      throw std::invalid_argument("Unsupported Lance write operation");
  }
  config.expectedReadVersion = binding.expected_read_version;
  config.storageBinding = binding.storage_binding;
  config.schemaFenceSha256 = binding.schema_fence_sha256;
  config.writeId = binding.write_id;
  config.tenantId = tenant_id;
  config.generation = binding.generation;
  config.transactionStagingPrefix = binding.transaction_staging_prefix;
  config.taskAttempt = binding.task_attempt;

  if (binding.write_column_names.size() != output_type->size()) {
    throw std::invalid_argument("Lance allocation columns differ from plan output width");
  }
  auto dataset_type = facebook::velox::ROW(binding.write_column_names, output_type->children());
  task_writer = std::make_shared<lance::LanceBatchWriter>(pool, dataset_type, config);
  auto writer = task_writer;
  int64_t total_rows = 0;
  std::exception_ptr operation_failure;
  try {
    drainWriteOutput(
      output_queue, producer_outcome, std::chrono::milliseconds(100),
      [&](const facebook::velox::RowVectorPtr& row_vector) {
    if (!row_vector || row_vector->size() == 0) {
      return;
    }
    if (!row_vector->type()->equivalent(*output_type)) {
      throw std::runtime_error(
          "Lance write batch schema differs from plan output schema");
    }
    auto dataset_batch = std::make_shared<facebook::velox::RowVector>(
        pool.get(), dataset_type, row_vector->nulls(), row_vector->size(), row_vector->children());
    writer->writeBatch(dataset_batch);
    total_rows += row_vector->size();
  }, [&] {
    auto metadata = writer->finish();
    if (writer->stats().rowsWritten != total_rows || metadata.empty()) {
      throw std::runtime_error(
          "Lance writer returned inconsistent usage or empty commit metadata");
    }
    result_channel.publishCommitMetadataForTask(task_id, metadata);
    PANGROVA_LOG_INFO << "lance_uncommitted_write_complete task_id=" << task_id
                    << " rows=" << writer->stats().rowsWritten
                    << " bytes=" << writer->stats().bytesWritten
                    << " metadata_bytes=" << metadata.size();
  });
  } catch (...) {
    operation_failure = std::current_exception();
  }
  closeSinkAfterOperation(*writer, operation_failure, "Dataset sink");
}

void drainOutputToPhysicalWriter(
    OutputQueue* queue, const TaskExecutorConfig& task,
    const facebook::velox::RowTypePtr& outputType,
    std::shared_ptr<facebook::velox::memory::MemoryPool> pool,
    const WorkerLocalSpillPolicy& spillPolicy,
    const WriteProducerOutcomeRef& outcome,
    PhysicalWriterOwner owner) {
  if (!task.write_config.physical_output) {
    throw std::invalid_argument("Physical sink requires its exact admitted binding");
  }
  auto lease = jni::GlobalWriterRegistry::instance().writerForTask(task.task_id);
  if (!lease) throw std::runtime_error("Physical sink requires its task metadata channel");
  auto [env, detach] = acquireJniEnv();
  JniDetachGuard guard{jni::globalJavaVM(), detach};
  write::NativeWriteExecutor channel(env, lease.get());
  const auto& admitted = *task.write_config.physical_output;
  lance::PhysicalWriterConfig config;
  config.binding = admitted.binding;
  config.taskAttempt = admitted.task_attempt;
  config.tenantId = task.tenant_id;
  config.storageBinding = admitted.storage_binding;
  config.spillDirectory = spillPolicy.directory;
  config.maxSpillBytes = static_cast<uint64_t>(std::max<int64_t>(0, spillPolicy.max_bytes));
  config.maxObjectWriteBytes = static_cast<uint64_t>(
      std::max<int64_t>(0, task.resource_limit.max_object_store_write_bytes));
  config.resourceUsagePublisher = [&metrics = owner.metrics](const protocol::v2::ResourceUsage& usage) {
    metrics.observeColumnarConversionCopiedBytes(usage.columnar_conversion_copied_bytes());
  };
  owner.writer = std::make_shared<lance::LancePhysicalBatchWriter>(
      std::move(pool), outputType, std::move(config));
  auto writer = owner.writer;
  std::exception_ptr operation_failure;
  try {
    drainWriteOutput(queue, outcome, std::chrono::milliseconds(100),
      [&](const facebook::velox::RowVectorPtr& batch) {
        if (!batch || batch->size() == 0) return;
        if (!batch->type()->equivalent(*outputType)) {
          throw std::runtime_error("Physical sink batch differs from its actual plan output");
        }
        writer->writeBatch(batch);
      }, [&] {
        const auto fragment = writer->finish();
        if (fragment.empty()) throw std::runtime_error("Physical writer returned empty metadata");
        channel.publishCommitMetadataForTask(task.task_id, fragment);
      });
  } catch (...) {
    operation_failure = std::current_exception();
  }
  closeSinkAfterOperation(*writer, operation_failure, "Physical sink");
}

void drainShuffleToEgress(
    pangrova::shuffle::SerializedShuffleQueue* queue,
    const TaskExecutorConfig& config) {
  if (!queue || !config.shuffle_egress) {
    throw std::logic_error("External shuffle drain is not configured");
  }
  pangrova::shuffle::drainSerializedShuffle(
      queue, config.queue_poll_timeout,
      [&config](int32_t partition_id, int64_t row_count,
                const uint8_t* data, int64_t size) {
        config.shuffle_egress->onPartitionBatch(
            config.query_id, config.task_id, partition_id, row_count,
            data, size);
      });
}


}  // namespace pangrova::executor
