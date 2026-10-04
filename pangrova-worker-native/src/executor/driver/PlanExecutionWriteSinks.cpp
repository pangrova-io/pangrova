#include "executor/driver/PlanExecutionWriteSinks.h"

#include <algorithm>
#include <stdexcept>
#include <unordered_set>
#include <arrow/c/bridge.h>
#include <folly/ScopeGuard.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <velox/vector/LazyVector.h>
#include <velox/vector/arrow/Bridge.h>

#include "columnar/CanonicalBatchStream.h"
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
#include "write/WriteBatchStream.h"

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

write::WriteDescriptor makeWriteDescriptor(const WriteConfigInfo& config,
                                           const std::string& taskId) {
  const auto parts = parseTableName(config.target_table);
  write::WriteDescriptor descriptor;
  descriptor.catalogName = parts.catalog;
  descriptor.schemaName = parts.schema;
  descriptor.tableName = parts.table;
  descriptor.writeMode = mapWriteMode(config.write_mode);
  descriptor.properties.emplace_back("taskId", taskId);
  addStaticPartitionProperties(config.partition_spec, descriptor);
  addWriteFormatProperties(config, descriptor);
  return descriptor;
}

std::shared_ptr<arrow::RecordBatch> exportWriteBatch(
    const facebook::velox::RowVectorPtr& batch,
    facebook::velox::memory::MemoryPool* pool) {
  facebook::velox::LazyVector::ensureLoadedRows(
      batch, facebook::velox::SelectivityVector(batch->size()));
  auto normalized = write::prepareWriteArrowExport(batch);
  ArrowArray array{};
  ArrowSchema schema{};
  auto release = folly::makeGuard([&] {
    if (array.release) array.release(&array);
    if (schema.release) schema.release(&schema);
  });
  ArrowOptions options;
  options.flattenDictionary = true;
  options.flattenConstant = true;
  facebook::velox::exportToArrow(normalized, array, pool, options);
  facebook::velox::exportToArrow(normalized, schema, options);
  auto imported = arrow::ImportRecordBatch(&array, &schema);
  if (!imported.ok()) {
    throw std::runtime_error("Write Arrow import failed: " + imported.status().ToString());
  }
  return *imported;
}

// One Java session consumes the same canonical Arrow stream codec as results and
// shuffle. Allocations are owned by the actual task pool throughout the upcall.
class JavaWriteSession {
 public:
  JavaWriteSession(JNIEnv* env, jobject writer, const WriteConfigInfo& config,
                   std::string taskId,
                   std::shared_ptr<facebook::velox::memory::MemoryPool> pool)
      : executor_(env, writer), task_id_(std::move(taskId)),
        descriptor_(makeWriteDescriptor(config, task_id_)),
        pool_(std::make_shared<memory::ColumnarMemoryPool>(std::move(pool))) {
    PANGROVA_LOG_INFO << "drain_output_start task_id=" << task_id_
                     << " target=" << config.target_table << " pool=valid";
  }

  void append(const facebook::velox::RowVectorPtr& batch) {
    if (!batch || batch->size() == 0) {
      PANGROVA_LOG_DEBUG << "drain_output_empty_batch task_id=" << task_id_;
      return;
    }
    PANGROVA_LOG_DEBUG << "drain_output_batch task_id=" << task_id_
                      << " rows=" << batch->size() << " batch=" << batches_;
    PANGROVA_LOG_DEBUG << "drain_output_schema task_id=" << task_id_
                      << " columns=" << batch->type()->size();
    if (handle_ < 0) begin(batch->type()->asRow());
    PANGROVA_LOG_DEBUG << "drain_output_export task_id=" << task_id_;
    auto record = exportWriteBatch(batch, pool_->source().get());
    if (!schema_) schema_ = record->schema();
    if (!schema_->Equals(*record->schema(), true)) {
      throw std::runtime_error("Write batch changed its declared Arrow schema");
    }
    auto encoded = write::encodeWriteBatch(*record, pool_);
    if (!encoded.ok()) {
      throw std::runtime_error("Write stream encoding failed: " + encoded.status().ToString());
    }
    const auto& bytes = *encoded;
    PANGROVA_LOG_DEBUG << "drain_output_append task_id=" << task_id_
                      << " bytes=" << bytes->size() << " rows=" << record->num_rows();
    executor_.appendBatchData(handle_, bytes->data(), bytes->size(),
        static_cast<int32_t>(record->num_rows()), static_cast<int32_t>(record->num_columns()));
    rows_ += record->num_rows();
    bytes_ += bytes->size();
    ++batches_;
    if (batches_ % kDrainLogBatchInterval == 0) {
      PANGROVA_LOG_INFO << "drain_output_progress task_id=" << task_id_
                       << " batches=" << batches_ << " total_rows=" << rows_
                       << " total_bytes=" << bytes_;
    }
  }

  void commit() {
    PANGROVA_LOG_INFO << "drain_output_complete task_id=" << task_id_
                     << " batches=" << batches_ << " total_rows=" << rows_
                     << " total_bytes=" << bytes_;
    if (handle_ < 0) return;
    PANGROVA_LOG_INFO << "drain_output_commit task_id=" << task_id_;
    executor_.commitWrite(handle_);
    committed_ = true;
    PANGROVA_LOG_INFO << "drain_output_commit_done task_id=" << task_id_;
  }

  void abort() {
    if (handle_ < 0 || committed_) return;
    PANGROVA_LOG_WARN << "drain_output_abort task_id=" << task_id_ << " handle=" << handle_;
    executor_.abortWrite(handle_);
    handle_ = -1;
  }

 private:
  void begin(const facebook::velox::RowType& rowType) {
    for (size_t i = 0; i < rowType.size(); ++i) {
      descriptor_.columns.emplace_back(rowType.nameOf(i), rowType.childAt(i)->toString());
    }
    PANGROVA_LOG_INFO << "drain_output_begin_write task_id=" << task_id_
                     << " columns=" << rowType.size();
    handle_ = executor_.beginWrite(descriptor_);
    PANGROVA_LOG_DEBUG << "drain_output_begin_write_done task_id=" << task_id_
                      << " handle=" << handle_;
  }

  write::NativeWriteExecutor executor_;
  std::string task_id_;
  write::WriteDescriptor descriptor_;
  std::shared_ptr<memory::ColumnarMemoryPool> pool_;
  std::shared_ptr<arrow::Schema> schema_;
  int64_t handle_{-1};
  int64_t rows_{0}, bytes_{0};
  int batches_{0};
  bool committed_{false};
};

}  // namespace

void drainOutputToWriter(
    OutputQueue* output_queue,
    const WriteConfigInfo& write_config,
    const std::string& task_id,
    std::shared_ptr<facebook::velox::memory::MemoryPool> pool,
    const WriteProducerOutcomeRef& producer_outcome) {
  if (task_id.empty() || !pool) {
    throw std::invalid_argument("Write task requires its canonical task identity and actual pool");
  }
  auto lease = jni::GlobalWriterRegistry::instance().writerForTask(task_id);
  if (!lease) throw std::runtime_error("Write task requires registered NativeWriter: task_id=" + task_id);
  auto [env, detach] = acquireJniEnv();
  JniDetachGuard guard{jni::globalJavaVM(), detach};
  JavaWriteSession session(env, lease.get(), write_config, task_id, std::move(pool));
  std::exception_ptr operationFailure;
  bool finalized = false;
  try {
    finalized = drainWriteOutput(output_queue, producer_outcome, std::chrono::milliseconds(100),
        [&](const facebook::velox::RowVectorPtr& batch) { session.append(batch); },
        [&] { session.commit(); });
  } catch (...) {
    operationFailure = std::current_exception();
  }
  if (!finalized) {
    try {
      session.abort();
    } catch (const std::exception& cleanup) {
      if (operationFailure) {
        try {
          std::rethrow_exception(operationFailure);
        } catch (const std::exception& original) {
          throw std::runtime_error(std::string(original.what()) +
                                   "; Java write cleanup: " + cleanup.what());
        }
      }
      throw;
    }
  }
  if (operationFailure) std::rethrow_exception(operationFailure);
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
