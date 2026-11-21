#include "lance/LancePhysicalBatchWriter.h"

#include <arrow/buffer.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/record_batch.h>

#include <limits>
#include <exception>
#include <stdexcept>
#include <utility>

#include "capabilities/capability/CanonicalPhysicalTypes.h"
#include "capabilities/capability/LogicalTypeContract.h"
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/io/zero_copy_stream_impl_lite.h"
#include "lance/LanceStorageContext.h"
#include "lance_c_ffi.h"
#include "engine/observability/PangrovaLog.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/vector/arrow/Bridge.h"
#include "engine/write/PhysicalWriteArrowExport.h"
#include "engine/write/WriteArrowExport.h"

namespace pangrova::lance {
namespace {

std::runtime_error ffiError(const char* operation) {
  const char* diagnostic = lance_ffi_last_error();
  return std::runtime_error(std::string(operation) + ": " +
      (diagnostic ? diagnostic : "missing FFI diagnostic"));
}

struct ExportedBatch {
  ArrowArray array{};
  ArrowSchema schema{};
  ~ExportedBatch() {
    if (array.release) array.release(&array);
    if (schema.release) schema.release(&schema);
  }
};

struct ReturnedFragment {
  LanceCBuffer value{};
  ~ReturnedFragment() { lance_ffi_buffer_free(value); }
  std::vector<uint8_t> copy() const {
    if (!value.data || value.len == 0 || value.len > 16 * 1024 * 1024) {
      throw std::runtime_error("Physical writer returned invalid typed receipt bytes");
    }
    return {value.data, value.data + value.len};
  }
};

struct PoolPayload {
  std::shared_ptr<facebook::velox::memory::MemoryPool> pool;
  void* data = nullptr;
  int64_t bytes = 0;
  ~PoolPayload() noexcept {
    if (data) {
      try { pool->free(data, bytes); }
      catch (...) { std::terminate(); } // Invalid pool ownership must not cross C.
    }
  }
  static void release(void* owner) noexcept {
    delete static_cast<PoolPayload*>(owner);
  }
};

std::string canonicalBinding(const protocol::v2::PhysicalFileWriteBinding& binding) {
  const auto size = binding.ByteSizeLong();
  if (size == 0 || size > 64 * 1024 * 1024) {
    throw std::invalid_argument("Physical writer binding size is invalid");
  }
  std::string bytes(size, '\0');
  google::protobuf::io::ArrayOutputStream output(bytes.data(), static_cast<int>(size));
  google::protobuf::io::CodedOutputStream coded(&output);
  coded.SetSerializationDeterministic(true);
  if (!binding.SerializeToCodedStream(&coded) || coded.HadError()) {
    throw std::runtime_error("Physical writer binding serialization failed");
  }
  return bytes;
}

}  // namespace

LancePhysicalBatchWriter::LancePhysicalBatchWriter(
    std::shared_ptr<facebook::velox::memory::MemoryPool> pool,
    facebook::velox::RowTypePtr outputType, PhysicalWriterConfig config)
    : pool_(std::move(pool)), output_type_(std::move(outputType)), config_(std::move(config)) {
  if (!pool_ || !output_type_ || config_.taskAttempt.empty() || config_.tenantId.empty() ||
      config_.spillDirectory.empty() || config_.maxSpillBytes == 0 ||
      config_.maxObjectWriteBytes == 0) {
    throw std::invalid_argument("Physical write requires actual task pool, schema, attempt and grants");
  }
  if (lance_ffi_abi_version() != PANGROVA_LANCE_C_FFI_ABI_VERSION) {
    throw std::runtime_error("Lance physical writer ABI version mismatch");
  }
  validateSchema();
  const auto binding = canonicalBinding(config_.binding);
  const LanceCPhysicalWriterOptions options{
      reinterpret_cast<const uint8_t*>(binding.data()), binding.size(),
      config_.taskAttempt.data(), config_.taskAttempt.size(), config_.tenantId.c_str(),
      config_.spillDirectory.c_str(), config_.maxSpillBytes, config_.maxObjectWriteBytes};
  LanceStorageContext storage(config_.storageBinding);
  handle_ = lance_ffi_physical_writer_open(storage.get(), &options);
  if (!handle_) throw ffiError("Physical writer open");
}

void LancePhysicalBatchWriter::validateSchema() {
  const auto& schema = config_.binding.physical_schema();
  if (schema.empty() || schema.size() > 16 * 1024 * 1024) {
    throw std::invalid_argument("Physical SDK schema metadata size is invalid");
  }
  auto buffer = std::make_shared<arrow::Buffer>(
      reinterpret_cast<const uint8_t*>(schema.data()), static_cast<int64_t>(schema.size()));
  auto input = std::make_shared<arrow::io::BufferReader>(std::move(buffer));
  auto reader = arrow::ipc::RecordBatchStreamReader::Open(input);
  if (!reader.ok()) {
    throw std::invalid_argument("Invalid SDK physical schema: " + reader.status().ToString());
  }
  physical_schema_ = (*reader)->schema();
  if (physical_schema_->num_fields() == 0 ||
      physical_schema_->num_fields() != config_.binding.columns_size() ||
      physical_schema_->num_fields() != static_cast<int>(output_type_->size())) {
    throw std::invalid_argument("Physical write schema must contain every output column");
  }
  for (int i = 0; i < physical_schema_->num_fields(); ++i) {
    const auto& column = config_.binding.columns(i);
    const auto& field = *physical_schema_->field(i);
    if (column.name() != field.name() || column.nullable() != field.nullable()) {
      throw std::invalid_argument("Physical write column identity/nullability differs from SDK schema");
    }
    capability::validateStorageLogicalType(column.logical_type(), field);
    auto physical = capability::canonicalPhysicalType(column.logical_type().substrait_type());
    if (!physical->equivalent(*output_type_->childAt(i))) {
      throw std::invalid_argument("Physical write runtime ordinal type differs from canonical declaration");
    }
  }
}

LancePhysicalBatchWriter::~LancePhysicalBatchWriter() {
  if (handle_) {
    try { refreshResourceUsage(); }
    catch (const std::exception& error) {
      PANGROVA_LOG(ERROR) << "event=physical_write_usage_collection_failed error=" << error.what();
    }
    try { close(); }
    catch (const std::exception& error) {
      PANGROVA_LOG(ERROR) << "event=physical_write_cleanup_failed error=" << error.what();
      if (isDrainedAndCleaned()) {
        try { releaseDrained(); }
        catch (const std::exception& release_error) {
          PANGROVA_LOG(ERROR) << "event=physical_write_owner_release_failed error=" << release_error.what();
        }
      }
    }
  }
}

void LancePhysicalBatchWriter::writeBatch(const facebook::velox::RowVectorPtr& batch) {
  if (!handle_ || finished_ || failed_) {
    throw std::runtime_error("Physical writer is closed, finished or poisoned");
  }
  failed_ = true;
  if (!batch || !batch->type()->equivalent(*output_type_) ||
      static_cast<uint64_t>(batch->size()) > PANGROVA_LANCE_C_FFI_MAX_BATCH_ROWS ||
      facebook::velox::BaseVector::countNulls(batch->nulls(), batch->size()) != 0) {
    throw std::invalid_argument("Physical write batch differs from its complete output schema");
  }
  auto normalized = write::prepareWriteArrowExport(batch);
  ExportedBatch source;
  ArrowOptions options;
  options.flattenDictionary = true;
  options.flattenConstant = true;
  facebook::velox::exportToArrow(normalized, source.array, pool_.get(), options);
  facebook::velox::exportToArrow(normalized, source.schema, options);
  auto imported = arrow::ImportRecordBatch(&source.array, &source.schema);
  if (!imported.ok()) {
    throw std::runtime_error("Physical write C Data import failed: " + imported.status().ToString());
  }
  auto rebound = write::bindPhysicalWriteMetadata(**imported, *physical_schema_);
  ExportedBatch output;
  auto exported = arrow::ExportRecordBatch(*rebound, &output.array, &output.schema);
  if (!exported.ok()) {
    throw std::runtime_error("Physical write C Data export failed: " + exported.ToString());
  }
  if (lance_ffi_physical_writer_write_batch(
      static_cast<LanceCPhysicalWriter*>(handle_), &output.array, &output.schema) != 0) {
    const auto error = ffiError("Physical writer append");
    refreshResourceUsage();
    throw error;
  }
  refreshResourceUsage();
  failed_ = false;
}

void LancePhysicalBatchWriter::close() {
  if (!handle_) return;
  std::exception_ptr usage_failure;
  try { refreshResourceUsage(); }
  catch (...) { usage_failure = std::current_exception(); }
  auto* handle = static_cast<LanceCPhysicalWriter*>(handle_);
  LanceCWriteDrainEvidence evidence{};
  const auto status = lance_ffi_physical_writer_drain(handle, &evidence);
  encoders_joined_ = evidence.encoders_and_uploads_joined == 1;
  private_stage_cleaned_ = evidence.private_stage_cleaned == 1;
  if (status != 0) {
    const auto drain_failure = ffiError("Physical writer drain");
    if (usage_failure) {
      try { std::rethrow_exception(usage_failure); }
      catch (const std::exception& error) {
        throw std::runtime_error(std::string(drain_failure.what()) + "; resource usage collection: " + error.what());
      }
    }
    throw drain_failure;
  }
  // An observation failure must still join the SDK; it cannot erase the failure
  // or allow a success receipt. The caller may release verified drained owners.
  if (usage_failure) std::rethrow_exception(usage_failure);
  releaseDrained();
}

void LancePhysicalBatchWriter::releaseDrained() {
  if (!handle_) return;
  if (!isDrainedAndCleaned()) {
    throw std::runtime_error("Physical owner release requires joined encoders/uploads and cleaned private staging");
  }
  if (lance_ffi_physical_writer_destroy(static_cast<LanceCPhysicalWriter*>(handle_)) != 0) {
    throw ffiError("Physical writer destroy");
  }
  handle_ = nullptr;
}

void LancePhysicalBatchWriter::refreshResourceUsage() {
  ReturnedFragment usage;
  if (lance_ffi_physical_writer_resource_usage(
      static_cast<LanceCPhysicalWriter*>(handle_), &usage.value) != 0) {
    throw ffiError("Physical writer resource usage");
  }
  protocol::v2::ResourceUsage observed;
  if (usage.value.len > 1024 * 1024 ||
      !observed.ParseFromArray(usage.value.data, static_cast<int>(usage.value.len)) ||
      observed.columnar_conversion_copied_bytes() < resource_usage_.columnar_conversion_copied_bytes()) {
    throw std::runtime_error("Physical writer returned invalid canonical resource usage");
  }
  resource_usage_ = std::move(observed);
  if (config_.resourceUsagePublisher) config_.resourceUsagePublisher(resource_usage_);
}

std::vector<uint8_t> LancePhysicalBatchWriter::finish() {
  if (!handle_ || finished_ || failed_) {
    throw std::runtime_error("Physical writer cannot finish twice or after a failed append");
  }
  failed_ = true;
  auto* handle = static_cast<LanceCPhysicalWriter*>(handle_);
  uint64_t bytes = 0;
  if (lance_ffi_physical_writer_finish_stage(handle, &bytes) != 0) {
    const auto error = ffiError("Physical writer stage close");
    refreshResourceUsage();
    throw error;
  }
  ReturnedFragment fragment;
  if (bytes == 0) {
    if (lance_ffi_physical_writer_finish_empty(handle, &fragment.value) != 0) {
      throw ffiError("Physical writer empty receipt");
    }
  } else {
    if (bytes > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
      throw std::runtime_error("Physical upload exceeds the Native allocation size range");
    }
    auto owner = std::make_unique<PoolPayload>();
    owner->pool = pool_;
    owner->bytes = static_cast<int64_t>(bytes);
    owner->data = pool_->allocate(owner->bytes); // Actual task cap, not an empty reservation.
    const LanceCNativePayload payload{static_cast<uint8_t*>(owner->data),
        static_cast<size_t>(bytes), owner.get(), &PoolPayload::release};
    owner.release(); // Ownership transfers on entry, including failed publication.
    if (lance_ffi_physical_writer_publish(handle, payload, &fragment.value) != 0) {
      const auto error = ffiError("Physical writer conditional object creation");
      refreshResourceUsage();
      throw error;
    }
  }
  auto metadata = fragment.copy();
  close();
  finished_ = true;
  failed_ = false;
  return metadata;
}

}  // namespace pangrova::lance
