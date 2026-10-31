// LanceBatchWriter: Implementation of Lance file format writer via C FFI.
//
// Data flow: Velox RowVector → Arrow C Data Interface export → Lance C FFI write.
//
// The Arrow export uses Velox's exportToArrow() which produces ArrowArray and
// ArrowSchema structs. These are passed directly to lance_ffi_writer_write_batch()
// which takes ownership of the ArrowArray data (caller must not release).
//
// Error handling: all Lance C FFI calls check return codes. On failure,
// lance_ffi_last_error() provides the error message, wrapped in std::runtime_error.

#include "lance/LanceBatchWriter.h"

#include <stdexcept>
#include <string>
#include <utility>

#include <arrow/c/abi.h>

#include "velox/vector/ComplexVector.h"
#include "velox/vector/arrow/Bridge.h"

#include "lance_c_ffi.h"
#include "lance/LanceStorageContext.h"
#include "engine/observability/PangrovaLog.h"

namespace pangrova::lance {

namespace {

// Materializes lazy-loaded vectors before Arrow export.
// Velox vectors may be lazily loaded (e.g., from file readers). Arrow export
// requires all data to be materialized in memory.
void materializeLazyChildren(const facebook::velox::RowVectorPtr& row) {
  for (facebook::velox::column_index_t i = 0; i < row->childrenSize(); ++i) {
    auto child = row->childAt(i);
    if (child && child->isLazy()) {
      const_cast<facebook::velox::BaseVector*>(child.get())->loadedVector();
    }
  }
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
};

}  // namespace

LanceBatchWriter::LanceBatchWriter(
    std::shared_ptr<facebook::velox::memory::MemoryPool> pool,
    const facebook::velox::RowTypePtr& outputType,
    const LanceWriterConfig& config)
    : pool_(std::move(pool)),
      output_type_(outputType),
      config_(config),
      writer_handle_(nullptr) {
  if (!pool_) {
    throw std::runtime_error(
        "LanceBatchWriter: memory pool must not be null");
  }
  if (!output_type_) {
    throw std::runtime_error(
        "LanceBatchWriter: output type must not be null");
  }
  if (config_.outputPath.empty()) {
    throw std::runtime_error(
        "LanceBatchWriter: output path must not be empty");
  }
  if (config_.writeId.empty()) {
    throw std::runtime_error("LanceBatchWriter: writeId must not be empty");
  }
  if (config_.tenantId.empty() || config_.generation.empty() ||
      config_.transactionStagingPrefix.empty()) {
    throw std::runtime_error(
        "LanceBatchWriter: tenant, generation, and transaction staging prefix are required");
  }
  if (config_.taskAttempt.empty()) {
    throw std::runtime_error("LanceBatchWriter: taskAttempt must not be empty");
  }
  if ((config_.operation == LanceWriteOperation::kCreate &&
       config_.expectedReadVersion != 0) ||
      (config_.operation != LanceWriteOperation::kCreate &&
       config_.expectedReadVersion == 0)) {
    throw std::runtime_error(
        "LanceBatchWriter: CREATE requires version 0 and APPEND/OVERWRITE require a non-zero version");
  }

  exportSchemaToArrow();
}

LanceBatchWriter::~LanceBatchWriter() {
  if (writer_handle_) {
    try { close(); }
    catch (const std::exception& error) {
      PANGROVA_LOG(ERROR) << "event=dataset_write_cleanup_failed error=" << error.what();
      if (isDrainedAndCleaned()) {
        try { releaseDrained(); }
        catch (const std::exception& release_error) {
          PANGROVA_LOG(ERROR) << "event=dataset_write_owner_release_failed error=" << release_error.what();
        }
      }
    }
  }
}

void LanceBatchWriter::close() {
  if (!writer_handle_) return;
  LanceCWriteDrainEvidence evidence{};
  auto* handle = static_cast<LanceCWriter*>(writer_handle_);
  const auto status = lance_ffi_writer_drain(handle, &evidence);
  encoders_joined_ = evidence.encoders_and_uploads_joined == 1;
  private_stage_cleaned_ = evidence.private_stage_cleaned == 1;
  if (status != 0) {
    const auto* error = lance_ffi_last_error();
    throw std::runtime_error(std::string("Dataset writer drain: ") + (error ? error : "missing FFI diagnostic"));
  }
  releaseDrained();
}

void LanceBatchWriter::releaseDrained() {
  if (!writer_handle_) return;
  if (!isDrainedAndCleaned()) throw std::runtime_error("Dataset writer owners have not actually drained");
  if (lance_ffi_writer_destroy(static_cast<LanceCWriter*>(writer_handle_)) != 0) {
    const auto* error = lance_ffi_last_error();
    throw std::runtime_error(std::string("Dataset writer destroy: ") + (error ? error : "missing FFI diagnostic"));
  }
  writer_handle_ = nullptr;
}

void LanceBatchWriter::exportSchemaToArrow() {
  if (lance_ffi_abi_version() != PANGROVA_LANCE_C_FFI_ABI_VERSION) {
    throw std::runtime_error("Lance C FFI ABI version mismatch");
  }
  if (lance_ffi_runtime_init() != 0) {
    const char* error = lance_ffi_last_error();
    throw std::runtime_error(
        std::string("Lance FFI runtime initialization failed") +
        (error == nullptr ? "" : std::string(": ") + error));
  }

  // Create an empty vector with the output schema to export the ArrowSchema.
  auto emptyVector =
      facebook::velox::BaseVector::create(output_type_, 0, pool_.get());

  ExportedBatch exported;
  facebook::velox::exportToArrow(emptyVector, exported.schema);

  LanceStorageContext storage(config_.storageBinding);
  auto* writer = lance_ffi_writer_open(storage.get(),
      config_.outputPath.c_str(),
      &exported.schema,
      static_cast<int32_t>(config_.operation),
      config_.expectedReadVersion,
      reinterpret_cast<const uint8_t*>(config_.acceptedBaseSnapshot.data()),
      config_.acceptedBaseSnapshot.size(),
      config_.schemaFenceSha256.data(),
      config_.schemaFenceSha256.size(),
      config_.writeId.c_str(),
      config_.tenantId.c_str(),
      config_.generation.c_str(),
      config_.transactionStagingPrefix.c_str(),
      config_.taskAttempt.data(),
      config_.taskAttempt.size());

  if (!writer) {
    const char* err = lance_ffi_last_error();
    std::string msg = "LanceBatchWriter: failed to open writer for path=";
    msg += config_.outputPath;
    if (err) {
      msg += " error=";
      msg += err;
    }
    throw std::runtime_error(msg);
  }

  writer_handle_ = writer;
}

void LanceBatchWriter::writeBatch(const facebook::velox::RowVectorPtr& batch) {
  if (!batch) {
    throw std::runtime_error(
        "LanceBatchWriter::writeBatch: batch must not be null");
  }
  if (!writer_handle_) {
    throw std::runtime_error(
        "LanceBatchWriter::writeBatch: writer is not open");
  }
  if (finished_) {
    throw std::runtime_error(
        "LanceBatchWriter::writeBatch: writer already finished");
  }

  materializeLazyChildren(batch);

  ExportedBatch exported;

  // Export Velox RowVector to Arrow C Data Interface.
  // flattenDictionary/flattenConstant ensures Lance receives plain arrays
  // without Velox-specific encodings.
  ArrowOptions arrowOpts;
  arrowOpts.flattenDictionary = true;
  arrowOpts.flattenConstant = true;
  facebook::velox::exportToArrow(batch, exported.array, pool_.get(), arrowOpts);
  facebook::velox::exportToArrow(batch, exported.schema, arrowOpts);

  // lance_ffi_writer_write_batch takes ownership of arrowArray data.
  // The FFI clears its ownership slot on both success and failure.
  int32_t rc = lance_ffi_writer_write_batch(
      static_cast<LanceCWriter*>(writer_handle_),
      &exported.array,
      &exported.schema);

  if (rc != 0) {
    const char* err = lance_ffi_last_error();
    std::string msg = "LanceBatchWriter::writeBatch: write failed";
    if (err) {
      msg += " error=";
      msg += err;
    }
    throw std::runtime_error(msg);
  }

  stats_.rowsWritten += batch->size();
  stats_.bytesWritten += batch->estimateFlatSize();
  stats_.batchesWritten++;
}

std::vector<uint8_t> LanceBatchWriter::finish() {
  if (!writer_handle_) {
    throw std::runtime_error(
        "LanceBatchWriter::finish: writer is not open");
  }
  if (finished_) {
    throw std::runtime_error(
        "LanceBatchWriter::finish: writer already finished");
  }

  ReturnedFragment returned;
  int32_t rc = lance_ffi_writer_finish(
      static_cast<LanceCWriter*>(writer_handle_), &returned.value);

  if (rc != 0) {
    const char* err = lance_ffi_last_error();
    std::string msg = "LanceBatchWriter::finish: finalize failed";
    if (err) {
      msg += " error=";
      msg += err;
    }
    throw std::runtime_error(msg);
  }

  const auto& fragment = returned.value;
  if (fragment.data == nullptr || fragment.len == 0) {
    throw std::runtime_error(
        "LanceBatchWriter::finish: empty commit metadata");
  }
  std::vector<uint8_t> metadata(fragment.data, fragment.data + fragment.len);
  finished_ = true;
  close();
  return metadata;
}

}  // namespace pangrova::lance
