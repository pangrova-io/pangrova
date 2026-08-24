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

#if PANGROVA_LANCE_AVAILABLE

#include <stdexcept>
#include <string>

#include <arrow/c/abi.h>

#include "velox/vector/ComplexVector.h"
#include "velox/vector/arrow/Bridge.h"

#include "lance_c_ffi.h"

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

}  // namespace

LanceBatchWriter::LanceBatchWriter(
    facebook::velox::memory::MemoryPool* pool,
    const facebook::velox::RowTypePtr& outputType,
    const LanceWriterConfig& config)
    : pool_(pool),
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

  exportSchemaToArrow();
}

LanceBatchWriter::~LanceBatchWriter() {
  if (writer_handle_) {
    lance_ffi_writer_close(static_cast<LanceCWriter*>(writer_handle_));
    writer_handle_ = nullptr;
  }
}

void LanceBatchWriter::exportSchemaToArrow() {
  // Create an empty vector with the output schema to export the ArrowSchema.
  auto emptyVector =
      facebook::velox::BaseVector::create(output_type_, 0, pool_);

  ArrowSchema arrowSchema;
  facebook::velox::exportToArrow(emptyVector, arrowSchema);

  auto* writer = lance_ffi_writer_open(
      config_.outputPath.c_str(),
      &arrowSchema,
      config_.append ? 1 : 0);

  // Release the schema — lance_ffi_writer_open does not take ownership.
  if (arrowSchema.release) {
    arrowSchema.release(&arrowSchema);
  }

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

  ArrowArray arrowArray;
  ArrowSchema arrowSchema;

  // Export Velox RowVector to Arrow C Data Interface.
  // flattenDictionary/flattenConstant ensures Lance receives plain arrays
  // without Velox-specific encodings.
  ArrowOptions arrowOpts;
  arrowOpts.flattenDictionary = true;
  arrowOpts.flattenConstant = true;
  facebook::velox::exportToArrow(batch, arrowArray, pool_, arrowOpts);
  facebook::velox::exportToArrow(batch, arrowSchema, arrowOpts);

  // lance_ffi_writer_write_batch takes ownership of arrowArray data.
  // Caller must not release arrowArray after this call.
  int32_t rc = lance_ffi_writer_write_batch(
      static_cast<LanceCWriter*>(writer_handle_),
      &arrowArray,
      &arrowSchema);

  // Release the schema (writer does not take ownership of schema).
  if (arrowSchema.release) {
    arrowSchema.release(&arrowSchema);
  }

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
  stats_.batchesWritten++;
}

void LanceBatchWriter::finish() {
  if (!writer_handle_) {
    throw std::runtime_error(
        "LanceBatchWriter::finish: writer is not open");
  }
  if (finished_) {
    throw std::runtime_error(
        "LanceBatchWriter::finish: writer already finished");
  }

  int32_t rc = lance_ffi_writer_finish(
      static_cast<LanceCWriter*>(writer_handle_));

  if (rc != 0) {
    const char* err = lance_ffi_last_error();
    std::string msg = "LanceBatchWriter::finish: finalize failed";
    if (err) {
      msg += " error=";
      msg += err;
    }
    throw std::runtime_error(msg);
  }

  finished_ = true;
}

}  // namespace pangrova::lance

#endif  // PANGROVA_LANCE_AVAILABLE
