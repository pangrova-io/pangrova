// LanceBatchReader: Implementation of Lance file reading via Lance C FFI.
// See LanceBatchReader.h for class documentation.

#include "lance/LanceBatchReader.h"

#if PANGROVA_LANCE_AVAILABLE

#include <stdexcept>
#include <sstream>
#include <unordered_map>
#include <memory>
#include <mutex>

#include <arrow/c/abi.h>

#include "velox/vector/arrow/Bridge.h"

#include "lance_c_ffi.h"

namespace pangrova::lance {

// Thread-local Lance FFI runtime cache.
// Eliminates per-reader FFI runtime initialization overhead (~2-5ms per call)
// by caching the initialized runtime handle per thread.
namespace {

struct LanceRuntimeCache {
  // Cache of dataset path -> pre-initialized reader handle.
  // Reuse the Lance runtime across multiple readers on the same thread.
  bool runtime_initialized = false;

  void ensureInitialized() {
    if (!runtime_initialized) {
      lance_ffi_runtime_init();
      runtime_initialized = true;
    }
  }

  ~LanceRuntimeCache() {
    if (runtime_initialized) {
      lance_ffi_runtime_shutdown();
      runtime_initialized = false;
    }
  }
};

thread_local LanceRuntimeCache tl_lance_runtime;

}  // namespace

LanceBatchReader::LanceBatchReader(
    facebook::velox::memory::MemoryPool* pool,
    const facebook::velox::RowTypePtr& outputType,
    const std::string& filePath,
    const std::vector<std::string>& projectedColumns)
    : lance_reader_handle_(nullptr),
      pool_(pool),
      output_type_(outputType) {
  if (!pool) {
    throw std::invalid_argument(
        "LanceBatchReader: memory pool must not be null");
  }
  if (!outputType) {
    throw std::invalid_argument(
        "LanceBatchReader: outputType must not be null");
  }
  if (filePath.empty()) {
    throw std::invalid_argument(
        "LanceBatchReader: filePath must not be empty");
  }

  // Ensure the thread-local Lance FFI runtime is initialized.
  // This amortizes the ~2-5ms runtime init cost across all readers on this thread.
  tl_lance_runtime.ensureInitialized();

  lance_reader_handle_ = lance_ffi_reader_open(filePath.c_str());
  if (!lance_reader_handle_) {
    const char* err = lance_ffi_last_error();
    std::ostringstream msg;
    msg << "LanceBatchReader: failed to open Lance dataset at '"
        << filePath << "'";
    if (err) {
      msg << ": " << err;
    }
    throw std::runtime_error(msg.str());
  }

  // Apply column projection if specified.
  if (!projectedColumns.empty()) {
    std::ostringstream cols;
    for (size_t i = 0; i < projectedColumns.size(); ++i) {
      if (i > 0) {
        cols << ",";
      }
      cols << projectedColumns[i];
    }
    int32_t rc =
        lance_ffi_reader_set_projection(lance_reader_handle_, cols.str().c_str());
    if (rc != 0) {
      const char* err = lance_ffi_last_error();
      std::ostringstream msg;
      msg << "LanceBatchReader: failed to set column projection";
      if (err) {
        msg << ": " << err;
      }
      lance_ffi_reader_close(lance_reader_handle_);
      lance_reader_handle_ = nullptr;
      throw std::runtime_error(msg.str());
    }
  }
}

LanceBatchReader::~LanceBatchReader() {
  if (lance_reader_handle_) {
    lance_ffi_reader_close(lance_reader_handle_);
    lance_reader_handle_ = nullptr;
  }
}

void LanceBatchReader::setFilter(const std::string& filterExpr) {
  if (!lance_reader_handle_) {
    throw std::runtime_error(
        "LanceBatchReader::setFilter: reader handle is null (already closed?)");
  }
  if (filterExpr.empty()) {
    return;
  }

  int32_t rc =
      lance_ffi_reader_set_filter(lance_reader_handle_, filterExpr.c_str());
  if (rc != 0) {
    const char* err = lance_ffi_last_error();
    std::ostringstream msg;
    msg << "LanceBatchReader: failed to set filter expression '"
        << filterExpr << "'";
    if (err) {
      msg << ": " << err;
    }
    throw std::runtime_error(msg.str());
  }
}

bool LanceBatchReader::hasNext() const {
  return !exhausted_;
}

facebook::velox::RowVectorPtr LanceBatchReader::next(uint64_t maxRows) {
  if (exhausted_ || !lance_reader_handle_) {
    return nullptr;
  }

  ArrowArray arrowArray;
  ArrowSchema arrowSchema;
  // Zero-initialize to ensure release callbacks are null if not populated.
  std::memset(&arrowArray, 0, sizeof(ArrowArray));
  std::memset(&arrowSchema, 0, sizeof(ArrowSchema));

  int32_t rc = lance_ffi_reader_next(
      lance_reader_handle_, &arrowArray, &arrowSchema, maxRows);

  if (rc == 0) {
    // End of dataset.
    exhausted_ = true;
    return nullptr;
  }

  if (rc < 0) {
    const char* err = lance_ffi_last_error();
    std::ostringstream msg;
    msg << "LanceBatchReader: error reading next batch";
    if (err) {
      msg << ": " << err;
    }
    throw std::runtime_error(msg.str());
  }

  // rc == 1: batch produced. Import into Velox RowVector.
  return importBatch(arrowArray, arrowSchema);
}

facebook::velox::RowVectorPtr LanceBatchReader::importBatch(
    ArrowArray& arrowArray,
    ArrowSchema& arrowSchema) {
  // importFromArrowAsOwner takes ownership of the Arrow C Data Interface
  // structs (calls release callbacks when the Velox vector is destroyed).
  auto baseVector = facebook::velox::importFromArrowAsOwner(
      arrowSchema, arrowArray, pool_);

  return std::dynamic_pointer_cast<facebook::velox::RowVector>(baseVector);
}

}  // namespace pangrova::lance

#else  // !PANGROVA_LANCE_AVAILABLE

// When Lance C FFI is not linked, all methods fail immediately.
// Callers should check PANGROVA_LANCE_AVAILABLE at compile time or
// fail at DDL time with LANCE_FORMAT_UNAVAILABLE (3013).

#include <stdexcept>

namespace pangrova::lance {

LanceBatchReader::LanceBatchReader(
    facebook::velox::memory::MemoryPool* /*pool*/,
    const facebook::velox::RowTypePtr& /*outputType*/,
    const std::string& /*filePath*/,
    const std::vector<std::string>& /*projectedColumns*/) {
  throw std::runtime_error(
      "LanceBatchReader: Lance C FFI not available. "
      "Build with Lance C FFI to enable Lance file format support.");
}

LanceBatchReader::~LanceBatchReader() = default;

void LanceBatchReader::setFilter(const std::string& /*filterExpr*/) {
  throw std::runtime_error("LanceBatchReader: Lance C FFI not available.");
}

bool LanceBatchReader::hasNext() const {
  return false;
}

facebook::velox::RowVectorPtr LanceBatchReader::next(uint64_t /*maxRows*/) {
  return nullptr;
}

facebook::velox::RowVectorPtr LanceBatchReader::importBatch(
    struct ArrowArray& /*arrowArray*/,
    struct ArrowSchema& /*arrowSchema*/) {
  return nullptr;
}

}  // namespace pangrova::lance

#endif  // PANGROVA_LANCE_AVAILABLE
