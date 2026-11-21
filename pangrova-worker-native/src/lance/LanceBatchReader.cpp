// LanceBatchReader: Implementation of Lance file reading via Lance C FFI.
// See LanceBatchReader.h for class documentation.

#include "lance/LanceBatchReader.h"

#include <cstring>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string_view>

#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <folly/ScopeGuard.h>

#include "velox/vector/arrow/Bridge.h"

#include "lance_c_ffi.h"
#include "lance/LanceStorageContext.h"
#include "engine/columnar/ColumnarImportLayout.h"
#include "engine/observability/ColumnarReadMetrics.h"
#include "velox/common/base/RuntimeMetrics.h"

namespace pangrova::lance {

namespace {

constexpr std::string_view kDatasetRowPosition = "$pangrova$row_position";

void ensureLanceRuntime() {
  // Rust owns one process-wide runtime shared by all handles. Initialize it
  // once without inventing a per-thread lifecycle that the FFI does not have.
  static std::once_flag initialized;
  std::call_once(initialized, [] {
    if (lance_ffi_abi_version() != PANGROVA_LANCE_C_FFI_ABI_VERSION) {
      throw std::runtime_error("Lance C FFI ABI version mismatch");
    }
    if (lance_ffi_runtime_init() != 0) {
      const char* error = lance_ffi_last_error();
      throw std::runtime_error(
          std::string("Lance FFI runtime initialization failed") +
          (error == nullptr ? "" : std::string(": ") + error));
    }
  });
}

facebook::velox::RowTypePtr readDatasetType(LanceCReader* reader) {
  ArrowSchema schema{};
  if (lance_ffi_reader_schema(reader, &schema) != 0) {
    const char* error = lance_ffi_last_error();
    throw std::runtime_error(
        std::string("LanceBatchReader: failed to read dataset schema") +
        (error == nullptr ? "" : std::string(": ") + error));
  }

  try {
    auto imported = arrow::ImportSchema(&schema);
    if (!imported.ok()) throw std::runtime_error(imported.status().ToString());
    auto normalized = columnar::normalizeImportSchema(**imported);
    if (!normalized.ok()) throw std::runtime_error(normalized.status().ToString());
    auto exported = arrow::ExportSchema(**normalized, &schema);
    if (!exported.ok()) throw std::runtime_error(exported.ToString());
    auto type = facebook::velox::importFromArrow(schema);
    if (schema.release != nullptr) {
      schema.release(&schema);
    }
    auto rowType =
        std::dynamic_pointer_cast<const facebook::velox::RowType>(type);
    if (!rowType || rowType->size() == 0) {
      throw std::runtime_error(
          "LanceBatchReader: dataset schema must be a non-empty row");
    }
    return rowType;
  } catch (...) {
    if (schema.release != nullptr) {
      schema.release(&schema);
    }
    throw;
  }
}

facebook::velox::RowTypePtr resolvePhysicalProjection(
    const facebook::velox::RowTypePtr& requestedType,
    const facebook::velox::RowTypePtr& datasetType,
    const std::vector<std::string>& projectedColumns, bool exactPhysicalFile) {
  if (projectedColumns.empty()) {
    if (requestedType->size() == 0) {
      return facebook::velox::ROW(
          {datasetType->nameOf(0)}, {datasetType->childAt(0)});
    }
    return datasetType;
  }
  if (projectedColumns.size() != requestedType->size()) {
    throw std::invalid_argument(
        "LanceBatchReader: projection and requested type size differ");
  }

  std::vector<std::string> names;
  std::vector<facebook::velox::TypePtr> types;
  names.reserve(projectedColumns.size());
  types.reserve(projectedColumns.size());
  for (size_t index = 0; index < projectedColumns.size(); ++index) {
    const auto& name = projectedColumns[index];
    if (name == kDatasetRowPosition && !exactPhysicalFile) {
      if (datasetType->containsChild(name) ||
          requestedType->childAt(index)->kind() != facebook::velox::TypeKind::BIGINT) {
        throw std::invalid_argument("Dataset physical position requires an unambiguous reserved BIGINT column");
      }
      names.push_back(name);
      types.push_back(facebook::velox::BIGINT());
      continue;
    }
    if (!datasetType->containsChild(name)) {
      if (exactPhysicalFile) throw std::invalid_argument("Physical Lance file lacks schema-owned column '" + name + "'");
      continue;
    }
    const auto physicalIndex = datasetType->getChildIdx(name);
    const auto& actualType = datasetType->childAt(physicalIndex);
    if (!actualType->equivalent(*requestedType->childAt(index))) {
      throw std::runtime_error(
          "LanceBatchReader: physical type mismatch for column '" + name +
          "': dataset=" + actualType->toString() +
          ", planned=" + requestedType->childAt(index)->toString());
    }
    names.push_back(name);
    types.push_back(actualType);
  }

  // A scan that only requests partition or newly-added columns still needs a
  // physical column to establish the dataset row count. The connector drops
  // this carrier column while materializing the planned logical schema.
  if (names.empty()) {
    names.push_back(datasetType->nameOf(0));
    types.push_back(datasetType->childAt(0));
  }
  return facebook::velox::ROW(std::move(names), std::move(types));
}

}  // namespace

LanceBatchReader::LanceBatchReader(
    facebook::velox::memory::MemoryPool* pool,
    const facebook::velox::RowTypePtr& outputType,
    const std::string& filePath,
    uint64_t version,
    const std::vector<std::string>& projectedColumns,
    const std::string& storageBinding,
    const std::string& fileScanDetail,
    uint64_t physicalFileSize)
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
  if (version == 0) {
    throw std::invalid_argument(
        "LanceBatchReader: version must be non-zero");
  }
  if (outputType->containsChild(kDatasetRowPosition) && projectedColumns.empty()) {
    throw std::invalid_argument("Dataset physical position requires an explicit metadata projection");
  }

  ensureLanceRuntime();
  columnar_pool_ = std::make_shared<memory::ColumnarMemoryPool>(pool->shared_from_this());

  LanceStorageContext storage(storageBinding);
  lance_reader_handle_ = lance_ffi_reader_open(storage.get(), filePath.c_str(), version,
      reinterpret_cast<const uint8_t*>(fileScanDetail.data()), fileScanDetail.size(), physicalFileSize);
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

  try {
    output_type_ = resolvePhysicalProjection(
        outputType, readDatasetType(lance_reader_handle_), projectedColumns, physicalFileSize > 0);
  } catch (...) {
    lance_ffi_reader_close(lance_reader_handle_);
    lance_reader_handle_ = nullptr;
    throw;
  }

  // Apply only the projection columns proven present in the physical schema.
  if (!projectedColumns.empty() || outputType->size() == 0) {
    std::vector<const char*> columnNames;
    columnNames.reserve(output_type_->size());
    for (uint32_t index = 0; index < output_type_->size(); ++index) {
      columnNames.push_back(output_type_->nameOf(index).c_str());
    }
    int32_t rc = lance_ffi_reader_set_projection(
        lance_reader_handle_, columnNames.data(), columnNames.size());
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

bool LanceBatchReader::hasNext() const {
  return !exhausted_;
}

uint64_t LanceBatchReader::lastBatchRowStart() const {
  uint64_t position = 0;
  if (lance_ffi_reader_last_batch_row_start(lance_reader_handle_, &position) != 0) {
    const auto* error = lance_ffi_last_error();
    throw std::runtime_error(std::string("Physical file row position failed: ") +
        (error ? error : "missing FFI error"));
  }
  return position;
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
  auto imported = arrow::ImportRecordBatch(&arrowArray, &arrowSchema);
  if (!imported.ok()) throw std::runtime_error(imported.status().ToString());
  columnar::ColumnarImportUsage usage;
  auto converted = columnar::normalizeImportLayout(**imported, columnar_pool_, usage);
  // Report completed allocations/copies even when a later column rejects input.
  columnar_copied_bytes_ += usage.copiedBytes;
  columnar_offset_bytes_ += usage.allocatedOffsetBytes;
  // Driver's operator scope owns these deltas even if conversion throws or a
  // split/MOR reader is discarded. Offset construction is not a payload copy.
  if (usage.copiedBytes > 0) {
    facebook::velox::addThreadLocalRuntimeStat(
        observability::kColumnarReadCopiedBytes,
        facebook::velox::RuntimeCounter(static_cast<int64_t>(usage.copiedBytes),
            facebook::velox::RuntimeCounter::Unit::kBytes));
  }
  if (usage.allocatedOffsetBytes > 0) {
    facebook::velox::addThreadLocalRuntimeStat(
        observability::kColumnarReadOffsetBytes,
        facebook::velox::RuntimeCounter(static_cast<int64_t>(usage.allocatedOffsetBytes),
            facebook::velox::RuntimeCounter::Unit::kBytes));
  }
  if (!converted.ok()) throw std::runtime_error(converted.status().ToString());
  auto exported = arrow::ExportRecordBatch(**converted, &arrowArray, &arrowSchema);
  if (!exported.ok()) throw std::runtime_error(exported.ToString());
  auto release = folly::makeGuard([&] {
    if (arrowArray.release) arrowArray.release(&arrowArray);
    if (arrowSchema.release) arrowSchema.release(&arrowSchema);
  });
  // Ownership must transfer on entry, including a failing task allocation.
  auto baseVector = facebook::velox::importFromArrowAsOwner(
      arrowSchema, arrowArray, pool_);
  auto rowVector =
      std::dynamic_pointer_cast<facebook::velox::RowVector>(baseVector);
  if (!rowVector) {
    throw std::runtime_error(
        "LanceBatchReader: Arrow batch root must be a row/struct");
  }
  auto actualType =
      std::dynamic_pointer_cast<const facebook::velox::RowType>(rowVector->type());
  bool schemaMatches = actualType && actualType->size() == output_type_->size();
  for (uint32_t i = 0; schemaMatches && i < output_type_->size(); ++i) {
    schemaMatches = actualType->nameOf(i) == output_type_->nameOf(i) &&
        actualType->childAt(i)->equivalent(*output_type_->childAt(i));
  }
  if (!schemaMatches) {
    throw std::runtime_error(
        "LanceBatchReader: projected Arrow schema differs from the planned output type");
  }
  return rowVector;
}

}  // namespace pangrova::lance
