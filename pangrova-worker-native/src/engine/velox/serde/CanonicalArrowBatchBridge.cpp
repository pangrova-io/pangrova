#include "engine/velox/serde/CanonicalArrowBatchBridge.h"

#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/util/bitmap_ops.h>

#include "engine/velox/serde/ArrowBatchVectorSerde.h"
#include "velox/common/base/Exceptions.h"
#include "velox/vector/arrow/Bridge.h"

namespace pangrova::velox {
namespace {
struct ExportedColumnarData {
  ArrowArray array{};
  ArrowSchema schema{};
  ~ExportedColumnarData() {
    if (array.release) array.release(&array);
    if (schema.release) schema.release(&schema);
  }
};

// Velox may retain an old cached null count after an expression clears its
// nulls. Arrow's C Data Interface requires the count to describe the exported
// validity bitmap, so derive it from that bitmap at the transport boundary.
void reconcileExportedNullCounts(ArrowArray& array) {
  VELOX_CHECK_GE(array.length, 0);
  VELOX_CHECK_GE(array.offset, 0);
  VELOX_CHECK_GT(array.n_buffers, 0);
  VELOX_CHECK_NOT_NULL(array.buffers);
  if (const auto* validity = static_cast<const uint8_t*>(array.buffers[0])) {
    array.null_count = array.length -
        arrow::internal::CountSetBits(validity, array.offset, array.length);
  } else {
    VELOX_CHECK_EQ(array.null_count, 0,
        "Exported Arrow array has nulls but no validity bitmap");
  }
  VELOX_CHECK_GE(array.n_children, 0);
  for (int64_t i = 0; i < array.n_children; ++i) {
    VELOX_CHECK_NOT_NULL(array.children);
    VELOX_CHECK_NOT_NULL(array.children[i]);
    reconcileExportedNullCounts(*array.children[i]);
  }
  if (array.dictionary) reconcileExportedNullCounts(*array.dictionary);
}

// Validates that all column types in the RowVector are representable in Arrow
// IPC format. Throws ArrowBatchUnsupportedTypeException for opaque types.
void validateExportType(const facebook::velox::TypePtr& type) {
  if (type->kind() == facebook::velox::TypeKind::OPAQUE) {
    throw ArrowBatchUnsupportedTypeException(type->toString());
  }
  for (uint32_t i = 0; i < type->size(); ++i) validateExportType(type->childAt(i));
}
}  // namespace

void validateArrowExportTypes(const facebook::velox::RowVectorPtr& vector) {
  validateExportType(vector->type());
}

// Exports a Velox RowVector to an Arrow RecordBatch via Arrow C Data Interface.
// Physical encodings are normalized so every batch has one logical wire schema.
std::shared_ptr<arrow::RecordBatch> exportArrowRecordBatch(
    const facebook::velox::RowVectorPtr& vector,
    facebook::velox::memory::MemoryPool* pool) {
  ExportedColumnarData data;
  ArrowOptions arrow_opts;
  // Transport schema is fixed by the logical types, not per-batch encodings.
  arrow_opts.flattenDictionary = true;
  arrow_opts.flattenConstant = true;
  facebook::velox::exportToArrow(vector, data.array, pool, arrow_opts);
  facebook::velox::exportToArrow(vector, data.schema, arrow_opts);
  reconcileExportedNullCounts(data.array);

  auto result = arrow::ImportRecordBatch(&data.array, &data.schema);
  VELOX_CHECK(
      result.ok(),
      "Failed to export RowVector to RecordBatch: {}",
      result.status().ToString());
  return *result;
}

facebook::velox::RowVectorPtr importArrowRecordBatch(
    const arrow::RecordBatch& batch, facebook::velox::memory::MemoryPool* pool) {
  ExportedColumnarData data;
  const auto exported = arrow::ExportRecordBatch(batch, &data.array, &data.schema);
  VELOX_CHECK(exported.ok(), "Arrow batch export failed: {}", exported.ToString());
  auto vector = std::dynamic_pointer_cast<facebook::velox::RowVector>(
      facebook::velox::importFromArrowAsOwner(data.schema, data.array, pool));
  VELOX_CHECK_NOT_NULL(vector);
  return vector;
}

}  // namespace pangrova::velox
