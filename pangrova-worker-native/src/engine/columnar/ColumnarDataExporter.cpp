#include "engine/columnar/ColumnarDataExporter.h"

#include <arrow/c/bridge.h>

#include "velox/vector/ComplexVector.h"
#include "velox/vector/DictionaryVector.h"
#include "velox/vector/LazyVector.h"
#include "velox/vector/arrow/Bridge.h"

namespace pangrova::jni {

namespace {

void materializeVector(const facebook::velox::BaseVector* vec) {
  if (!vec) {
    return;
  }
  if (vec->isLazy()) {
    const_cast<facebook::velox::BaseVector*>(vec)->loadedVector();
    return;
  }
  switch (vec->encoding()) {
    case facebook::velox::VectorEncoding::Simple::DICTIONARY:
    case facebook::velox::VectorEncoding::Simple::SEQUENCE:
    case facebook::velox::VectorEncoding::Simple::CONSTANT: {
      auto* inner = vec->valueVector().get();
      materializeVector(inner);
      break;
    }
    case facebook::velox::VectorEncoding::Simple::ROW: {
      auto* rowVec = vec->as<facebook::velox::RowVector>();
      for (facebook::velox::column_index_t i = 0;
           i < rowVec->childrenSize(); ++i) {
        materializeVector(rowVec->childAt(i).get());
      }
      break;
    }
    case facebook::velox::VectorEncoding::Simple::ARRAY: {
      materializeVector(
          vec->as<facebook::velox::ArrayVector>()->elements().get());
      break;
    }
    case facebook::velox::VectorEncoding::Simple::MAP: {
      auto* mapVec = vec->as<facebook::velox::MapVector>();
      materializeVector(mapVec->mapKeys().get());
      materializeVector(mapVec->mapValues().get());
      break;
    }
    default:
      break;
  }
}

void materializeLazyChildren(const facebook::velox::RowVectorPtr& row) {
  for (facebook::velox::column_index_t i = 0; i < row->childrenSize(); ++i) {
    materializeVector(row->childAt(i).get());
  }
}

}  // namespace

void ColumnarDataExporter::exportBatchToJava(
    const facebook::velox::RowVectorPtr& row_vector,
    ArrowArray* out_array,
    ArrowSchema* out_schema,
    facebook::velox::memory::MemoryPool* pool) {
  if (!row_vector) {
    throw std::runtime_error(
        "ColumnarDataExporter::exportBatchToJava: row_vector must not be null");
  }
  if (!out_array || !out_schema) {
    throw std::runtime_error(
        "ColumnarDataExporter::exportBatchToJava: "
        "out_array and out_schema pointers must not be null");
  }
  if (!pool) {
    throw std::runtime_error(
        "ColumnarDataExporter::exportBatchToJava: pool must not be null");
  }

  materializeLazyChildren(row_vector);

  ArrowOptions arrow_opts;
  arrow_opts.flattenDictionary = true;
  arrow_opts.flattenConstant = true;
  facebook::velox::exportToArrow(row_vector, *out_array, pool, arrow_opts);
  facebook::velox::exportToArrow(row_vector, *out_schema, arrow_opts);

  // Verify that export produced valid Arrow C Data Interface structs.
  // A null release callback means the struct was not properly initialized,
  // which would cause undefined behavior when the consumer calls release.
  if (!out_array->release) {
    throw std::runtime_error(
        "ColumnarDataExporter::exportBatchToJava: "
        "ArrowArray release callback is null after export — "
        "exportToArrow failed silently");
  }
  if (!out_schema->release) {
    // Clean up the array since schema export failed.
    out_array->release(out_array);
    throw std::runtime_error(
        "ColumnarDataExporter::exportBatchToJava: "
        "ArrowSchema release callback is null after export — "
        "exportToArrow failed silently");
  }
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>>
ColumnarDataExporter::importBatchFromJava(
    ArrowArray* array, ArrowSchema* schema) {
  if (!array || !schema) {
    return arrow::Status::Invalid(
        "ArrowArray and ArrowSchema pointers must not be null");
  }
  return arrow::ImportRecordBatch(array, schema);
}

}  // namespace pangrova::jni
