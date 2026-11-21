#pragma once
#include <arrow/api.h>
#include <arrow/c/abi.h>
#include <memory>

#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::memory {
class MemoryPool;
}  // namespace facebook::velox::memory

namespace pangrova::jni {

// JNI columnar data transfer using Arrow C Data Interface.
// Handles zero-copy export/import of columnar batches across the JNI boundary.
//
// exportBatchToJava: primary output path — columnar batch → C Data Interface structs
//   zero-copy for flat column types (buffer pointers reference source memory)
//   nested types (Map, Array, Struct) recursively exported with correct offsets
//   dictionary encoded columns exported via dictionary field
//
// importBatchFromJava: exchange input path — C Data Interface structs → RecordBatch
class ColumnarDataExporter {
 public:
  // Export a Velox RowVector to Arrow C Data Interface structs for JNI transfer.
  static void exportBatchToJava(
      const facebook::velox::RowVectorPtr& row_vector,
      ArrowArray* out_array,
      ArrowSchema* out_schema,
      facebook::velox::memory::MemoryPool* pool);

  // Import an Arrow C Data Interface batch from Java side.
  static arrow::Result<std::shared_ptr<arrow::RecordBatch>> importBatchFromJava(
      ArrowArray* array, ArrowSchema* schema);
};

}  // namespace pangrova::jni
