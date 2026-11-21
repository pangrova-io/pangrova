// LanceBatchReader: Reads Lance files via Lance C FFI, returning Velox RowVector.
//
// Uses the Arrow C Data Interface to bridge Lance's Arrow-native output into
// Velox's columnar representation. Compatible value buffers remain shared;
// fixed-list offsets and sliced bitmap alignment use the actual task pool.
// The reader
// resolves projection against the physical dataset schema so a caller can
// request columns that were added after the dataset was written.
//
// Lifecycle:
//   1. Construct with file path, output type, and projected columns.
//   2. Call next() in a loop until it returns nullptr (or hasNext() == false).
//   3. Destructor closes the Lance C FFI reader handle.
// Predicates belong to canonical FilterRel / Velox FilterNode execution.
//
// Thread safety: NOT thread-safe. Single-threaded use within one DataSource.
//
// Placement: lance/ — Lance file format integration layer.
#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "velox/vector/ComplexVector.h"

// Forward-declare the opaque Lance C FFI handle to avoid leaking the C header
// into all translation units that include this header.
struct LanceCReader;

// Forward-declare the Arrow C Data Interface structs at global scope so the
// member signatures below refer to ::ArrowArray / ::ArrowSchema (defined in
// <arrow/c/abi.h>) instead of accidentally declaring new incomplete types
// inside the pangrova::lance namespace.
struct ArrowArray;
struct ArrowSchema;
namespace pangrova::memory { class ColumnarMemoryPool; }

namespace pangrova::lance {

class LanceBatchReader {
 public:
  // Construct a reader for a Lance file.
  //
  // @param pool             Velox memory pool for allocating output vectors.
  // @param outputType       Expected output schema (after projection + evolution).
  // @param filePath         Path to the Lance dataset (local or object store URI).
  // @param version          Exact, non-zero catalog-pinned Lance version.
  // @param projectedColumns Column names to read (projection pushdown).
  //                         Empty vector means read all columns.
  explicit LanceBatchReader(
      facebook::velox::memory::MemoryPool* pool,
      const facebook::velox::RowTypePtr& outputType,
      const std::string& filePath,
      uint64_t version,
      const std::vector<std::string>& projectedColumns,
      const std::string& storageBinding,
      const std::string& fileScanDetail,
      uint64_t physicalFileSize);

  ~LanceBatchReader();

  // Non-copyable, non-movable (owns opaque C handle).
  LanceBatchReader(const LanceBatchReader&) = delete;
  LanceBatchReader& operator=(const LanceBatchReader&) = delete;
  LanceBatchReader(LanceBatchReader&&) = delete;
  LanceBatchReader& operator=(LanceBatchReader&&) = delete;

  // Read the next batch of up to maxRows rows.
  // Returns nullptr when the dataset is exhausted.
  facebook::velox::RowVectorPtr next(uint64_t maxRows);

  // Physical schema produced by next(). Missing requested columns are omitted
  // and must be materialized by the connector's schema-evolution stage.
  const facebook::velox::RowTypePtr& outputType() const {
    return output_type_;
  }

  // Returns true if there are more batches to read.
  bool hasNext() const;

  uint64_t lastBatchRowStart() const;

  uint64_t columnarConversionCopiedBytes() const { return columnar_copied_bytes_; }
  uint64_t columnarLayoutOffsetBytes() const { return columnar_offset_bytes_; }

 private:
  // Convert Arrow C Data Interface output to Velox RowVector.
  facebook::velox::RowVectorPtr importBatch(
      struct ArrowArray& arrowArray,
      struct ArrowSchema& arrowSchema);

  LanceCReader* lance_reader_handle_;
  facebook::velox::memory::MemoryPool* pool_;
  facebook::velox::RowTypePtr output_type_;
  std::shared_ptr<memory::ColumnarMemoryPool> columnar_pool_;
  uint64_t columnar_copied_bytes_ = 0;
  uint64_t columnar_offset_bytes_ = 0;
  bool exhausted_ = false;
};

}  // namespace pangrova::lance
