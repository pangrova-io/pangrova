// LanceBatchReader: Reads Lance files via Lance C FFI, returning Velox RowVector.
//
// Uses the Arrow C Data Interface to bridge Lance's Arrow-native output into
// Velox's columnar representation without intermediate copies. Supports column
// projection, predicate pushdown, and schema evolution (column mapping from
// SplitConfig).
//
// Lifecycle:
//   1. Construct with file path, output type, and projected columns.
//   2. Optionally call setFilter() for predicate pushdown.
//   3. Call next() in a loop until it returns nullptr (or hasNext() == false).
//   4. Destructor closes the Lance C FFI reader handle.
//
// Thread safety: NOT thread-safe. Single-threaded use within one DataSource.
//
// Placement: lance/ — Lance file format integration layer.
#pragma once

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

namespace pangrova::lance {

class LanceBatchReader {
 public:
  // Construct a reader for a Lance file.
  //
  // @param pool             Velox memory pool for allocating output vectors.
  // @param outputType       Expected output schema (after projection + evolution).
  // @param filePath         Path to the Lance dataset (local or object store URI).
  // @param projectedColumns Column names to read (projection pushdown).
  //                         Empty vector means read all columns.
  explicit LanceBatchReader(
      facebook::velox::memory::MemoryPool* pool,
      const facebook::velox::RowTypePtr& outputType,
      const std::string& filePath,
      const std::vector<std::string>& projectedColumns);

  ~LanceBatchReader();

  // Non-copyable, non-movable (owns opaque C handle).
  LanceBatchReader(const LanceBatchReader&) = delete;
  LanceBatchReader& operator=(const LanceBatchReader&) = delete;
  LanceBatchReader(LanceBatchReader&&) = delete;
  LanceBatchReader& operator=(LanceBatchReader&&) = delete;

  // Read the next batch of up to maxRows rows.
  // Returns nullptr when the dataset is exhausted.
  facebook::velox::RowVectorPtr next(uint64_t maxRows);

  // Set a filter expression for predicate pushdown.
  // Must be called before the first next() call.
  // Filter uses SQL-like syntax (e.g., "id > 10 AND category = 'tech'").
  void setFilter(const std::string& filterExpr);

  // Returns true if there are more batches to read.
  bool hasNext() const;

 private:
  // Convert Arrow C Data Interface output to Velox RowVector.
  facebook::velox::RowVectorPtr importBatch(
      struct ArrowArray& arrowArray,
      struct ArrowSchema& arrowSchema);

#if PANGROVA_LANCE_AVAILABLE
    LanceCReader* lance_reader_handle_;
    facebook::velox::memory::MemoryPool* pool_;
    facebook::velox::RowTypePtr output_type_;
    bool exhausted_ = false;
#endif
};

}  // namespace pangrova::lance
