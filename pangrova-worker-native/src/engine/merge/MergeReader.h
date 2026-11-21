// MergeReader: Abstract merge reader abstraction for Paimon MOR
// (Merge-On-Read) multi-way merge. Operates directly on Velox RowVector
// without Arrow RecordBatch conversion, eliminating serialization overhead.
//
// Supports four merge engine types: DEDUPLICATE, PARTIAL_UPDATE,
// AGGREGATION, FIRST_ROW.
//
// Placement: merge/ — merge reader abstractions.
// Validates: Requirements R8.1, R8.2
#pragma once

#include <cstdint>
#include <limits>
#include <memory>
#include <vector>

#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::memory {
class MemoryPool;
}  // namespace facebook::velox::memory

namespace pangrova::merge {

// Sentinel value indicating no sequence column is configured.
// When passed as sequence_column_index, merge readers treat all rows
// as having equal sequence priority (minimum int64_t).
constexpr facebook::velox::column_index_t kNoSequenceColumn =
    std::numeric_limits<facebook::velox::column_index_t>::max();

// Merge engine types for Paimon primary-key tables.
enum class MergeEngineType {
  DEDUPLICATE,
  PARTIAL_UPDATE,
  AGGREGATION,
  FIRST_ROW
};

// Statistics collected during merge processing.
struct MergeStats {
  int64_t input_rows = 0;
  int64_t output_rows = 0;
  int64_t merged_rows = 0;
  int64_t spilled_bytes = 0;
};

struct MergeDeletePolicy {
  bool ignore_delete = false;
  bool remove_record_on_delete = false;
};

// Abstract wrapper around a RowReader that produces RowVectorPtr.
class RowReaderWrapper {
 public:
  virtual ~RowReaderWrapper() = default;

  virtual facebook::velox::RowVectorPtr next(
      uint64_t size,
      facebook::velox::memory::MemoryPool* pool) = 0;

  virtual bool hasNext() const = 0;
};

// Abstract interface for multi-way merge readers.
class MergeReader {
 public:
  virtual ~MergeReader() = default;

  virtual void initialize(
      std::vector<std::unique_ptr<RowReaderWrapper>> readers) = 0;

  virtual facebook::velox::RowVectorPtr next(
      uint64_t size,
      facebook::velox::memory::MemoryPool* pool) = 0;

  virtual bool hasNext() const = 0;

  virtual MergeStats getStats() const = 0;

  virtual void close() = 0;
};

}  // namespace pangrova::merge
