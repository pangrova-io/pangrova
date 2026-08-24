// DeduplicateMergeReader: DEDUPLICATE merge reader for Paimon MOR
// primary-key tables. Multi-way merge of K sorted input streams using
// a min-heap, achieving O(N log K) merge complexity.
//
// Placement: merge/ — merge reader implementations.
// Validates: Requirements R8.4, R8.8
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <queue>
#include <vector>

#include "merge/MergeReader.h"
#include "velox/vector/ComplexVector.h"

namespace pangrova::merge {

// Per-reader state tracking current position within buffered batches.
struct ReaderState {
  std::unique_ptr<RowReaderWrapper> reader;
  facebook::velox::RowVectorPtr current_batch;
  int64_t current_row = 0;
  bool exhausted = false;
};

// Entry in the min-heap representing a single row from one reader.
struct HeapEntry {
  int reader_index;
  int64_t row;
  int64_t sequence_num;
  facebook::velox::RowVectorPtr batch;
};

// Comparator for the min-heap. Orders by primary key ascending.
// For equal keys, orders by sequence number descending so the latest
// version is popped first.
class HeapComparator {
 public:
  explicit HeapComparator(
      std::vector<facebook::velox::column_index_t> key_indices);

  bool operator()(const HeapEntry& lhs, const HeapEntry& rhs) const;

 private:
  std::vector<facebook::velox::column_index_t> key_indices_;
};

// DEDUPLICATE merge reader: retains only the latest version per key.
class DeduplicateMergeReader final : public MergeReader {
 public:
  DeduplicateMergeReader(
      std::vector<facebook::velox::column_index_t> primary_key_indices,
      facebook::velox::column_index_t sequence_column_index,
      facebook::velox::RowTypePtr output_type);

  ~DeduplicateMergeReader() override { close(); }

  void initialize(
      std::vector<std::unique_ptr<RowReaderWrapper>> readers) override;

  facebook::velox::RowVectorPtr next(
      uint64_t size,
      facebook::velox::memory::MemoryPool* pool) override;

  bool hasNext() const override;
  MergeStats getStats() const override;
  void close() override;

 private:
  // A reference to a source row: batch pointer + row index within that batch.
  // Used to collect winner rows without intermediate single-row vector copies.
  struct SourceRow {
    facebook::velox::RowVectorPtr batch;
    facebook::velox::vector_size_t row;
  };

  void advanceReader(int reader_index,
                     facebook::velox::memory::MemoryPool* pool);
  void pushCurrentRow(int reader_index);
  bool keysEqual(const HeapEntry& a, const HeapEntry& b) const;
  int64_t extractSequenceNumber(const HeapEntry& entry) const;
  facebook::velox::RowVectorPtr assembleOutput(
      const std::vector<SourceRow>& source_rows,
      facebook::velox::memory::MemoryPool* pool) const;

  std::vector<facebook::velox::column_index_t> key_indices_;
  facebook::velox::column_index_t sequence_index_;
  facebook::velox::RowTypePtr output_type_;

  std::vector<ReaderState> readers_;
  using MinHeap = std::priority_queue<
      HeapEntry, std::vector<HeapEntry>, HeapComparator>;
  std::unique_ptr<MinHeap> heap_;

  MergeStats stats_;
  bool initialized_ = false;
  bool closed_ = false;
  bool heap_primed_ = false;
  static constexpr uint64_t kDefaultBatchSize = 4096;
};

}  // namespace pangrova::merge
