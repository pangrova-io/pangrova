// FirstRowMergeReader: FIRST_ROW merge reader for Paimon MOR
// primary-key tables. Retains only the first occurrence (lowest
// sequence number) for each primary key.
//
// Placement: merge/ — merge reader implementations.
// Validates: Requirements R8.7
#pragma once

#include <cstdint>
#include <memory>
#include <queue>
#include <vector>

#include "merge/DeduplicateMergeReader.h"
#include "merge/MergeReader.h"
#include "velox/vector/ComplexVector.h"

namespace pangrova::merge {

// Comparator for FIRST_ROW merge. For equal keys, orders by sequence
// number ascending so the first occurrence is popped first.
class FirstRowHeapComparator {
 public:
  explicit FirstRowHeapComparator(
      std::vector<facebook::velox::column_index_t> key_indices);

  bool operator()(const HeapEntry& lhs, const HeapEntry& rhs) const;

 private:
  std::vector<facebook::velox::column_index_t> key_indices_;
};

// FIRST_ROW merge reader: retains only the first occurrence per key.
class FirstRowMergeReader final : public MergeReader {
 public:
  FirstRowMergeReader(
      std::vector<facebook::velox::column_index_t> primary_key_indices,
      facebook::velox::column_index_t sequence_column_index,
      facebook::velox::RowTypePtr output_type);

  ~FirstRowMergeReader() override { close(); }

  void initialize(
      std::vector<std::unique_ptr<RowReaderWrapper>> readers) override;
  facebook::velox::RowVectorPtr next(
      uint64_t size, facebook::velox::memory::MemoryPool* pool) override;
  bool hasNext() const override;
  MergeStats getStats() const override;
  void close() override;

 private:
  void advanceReader(int reader_index,
                     facebook::velox::memory::MemoryPool* pool);
  void pushCurrentRow(int reader_index);
  bool keysEqual(const HeapEntry& a, const HeapEntry& b) const;
  int64_t extractSequenceNumber(const HeapEntry& entry) const;
  facebook::velox::RowVectorPtr copyRow(
      const HeapEntry& entry,
      facebook::velox::memory::MemoryPool* pool) const;
  facebook::velox::RowVectorPtr assembleOutput(
      std::vector<facebook::velox::RowVectorPtr>& rows,
      facebook::velox::memory::MemoryPool* pool) const;

  std::vector<facebook::velox::column_index_t> key_indices_;
  facebook::velox::column_index_t sequence_index_;
  facebook::velox::RowTypePtr output_type_;

  std::vector<ReaderState> readers_;
  using FirstRowHeap = std::priority_queue<
      HeapEntry, std::vector<HeapEntry>, FirstRowHeapComparator>;
  std::unique_ptr<FirstRowHeap> heap_;

  MergeStats stats_;
  bool initialized_ = false;
  bool closed_ = false;
  bool heap_primed_ = false;
  static constexpr uint64_t kDefaultBatchSize = 4096;
};

}  // namespace pangrova::merge
