// PartialUpdateMergeReader: PARTIAL_UPDATE merge reader for Paimon MOR
// primary-key tables. Merges non-null fields from multiple versions of
// the same primary key.
//
// Placement: merge/ — merge reader implementations.
// Validates: Requirements R8.5
#pragma once

#include <cstdint>
#include <memory>
#include <queue>
#include <vector>

#include "merge/DeduplicateMergeReader.h"
#include "merge/MergeReader.h"
#include "velox/vector/ComplexVector.h"

namespace pangrova::merge {

// PARTIAL_UPDATE merge reader: merges non-null fields from multiple
// versions of the same primary key across K sorted input streams.
class PartialUpdateMergeReader final : public MergeReader {
 public:
  PartialUpdateMergeReader(
      std::vector<facebook::velox::column_index_t> primary_key_indices,
      facebook::velox::column_index_t sequence_column_index,
      facebook::velox::RowTypePtr output_type);

  ~PartialUpdateMergeReader() override { close(); }

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
  void mergeNonNullFields(
      const facebook::velox::RowVectorPtr& target,
      const HeapEntry& source) const;
  facebook::velox::RowVectorPtr assembleOutput(
      std::vector<facebook::velox::RowVectorPtr>& rows,
      facebook::velox::memory::MemoryPool* pool) const;
  bool isKeyOrSequenceColumn(facebook::velox::column_index_t col) const;

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
