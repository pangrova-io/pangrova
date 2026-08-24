// AggregationMergeReader: AGGREGATION merge reader for Paimon MOR
// primary-key tables. Multi-way merge with configurable aggregation
// functions per column.
//
// Placement: merge/ — merge reader implementations.
// Validates: Requirements R8.6
#pragma once

#include <cstdint>
#include <memory>
#include <queue>
#include <unordered_map>
#include <vector>

#include "merge/DeduplicateMergeReader.h"
#include "merge/MergeReader.h"
#include "velox/vector/ComplexVector.h"

namespace pangrova::merge {

// Supported aggregation function types for Paimon AGGREGATION merge engine.
enum class AggFunction {
  SUM,
  COUNT,
  MAX,
  MIN,
  LAST_VALUE,
  LAST_NON_NULL_VALUE,
  LISTAGG,
  BOOL_AND,
  BOOL_OR,
  COLLECT,
  MERGE_MAP,
  FIRST_VALUE,
  PRODUCT,
  NESTED_UPDATE
};

// Parse a string function name (case-insensitive) to AggFunction enum.
AggFunction parseAggFunction(const std::string& name);

// AGGREGATION merge reader: applies configured aggregation functions to
// merge rows with the same primary key across K sorted input streams.
class AggregationMergeReader final : public MergeReader {
 public:
  AggregationMergeReader(
      std::vector<facebook::velox::column_index_t> primary_key_indices,
      facebook::velox::column_index_t sequence_column_index,
      facebook::velox::RowTypePtr output_type,
      std::unordered_map<facebook::velox::column_index_t, AggFunction>
          agg_specs);

  ~AggregationMergeReader() override { close(); }

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
  void aggregateRow(
      const facebook::velox::RowVectorPtr& target,
      const HeapEntry& source) const;
  void applyAggFunction(
      AggFunction func,
      const facebook::velox::VectorPtr& target,
      const facebook::velox::VectorPtr& source,
      facebook::velox::vector_size_t source_row) const;
  facebook::velox::RowVectorPtr assembleOutput(
      std::vector<facebook::velox::RowVectorPtr>& rows,
      facebook::velox::memory::MemoryPool* pool) const;
  bool isKeyOrSequenceColumn(facebook::velox::column_index_t col) const;
  AggFunction getAggFunction(facebook::velox::column_index_t col) const;

  std::vector<facebook::velox::column_index_t> key_indices_;
  facebook::velox::column_index_t sequence_index_;
  facebook::velox::RowTypePtr output_type_;
  std::unordered_map<facebook::velox::column_index_t, AggFunction> agg_specs_;

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
