// FirstRowMergeReader implementation.
// Validates: Requirements R8.7

#include "merge/FirstRowMergeReader.h"

#include <stdexcept>

#include "velox/vector/BaseVector.h"
#include "velox/vector/FlatVector.h"
#include "merge/SequenceNumberReader.h"
#include "merge/MergeVersionGroup.h"

namespace pangrova::merge {

// =============================================================================
// FirstRowHeapComparator
// =============================================================================

FirstRowHeapComparator::FirstRowHeapComparator(
    std::vector<facebook::velox::column_index_t> key_indices)
    : key_indices_(std::move(key_indices)) {}

bool FirstRowHeapComparator::operator()(
    const HeapEntry& lhs,
    const HeapEntry& rhs) const {
  // Min-heap: return true if lhs should come AFTER rhs.
  // We want smallest primary key popped first.
  // For equal keys, lowest sequence number popped first (first row).
  facebook::velox::CompareFlags flags;
  flags.nullsFirst = false;
  flags.ascending = true;

  for (auto idx : key_indices_) {
    auto cmp = lhs.batch->childAt(idx)->compare(
        rhs.batch->childAt(idx).get(), lhs.row, rhs.row, flags);
    if (cmp.has_value() && cmp.value() != 0) {
      return cmp.value() > 0;
    }
  }

  // Keys are equal: pop lower sequence number first (ascending).
  // lhs should come AFTER rhs if lhs.seq > rhs.seq.
  return lhs.sequence_num > rhs.sequence_num;
}

// =============================================================================
// FirstRowMergeReader
// =============================================================================

FirstRowMergeReader::FirstRowMergeReader(
    std::vector<facebook::velox::column_index_t> primary_key_indices,
    facebook::velox::column_index_t sequence_column_index,
    facebook::velox::RowTypePtr output_type, MergeDeletePolicy delete_policy)
    : key_indices_(std::move(primary_key_indices)),
      sequence_index_(sequence_column_index),
      output_type_(std::move(output_type)), delete_policy_(delete_policy) {
  if (key_indices_.empty()) {
    throw std::invalid_argument(
        "FirstRowMergeReader requires at least one primary key column");
  }
}

void FirstRowMergeReader::initialize(
    std::vector<std::unique_ptr<RowReaderWrapper>> readers) {
  if (initialized_) {
    throw std::logic_error(
        "FirstRowMergeReader::initialize() called more than once");
  }
  initialized_ = true;

  if (readers.empty()) {
    return;
  }

  readers_.reserve(readers.size());
  for (auto& r : readers) {
    ReaderState state;
    state.reader = std::move(r);
    state.exhausted = false;
    readers_.push_back(std::move(state));
  }

  heap_ = std::make_unique<FirstRowHeap>(
      FirstRowHeapComparator(key_indices_));

  // Heap priming is deferred to the first next() call where a valid
  // MemoryPool is available. RowReaderWrapper::next() requires a non-null
  // pool for vector allocation.
  heap_primed_ = false;
}

facebook::velox::RowVectorPtr FirstRowMergeReader::next(
    uint64_t size,
    facebook::velox::memory::MemoryPool* pool) {
  if (!initialized_ || !pool || !heap_) {
    return nullptr;
  }

  // Lazy-prime the heap on first next() call with a valid pool.
  if (!heap_primed_) {
    heap_primed_ = true;
    for (int i = 0; i < static_cast<int>(readers_.size()); ++i) {
      advanceReader(i, pool);
    }
  }

  if (heap_->empty()) {
    return nullptr;
  }

  auto batch_size = (size > 0) ? size : kDefaultBatchSize;
  std::vector<facebook::velox::RowVectorPtr> output_rows;
  output_rows.reserve(std::min(batch_size, static_cast<uint64_t>(1024)));

  while (!heap_->empty() && output_rows.size() < batch_size) {
    auto versions = collectKeyVersions(*heap_, stats_,
        [&](const auto& first, const auto& second) { return keysEqual(first, second); },
        [&](const auto& entry) {
          readers_[entry.reader_index].current_row = entry.row + 1;
          advanceReader(entry.reader_index, pool);
        });
    std::optional<HeapEntry> first;
    for (const auto& version : versions) {
      const auto kind = persistedValueKind(version);
      if (kind == 1 || kind == 3) {
        if (!delete_policy_.ignore_delete) throw std::invalid_argument("First-row merge rejects DELETE/UPDATE_BEFORE");
        continue;
      }
      if (!first) first = version;
    }
    if (first) {
      output_rows.push_back(copyRow(*first, pool));
      stats_.output_rows++;
    }
  }

  if (output_rows.empty()) {
    return nullptr;
  }

  return assembleOutput(output_rows, pool);
}

bool FirstRowMergeReader::hasNext() const {
  if (!initialized_ || !heap_) {
    return false;
  }
  if (!heap_primed_) {
    for (const auto& state : readers_) {
      if (!state.exhausted && state.reader && state.reader->hasNext()) {
        return true;
      }
    }
    return false;
  }
  return !heap_->empty();
}

MergeStats FirstRowMergeReader::getStats() const {
  return stats_;
}

void FirstRowMergeReader::close() {
  if (closed_) {
    return;
  }
  closed_ = true;
  readers_.clear();
  heap_.reset();
}

// =============================================================================
// Private helpers
// =============================================================================

void FirstRowMergeReader::advanceReader(
    int reader_index,
    facebook::velox::memory::MemoryPool* pool) {
  auto& state = readers_[reader_index];
  if (state.exhausted) {
    return;
  }

  while (!state.current_batch ||
         state.current_row >= state.current_batch->size()) {
    if (!state.reader->hasNext()) {
      state.exhausted = true;
      state.current_batch = nullptr;
      return;
    }
    state.current_batch = state.reader->next(kDefaultBatchSize, pool);
    state.current_row = 0;

    if (!state.current_batch || state.current_batch->size() == 0) {
      state.current_batch = nullptr;
      continue;
    }
  }

  pushCurrentRow(reader_index);
}

void FirstRowMergeReader::pushCurrentRow(int reader_index) {
  auto& state = readers_[reader_index];
  if (state.exhausted || !state.current_batch ||
      state.current_row >= state.current_batch->size()) {
    return;
  }

  HeapEntry entry;
  entry.reader_index = reader_index;
  entry.row = state.current_row;
  entry.batch = state.current_batch;
  validatePrimaryKey(entry, key_indices_);
  entry.sequence_num = extractSequenceNumber(entry);

  heap_->push(std::move(entry));
}

bool FirstRowMergeReader::keysEqual(
    const HeapEntry& a,
    const HeapEntry& b) const {
  facebook::velox::CompareFlags flags;
  flags.nullsFirst = false;
  flags.ascending = true;

  for (auto idx : key_indices_) {
    auto cmp = a.batch->childAt(idx)->compare(
        b.batch->childAt(idx).get(), a.row, b.row, flags);
    if (cmp.has_value() && cmp.value() != 0) {
      return false;
    }
  }
  return true;
}

int64_t FirstRowMergeReader::extractSequenceNumber(
    const HeapEntry& entry) const {
  if (sequence_index_ == kNoSequenceColumn) {
    // No explicit sequence column: use reader_index as synthetic ordering.
    // LSM levels are sorted high→low (old→new) by Java side, so lower
    // reader_index = older data. FirstRow heap pops lowest sequence first,
    // so reader_index directly gives oldest files priority.
    return static_cast<int64_t>(entry.reader_index);
  }
  return decodedSequenceNumber(entry.batch, sequence_index_, entry.row);
}

facebook::velox::RowVectorPtr FirstRowMergeReader::copyRow(
    const HeapEntry& entry,
    facebook::velox::memory::MemoryPool* pool) const {
  auto num_children = entry.batch->childrenSize();
  std::vector<facebook::velox::VectorPtr> children(num_children);

  for (decltype(num_children) i = 0; i < num_children; ++i) {
    auto source = entry.batch->childAt(i);
    children[i] = facebook::velox::BaseVector::create(
        source->type(), 1, pool);
    children[i]->copy(source.get(), 0, entry.row, 1);
  }

  return std::make_shared<facebook::velox::RowVector>(
      pool, output_type_, nullptr, 1, std::move(children));
}

facebook::velox::RowVectorPtr FirstRowMergeReader::assembleOutput(
    std::vector<facebook::velox::RowVectorPtr>& rows,
    facebook::velox::memory::MemoryPool* pool) const {
  if (rows.empty()) {
    return nullptr;
  }
  if (rows.size() == 1) {
    return rows[0];
  }

  auto total_rows = static_cast<facebook::velox::vector_size_t>(rows.size());
  auto num_children = output_type_->size();
  std::vector<facebook::velox::VectorPtr> children(num_children);

  for (decltype(num_children) col = 0; col < num_children; ++col) {
    auto col_type = output_type_->childAt(col);
    auto combined = facebook::velox::BaseVector::create(
        col_type, total_rows, pool);

    for (facebook::velox::vector_size_t r = 0; r < total_rows; ++r) {
      combined->copy(rows[r]->childAt(col).get(), r, 0, 1);
    }
    children[col] = std::move(combined);
  }

  return std::make_shared<facebook::velox::RowVector>(
      pool, output_type_, nullptr, total_rows, std::move(children));
}

}  // namespace pangrova::merge
