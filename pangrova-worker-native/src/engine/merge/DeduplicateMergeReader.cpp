// DeduplicateMergeReader implementation.
// Validates: Requirements R8.4, R8.8

#include "engine/merge/DeduplicateMergeReader.h"

#include <stdexcept>

#include "velox/vector/BaseVector.h"
#include "velox/vector/FlatVector.h"
#include "engine/merge/SequenceNumberReader.h"
#include "engine/merge/MergeVersionGroup.h"
#include "velox/vector/DecodedVector.h"

namespace pangrova::merge {

// =============================================================================
// HeapComparator
// =============================================================================

HeapComparator::HeapComparator(
  std::vector<facebook::velox::column_index_t> key_indices)
  : key_indices_(std::move(key_indices)) {}

bool HeapComparator::operator()(
    const HeapEntry& lhs,
    const HeapEntry& rhs) const {
  // Min-heap: return true if lhs should come AFTER rhs.
  // We want smallest primary key popped first.
  // For equal keys, highest sequence number popped first.
  facebook::velox::CompareFlags flags;
  flags.nullsFirst = false;
  flags.ascending = true;

  for (auto idx : key_indices_) {
    auto cmp = lhs.batch->childAt(idx)->compare(
        rhs.batch->childAt(idx).get(), lhs.row, rhs.row, flags);
    if (cmp.has_value()) {
      if (cmp.value() != 0) {
        // lhs key > rhs key → lhs should be popped later → return true
        return cmp.value() > 0;
      }
    }
  }

  // Keys are equal: pop higher sequence number first (descending).
  return lhs.sequence_num < rhs.sequence_num;
}

// =============================================================================
// DeduplicateMergeReader
// =============================================================================

DeduplicateMergeReader::DeduplicateMergeReader(
    std::vector<facebook::velox::column_index_t> primary_key_indices,
    facebook::velox::column_index_t sequence_column_index,
    facebook::velox::RowTypePtr output_type, MergeDeletePolicy delete_policy)
    : key_indices_(std::move(primary_key_indices)),
      sequence_index_(sequence_column_index),
      output_type_(std::move(output_type)), delete_policy_(delete_policy) {
  if (key_indices_.empty()) {
    throw std::invalid_argument(
        "DeduplicateMergeReader requires at least one primary key column");
  }
}

void DeduplicateMergeReader::initialize(
    std::vector<std::unique_ptr<RowReaderWrapper>> readers) {
  if (initialized_) {
    throw std::logic_error(
        "DeduplicateMergeReader::initialize() called more than once");
  }
  initialized_ = true;

  if (readers.empty()) {
    return;
  }

  // Build reader states.
  readers_.reserve(readers.size());
  for (auto& r : readers) {
    ReaderState state;
    state.reader = std::move(r);
    state.exhausted = false;
    readers_.push_back(std::move(state));
  }

  // Create the min-heap with our comparator.
  heap_ = std::make_unique<MinHeap>(
      HeapComparator(key_indices_));

  // Heap priming is deferred to the first next() call where a valid
  // MemoryPool is available. RowReaderWrapper::next() requires a non-null
  // pool for vector allocation.
  heap_primed_ = false;
}

facebook::velox::RowVectorPtr DeduplicateMergeReader::next(
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
  std::vector<SourceRow> source_rows;
  source_rows.reserve(std::min(batch_size, static_cast<uint64_t>(1024)));

  while (!heap_->empty() && source_rows.size() < batch_size) {
    auto versions = collectKeyVersions(*heap_, stats_,
        [&](const auto& first, const auto& second) { return keysEqual(first, second); },
        [&](const auto& entry) {
          readers_[entry.reader_index].current_row = entry.row + 1;
          advanceReader(entry.reader_index, pool);
        });
    std::optional<HeapEntry> winner;
    for (const auto& version : versions) {
      const auto kind = persistedValueKind(version);
      if (delete_policy_.ignore_delete && (kind == 1 || kind == 3)) continue;
      winner = version;
    }
    if (winner && (persistedValueKind(*winner) == 0 || persistedValueKind(*winner) == 2)) {
      source_rows.push_back(SourceRow{winner->batch,
          static_cast<facebook::velox::vector_size_t>(winner->row)});
      stats_.output_rows++;
    }
  }

  if (source_rows.empty()) {
    return nullptr;
  }

  return assembleOutput(source_rows, pool);
}

bool DeduplicateMergeReader::hasNext() const {
  if (!initialized_ || !heap_) {
    return false;
  }
  // Before heap is primed, check if any reader has data.
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

MergeStats DeduplicateMergeReader::getStats() const {
  return stats_;
}

void DeduplicateMergeReader::close() {
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

void DeduplicateMergeReader::advanceReader(
    int reader_index,
    facebook::velox::memory::MemoryPool* pool) {
  auto& state = readers_[reader_index];
  if (state.exhausted) {
    return;
  }

  // If current batch is exhausted or not yet loaded, fetch next batch.
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

    // L4-10: Selectively materialize only key and sequence columns needed
    // for heap comparison. Payload columns remain lazy until assembleOutput(),
    // saving IO for wide tables when most batch rows are duplicates.
    for (auto idx : key_indices_) {
      state.current_batch->childAt(idx)->loadedVector();
    }
    if (sequence_index_ != kNoSequenceColumn) {
      state.current_batch->childAt(sequence_index_)->loadedVector();
    }
  }

  pushCurrentRow(reader_index);
}

void DeduplicateMergeReader::pushCurrentRow(int reader_index) {
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

bool DeduplicateMergeReader::keysEqual(
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

int64_t DeduplicateMergeReader::extractSequenceNumber(
    const HeapEntry& entry) const {
  if (sequence_index_ == kNoSequenceColumn) {
    // No explicit sequence column: use reader_index as synthetic ordering.
    // Paimon DataSplit.dataFiles() returns files in write order within each
    // LSM level — older files first, newer files last. initializeMergeReader
    // iterates levels then files in order, so higher reader_index = newer data.
    // For DEDUPLICATE semantics, the newest entry must win for equal keys.
    // The heap pops highest sequence_num first for equal keys, so using
    // reader_index directly gives newer files higher priority.
    return static_cast<int64_t>(entry.reader_index);
  }
  return decodedSequenceNumber(entry.batch, sequence_index_, entry.row);
}

// Batch-assemble output directly from source (batch, row) references.
// Eliminates intermediate single-row RowVector allocations: each column
// is created once at final size, then populated via per-row copy from
// the original source batches.
//
// Fast path: when all rows come from the same source batch as a
// contiguous range [start, start+N), use BaseVector::slice() to avoid
// any per-row copying. This is common when a single input batch has
// many unique keys (no dedup needed).
facebook::velox::RowVectorPtr DeduplicateMergeReader::assembleOutput(
    const std::vector<SourceRow>& source_rows,
    facebook::velox::memory::MemoryPool* pool) const {
  if (source_rows.empty()) {
    return nullptr;
  }

  auto total_rows =
      static_cast<facebook::velox::vector_size_t>(source_rows.size());

  // Fast path: single contiguous range from one batch → slice, zero copy.
  const auto& first = source_rows.front();
  bool contiguous = true;
  for (facebook::velox::vector_size_t i = 1; i < total_rows; ++i) {
    if (source_rows[i].batch.get() != first.batch.get() ||
        source_rows[i].row != first.row + i) {
      contiguous = false;
      break;
    }
  }

  if (contiguous) {
    // All rows are [first.row .. first.row + total_rows) in the same batch.
    auto num_children = output_type_->size();
    std::vector<facebook::velox::VectorPtr> children(num_children);
    for (decltype(num_children) col = 0; col < num_children; ++col) {
      // Materialize lazy vectors before slicing — Velox readers may produce
      // lazy-loaded columns that cannot be sliced until loaded.
      auto child = first.batch->childAt(col);
      child->loadedVector();
      children[col] = child->slice(first.row, total_rows);
    }
    return std::make_shared<facebook::velox::RowVector>(
        pool, output_type_, nullptr, total_rows, std::move(children));
  }

  // General path: create each column once at final size, copy row by row
  // directly from source batches (no intermediate single-row vectors).
  auto num_children = output_type_->size();
  std::vector<facebook::velox::VectorPtr> children(num_children);

  for (decltype(num_children) col = 0; col < num_children; ++col) {
    auto col_type = output_type_->childAt(col);
    auto combined = facebook::velox::BaseVector::create(
        col_type, total_rows, pool);

    for (facebook::velox::vector_size_t r = 0; r < total_rows; ++r) {
      // L4-10: Materialize lazy column before copy (idempotent per-batch).
      source_rows[r].batch->childAt(col)->loadedVector();
      combined->copy(
          source_rows[r].batch->childAt(col).get(),
          r,
          source_rows[r].row,
          1);
    }
    children[col] = std::move(combined);
  }

  return std::make_shared<facebook::velox::RowVector>(
      pool, output_type_, nullptr, total_rows, std::move(children));
}

}  // namespace pangrova::merge
