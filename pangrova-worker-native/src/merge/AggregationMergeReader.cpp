// AggregationMergeReader implementation.
// Validates: Requirements R8.6

#include "merge/AggregationMergeReader.h"

#include <algorithm>
#include <stdexcept>
#include <type_traits>

#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/SimpleVector.h"

namespace pangrova::merge {

namespace {

// Convert string to uppercase for case-insensitive comparison.
std::string toUpper(const std::string& s) {
  std::string result = s;
  std::transform(result.begin(), result.end(), result.begin(), ::toupper);
  return result;
}

// Apply SUM aggregation for numeric types.
// Adds source value to target value. Null source is treated as zero.
// P1-TC-12: Use __builtin_add_overflow for integer types to detect overflow.
// P0-TC-SIGSEGV: Use SimpleVector for source to handle ConstantVector and
// DictionaryVector safely (asFlatVector returns nullptr for non-flat).
template <typename T>
void applySumTyped(
    const facebook::velox::VectorPtr& target,
    const facebook::velox::VectorPtr& source,
    facebook::velox::vector_size_t source_row) {
  auto target_flat = target->asFlatVector<T>();
  auto source_sv = source->as<facebook::velox::SimpleVector<T>>();
  if (!source_sv || source_sv->isNullAt(source_row)) {
    return;  // null or incompatible encoding contributes nothing to SUM
  }
  T src_val = source_sv->valueAt(source_row);
  if (target_flat->isNullAt(0)) {
    target_flat->setNull(0, false);
    target_flat->set(0, src_val);
  } else {
    T cur = target_flat->valueAt(0);
    T result;
    if constexpr (std::is_integral_v<T>) {
      if (__builtin_add_overflow(cur, src_val, &result)) {
        throw std::overflow_error(
            "AggregationMergeReader: integer SUM overflow detected");
      }
      target_flat->set(0, result);
    } else {
      target_flat->set(0, cur + src_val);
    }
  }
}

// Apply MAX aggregation for comparable types.
// P0-TC-SIGSEGV: Use SimpleVector for source (see applySumTyped comment).
template <typename T>
void applyMaxTyped(
    const facebook::velox::VectorPtr& target,
    const facebook::velox::VectorPtr& source,
    facebook::velox::vector_size_t source_row) {
  auto target_flat = target->asFlatVector<T>();
  auto source_sv = source->as<facebook::velox::SimpleVector<T>>();
  if (!source_sv || source_sv->isNullAt(source_row)) {
    return;
  }
  T src_val = source_sv->valueAt(source_row);
  if (target_flat->isNullAt(0)) {
    target_flat->setNull(0, false);
    target_flat->set(0, src_val);
  } else if (src_val > target_flat->valueAt(0)) {
    target_flat->set(0, src_val);
  }
}

// Apply MIN aggregation for comparable types.
// P0-TC-SIGSEGV: Use SimpleVector for source (see applySumTyped comment).
template <typename T>
void applyMinTyped(
    const facebook::velox::VectorPtr& target,
    const facebook::velox::VectorPtr& source,
    facebook::velox::vector_size_t source_row) {
  auto target_flat = target->asFlatVector<T>();
  auto source_sv = source->as<facebook::velox::SimpleVector<T>>();
  if (!source_sv || source_sv->isNullAt(source_row)) {
    return;
  }
  T src_val = source_sv->valueAt(source_row);
  if (target_flat->isNullAt(0)) {
    target_flat->setNull(0, false);
    target_flat->set(0, src_val);
  } else if (src_val < target_flat->valueAt(0)) {
    target_flat->set(0, src_val);
  }
}

}  // namespace

// =============================================================================
// parseAggFunction
// =============================================================================

AggFunction parseAggFunction(const std::string& name) {
  auto upper = toUpper(name);
  if (upper == "SUM") return AggFunction::SUM;
  if (upper == "COUNT") return AggFunction::COUNT;
  if (upper == "MAX") return AggFunction::MAX;
  if (upper == "MIN") return AggFunction::MIN;
  if (upper == "LAST_VALUE") return AggFunction::LAST_VALUE;
  if (upper == "LAST_NON_NULL_VALUE") return AggFunction::LAST_NON_NULL_VALUE;
  if (upper == "LISTAGG") return AggFunction::LISTAGG;
  if (upper == "BOOL_AND") return AggFunction::BOOL_AND;
  if (upper == "BOOL_OR") return AggFunction::BOOL_OR;
  if (upper == "COLLECT") return AggFunction::COLLECT;
  if (upper == "MERGE_MAP") return AggFunction::MERGE_MAP;
  if (upper == "FIRST_VALUE") return AggFunction::FIRST_VALUE;
  if (upper == "PRODUCT") return AggFunction::PRODUCT;
  if (upper == "NESTED_UPDATE") return AggFunction::NESTED_UPDATE;
  throw std::invalid_argument(
      "Unknown aggregation function: " + name);
}

// =============================================================================
// AggregationMergeReader
// =============================================================================

AggregationMergeReader::AggregationMergeReader(
    std::vector<facebook::velox::column_index_t> primary_key_indices,
    facebook::velox::column_index_t sequence_column_index,
    facebook::velox::RowTypePtr output_type,
    std::unordered_map<facebook::velox::column_index_t, AggFunction> agg_specs)
    : key_indices_(std::move(primary_key_indices)),
      sequence_index_(sequence_column_index),
      output_type_(std::move(output_type)),
      agg_specs_(std::move(agg_specs)) {
  if (key_indices_.empty()) {
    throw std::invalid_argument(
        "AggregationMergeReader requires at least one primary key column");
  }
}

void AggregationMergeReader::initialize(
    std::vector<std::unique_ptr<RowReaderWrapper>> readers) {
  if (initialized_) {
    throw std::logic_error(
        "AggregationMergeReader::initialize() called more than once");
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

  heap_ = std::make_unique<MinHeap>(
      HeapComparator(key_indices_));

  // Heap priming is deferred to the first next() call where a valid
  // MemoryPool is available. RowReaderWrapper::next() requires a non-null
  // pool for vector allocation.
  heap_primed_ = false;
}

facebook::velox::RowVectorPtr AggregationMergeReader::next(
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
    auto winner = heap_->top();
    heap_->pop();
    stats_.input_rows++;

    // Start with the latest version as the accumulated row.
    auto accumulated = copyRow(winner, pool);

    // Advance the winner's reader.
    readers_[winner.reader_index].current_row = winner.row + 1;
    advanceReader(winner.reader_index, pool);

    // Consume all subsequent entries with the same primary key and
    // aggregate their values into the accumulated row.
    while (!heap_->empty() && keysEqual(winner, heap_->top())) {
      auto older = heap_->top();
      heap_->pop();
      stats_.input_rows++;
      stats_.merged_rows++;

      aggregateRow(accumulated, older);

      readers_[older.reader_index].current_row = older.row + 1;
      advanceReader(older.reader_index, pool);
    }

    output_rows.push_back(std::move(accumulated));
    stats_.output_rows++;
  }

  if (output_rows.empty()) {
    return nullptr;
  }

  return assembleOutput(output_rows, pool);
}

bool AggregationMergeReader::hasNext() const {
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

MergeStats AggregationMergeReader::getStats() const {
  return stats_;
}

void AggregationMergeReader::close() {
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

void AggregationMergeReader::advanceReader(
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

    // L4-10: Selectively materialize only key and sequence columns needed
    // for heap comparison. Payload columns remain lazy until accessed in
    // copyRow() or aggregateRow(), saving IO for wide tables where many
    // columns are not needed for ordering.
    for (auto idx : key_indices_) {
      state.current_batch->childAt(idx)->loadedVector();
    }
    if (sequence_index_ != kNoSequenceColumn) {
      state.current_batch->childAt(sequence_index_)->loadedVector();
    }
  }

  pushCurrentRow(reader_index);
}

void AggregationMergeReader::pushCurrentRow(int reader_index) {
  auto& state = readers_[reader_index];
  if (state.exhausted || !state.current_batch ||
      state.current_row >= state.current_batch->size()) {
    return;
  }

  HeapEntry entry;
  entry.reader_index = reader_index;
  entry.row = state.current_row;
  entry.batch = state.current_batch;
  entry.sequence_num = extractSequenceNumber(entry);

  heap_->push(std::move(entry));
}

bool AggregationMergeReader::keysEqual(
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

int64_t AggregationMergeReader::extractSequenceNumber(
    const HeapEntry& entry) const {
  if (sequence_index_ == kNoSequenceColumn) {
    // No explicit sequence column: use reader_index as synthetic ordering.
    // For aggregation, all versions are merged so ordering is less critical,
    // but consistent ordering ensures deterministic output.
    return static_cast<int64_t>(entry.reader_index);
  }
  auto seq_vector = entry.batch->childAt(sequence_index_);
  // P1-TC-10: Dispatch on typeKind for INTEGER/SMALLINT/TINYINT, widen to int64_t.
  switch (seq_vector->typeKind()) {
    case facebook::velox::TypeKind::BIGINT: {
      auto flat = seq_vector->asFlatVector<int64_t>();
      if (flat && !flat->isNullAt(entry.row)) {
        return flat->valueAt(entry.row);
      }
      break;
    }
    case facebook::velox::TypeKind::INTEGER: {
      auto flat = seq_vector->asFlatVector<int32_t>();
      if (flat && !flat->isNullAt(entry.row)) {
        return static_cast<int64_t>(flat->valueAt(entry.row));
      }
      break;
    }
    case facebook::velox::TypeKind::SMALLINT: {
      auto flat = seq_vector->asFlatVector<int16_t>();
      if (flat && !flat->isNullAt(entry.row)) {
        return static_cast<int64_t>(flat->valueAt(entry.row));
      }
      break;
    }
    case facebook::velox::TypeKind::TINYINT: {
      auto flat = seq_vector->asFlatVector<int8_t>();
      if (flat && !flat->isNullAt(entry.row)) {
        return static_cast<int64_t>(flat->valueAt(entry.row));
      }
      break;
    }
    default:
      break;
  }
  return std::numeric_limits<int64_t>::min();
}

facebook::velox::RowVectorPtr AggregationMergeReader::copyRow(
    const HeapEntry& entry,
    facebook::velox::memory::MemoryPool* pool) const {
  auto num_children = entry.batch->childrenSize();
  std::vector<facebook::velox::VectorPtr> children(num_children);

  for (decltype(num_children) i = 0; i < num_children; ++i) {
    auto source = entry.batch->childAt(i);
    // L4-10: Materialize lazy payload columns before copy.
    source->loadedVector();
    children[i] = facebook::velox::BaseVector::create(
        source->type(), 1, pool);
    children[i]->copy(source.get(), 0, entry.row, 1);
  }

  return std::make_shared<facebook::velox::RowVector>(
      pool, output_type_, nullptr, 1, std::move(children));
}

void AggregationMergeReader::aggregateRow(
    const facebook::velox::RowVectorPtr& target,
    const HeapEntry& source) const {
  auto num_children = target->childrenSize();

  for (decltype(num_children) col = 0; col < num_children; ++col) {
    if (isKeyOrSequenceColumn(col)) {
      continue;
    }

    // L4-10: Materialize lazy payload column before aggregation access.
    source.batch->childAt(col)->loadedVector();
    auto func = getAggFunction(col);
    applyAggFunction(
        func, target->childAt(col), source.batch->childAt(col), source.row);
  }
}

// =============================================================================
// Type-dispatched aggregation helpers (reduces branch complexity per function)
// =============================================================================

namespace {

// Dispatch a typed aggregation across numeric types via template-template param.
template <template <typename> class TypedOp>
void dispatchNumericAgg(
    const facebook::velox::VectorPtr& target,
    const facebook::velox::VectorPtr& source,
    facebook::velox::vector_size_t source_row) {
  using namespace facebook::velox;
  switch (target->typeKind()) {
    case TypeKind::BIGINT:
      TypedOp<int64_t>::apply(target, source, source_row);
      break;
    case TypeKind::INTEGER:
      TypedOp<int32_t>::apply(target, source, source_row);
      break;
    case TypeKind::SMALLINT:
      TypedOp<int16_t>::apply(target, source, source_row);
      break;
    // P1-TC-11: Add TINYINT case.
    case TypeKind::TINYINT:
      TypedOp<int8_t>::apply(target, source, source_row);
      break;
    case TypeKind::DOUBLE:
      TypedOp<double>::apply(target, source, source_row);
      break;
    case TypeKind::REAL:
      TypedOp<float>::apply(target, source, source_row);
      break;
    default:
      break;
  }
}

template <typename T>
struct SumOp {
  static void apply(const facebook::velox::VectorPtr& t,
                    const facebook::velox::VectorPtr& s,
                    facebook::velox::vector_size_t r) { applySumTyped<T>(t, s, r); }
};
template <typename T>
struct MaxOp {
  static void apply(const facebook::velox::VectorPtr& t,
                    const facebook::velox::VectorPtr& s,
                    facebook::velox::vector_size_t r) { applyMaxTyped<T>(t, s, r); }
};
template <typename T>
struct MinOp {
  static void apply(const facebook::velox::VectorPtr& t,
                    const facebook::velox::VectorPtr& s,
                    facebook::velox::vector_size_t r) { applyMinTyped<T>(t, s, r); }
};

// Apply PRODUCT aggregation for numeric types.
// Multiplies source value into target value. Null source is treated as identity (1).
template <typename T>
void applyProductTyped(
    const facebook::velox::VectorPtr& target,
    const facebook::velox::VectorPtr& source,
    facebook::velox::vector_size_t source_row) {
  auto target_flat = target->asFlatVector<T>();
  auto source_sv = source->as<facebook::velox::SimpleVector<T>>();
  if (!source_sv || source_sv->isNullAt(source_row)) {
    return;  // null contributes nothing (identity element for multiplication)
  }
  T src_val = source_sv->valueAt(source_row);
  if (target_flat->isNullAt(0)) {
    target_flat->setNull(0, false);
    target_flat->set(0, src_val);
  } else {
    T cur = target_flat->valueAt(0);
    if constexpr (std::is_integral_v<T>) {
      T result;
      if (__builtin_mul_overflow(cur, src_val, &result)) {
        throw std::overflow_error(
            "AggregationMergeReader: integer PRODUCT overflow detected");
      }
      target_flat->set(0, result);
    } else {
      target_flat->set(0, cur * src_val);
    }
  }
}

template <typename T>
struct ProductOp {
  static void apply(const facebook::velox::VectorPtr& t,
                    const facebook::velox::VectorPtr& s,
                    facebook::velox::vector_size_t r) { applyProductTyped<T>(t, s, r); }
};

void applyCountAgg(const facebook::velox::VectorPtr& target,
                   const facebook::velox::VectorPtr& source,
                   facebook::velox::vector_size_t source_row) {
  auto target_flat = target->asFlatVector<int64_t>();
  if (source->isNullAt(source_row)) return;
  if (target_flat->isNullAt(0)) {
    target_flat->setNull(0, false);
    target_flat->set(0, static_cast<int64_t>(1));
  } else {
    target_flat->set(0, target_flat->valueAt(0) + 1);
  }
}

void applyListaggAgg(const facebook::velox::VectorPtr& target,
                     const facebook::velox::VectorPtr& source,
                     facebook::velox::vector_size_t source_row) {
  using facebook::velox::StringView;
  auto target_flat = target->asFlatVector<StringView>();
  auto source_sv = source->as<facebook::velox::SimpleVector<StringView>>();
  if (!source_sv || source_sv->isNullAt(source_row)) return;
  auto src_val = source_sv->valueAt(source_row).str();
  if (target_flat->isNullAt(0)) {
    target_flat->setNull(0, false);
    // P1-TC-13: Use set() with std::string to let FlatVector manage buffer
    // ownership, avoiding dangling pointer from temporary StringView.
    target_flat->set(0, StringView(src_val));
  } else {
    auto combined = target_flat->valueAt(0).str() + "," + src_val;
    // FlatVector::set with StringView copies data into the vector's
    // string buffer arena, ensuring the data outlives the temporary.
    target_flat->set(0, StringView(combined));
  }
}

void applyBoolAgg(const facebook::velox::VectorPtr& target,
                  const facebook::velox::VectorPtr& source,
                  facebook::velox::vector_size_t source_row,
                  bool is_and) {
  auto target_flat = target->asFlatVector<bool>();
  auto source_sv = source->as<facebook::velox::SimpleVector<bool>>();
  if (!source_sv || source_sv->isNullAt(source_row)) return;
  bool src_val = source_sv->valueAt(source_row);
  if (target_flat->isNullAt(0)) {
    target_flat->setNull(0, false);
    target_flat->set(0, src_val);
  } else {
    bool cur = target_flat->valueAt(0);
    target_flat->set(0, is_and ? (cur && src_val) : (cur || src_val));
  }
}

}  // namespace

void AggregationMergeReader::applyAggFunction(
    AggFunction func,
    const facebook::velox::VectorPtr& target,
    const facebook::velox::VectorPtr& source,
    facebook::velox::vector_size_t source_row) const {
  switch (func) {
    case AggFunction::SUM:
      dispatchNumericAgg<SumOp>(target, source, source_row);
      break;
    case AggFunction::COUNT:
      applyCountAgg(target, source, source_row);
      break;
    case AggFunction::MAX:
      dispatchNumericAgg<MaxOp>(target, source, source_row);
      break;
    case AggFunction::MIN:
      dispatchNumericAgg<MinOp>(target, source, source_row);
      break;
    case AggFunction::LAST_VALUE:
      // Winner (target) is already the latest version — no-op.
      // Older rows must NOT overwrite the winner's value.
      break;
    case AggFunction::LAST_NON_NULL_VALUE:
      // Only backfill from older row if the winner's column is null.
      if (target->isNullAt(0) && !source->isNullAt(source_row)) {
        target->copy(source.get(), 0, source_row, 1);
      }
      break;
    case AggFunction::LISTAGG:
      applyListaggAgg(target, source, source_row);
      break;
    case AggFunction::BOOL_AND:
      applyBoolAgg(target, source, source_row, true);
      break;
    case AggFunction::BOOL_OR:
      applyBoolAgg(target, source, source_row, false);
      break;
    case AggFunction::COLLECT:
      // Collect appends values into a comma-separated list (same as LISTAGG).
      applyListaggAgg(target, source, source_row);
      break;
    case AggFunction::MERGE_MAP:
      // Merge map: for MAP columns, newer entries overwrite older ones.
      // The winner (target) already has the latest map; older maps backfill
      // only when target is null.
      if (target->isNullAt(0) && !source->isNullAt(source_row)) {
        target->copy(source.get(), 0, source_row, 1);
      }
      break;
    case AggFunction::FIRST_VALUE:
      // First value: keep the oldest (source) value, overwrite the winner.
      // The heap pops newest first, so source is older — always overwrite
      // target with source unless source is null.
      if (!source->isNullAt(source_row)) {
        target->copy(source.get(), 0, source_row, 1);
      }
      break;
    case AggFunction::PRODUCT:
      dispatchNumericAgg<ProductOp>(target, source, source_row);
      break;
    case AggFunction::NESTED_UPDATE:
      // Nested update: for STRUCT/ROW columns, merge non-null fields from
      // older rows into the target. At the flat vector level, backfill nulls.
      if (target->isNullAt(0) && !source->isNullAt(source_row)) {
        target->copy(source.get(), 0, source_row, 1);
      }
      break;
  }
}

bool AggregationMergeReader::isKeyOrSequenceColumn(
    facebook::velox::column_index_t col) const {
  if (col == sequence_index_) {
    return true;
  }
  return std::find(key_indices_.begin(), key_indices_.end(), col) !=
         key_indices_.end();
}

AggFunction AggregationMergeReader::getAggFunction(
    facebook::velox::column_index_t col) const {
  auto it = agg_specs_.find(col);
  if (it != agg_specs_.end()) {
    return it->second;
  }
  // Non-aggregated, non-key columns default to LAST_VALUE.
  return AggFunction::LAST_VALUE;
}

facebook::velox::RowVectorPtr AggregationMergeReader::assembleOutput(
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
