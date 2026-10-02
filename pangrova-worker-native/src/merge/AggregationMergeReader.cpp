// AggregationMergeReader implementation.
// Validates: Requirements R8.6

#include "merge/AggregationMergeReader.h"

#include <algorithm>
#include <stdexcept>
#include <type_traits>
#include <limits>

#include "velox/type/Type.h"
#include "velox/type/DecimalUtil.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/FlatVector.h"
#include "merge/SequenceNumberReader.h"
#include "merge/MergeVersionGroup.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/SimpleVector.h"

namespace pangrova::merge {

namespace {

void validateComplexAggregateInput(const facebook::velox::VectorPtr& input,
    facebook::velox::vector_size_t row, AggFunction function);

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
  facebook::velox::DecodedVector decoded(*source);
  if (decoded.isNullAt(source_row)) {
    return;
  }
  T src_val = decoded.valueAt<T>(source_row);
  if (target_flat->isNullAt(0)) {
    target_flat->setNull(0, false);
    target_flat->set(0, src_val);
  } else {
    T cur = target_flat->valueAt(0);
    T result;
    if constexpr (std::is_integral_v<T> || std::is_same_v<T, facebook::velox::int128_t>) {
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
  facebook::velox::DecodedVector decoded(*source);
  if (decoded.isNullAt(source_row)) {
    return;
  }
  T src_val = decoded.valueAt<T>(source_row);
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
  facebook::velox::DecodedVector decoded(*source);
  if (decoded.isNullAt(source_row)) {
    return;
  }
  T src_val = decoded.valueAt<T>(source_row);
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
  if (upper == "FIRST_NON_NULL_VALUE") return AggFunction::FIRST_NON_NULL_VALUE;
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
    std::unordered_map<facebook::velox::column_index_t, AggFunction> agg_specs, MergeDeletePolicy delete_policy)
    : key_indices_(std::move(primary_key_indices)),
      sequence_index_(sequence_column_index),
      output_type_(std::move(output_type)),
      delete_policy_(delete_policy),
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
    auto versions = collectKeyVersions(*heap_, stats_,
        [&](const auto& first, const auto& second) { return keysEqual(first, second); },
        [&](const auto& entry) {
          readers_[entry.reader_index].current_row = entry.row + 1;
          advanceReader(entry.reader_index, pool);
        });
    facebook::velox::RowVectorPtr accumulated;
    bool deleted = false;
    for (const auto& version : versions) {
      for (uint32_t column = 0; column < output_type_->size(); ++column) {
        if (!isKeyOrSequenceColumn(column)) {
          validateComplexAggregateInput(version.batch->childAt(column), version.row, getAggFunction(column));
        }
      }
      const auto kind = persistedValueKind(version);
      deleted = delete_policy_.remove_record_on_delete && kind == 3;
      if (deleted) accumulated = copyRow(version, pool);
      else if (!accumulated && (kind == 0 || kind == 2)) accumulated = copyRow(version, pool);
      else {
        if (!accumulated) {
          accumulated = copyRow(version, pool);
          for (uint32_t col = 0; col < output_type_->size(); ++col) {
            if (!isKeyOrSequenceColumn(col)) accumulated->childAt(col)->setNull(0, true);
          }
        }
        aggregateRow(accumulated, version, kind == 1 || kind == 3);
      }
    }
    if (accumulated && !deleted) {
      output_rows.push_back(std::move(accumulated));
      stats_.output_rows++;
    }
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
  validatePrimaryKey(entry, key_indices_);
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
  return decodedSequenceNumber(entry.batch, sequence_index_, entry.row);
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
    const HeapEntry& source, bool retract) const {
  auto num_children = target->childrenSize();

  for (decltype(num_children) col = 0; col < num_children; ++col) {
    if (isKeyOrSequenceColumn(col)) {
      continue;
    }

    // L4-10: Materialize lazy payload column before aggregation access.
    source.batch->childAt(col)->loadedVector();
    auto func = getAggFunction(col);
    if (retract) applyRetractFunction(func, target->childAt(col), source.batch->childAt(col), source.row);
    else applyAggFunction(func, target->childAt(col), source.batch->childAt(col), source.row);
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
  if (target->type()->isLongDecimal()) {
    TypedOp<int128_t>::apply(target, source, source_row);
    return;
  }
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
      throw std::invalid_argument("Paimon numeric aggregator received a non-numeric physical type");
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
  facebook::velox::DecodedVector decoded(*source);
  if (decoded.isNullAt(source_row)) {
    return;
  }
  T src_val = decoded.valueAt<T>(source_row);
  if (target_flat->isNullAt(0)) {
    target_flat->setNull(0, false);
    target_flat->set(0, src_val);
  } else {
    T cur = target_flat->valueAt(0);
    if constexpr (std::is_integral_v<T> || std::is_same_v<T, facebook::velox::int128_t>) {
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

template <typename T, bool Product>
struct RetractNumericOp {
  static void apply(const facebook::velox::VectorPtr& target,
      const facebook::velox::VectorPtr& source, facebook::velox::vector_size_t row) {
    facebook::velox::DecodedVector decoded(*source);
    if (decoded.isNullAt(row)) return;
    auto* result = target->asFlatVector<T>();
    const T input = decoded.valueAt<T>(row);
    if constexpr (Product) {
      if (result->isNullAt(0)) return;
      if (input == 0) throw std::invalid_argument("Paimon PRODUCT retract divides by zero");
      if constexpr (std::is_integral_v<T> || std::is_same_v<T, facebook::velox::int128_t>) {
        if (input == -1 && result->valueAt(0) == std::numeric_limits<T>::min()) {
          throw std::overflow_error("Paimon PRODUCT retract overflow");
        }
      }
      result->set(0, result->valueAt(0) / input);
    } else {
      const T previous = result->isNullAt(0) ? 0 : result->valueAt(0);
      T value;
      if constexpr (std::is_integral_v<T> || std::is_same_v<T, facebook::velox::int128_t>) {
        if (__builtin_sub_overflow(previous, input, &value)) throw std::overflow_error("Paimon SUM retract overflow");
      } else value = previous - input;
      result->setNull(0, false);
      result->set(0, value);
    }
  }
};

template <typename T> using RetractSumOp = RetractNumericOp<T, false>;
template <typename T> using RetractProductOp = RetractNumericOp<T, true>;

void applyListaggAgg(const facebook::velox::VectorPtr& target,
                     const facebook::velox::VectorPtr& source,
                     facebook::velox::vector_size_t source_row) {
  using facebook::velox::StringView;
  auto target_flat = target->asFlatVector<StringView>();
  facebook::velox::DecodedVector decoded(*source);
  if (decoded.isNullAt(source_row)) return;
  auto src_val = decoded.valueAt<StringView>(source_row).str();
  if (src_val.empty() && !target_flat->isNullAt(0)) return;
  if (target_flat->isNullAt(0)) {
    target_flat->setNull(0, false);
    // P1-TC-13: Use set() with std::string to let FlatVector manage buffer
    // ownership, avoiding dangling pointer from temporary StringView.
    target_flat->set(0, StringView(src_val));
  } else if (target_flat->valueAt(0).empty()) {
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
  facebook::velox::DecodedVector decoded(*source);
  if (decoded.isNullAt(source_row)) return;
  bool src_val = decoded.valueAt<bool>(source_row);
  if (target_flat->isNullAt(0)) {
    target_flat->setNull(0, false);
    target_flat->set(0, src_val);
  } else {
    bool cur = target_flat->valueAt(0);
    target_flat->set(0, is_and ? (cur && src_val) : (cur || src_val));
  }
}

struct ElementReference {
  facebook::velox::VectorPtr values;
  facebook::velox::vector_size_t row;
};

void validateComplexAggregateInput(const facebook::velox::VectorPtr& input,
    facebook::velox::vector_size_t row, AggFunction function) {
  if (function != AggFunction::COLLECT && function != AggFunction::NESTED_UPDATE && function != AggFunction::MERGE_MAP) return;
  facebook::velox::DecodedVector decoded(*input);
  if (decoded.isNullAt(row)) return;
  const auto* values = function == AggFunction::MERGE_MAP
      ? static_cast<const facebook::velox::ArrayVectorBase*>(decoded.base()->as<facebook::velox::MapVector>())
      : static_cast<const facebook::velox::ArrayVectorBase*>(decoded.base()->as<facebook::velox::ArrayVector>());
  if (!values) throw std::invalid_argument("Paimon complex aggregate requires exact physical type");
  if (values->sizeAt(decoded.index(row)) > 65536) throw std::runtime_error("Paimon complex aggregate input exceeds 65536 elements");
}

void validateDecimalAggregate(const facebook::velox::VectorPtr& value) {
  if (!value->type()->isDecimal() || value->isNullAt(0)) return;
  const auto [precision, scale] = facebook::velox::getDecimalPrecisionScale(*value->type());
  (void)scale;
  const auto number = value->type()->isShortDecimal() ? value->asFlatVector<int64_t>()->valueAt(0)
      : value->asFlatVector<facebook::velox::int128_t>()->valueAt(0);
  if (!facebook::velox::DecimalUtil::valueInPrecisionRange(number, precision)) {
    throw std::overflow_error("Paimon decimal aggregate exceeds physical precision");
  }
}

bool sameElement(const ElementReference& first, const ElementReference& second) {
  const auto comparison = first.values->compare(second.values.get(), first.row, second.row,
      facebook::velox::CompareFlags{});
  if (!comparison) throw std::invalid_argument("Paimon complex aggregate requires exact element equality");
  return *comparison == 0;
}

facebook::velox::BufferPtr singleOffset(facebook::velox::memory::MemoryPool* pool,
    facebook::velox::vector_size_t value) {
  auto buffer = facebook::velox::AlignedBuffer::allocate<facebook::velox::vector_size_t>(1, pool);
  buffer->asMutable<facebook::velox::vector_size_t>()[0] = value;
  return buffer;
}

void applyMapAgg(const facebook::velox::VectorPtr& target, const facebook::velox::VectorPtr& source,
    facebook::velox::vector_size_t row, bool retract) {
  using namespace facebook::velox;
  if (target->typeKind() != TypeKind::MAP) throw std::invalid_argument("Paimon merge_map requires MAP");
  DecodedVector input(*source);
  if (input.isNullAt(row) || (retract && target->isNullAt(0))) return;
  if (target->isNullAt(0)) {
    target->copy(source.get(), 0, row, 1);
    return;
  }
  auto* old = target->as<MapVector>();
  auto* next = input.base()->as<MapVector>();
  const auto input_row = input.index(row);
  if (old->sizeAt(0) > 65536 || next->sizeAt(input_row) > 65536) {
    throw std::runtime_error("Paimon merge_map input exceeds 65536 elements");
  }
  std::vector<std::pair<ElementReference, ElementReference>> entries;
  for (vector_size_t i = old->offsetAt(0); i < old->offsetAt(0) + old->sizeAt(0); ++i) {
    entries.push_back({{old->mapKeys(), i}, {old->mapValues(), i}});
  }
  for (vector_size_t i = next->offsetAt(input_row); i < next->offsetAt(input_row) + next->sizeAt(input_row); ++i) {
    ElementReference key{next->mapKeys(), i};
    auto existing = std::find_if(entries.begin(), entries.end(), [&](const auto& value) {
      return sameElement(value.first, key);
    });
    if (retract) {
      if (existing != entries.end()) entries.erase(existing);
    } else if (existing != entries.end()) {
      existing->second = {next->mapValues(), i};
    } else {
      if (entries.size() == 65536) throw std::runtime_error("Paimon merge_map exceeds 65536 elements");
      entries.push_back({key, {next->mapValues(), i}});
    }
  }
  auto keys = BaseVector::create(target->type()->childAt(0), entries.size(), target->pool());
  auto values = BaseVector::create(target->type()->childAt(1), entries.size(), target->pool());
  for (size_t i = 0; i < entries.size(); ++i) {
    keys->copy(entries[i].first.values.get(), i, entries[i].first.row, 1);
    values->copy(entries[i].second.values.get(), i, entries[i].second.row, 1);
  }
  auto merged = std::make_shared<MapVector>(target->pool(), target->type(), nullptr, 1,
      singleOffset(target->pool(), 0), singleOffset(target->pool(), entries.size()), keys, values);
  target->copy(merged.get(), 0, 0, 1);
}

void applyArrayAgg(const facebook::velox::VectorPtr& target, const facebook::velox::VectorPtr& source,
    facebook::velox::vector_size_t row, bool retract, bool nested) {
  using namespace facebook::velox;
  if (target->typeKind() != TypeKind::ARRAY || (nested && target->type()->childAt(0)->kind() != TypeKind::ROW)) {
    throw std::invalid_argument("Paimon collect/nested_update requires its exact ARRAY element type");
  }
  DecodedVector input(*source);
  if (input.isNullAt(row) || (retract && target->isNullAt(0))) return;
  if (target->isNullAt(0) && !nested) {
    target->copy(source.get(), 0, row, 1);
    return;
  }
  auto* next = input.base()->as<ArrayVector>();
  const auto input_row = input.index(row);
  std::vector<ElementReference> entries;
  if (!target->isNullAt(0)) {
    auto* old = target->as<ArrayVector>();
    if (old->sizeAt(0) > 65536) throw std::runtime_error("Paimon array aggregate input exceeds 65536 elements");
    for (vector_size_t i = old->offsetAt(0); i < old->offsetAt(0) + old->sizeAt(0); ++i) {
      if (!nested || !old->elements()->isNullAt(i)) entries.push_back({old->elements(), i});
    }
  }
  if (next->sizeAt(input_row) > 65536) throw std::runtime_error("Paimon array aggregate input exceeds 65536 elements");
  for (vector_size_t i = next->offsetAt(input_row); i < next->offsetAt(input_row) + next->sizeAt(input_row); ++i) {
    if (nested && next->elements()->isNullAt(i)) continue;
    ElementReference element{next->elements(), i};
    if (retract) {
      auto matches = [&](const auto& value) { return sameElement(value, element); };
      if (nested) entries.erase(std::remove_if(entries.begin(), entries.end(), matches), entries.end());
      else if (auto found = std::find_if(entries.begin(), entries.end(), matches); found != entries.end()) {
        entries.erase(found);
      }
    } else {
      if (entries.size() == 65536) throw std::runtime_error("Paimon array aggregate exceeds 65536 elements");
      entries.push_back(element);
    }
  }
  auto elements = BaseVector::create(target->type()->childAt(0), entries.size(), target->pool());
  for (size_t i = 0; i < entries.size(); ++i) elements->copy(entries[i].values.get(), i, entries[i].row, 1);
  auto merged = std::make_shared<ArrayVector>(target->pool(), target->type(), nullptr, 1,
      singleOffset(target->pool(), 0), singleOffset(target->pool(), entries.size()), elements);
  target->copy(merged.get(), 0, 0, 1);
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
    case AggFunction::MAX:
      dispatchNumericAgg<MaxOp>(target, source, source_row);
      break;
    case AggFunction::MIN:
      dispatchNumericAgg<MinOp>(target, source, source_row);
      break;
    case AggFunction::LAST_VALUE:
      target->copy(source.get(), 0, source_row, 1);
      break;
    case AggFunction::LAST_NON_NULL_VALUE:
      if (!source->isNullAt(source_row)) {
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
      applyArrayAgg(target, source, source_row, false, false);
      break;
    case AggFunction::MERGE_MAP:
      applyMapAgg(target, source, source_row, false);
      break;
    case AggFunction::FIRST_VALUE:
      // The first chronological value, including NULL, remains authoritative.
      break;
    case AggFunction::FIRST_NON_NULL_VALUE:
      if (target->isNullAt(0) && !source->isNullAt(source_row)) target->copy(source.get(), 0, source_row, 1);
      break;
    case AggFunction::PRODUCT:
      dispatchNumericAgg<ProductOp>(target, source, source_row);
      break;
    case AggFunction::NESTED_UPDATE:
      applyArrayAgg(target, source, source_row, false, true);
      break;
  }
  validateDecimalAggregate(target);
}

void AggregationMergeReader::applyRetractFunction(AggFunction func,
    const facebook::velox::VectorPtr& target, const facebook::velox::VectorPtr& source,
    facebook::velox::vector_size_t row) const {
  if (func == AggFunction::SUM) dispatchNumericAgg<RetractSumOp>(target, source, row);
  else if (func == AggFunction::PRODUCT) dispatchNumericAgg<RetractProductOp>(target, source, row);
  else if (func == AggFunction::MERGE_MAP) applyMapAgg(target, source, row, true);
  else if (func == AggFunction::COLLECT) applyArrayAgg(target, source, row, true, false);
  else if (func == AggFunction::NESTED_UPDATE) applyArrayAgg(target, source, row, true, true);
  else throw std::invalid_argument("Paimon aggregate function does not support retraction");
  validateDecimalAggregate(target);
}

bool AggregationMergeReader::isKeyOrSequenceColumn(
    facebook::velox::column_index_t col) const {
  if (col == sequence_index_ || output_type_->nameOf(col) == "_VALUE_KIND") {
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
  // Paimon AggregateMergeFunction's final default is last_non_null_value.
  return AggFunction::LAST_NON_NULL_VALUE;
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
