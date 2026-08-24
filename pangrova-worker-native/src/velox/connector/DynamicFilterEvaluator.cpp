// DynamicFilterEvaluator: Stateless evaluation of dynamic filters against
// a RowVector batch. Extracted from PangrovaVeloxDataSource::applyDynamicFilters
// for reuse and testability.
//
// Uses DecodedVector for encoding-agnostic access — correctly handles flat,
// dictionary, constant, and lazy vector encodings without per-row virtual
// dispatch overhead.
#include "velox/connector/DynamicFilterEvaluator.h"

#include <cstring>

#include "velox/vector/BaseVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/SelectivityVector.h"

namespace pangrova::velox {

namespace {

// Evaluate a dynamic filter on an integer-typed column (BIGINT/INTEGER/
// SMALLINT/TINYINT). Uses DecodedVector for encoding-agnostic access.
// Values are widened to int64_t for filter->testInt64().
template <typename T>
void evaluateIntegerFilter(
    facebook::velox::DecodedVector& decoded,
    const std::shared_ptr<facebook::velox::common::Filter>& filter,
    facebook::velox::SelectivityVector& rows) {
  rows.applyToSelected([&](auto r) {
    if (decoded.isNullAt(r)) {
      if (!filter->testNull()) {
        rows.setValid(r, false);
      }
    } else {
      if (!filter->testInt64(
              static_cast<int64_t>(decoded.valueAt<T>(r)))) {
        rows.setValid(r, false);
      }
    }
  });
}

// Evaluate a dynamic filter on a DOUBLE column.
// Uses ordered-int bit-pattern encoding consistent with
// DynamicFilterConverter::doubleToOrderedInt.
void evaluateDoubleFilter(
    facebook::velox::DecodedVector& decoded,
    const std::shared_ptr<facebook::velox::common::Filter>& filter,
    facebook::velox::SelectivityVector& rows) {
  rows.applyToSelected([&](auto r) {
    if (decoded.isNullAt(r)) {
      if (!filter->testNull()) {
        rows.setValid(r, false);
      }
    } else {
      double val = decoded.valueAt<double>(r);
      int64_t bits;
      std::memcpy(&bits, &val, sizeof(int64_t));
      if (bits < 0) {
        bits ^= INT64_C(0x7FFFFFFFFFFFFFFF);
      }
      if (!filter->testInt64(bits)) {
        rows.setValid(r, false);
      }
    }
  });
}

// Evaluate a dynamic filter on a REAL (float) column.
// Widens to double, then applies ordered-int encoding consistent with
// DynamicFilterConverter.
void evaluateRealFilter(
    facebook::velox::DecodedVector& decoded,
    const std::shared_ptr<facebook::velox::common::Filter>& filter,
    facebook::velox::SelectivityVector& rows) {
  rows.applyToSelected([&](auto r) {
    if (decoded.isNullAt(r)) {
      if (!filter->testNull()) {
        rows.setValid(r, false);
      }
    } else {
      double val = static_cast<double>(decoded.valueAt<float>(r));
      int64_t bits;
      std::memcpy(&bits, &val, sizeof(int64_t));
      if (bits < 0) {
        bits ^= INT64_C(0x7FFFFFFFFFFFFFFF);
      }
      if (!filter->testInt64(bits)) {
        rows.setValid(r, false);
      }
    }
  });
}

// Evaluate a dynamic filter on a VARCHAR/VARBINARY column.
void evaluateStringFilter(
    facebook::velox::DecodedVector& decoded,
    const std::shared_ptr<facebook::velox::common::Filter>& filter,
    facebook::velox::SelectivityVector& rows) {
  rows.applyToSelected([&](auto r) {
    if (decoded.isNullAt(r)) {
      if (!filter->testNull()) {
        rows.setValid(r, false);
      }
    } else {
      auto view = decoded.valueAt<facebook::velox::StringView>(r);
      if (!filter->testBytes(view.data(), view.size())) {
        rows.setValid(r, false);
      }
    }
  });
}

}  // namespace

facebook::velox::RowVectorPtr evaluateDynamicFilters(
    facebook::velox::RowVectorPtr rowVector,
    const std::vector<std::pair<facebook::velox::column_index_t,
                                std::shared_ptr<facebook::velox::common::Filter>>>& filters,
    const facebook::velox::RowTypePtr& outputType,
    facebook::velox::memory::MemoryPool* pool) {
  if (!rowVector || rowVector->size() == 0 || filters.empty()) {
    return rowVector;
  }

  facebook::velox::SelectivityVector rows(rowVector->size());

  for (const auto& [colIdx, filter] : filters) {
    if (colIdx >= rowVector->childrenSize()) {
      continue;
    }
    auto child = rowVector->childAt(colIdx);
    if (!child) {
      continue;
    }

    // Decode the column once — handles flat, dictionary, constant, and
    // lazy encodings uniformly. The decode cost is O(n) for dictionary
    // vectors but subsequent valueAt<T>() calls are O(1) flat access.
    facebook::velox::DecodedVector decoded(*child, rows);

    auto colType = outputType->childAt(colIdx);
    if (colType->isBigint()) {
      evaluateIntegerFilter<int64_t>(decoded, filter, rows);
    } else if (colType->isInteger()) {
      evaluateIntegerFilter<int32_t>(decoded, filter, rows);
    } else if (colType->isSmallint()) {
      evaluateIntegerFilter<int16_t>(decoded, filter, rows);
    } else if (colType->isTinyint()) {
      evaluateIntegerFilter<int8_t>(decoded, filter, rows);
    } else if (colType->isDouble()) {
      evaluateDoubleFilter(decoded, filter, rows);
    } else if (colType->isReal()) {
      evaluateRealFilter(decoded, filter, rows);
    } else if (colType->isVarchar() || colType->isVarbinary()) {
      evaluateStringFilter(decoded, filter, rows);
    }
    // Unsupported types pass through (no filtering).
  }

  rows.updateBounds();

  auto selected = rows.countSelected();
  if (selected == rowVector->size()) {
    return rowVector;
  }
  if (selected == 0) {
    return nullptr;
  }

  // Compact by constructing a new RowVector with dictionary-wrapped children.
  // BaseVector::wrapInDictionary on a RowVector creates DictionaryVector<ComplexType>
  // which is NOT a RowVector subclass — dynamic_pointer_cast<RowVector> returns nullptr.
  auto indices = facebook::velox::allocateIndices(selected, pool);
  auto* rawIndices = indices->asMutable<facebook::velox::vector_size_t>();
  facebook::velox::vector_size_t cnt = 0;
  rows.applyToSelected([&](auto r) { rawIndices[cnt++] = r; });

  std::vector<facebook::velox::VectorPtr> wrappedChildren(
      rowVector->childrenSize());
  for (size_t i = 0; i < rowVector->childrenSize(); ++i) {
    wrappedChildren[i] = facebook::velox::BaseVector::wrapInDictionary(
        nullptr, indices, cnt, rowVector->childAt(i));
  }
  return std::make_shared<facebook::velox::RowVector>(
      pool, rowVector->type(), nullptr, cnt, std::move(wrappedChildren));
}

}  // namespace pangrova::velox
