// RowDeleteFilter: Position and equality delete filter implementations.
// Operates directly on RowVector + SelectivityVector.

#include "engine/delete/RowDeleteFilter.h"

#include <algorithm>
#include <cstring>
#include <string>
#include <stdexcept>

#include "velox/type/Type.h"
#include "velox/vector/FlatVector.h"

namespace pangrova::delete_filter {

// --- PositionDeleteFilter ---

PositionDeleteFilter::PositionDeleteFilter(
    std::vector<int64_t> positions)
    : positions_(std::move(positions)) {
  std::sort(positions_.begin(), positions_.end());
}

void PositionDeleteFilter::apply(
    const facebook::velox::RowVector& batch,
    facebook::velox::SelectivityVector& rows,
    int64_t row_offset) const {
  if (positions_.empty()) return;

  int64_t batch_end = row_offset + batch.size();
  // Find the range of positions that fall within this batch.
  auto lo = std::lower_bound(positions_.begin(), positions_.end(), row_offset);
  auto hi = std::lower_bound(lo, positions_.end(), batch_end);

  for (auto it = lo; it != hi; ++it) {
    auto local_idx = static_cast<facebook::velox::vector_size_t>(*it - row_offset);
    if (rows.isValid(local_idx)) {
      rows.setValid(local_idx, false);
    }
  }
  rows.updateBounds();
}

// --- EqualityDeleteFilter ---

EqualityDeleteFilter::EqualityDeleteFilter(
    std::vector<std::string> columns,
    std::unordered_set<std::string> deleted_keys)
    : columns_(std::move(columns)),
      deleted_keys_(std::move(deleted_keys)) {}

void EqualityDeleteFilter::apply(
    const facebook::velox::RowVector& batch,
    facebook::velox::SelectivityVector& rows,
    int64_t /*row_offset*/) const {
  if (deleted_keys_.empty() || columns_.empty()) return;

  // Resolve column indices from names.
  const auto& row_type = batch.type()->asRow();
  std::vector<facebook::velox::column_index_t> col_indices;
  col_indices.reserve(columns_.size());
  for (const auto& col_name : columns_) {
    auto idx = row_type.getChildIdxIfExists(col_name);
    if (!idx.has_value()) {
      throw std::runtime_error("Missing equality delete column: " + col_name);
    }
    col_indices.push_back(idx.value());
  }

  std::vector<facebook::velox::VectorPtr> decoded_columns;
  decoded_columns.reserve(col_indices.size());
  for (auto index : col_indices) {
    auto child = batch.childAt(index);
    facebook::velox::BaseVector::flattenVector(child);
    decoded_columns.push_back(std::move(child));
  }

  // Pre-allocate key buffer outside the row loop to avoid per-row allocation.
  std::string key;
  key.reserve(128);

  for (facebook::velox::vector_size_t i = 0; i < batch.size(); ++i) {
    if (!rows.isValid(i)) continue;

    // Build composite key from equality columns using string append
    // instead of ostringstream to avoid per-row heap allocation overhead.
    key.clear();
    for (size_t c = 0; c < col_indices.size(); ++c) {
      if (c > 0) key.push_back('\0');  // null separator
      const auto& child = decoded_columns[c];
      if (child->isNullAt(i)) {
        key.append("__NULL__");
      } else {
        // P1-BUG-8: Type-dispatched serialization matching
        // DeleteFileReader::readEqualityKeys() exactly.
        // Using child->toString(i) would produce different formats
        // for float/double/bool, causing key mismatch and missed deletes.
        switch (child->typeKind()) {
          case facebook::velox::TypeKind::VARCHAR:
          case facebook::velox::TypeKind::VARBINARY: {
            auto sv = child->asFlatVector<facebook::velox::StringView>()
                          ->valueAt(i);
            key.append(sv.data(), sv.size());
            break;
          }
          case facebook::velox::TypeKind::BIGINT:
            key.append(std::to_string(
                child->asFlatVector<int64_t>()->valueAt(i)));
            break;
          case facebook::velox::TypeKind::INTEGER:
            key.append(std::to_string(
                child->asFlatVector<int32_t>()->valueAt(i)));
            break;
          case facebook::velox::TypeKind::SMALLINT:
            key.append(std::to_string(
                child->asFlatVector<int16_t>()->valueAt(i)));
            break;
          case facebook::velox::TypeKind::TINYINT:
            key.append(std::to_string(
                child->asFlatVector<int8_t>()->valueAt(i)));
            break;
          case facebook::velox::TypeKind::BOOLEAN:
            key.append(
                child->asFlatVector<bool>()->valueAt(i) ? "true" : "false");
            break;
          case facebook::velox::TypeKind::DOUBLE: {
            char buf[32];
            snprintf(buf, sizeof(buf), "%.17g",
                child->asFlatVector<double>()->valueAt(i));
            key.append(buf);
            break;
          }
          case facebook::velox::TypeKind::REAL: {
            char buf[32];
            snprintf(buf, sizeof(buf), "%.9g",
                child->asFlatVector<float>()->valueAt(i));
            key.append(buf);
            break;
          }
          // DATE is stored as int32 in this Velox version — handled by INTEGER case above.
          case facebook::velox::TypeKind::TIMESTAMP: {
            auto ts = child->asFlatVector<facebook::velox::Timestamp>()
                          ->valueAt(i);
            key.append(std::to_string(ts.toMicros()));
            break;
          }
          case facebook::velox::TypeKind::HUGEINT: {
            // Serialize 128-bit integer as big-endian byte string for
            // deterministic key ordering. This matches the serialization
            // used by DeleteFileReader::readEqualityKeys().
            auto val = child->asFlatVector<folly::int128_t>()->valueAt(i);
            uint8_t bytes[sizeof(folly::int128_t)];
            std::memcpy(bytes, &val, sizeof(val));
            key.append(reinterpret_cast<const char*>(bytes), sizeof(bytes));
            break;
          }
          default: {
            auto sv = child->asFlatVector<facebook::velox::StringView>();
            if (sv) {
              auto val = sv->valueAt(i);
              key.append(val.data(), val.size());
            }
            break;
          }
        }
      }
    }

    if (deleted_keys_.count(key) > 0) {
      rows.setValid(i, false);
    }
  }
  rows.updateBounds();
}

}  // namespace pangrova::delete_filter
