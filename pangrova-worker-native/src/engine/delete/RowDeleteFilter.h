// RowDeleteFilter: Abstract base for row-level delete filters.
// Operates directly on RowVector + SelectivityVector to avoid
// intermediate conversion overhead.
//
// Placement: delete/ — delete filter abstractions.
#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "velox/vector/ComplexVector.h"

namespace pangrova::delete_filter {

// Base class for row-level delete filters.
// Subclasses mark deleted rows as invalid in the SelectivityVector.
class RowDeleteFilter {
 public:
  virtual ~RowDeleteFilter() = default;

  // Apply the delete filter to the given batch.
  // Marks deleted rows as invalid in `rows`.
  // `row_offset` is the global row index offset for position-based deletes.
  virtual void apply(
      const facebook::velox::RowVector& batch,
      facebook::velox::SelectivityVector& rows,
      int64_t row_offset) const = 0;
};

// Position delete filter: marks rows at specific global positions as deleted.
// Uses a sorted position set for O(log n) lookup per row.
class PositionDeleteFilter final : public RowDeleteFilter {
 public:
  explicit PositionDeleteFilter(std::vector<int64_t> positions);

  void apply(
      const facebook::velox::RowVector& batch,
      facebook::velox::SelectivityVector& rows,
      int64_t row_offset) const override;

 private:
  std::vector<int64_t> positions_;  // sorted
};

// Equality delete filter: marks rows matching deleted key values.
// Concatenates equality column values into a composite key for set lookup.
class EqualityDeleteFilter final : public RowDeleteFilter {
 public:
  EqualityDeleteFilter(
      std::vector<std::string> columns,
      std::unordered_set<std::string> deleted_keys);

  void apply(
      const facebook::velox::RowVector& batch,
      facebook::velox::SelectivityVector& rows,
      int64_t row_offset) const override;

 private:
  std::vector<std::string> columns_;
  std::unordered_set<std::string> deleted_keys_;
};

}  // namespace pangrova::delete_filter
