// DeletionVectorRowFilter: Deletion vector filter using Roaring Bitmap
// for O(1) per-row deleted-position lookup.
// Consistent with Iceberg v3 DV format and Paimon MOW DV format.
//
// Placement: delete/ — delete filter implementations.
#pragma once

#include <roaring.hh>
#include <roaring64map.hh>

#include "delete/RowDeleteFilter.h"

namespace pangrova::delete_filter {

// Deletion vector filter operating on RowVector + SelectivityVector.
// Holds a Roaring Bitmap of deleted row positions and marks matching
// rows as invalid during apply(). Achieves O(1) per-row lookup via
// bitmap.contains().
class DeletionVectorRowFilter final : public RowDeleteFilter {
 public:
  // Constructs the filter by taking ownership of the bitmap.
  explicit DeletionVectorRowFilter(roaring::Roaring bitmap);
  explicit DeletionVectorRowFilter(roaring::Roaring64Map bitmap);

  // Checks each row in [row_offset, row_offset + batch.size()) against
  // the bitmap. Rows whose global position is present in the bitmap
  // are marked invalid in `rows`.
  void apply(
      const facebook::velox::RowVector& batch,
      facebook::velox::SelectivityVector& rows,
      int64_t row_offset) const override;

 private:
  roaring::Roaring64Map bitmap_;
};

}  // namespace pangrova::delete_filter
