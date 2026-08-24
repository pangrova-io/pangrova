// DeletionVectorRowFilter: Roaring Bitmap-based deletion vector filter.

#include "delete/DeletionVectorRowFilter.h"

namespace pangrova::delete_filter {

DeletionVectorRowFilter::DeletionVectorRowFilter(roaring::Roaring bitmap)
  : bitmap_(std::move(bitmap)) {}

void DeletionVectorRowFilter::apply(
    const facebook::velox::RowVector& batch,
    facebook::velox::SelectivityVector& rows,
    int64_t row_offset) const {
  if (bitmap_.isEmpty()) {
    return;
  }

  auto batch_size = batch.size();
  for (facebook::velox::vector_size_t i = 0; i < batch_size; ++i) {
    if (!rows.isValid(i)) {
      continue;
    }
    // P1-TC-15: Bounds check — Roaring bitmap uses uint32_t keys.
    auto global_pos_64 = row_offset + static_cast<int64_t>(i);
    if (global_pos_64 < 0 || global_pos_64 > static_cast<int64_t>(UINT32_MAX)) {
      continue;  // Out of Roaring bitmap range — cannot be deleted.
    }
    auto global_pos = static_cast<uint32_t>(global_pos_64);
    if (bitmap_.contains(global_pos)) {
      rows.setValid(i, false);
    }
  }
  rows.updateBounds();
}

}  // namespace pangrova::delete_filter
