// DeletionVectorRowFilter: Roaring Bitmap-based deletion vector filter.

#include "engine/delete/DeletionVectorRowFilter.h"

namespace pangrova::delete_filter {

DeletionVectorRowFilter::DeletionVectorRowFilter(roaring::Roaring bitmap)
  : bitmap_(std::move(bitmap)) {}

DeletionVectorRowFilter::DeletionVectorRowFilter(roaring::Roaring64Map bitmap)
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
    if (row_offset < 0 || row_offset > INT64_MAX - static_cast<int64_t>(i)) {
      throw std::runtime_error("Invalid deletion-vector row offset");
    }
    const auto global_pos = static_cast<uint64_t>(row_offset + static_cast<int64_t>(i));
    if (bitmap_.contains(global_pos)) {
      rows.setValid(i, false);
    }
  }
  rows.updateBounds();
}

}  // namespace pangrova::delete_filter
