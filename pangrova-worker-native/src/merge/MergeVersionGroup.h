#pragma once

#include <algorithm>
#include <optional>
#include <stdexcept>
#include "merge/DeduplicateMergeReader.h"
#include "velox/vector/DecodedVector.h"

namespace pangrova::merge {

inline void validatePrimaryKey(const HeapEntry& entry,
    const std::vector<facebook::velox::column_index_t>& columns) {
  for (const auto column : columns) {
    facebook::velox::DecodedVector key(*entry.batch->childAt(column));
    if (key.isNullAt(entry.row)) throw std::invalid_argument("Paimon persisted primary key must not be null");
  }
}

inline int8_t persistedValueKind(const HeapEntry& entry) {
  const auto index = entry.batch->type()->asRow().getChildIdxIfExists("_VALUE_KIND");
  if (!index) return 0;
  facebook::velox::DecodedVector kinds(*entry.batch->childAt(*index));
  if (kinds.isNullAt(entry.row)) throw std::invalid_argument("Paimon value kind must not be null");
  const auto kind = kinds.valueAt<int8_t>(entry.row);
  if (kind < 0 || kind > 3) throw std::invalid_argument("Invalid Paimon persisted value kind");
  return kind;
}

// Consume every version, including later versions within the same reader, then
// apply the SDK merge semantics in persisted chronological sequence order.
template <typename Heap, typename Equal, typename Advance>
std::vector<HeapEntry> collectKeyVersions(Heap& heap, MergeStats& stats, Equal equal, Advance advance) {
  const auto key = heap.top();
  std::vector<HeapEntry> versions;
  do {
    if (versions.size() == 65536) throw std::runtime_error("Native MOR exceeds 65536 versions for one primary key");
    auto entry = heap.top();
    heap.pop();
    persistedValueKind(entry);
    versions.push_back(entry);
    ++stats.input_rows;
    advance(entry);
  } while (!heap.empty() && equal(key, heap.top()));
  stats.merged_rows += versions.size() - 1;
  std::stable_sort(versions.begin(), versions.end(), [](const auto& first, const auto& second) {
    if (first.sequence_num != second.sequence_num) return first.sequence_num < second.sequence_num;
    return first.reader_index < second.reader_index;
  });
  return versions;
}

}  // namespace pangrova::merge
