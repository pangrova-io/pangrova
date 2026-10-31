#pragma once

#include <string>
#include <vector>

#include "velox/type/Type.h"

namespace pangrova::velox {

// Matches protocol SubstraitRelationLayout.physicalAlias. Names identify a
// task-local ordinal only; source column resolution always uses exact handles.
inline std::string physicalFieldAlias(size_t ordinal) {
  return "$pangrova$column_" + std::to_string(ordinal);
}

inline facebook::velox::RowTypePtr physicalRowType(
    const facebook::velox::RowTypePtr& logical,
    size_t offset = 0) {
  std::vector<std::string> names;
  names.reserve(logical->size());
  for (size_t field = 0; field < logical->size(); ++field) {
    names.push_back(physicalFieldAlias(offset + field));
  }
  return facebook::velox::ROW(std::move(names), logical->children());
}

}  // namespace pangrova::velox
