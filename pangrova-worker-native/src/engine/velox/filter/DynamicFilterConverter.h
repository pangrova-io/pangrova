// DynamicFilterConverter: Proto → Velox Filter direct conversion.
//
// Converts canonical v2 RuntimeFilter messages to velox::common::Filter
// subclasses, bypassing Arrow Expression intermediate layer.
//
// Supported mappings:
//   IN_LIST (bigint)  → BigintValuesUsingHashTable
//   IN_LIST (string)  → BytesValues
//   RANGE   (bigint)  → BigintRange
//   RANGE   (double)  → DoubleRange
//
// Error handling: column index out of bounds → WARN log + skip filter.
//
// Placement: velox/ — Velox integration adapters.
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "pangrova/protocol/v2/runtime.pb.h"
#include "velox/type/Filter.h"

namespace facebook::velox {
class RowType;
using RowTypePtr = std::shared_ptr<const RowType>;
}  // namespace facebook::velox

namespace pangrova::velox {

// Result of converting a single RuntimeFilter.
struct ConvertedDynamicFilter {
  std::string column_name;
  std::unique_ptr<facebook::velox::common::Filter> filter;
};

// Convert a RuntimeFilter proto to a Velox Filter.
// Returns nullptr if the filter type is unsupported or data is invalid.
std::unique_ptr<facebook::velox::common::Filter> convertDynamicFilterEntry(
    const pangrova::protocol::v2::RuntimeFilter& entry);

// Convert all RuntimeFilter messages from an InjectRuntimeFilterRequest.
// DPP-only entries are skipped explicitly. Unsupported Native entries throw.
std::vector<ConvertedDynamicFilter> convertDynamicFilters(
    const pangrova::protocol::v2::InjectRuntimeFilterRequest& payload);

// Resolve column name to column index in the given output type.
// Returns -1 if column not found (caller should log WARN and skip).
int resolveColumnIndex(
    const std::string& column_name,
    const facebook::velox::RowTypePtr& output_type);

}  // namespace pangrova::velox
