// DynamicFilterConverter: Proto → Velox Filter direct conversion.
//
// Converts DynamicFilterEntry proto messages to velox::common::Filter
// subclasses, bypassing Arrow Expression intermediate layer.
//
// Supported mappings:
//   IN_LIST (bigint)  → BigintValuesUsingHashTable
//   IN_LIST (string)  → BytesValues
//   RANGE   (bigint)  → BigintRange
//   RANGE   (double)  → BigintRange on raw bit representation
//
// Error handling: column index out of bounds → WARN log + skip filter.
//
// Placement: velox/ — Velox integration adapters.
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "velox/type/Filter.h"

#if __has_include("pangrova/native/protocol/v1/dynamic_filter.pb.h")
#include "pangrova/native/protocol/v1/dynamic_filter.pb.h"
#define PANGROVA_HAS_FILTER_PROTO 1
#else
#define PANGROVA_HAS_FILTER_PROTO 0
#endif

namespace facebook::velox {
class RowType;
using RowTypePtr = std::shared_ptr<const RowType>;
}  // namespace facebook::velox

namespace pangrova::velox {

// Result of converting a single DynamicFilterEntry.
struct ConvertedDynamicFilter {
  std::string column_name;
  std::unique_ptr<facebook::velox::common::Filter> filter;
};

#if PANGROVA_HAS_FILTER_PROTO

// Convert a DynamicFilterEntry proto to a Velox Filter.
// Returns nullptr if the filter type is unsupported or data is invalid.
std::unique_ptr<facebook::velox::common::Filter> convertDynamicFilterEntry(
    const pangrova::native::protocol::v1::DynamicFilterEntry& entry);

// Convert all DynamicFilterEntry messages from an InjectDynamicFilterPayload.
// Filters with unsupported types are skipped with a WARN log.
std::vector<ConvertedDynamicFilter> convertDynamicFilters(
    const pangrova::native::protocol::v1::InjectDynamicFilterPayload& payload);

#endif  // PANGROVA_HAS_FILTER_PROTO

// Resolve column name to column index in the given output type.
// Returns -1 if column not found (caller should log WARN and skip).
int resolveColumnIndex(
    const std::string& column_name,
    const facebook::velox::RowTypePtr& output_type);

}  // namespace pangrova::velox
