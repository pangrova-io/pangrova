// DynamicFilterConverter: Proto → Velox Filter direct conversion.
// See header for supported mappings and error handling strategy.
//
// P1-CHAIN-7: Dynamic Filter Dual-Path Architecture
// =================================================
// Dynamic filters in Pangrova operate through two complementary paths:
//
// 1. Java-side DPP (Dynamic Partition Pruning):
//    - Applied in TaskRequestBuilder.applyDynamicFilters() BEFORE native execution.
//    - Operates on Base64-encoded SplitInfo proto bytes.
//    - Prunes entire splits (partitions) based on partition column values.
//    - Uses DynamicFilterApplicator with DynamicFilterPayload from coordinator.
//    - Effective for partition-level filtering (e.g., date partitions).
//
// 2. Native-side Dynamic Filters (row-level):
//    - Applied in PangrovaVeloxDataSource::applyDynamicFilters() DURING scan.
//    - Injected via PendingFilterRegistry (Java → JNI → nativeInjectDynamicFilter).
//    - Converted from proto DynamicFilterEntry to Velox Filter objects here.
//    - Operates on individual rows within a batch using column-level evaluation.
//    - Effective for runtime bloom/range filters from hash join build side.
//
// Both paths share the same column naming convention (output_type_ column names)
// but operate at different granularities. A filter applied at the wrong level
// (e.g., row-level filter used for partition pruning) would be functionally
// correct but suboptimal. The Java coordinator decides which path to use based
// on whether the filter column is a partition column.
#include "velox/filter/DynamicFilterConverter.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <unordered_set>
#include <vector>

#include "velox/type/Filter.h"
#include "velox/type/Type.h"

// Logging: use unified PangrovaLog macros.
#include "observability/PangrovaLog.h"

namespace pangrova::velox {

int resolveColumnIndex(
    const std::string& column_name,
    const facebook::velox::RowTypePtr& output_type) {
  if (!output_type) {
    return -1;
  }
  for (size_t i = 0; i < output_type->size(); ++i) {
    if (output_type->nameOf(i) == column_name) {
      return static_cast<int>(i);
    }
  }
  return -1;
}

#if PANGROVA_HAS_FILTER_PROTO

namespace proto = pangrova::native::protocol::v1;

namespace {

// Build BigintValuesUsingHashTable from BigintInList oneof.
// For a single value, uses BigintRange instead because Velox's
// BigintValuesUsingHashTable requires min < max.
std::unique_ptr<facebook::velox::common::Filter> buildBigintInList(
    const proto::DynamicFilterEntry& entry) {
  const auto& inList = entry.bigint_in_list();
  std::vector<int64_t> values;
  values.reserve(inList.values_size());
  for (int i = 0; i < inList.values_size(); ++i) {
    values.push_back(inList.values(i));
  }
  if (values.empty()) {
    return nullptr;
  }
  // Single value: BigintValuesUsingHashTable requires min < max,
  // so use BigintRange(value, value) for exact match.
  if (values.size() == 1) {
    return std::make_unique<facebook::velox::common::BigintRange>(
        values[0], values[0], /*nullAllowed=*/false);
  }
  return std::make_unique<facebook::velox::common::BigintValuesUsingHashTable>(
      *std::min_element(values.begin(), values.end()),
      *std::max_element(values.begin(), values.end()),
      values,
      /*nullAllowed=*/false);
}

// Build BytesValues from StringInList oneof.
std::unique_ptr<facebook::velox::common::Filter> buildBytesInList(
    const proto::DynamicFilterEntry& entry) {
  const auto& inList = entry.string_in_list();
  std::vector<std::string> values;
  values.reserve(inList.values_size());
  for (int i = 0; i < inList.values_size(); ++i) {
    values.push_back(inList.values(i));
  }
  if (values.empty()) {
    return nullptr;
  }
  return std::make_unique<facebook::velox::common::BytesValues>(
      values, /*nullAllowed=*/false);
}

// Build BigintRange from BigintRange oneof.
std::unique_ptr<facebook::velox::common::Filter> buildBigintRange(
    const proto::DynamicFilterEntry& entry) {
  const auto& range = entry.bigint_range();
  return std::make_unique<facebook::velox::common::BigintRange>(
      range.lower(),
      range.upper(),
      /*nullAllowed=*/false);
}

// Converts a double to an ordered int64 representation that preserves
// comparison order. IEEE 754 doubles have the property that positive values
// are ordered the same as their bit patterns when interpreted as int64.
// For negative values, the bit pattern order is inverted, so we flip all
// bits except the sign bit. This gives a total ordering:
//   -Inf < -max < ... < -0 < +0 < ... < +max < +Inf
// NaN is mapped to INT64_MAX (excluded from valid ranges).
int64_t doubleToOrderedInt(double value) {
  if (std::isnan(value)) {
    return std::numeric_limits<int64_t>::max();
  }
  int64_t bits;
  std::memcpy(&bits, &value, sizeof(int64_t));
  if (bits < 0) {
    // Negative: flip all bits except sign to reverse ordering
    bits ^= INT64_C(0x7FFFFFFFFFFFFFFF);
  }
  return bits;
}

// Build a filter for double range using BigintRange on ordered bit patterns.
// Uses doubleToOrderedInt() to correctly handle negative doubles, NaN, and
// ±Infinity without any AlwaysTrue fallback.
std::unique_ptr<facebook::velox::common::Filter> buildDoubleRange(
    const proto::DynamicFilterEntry& entry) {
  const auto& range = entry.double_range();
  double lower = range.lower();
  double upper = range.upper();

  // NaN bounds are invalid — skip filter creation
  if (std::isnan(lower) || std::isnan(upper)) {
    PANGROVA_LOG_WARN
        << "DynamicFilterConverter: NaN bound in double range, skipping filter";
    return nullptr;
  }

  int64_t lower_ordered = doubleToOrderedInt(lower);
  int64_t upper_ordered = doubleToOrderedInt(upper);

  return std::make_unique<facebook::velox::common::BigintRange>(
      lower_ordered, upper_ordered, /*nullAllowed=*/false);
}

}  // namespace

std::unique_ptr<facebook::velox::common::Filter> convertDynamicFilterEntry(
    const proto::DynamicFilterEntry& entry) {
  switch (entry.filter_type()) {
    case proto::DYNAMIC_FILTER_TYPE_IN_LIST:
      if (entry.has_string_in_list()) {
        return buildBytesInList(entry);
      }
      if (entry.has_bigint_in_list()) {
        return buildBigintInList(entry);
      }
      return nullptr;

    case proto::DYNAMIC_FILTER_TYPE_RANGE:
      if (entry.has_double_range()) {
        return buildDoubleRange(entry);
      }
      if (entry.has_bigint_range()) {
        return buildBigintRange(entry);
      }
      return nullptr;

    default:
      PANGROVA_LOG_WARN
          << "DynamicFilterConverter: unsupported filter type "
          << static_cast<int>(entry.filter_type())
          << " for column " << entry.column_name();
      return nullptr;
  }
}

std::vector<ConvertedDynamicFilter> convertDynamicFilters(
    const proto::InjectDynamicFilterPayload& payload) {
  std::vector<ConvertedDynamicFilter> results;
  results.reserve(payload.filters_size());

  for (int i = 0; i < payload.filters_size(); ++i) {
    const auto& entry = payload.filters(i);
    auto filter = convertDynamicFilterEntry(entry);
    if (filter) {
      results.push_back(
          {entry.column_name(), std::move(filter)});
    }
  }
  return results;
}

#endif  // PANGROVA_HAS_FILTER_PROTO

}  // namespace pangrova::velox
