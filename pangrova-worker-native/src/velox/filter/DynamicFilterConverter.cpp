// DynamicFilterConverter: Proto → Velox Filter direct conversion.
// See header for supported mappings and error handling strategy.
//
// A canonical RuntimeFilter contract carries two complementary scopes:
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
// Both scopes share the same canonical protocol and column naming convention,
// but execute at different granularities. The coordinator assigns scope from
// table partition metadata; the Worker consumes each scope exactly once.
#include "velox/filter/DynamicFilterConverter.h"

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <unordered_set>
#include <vector>

#include <folly/dynamic.h>

#include "index/FileIndexFilter.h"
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

namespace proto = pangrova::protocol::v2;

namespace {

class SbbfDynamicFilter final : public facebook::velox::common::Filter {
 public:
  SbbfDynamicFilter(std::string serialized_filter,
                    proto::ValueEncoding encoding)
      : Filter(
            true,
            false,
            facebook::velox::common::FilterKind::kBigintValuesUsingBloomFilter),
        serialized_filter_(std::move(serialized_filter)),
        encoding_(encoding) {}

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    if (nullAllowed.value_or(false)) {
      throw std::invalid_argument("SBBF dynamic filters do not admit nulls");
    }
    return std::make_unique<SbbfDynamicFilter>(serialized_filter_, encoding_);
  }

  bool testingEquals(const Filter& other) const final {
    auto* candidate = dynamic_cast<const SbbfDynamicFilter*>(&other);
    return candidate != nullptr && candidate->encoding_ == encoding_ &&
        candidate->serialized_filter_ == serialized_filter_;
  }

  folly::dynamic serialize() const final {
    auto value = serializeBase();
    value["pangrovaEncoding"] = static_cast<int>(encoding_);
    value["pangrovaSerializedBytes"] = serialized_filter_.size();
    return value;
  }

  std::string toString() const final {
    return "PangrovaSbbfDynamicFilter(encoding=" +
        std::to_string(static_cast<int>(encoding_)) + ",bytes=" +
        std::to_string(serialized_filter_.size()) + ")";
  }

  bool testNonNull() const final { return true; }

  bool testInt64(int64_t value) const final {
    if (encoding_ != proto::VALUE_ENCODING_INT64_LE) {
      return true;
    }
    std::array<char, sizeof(int64_t)> bytes{};
    auto unsigned_value = static_cast<uint64_t>(value);
    for (size_t i = 0; i < bytes.size(); ++i) {
      bytes[i] = static_cast<char>((unsigned_value >> (i * 8)) & 0xff);
    }
    return probe(std::string(bytes.data(), bytes.size()));
  }

  bool testDouble(double value) const final {
    if (encoding_ != proto::VALUE_ENCODING_DOUBLE_LE) {
      return true;
    }
    uint64_t bits = 0;
    std::memcpy(&bits, &value, sizeof(bits));
    std::array<char, sizeof(double)> bytes{};
    for (size_t i = 0; i < bytes.size(); ++i) {
      bytes[i] = static_cast<char>((bits >> (i * 8)) & 0xff);
    }
    return probe(std::string(bytes.data(), bytes.size()));
  }

  bool testFloat(float value) const final {
    return testDouble(static_cast<double>(value));
  }

  bool testBytes(const char* value, int32_t length) const final {
    if (encoding_ != proto::VALUE_ENCODING_UTF8) {
      return true;
    }
    return probe(std::string(value, static_cast<size_t>(length)));
  }

  bool testInt64Range(int64_t, int64_t, bool) const final { return true; }
  bool testDoubleRange(double, double, bool) const final { return true; }
  bool testBytesRange(
      std::optional<std::string_view>,
      std::optional<std::string_view>,
      bool) const final { return true; }
  bool testLength(int32_t) const final { return true; }

 private:
  bool probe(const std::string& value) const {
    return pangrova::index::FileIndexFilter::mayContainValue(
        serialized_filter_, "", value);
  }

  std::string serialized_filter_;
  proto::ValueEncoding encoding_;
};

// Build BigintValuesUsingHashTable from a homogeneous TypedValue list.
// For a single value, use BigintRange because Velox requires min < max for
// BigintValuesUsingHashTable.
std::unique_ptr<facebook::velox::common::Filter> buildBigintInList(
    const proto::InListFilter& inList) {
  std::vector<int64_t> values;
  values.reserve(inList.values_size());
  for (const auto& value : inList.values()) {
    if (!value.has_int64_value()) {
      return nullptr;
    }
    values.push_back(value.int64_value());
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

// Build BytesValues from a homogeneous TypedValue list.
std::unique_ptr<facebook::velox::common::Filter> buildBytesInList(
    const proto::InListFilter& inList) {
  std::vector<std::string> values;
  values.reserve(inList.values_size());
  for (const auto& value : inList.values()) {
    if (!value.has_string_value()) {
      return nullptr;
    }
    values.push_back(value.string_value());
  }
  if (values.empty()) {
    return nullptr;
  }
  return std::make_unique<facebook::velox::common::BytesValues>(
      values, /*nullAllowed=*/false);
}

std::unique_ptr<facebook::velox::common::Filter> buildBigintRange(
    const proto::RangeFilter& range) {
  if (!range.has_lower() || !range.has_upper() ||
      !range.lower().has_int64_value() ||
      !range.upper().has_int64_value()) {
    return nullptr;
  }
  auto lower = range.lower().int64_value();
  auto upper = range.upper().int64_value();
  if (!range.lower_inclusive()) {
    if (lower == std::numeric_limits<int64_t>::max()) {
      return nullptr;
    }
    ++lower;
  }
  if (!range.upper_inclusive()) {
    if (upper == std::numeric_limits<int64_t>::min()) {
      return nullptr;
    }
    --upper;
  }
  if (lower > upper) {
    return nullptr;
  }
  return std::make_unique<facebook::velox::common::BigintRange>(
      lower,
      upper,
      /*nullAllowed=*/false);
}

std::unique_ptr<facebook::velox::common::Filter> buildDoubleRange(
    const proto::RangeFilter& range) {
  if (!range.has_lower() || !range.has_upper() ||
      !range.lower().has_double_value() ||
      !range.upper().has_double_value()) {
    return nullptr;
  }
  const double lower = range.lower().double_value();
  const double upper = range.upper().double_value();

  // NaN bounds are invalid — skip filter creation
  if (std::isnan(lower) || std::isnan(upper)) {
    PANGROVA_LOG_WARN
        << "DynamicFilterConverter: NaN bound in double range, skipping filter";
    return nullptr;
  }

  return std::make_unique<facebook::velox::common::DoubleRange>(
      lower,
      /*lowerUnbounded=*/false,
      /*lowerExclusive=*/!range.lower_inclusive(),
      upper,
      /*upperUnbounded=*/false,
      /*upperExclusive=*/!range.upper_inclusive(),
      /*nullAllowed=*/false);
}

std::unique_ptr<facebook::velox::common::Filter> buildStringRange(
    const proto::RangeFilter& range) {
  if (!range.has_lower() || !range.has_upper() ||
      !range.lower().has_string_value() ||
      !range.upper().has_string_value() ||
      range.lower().string_value() > range.upper().string_value()) {
    return nullptr;
  }
  return std::make_unique<facebook::velox::common::BytesRange>(
      range.lower().string_value(),
      /*lowerUnbounded=*/false,
      /*lowerExclusive=*/!range.lower_inclusive(),
      range.upper().string_value(),
      /*upperUnbounded=*/false,
      /*upperExclusive=*/!range.upper_inclusive(),
      /*nullAllowed=*/false);
}

std::unique_ptr<facebook::velox::common::Filter> buildSbbfBloom(
    const proto::BloomFilter& bloom) {
  if (bloom.algorithm() != "SBBF" || bloom.serialized_filter().empty() ||
      bloom.value_encoding() == proto::VALUE_ENCODING_UNSPECIFIED) {
    return nullptr;
  }
  return std::make_unique<SbbfDynamicFilter>(
      bloom.serialized_filter(), bloom.value_encoding());
}

}  // namespace

std::unique_ptr<facebook::velox::common::Filter> convertDynamicFilterEntry(
    const proto::RuntimeFilter& entry) {
  if (entry.scope() == proto::RUNTIME_FILTER_SCOPE_PARTITION_PRUNING) {
    return nullptr;
  }
  if (entry.scope() != proto::RUNTIME_FILTER_SCOPE_ROW_FILTER &&
      entry.scope() != proto::RUNTIME_FILTER_SCOPE_BOTH) {
    return nullptr;
  }
  switch (entry.predicate_case()) {
    case proto::RuntimeFilter::kInList:
      if (entry.in_list().values_size() == 0) {
        return nullptr;
      }
      switch (entry.in_list().values(0).value_case()) {
        case proto::TypedValue::kInt64Value:
          return buildBigintInList(entry.in_list());
        case proto::TypedValue::kStringValue:
          return buildBytesInList(entry.in_list());
        default:
          return nullptr;
      }

    case proto::RuntimeFilter::kRange:
      if (!entry.range().has_lower() || !entry.range().has_upper() ||
          entry.range().lower().value_case() !=
              entry.range().upper().value_case()) {
        return nullptr;
      }
      switch (entry.range().lower().value_case()) {
        case proto::TypedValue::kInt64Value:
          return buildBigintRange(entry.range());
        case proto::TypedValue::kDoubleValue:
          return buildDoubleRange(entry.range());
        case proto::TypedValue::kStringValue:
          return buildStringRange(entry.range());
        default:
          return nullptr;
      }

    case proto::RuntimeFilter::kBloom:
      return buildSbbfBloom(entry.bloom());

    default:
      PANGROVA_LOG_WARN
          << "DynamicFilterConverter: unsupported predicate "
          << static_cast<int>(entry.predicate_case())
          << " for column " << entry.column_name();
      return nullptr;
  }
}

std::vector<ConvertedDynamicFilter> convertDynamicFilters(
    const proto::InjectRuntimeFilterRequest& payload) {
  std::vector<ConvertedDynamicFilter> results;
  results.reserve(payload.filters_size());

  for (int i = 0; i < payload.filters_size(); ++i) {
    const auto& entry = payload.filters(i);
    if (entry.scope() == proto::RUNTIME_FILTER_SCOPE_PARTITION_PRUNING) {
      continue;
    }
    auto filter = convertDynamicFilterEntry(entry);
    if (!filter) {
      throw std::invalid_argument(
          "Unsupported or invalid Native dynamic filter for column " +
          entry.column_name() + " predicate=" +
          std::to_string(static_cast<int>(entry.predicate_case())));
    }
    results.push_back({entry.column_name(), std::move(filter)});
  }
  return results;
}

}  // namespace pangrova::velox
