// Canonical v2 RuntimeFilter -> Velox Filter conversion tests.

#include <gtest/gtest.h>

#include <algorithm>
#include <limits>
#include <string>

#include "engine/velox/filter/DynamicFilterConverter.h"
#include "velox/type/Type.h"

namespace pangrova::velox::test {
namespace {

namespace proto = pangrova::protocol::v2;

proto::TypedValue intValue(int64_t value) {
  proto::TypedValue typed;
  typed.set_int64_value(value);
  return typed;
}

proto::TypedValue doubleValue(double value) {
  proto::TypedValue typed;
  typed.set_double_value(value);
  return typed;
}

proto::TypedValue stringValue(std::string value) {
  proto::TypedValue typed;
  typed.set_string_value(std::move(value));
  return typed;
}

proto::RuntimeFilter rowFilter(std::string column) {
  proto::RuntimeFilter filter;
  filter.set_column_name(std::move(column));
  filter.set_scope(proto::RUNTIME_FILTER_SCOPE_ROW_FILTER);
  return filter;
}

TEST(DynamicFilterConverterTest, ResolveColumnIndex) {
  auto rowType = facebook::velox::ROW(
      {"id", "name"}, {facebook::velox::BIGINT(), facebook::velox::VARCHAR()});
  EXPECT_EQ(resolveColumnIndex("id", rowType), 0);
  EXPECT_EQ(resolveColumnIndex("name", rowType), 1);
  EXPECT_EQ(resolveColumnIndex("missing", rowType), -1);
  EXPECT_EQ(resolveColumnIndex("id", nullptr), -1);
}

TEST(DynamicFilterConverterTest, BigintInList) {
  auto entry = rowFilter("id");
  *entry.mutable_in_list()->add_values() = intValue(10);
  *entry.mutable_in_list()->add_values() = intValue(20);
  auto filter = convertDynamicFilterEntry(entry);
  ASSERT_NE(filter, nullptr);
  EXPECT_TRUE(filter->testInt64(10));
  EXPECT_TRUE(filter->testInt64(20));
  EXPECT_FALSE(filter->testInt64(15));
}

TEST(DynamicFilterConverterTest, SingleBigintInList) {
  auto entry = rowFilter("id");
  *entry.mutable_in_list()->add_values() = intValue(42);
  auto filter = convertDynamicFilterEntry(entry);
  ASSERT_NE(filter, nullptr);
  EXPECT_TRUE(filter->testInt64(42));
  EXPECT_FALSE(filter->testInt64(41));
}

TEST(DynamicFilterConverterTest, StringInList) {
  auto entry = rowFilter("region");
  *entry.mutable_in_list()->add_values() = stringValue("us-east-1");
  *entry.mutable_in_list()->add_values() = stringValue("eu-west-1");
  auto filter = convertDynamicFilterEntry(entry);
  ASSERT_NE(filter, nullptr);
  EXPECT_TRUE(filter->testBytes("us-east-1", 9));
  EXPECT_FALSE(filter->testBytes("ap-south-1", 10));
}

TEST(DynamicFilterConverterTest, RejectsEmptyAndMixedInList) {
  auto empty = rowFilter("id");
  empty.mutable_in_list();
  EXPECT_EQ(convertDynamicFilterEntry(empty), nullptr);

  auto mixed = rowFilter("id");
  *mixed.mutable_in_list()->add_values() = intValue(1);
  *mixed.mutable_in_list()->add_values() = stringValue("1");
  EXPECT_EQ(convertDynamicFilterEntry(mixed), nullptr);
}

TEST(DynamicFilterConverterTest, BigintRangeHonorsExclusiveBounds) {
  auto entry = rowFilter("id");
  auto* range = entry.mutable_range();
  *range->mutable_lower() = intValue(10);
  *range->mutable_upper() = intValue(20);
  range->set_lower_inclusive(false);
  range->set_upper_inclusive(false);
  auto filter = convertDynamicFilterEntry(entry);
  ASSERT_NE(filter, nullptr);
  EXPECT_FALSE(filter->testInt64(10));
  EXPECT_TRUE(filter->testInt64(11));
  EXPECT_TRUE(filter->testInt64(19));
  EXPECT_FALSE(filter->testInt64(20));
}

TEST(DynamicFilterConverterTest, DoubleRangeHonorsExclusiveBounds) {
  auto entry = rowFilter("price");
  auto* range = entry.mutable_range();
  *range->mutable_lower() = doubleValue(1.0);
  *range->mutable_upper() = doubleValue(10.0);
  range->set_lower_inclusive(true);
  range->set_upper_inclusive(false);
  auto filter = convertDynamicFilterEntry(entry);
  ASSERT_NE(filter, nullptr);
  EXPECT_TRUE(filter->testDouble(1.0));
  EXPECT_TRUE(filter->testDouble(5.0));
  EXPECT_FALSE(filter->testDouble(10.0));
}

TEST(DynamicFilterConverterTest, StringRangeHonorsBounds) {
  auto entry = rowFilter("region");
  auto* range = entry.mutable_range();
  *range->mutable_lower() = stringValue("b");
  *range->mutable_upper() = stringValue("d");
  range->set_lower_inclusive(true);
  range->set_upper_inclusive(false);
  auto filter = convertDynamicFilterEntry(entry);
  ASSERT_NE(filter, nullptr);
  EXPECT_TRUE(filter->testBytes("b", 1));
  EXPECT_TRUE(filter->testBytes("c", 1));
  EXPECT_FALSE(filter->testBytes("d", 1));
}

TEST(DynamicFilterConverterTest, RejectsInvalidRanges) {
  auto mixed = rowFilter("bad");
  *mixed.mutable_range()->mutable_lower() = intValue(1);
  *mixed.mutable_range()->mutable_upper() = doubleValue(2.0);
  EXPECT_EQ(convertDynamicFilterEntry(mixed), nullptr);

  auto nan = rowFilter("bad");
  *nan.mutable_range()->mutable_lower() =
      doubleValue(std::numeric_limits<double>::quiet_NaN());
  *nan.mutable_range()->mutable_upper() = doubleValue(2.0);
  EXPECT_EQ(convertDynamicFilterEntry(nan), nullptr);

  auto emptyExclusive = rowFilter("bad");
  *emptyExclusive.mutable_range()->mutable_lower() = intValue(5);
  *emptyExclusive.mutable_range()->mutable_upper() = intValue(5);
  EXPECT_EQ(convertDynamicFilterEntry(emptyExclusive), nullptr);
}

TEST(DynamicFilterConverterTest, PartitionOnlyFilterIsNotNative) {
  auto entry = rowFilter("partition_key");
  entry.set_scope(proto::RUNTIME_FILTER_SCOPE_PARTITION_PRUNING);
  *entry.mutable_in_list()->add_values() = stringValue("p1");
  EXPECT_EQ(convertDynamicFilterEntry(entry), nullptr);
}

TEST(DynamicFilterConverterTest, UnspecifiedScopeFailsClosed) {
  proto::RuntimeFilter entry;
  entry.set_column_name("id");
  *entry.mutable_in_list()->add_values() = intValue(1);
  EXPECT_EQ(convertDynamicFilterEntry(entry), nullptr);
}

TEST(DynamicFilterConverterTest, SbbfBloomUsesDeclaredEncoding) {
  std::string serialized(16 + 32, '\0');
  serialized[0] = 'S';
  serialized[1] = 'B';
  serialized[2] = 'B';
  serialized[3] = 'F';
  serialized[7] = 1;
  serialized[11] = 1;
  std::fill(serialized.begin() + 16, serialized.end(), static_cast<char>(0xff));

  auto entry = rowFilter("region");
  auto* bloom = entry.mutable_bloom();
  bloom->set_algorithm("SBBF");
  bloom->set_serialized_filter(serialized);
  bloom->set_value_encoding(proto::VALUE_ENCODING_UTF8);
  auto filter = convertDynamicFilterEntry(entry);
  ASSERT_NE(filter, nullptr);
  EXPECT_TRUE(filter->testBytes("apac", 4));
}

TEST(DynamicFilterConverterTest, BloomRequiresKnownAlgorithmAndEncoding) {
  auto entry = rowFilter("region");
  entry.mutable_bloom()->set_algorithm("UNKNOWN");
  entry.mutable_bloom()->set_serialized_filter("bytes");
  EXPECT_EQ(convertDynamicFilterEntry(entry), nullptr);
}

TEST(DynamicFilterConverterTest, BatchSkipsDppAndRejectsInvalidNativeFilter) {
  proto::RuntimeFilterBinding payload;
  payload.set_plan_node_id("node_0");
  auto* dpp = payload.add_filters();
  dpp->set_column_name("partition_key");
  dpp->set_scope(proto::RUNTIME_FILTER_SCOPE_PARTITION_PRUNING);
  *dpp->mutable_in_list()->add_values() = stringValue("p1");

  auto* row = payload.add_filters();
  *row = rowFilter("id");
  *row->mutable_in_list()->add_values() = intValue(1);
  auto converted = convertDynamicFilters(payload.filters());
  ASSERT_EQ(converted.size(), 1);
  EXPECT_EQ(converted[0].column_name, "id");

  auto* invalid = payload.add_filters();
  invalid->set_column_name("bad");
  invalid->set_scope(proto::RUNTIME_FILTER_SCOPE_ROW_FILTER);
  EXPECT_THROW(convertDynamicFilters(payload.filters()), std::invalid_argument);
}

TEST(DynamicFilterConverterTest, EmptyFilterListReturnsEmpty) {
  proto::RuntimeFilterBinding payload;
  EXPECT_TRUE(convertDynamicFilters(payload.filters()).empty());
}

}  // namespace
}  // namespace pangrova::velox::test
