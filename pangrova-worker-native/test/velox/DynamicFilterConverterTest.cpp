// DynamicFilterConverterTest: Unit tests for Proto → Velox Filter conversion.
// Validates: Requirements R16.3
//
// Tests cover all four oneof filter types (BigintInList, StringInList,
// BigintRange, DoubleRange), column index resolution, and edge cases
// (empty values, NaN bounds, unknown filter types).

#include <gtest/gtest.h>

#include <cmath>
#include <cstring>
#include <limits>
#include <string>
#include <vector>

#include "velox/filter/DynamicFilterConverter.h"
#include "velox/type/Type.h"

#if PANGROVA_HAS_FILTER_PROTO

namespace pangrova::velox::test {
namespace {

namespace proto = pangrova::native::protocol::v1;

// --- resolveColumnIndex ---

TEST(DynamicFilterConverterTest, ResolveColumnIndexFound) {
  auto rowType = facebook::velox::ROW(
      {"id", "name", "date"}, {
          facebook::velox::BIGINT(),
          facebook::velox::VARCHAR(),
          facebook::velox::DATE()});
  EXPECT_EQ(resolveColumnIndex("id", rowType), 0);
  EXPECT_EQ(resolveColumnIndex("name", rowType), 1);
  EXPECT_EQ(resolveColumnIndex("date", rowType), 2);
}

TEST(DynamicFilterConverterTest, ResolveColumnIndexNotFound) {
  auto rowType = facebook::velox::ROW(
      {"id", "name"}, {
          facebook::velox::BIGINT(),
          facebook::velox::VARCHAR()});
  EXPECT_EQ(resolveColumnIndex("nonexistent", rowType), -1);
}

TEST(DynamicFilterConverterTest, ResolveColumnIndexNullType) {
  EXPECT_EQ(resolveColumnIndex("any", nullptr), -1);
}

// --- BigintInList ---

TEST(DynamicFilterConverterTest, BigintInListBasic) {
  proto::DynamicFilterEntry entry;
  entry.set_column_name("id");
  entry.set_filter_type(proto::DYNAMIC_FILTER_TYPE_IN_LIST);
  auto* inList = entry.mutable_bigint_in_list();
  inList->add_values(10);
  inList->add_values(20);
  inList->add_values(30);

  auto filter = convertDynamicFilterEntry(entry);
  ASSERT_NE(filter, nullptr);

  // BigintValuesUsingHashTable should accept values in the set.
  EXPECT_TRUE(filter->testInt64(10));
  EXPECT_TRUE(filter->testInt64(20));
  EXPECT_TRUE(filter->testInt64(30));
  EXPECT_FALSE(filter->testInt64(15));
  EXPECT_FALSE(filter->testInt64(0));
}

TEST(DynamicFilterConverterTest, BigintInListEmpty) {
  proto::DynamicFilterEntry entry;
  entry.set_column_name("id");
  entry.set_filter_type(proto::DYNAMIC_FILTER_TYPE_IN_LIST);
  entry.mutable_bigint_in_list();  // empty values

  auto filter = convertDynamicFilterEntry(entry);
  EXPECT_EQ(filter, nullptr);
}

TEST(DynamicFilterConverterTest, BigintInListSingleValue) {
  proto::DynamicFilterEntry entry;
  entry.set_column_name("id");
  entry.set_filter_type(proto::DYNAMIC_FILTER_TYPE_IN_LIST);
  entry.mutable_bigint_in_list()->add_values(42);

  auto filter = convertDynamicFilterEntry(entry);
  ASSERT_NE(filter, nullptr);
  EXPECT_TRUE(filter->testInt64(42));
  EXPECT_FALSE(filter->testInt64(41));
}

// --- StringInList ---

TEST(DynamicFilterConverterTest, StringInListBasic) {
  proto::DynamicFilterEntry entry;
  entry.set_column_name("region");
  entry.set_filter_type(proto::DYNAMIC_FILTER_TYPE_IN_LIST);
  auto* inList = entry.mutable_string_in_list();
  inList->add_values("us-east-1");
  inList->add_values("eu-west-1");

  auto filter = convertDynamicFilterEntry(entry);
  ASSERT_NE(filter, nullptr);

  EXPECT_TRUE(filter->testBytes("us-east-1", 9));
  EXPECT_TRUE(filter->testBytes("eu-west-1", 9));
  EXPECT_FALSE(filter->testBytes("ap-south-1", 10));
}

TEST(DynamicFilterConverterTest, StringInListEmpty) {
  proto::DynamicFilterEntry entry;
  entry.set_column_name("region");
  entry.set_filter_type(proto::DYNAMIC_FILTER_TYPE_IN_LIST);
  entry.mutable_string_in_list();  // empty

  auto filter = convertDynamicFilterEntry(entry);
  EXPECT_EQ(filter, nullptr);
}

// --- BigintRange ---

TEST(DynamicFilterConverterTest, BigintRangeBasic) {
  proto::DynamicFilterEntry entry;
  entry.set_column_name("ts");
  entry.set_filter_type(proto::DYNAMIC_FILTER_TYPE_RANGE);
  auto* range = entry.mutable_bigint_range();
  range->set_lower(100);
  range->set_upper(200);

  auto filter = convertDynamicFilterEntry(entry);
  ASSERT_NE(filter, nullptr);

  EXPECT_TRUE(filter->testInt64(100));
  EXPECT_TRUE(filter->testInt64(150));
  EXPECT_TRUE(filter->testInt64(200));
  EXPECT_FALSE(filter->testInt64(99));
  EXPECT_FALSE(filter->testInt64(201));
}

TEST(DynamicFilterConverterTest, BigintRangeSinglePoint) {
  proto::DynamicFilterEntry entry;
  entry.set_column_name("ts");
  entry.set_filter_type(proto::DYNAMIC_FILTER_TYPE_RANGE);
  auto* range = entry.mutable_bigint_range();
  range->set_lower(42);
  range->set_upper(42);

  auto filter = convertDynamicFilterEntry(entry);
  ASSERT_NE(filter, nullptr);
  EXPECT_TRUE(filter->testInt64(42));
  EXPECT_FALSE(filter->testInt64(41));
  EXPECT_FALSE(filter->testInt64(43));
}

// --- DoubleRange ---

TEST(DynamicFilterConverterTest, DoubleRangePositive) {
  proto::DynamicFilterEntry entry;
  entry.set_column_name("price");
  entry.set_filter_type(proto::DYNAMIC_FILTER_TYPE_RANGE);
  auto* range = entry.mutable_double_range();
  range->set_lower(1.0);
  range->set_upper(10.0);

  auto filter = convertDynamicFilterEntry(entry);
  ASSERT_NE(filter, nullptr);
  // DoubleRange uses ordered bit pattern — verify it was created.
}

TEST(DynamicFilterConverterTest, DoubleRangeNaNBoundsReturnsNull) {
  proto::DynamicFilterEntry entry;
  entry.set_column_name("price");
  entry.set_filter_type(proto::DYNAMIC_FILTER_TYPE_RANGE);
  auto* range = entry.mutable_double_range();
  range->set_lower(std::numeric_limits<double>::quiet_NaN());
  range->set_upper(10.0);

  auto filter = convertDynamicFilterEntry(entry);
  EXPECT_EQ(filter, nullptr);
}

// --- Unknown filter type ---

TEST(DynamicFilterConverterTest, UnknownFilterTypeReturnsNull) {
  proto::DynamicFilterEntry entry;
  entry.set_column_name("col");
  entry.set_filter_type(proto::DYNAMIC_FILTER_TYPE_UNKNOWN);

  auto filter = convertDynamicFilterEntry(entry);
  EXPECT_EQ(filter, nullptr);
}

// --- convertDynamicFilters (batch) ---

TEST(DynamicFilterConverterTest, BatchConversionSkipsInvalid) {
  proto::InjectDynamicFilterPayload payload;
  payload.set_plan_node_id("node_0");

  // Valid bigint in-list.
  auto* e1 = payload.add_filters();
  e1->set_column_name("id");
  e1->set_filter_type(proto::DYNAMIC_FILTER_TYPE_IN_LIST);
  e1->mutable_bigint_in_list()->add_values(1);

  // Invalid: unknown type.
  auto* e2 = payload.add_filters();
  e2->set_column_name("bad");
  e2->set_filter_type(proto::DYNAMIC_FILTER_TYPE_UNKNOWN);

  // Valid string in-list.
  auto* e3 = payload.add_filters();
  e3->set_column_name("region");
  e3->set_filter_type(proto::DYNAMIC_FILTER_TYPE_IN_LIST);
  e3->mutable_string_in_list()->add_values("us");

  auto results = convertDynamicFilters(payload);
  // Only 2 valid filters should be returned.
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(results[0].column_name, "id");
  EXPECT_EQ(results[1].column_name, "region");
}

TEST(DynamicFilterConverterTest, EmptyPayloadReturnsEmpty) {
  proto::InjectDynamicFilterPayload payload;
  auto results = convertDynamicFilters(payload);
  EXPECT_TRUE(results.empty());
}

// --- IN_LIST type dispatch: bigint vs string ---

TEST(DynamicFilterConverterTest, InListDispatchesByOneofField) {
  // IN_LIST with bigint_in_list set → BigintValuesUsingHashTable.
  proto::DynamicFilterEntry bigintEntry;
  bigintEntry.set_filter_type(proto::DYNAMIC_FILTER_TYPE_IN_LIST);
  bigintEntry.mutable_bigint_in_list()->add_values(5);
  auto bigintFilter = convertDynamicFilterEntry(bigintEntry);
  ASSERT_NE(bigintFilter, nullptr);
  EXPECT_TRUE(bigintFilter->testInt64(5));

  // IN_LIST with string_in_list set → BytesValues.
  proto::DynamicFilterEntry stringEntry;
  stringEntry.set_filter_type(proto::DYNAMIC_FILTER_TYPE_IN_LIST);
  stringEntry.mutable_string_in_list()->add_values("abc");
  auto stringFilter = convertDynamicFilterEntry(stringEntry);
  ASSERT_NE(stringFilter, nullptr);
  EXPECT_TRUE(stringFilter->testBytes("abc", 3));

  // IN_LIST with neither set → nullptr.
  proto::DynamicFilterEntry emptyEntry;
  emptyEntry.set_filter_type(proto::DYNAMIC_FILTER_TYPE_IN_LIST);
  auto emptyFilter = convertDynamicFilterEntry(emptyEntry);
  EXPECT_EQ(emptyFilter, nullptr);
}

// --- RANGE type dispatch: bigint vs double ---

TEST(DynamicFilterConverterTest, RangeDispatchesByOneofField) {
  // RANGE with bigint_range set.
  proto::DynamicFilterEntry bigintEntry;
  bigintEntry.set_filter_type(proto::DYNAMIC_FILTER_TYPE_RANGE);
  auto* br = bigintEntry.mutable_bigint_range();
  br->set_lower(0);
  br->set_upper(100);
  auto bigintFilter = convertDynamicFilterEntry(bigintEntry);
  ASSERT_NE(bigintFilter, nullptr);

  // RANGE with double_range set.
  proto::DynamicFilterEntry doubleEntry;
  doubleEntry.set_filter_type(proto::DYNAMIC_FILTER_TYPE_RANGE);
  auto* dr = doubleEntry.mutable_double_range();
  dr->set_lower(0.0);
  dr->set_upper(100.0);
  auto doubleFilter = convertDynamicFilterEntry(doubleEntry);
  ASSERT_NE(doubleFilter, nullptr);

  // RANGE with neither set → nullptr.
  proto::DynamicFilterEntry emptyEntry;
  emptyEntry.set_filter_type(proto::DYNAMIC_FILTER_TYPE_RANGE);
  auto emptyFilter = convertDynamicFilterEntry(emptyEntry);
  EXPECT_EQ(emptyFilter, nullptr);
}

}  // namespace
}  // namespace pangrova::velox::test

#endif  // PANGROVA_HAS_FILTER_PROTO
