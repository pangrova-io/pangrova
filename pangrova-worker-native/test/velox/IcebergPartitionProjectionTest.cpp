#include <gtest/gtest.h>

#include <limits>
#include <bit>

#include "TestMemoryHelper.h"
#include "capability/CanonicalPhysicalTypes.h"
#include "folly/json.h"
#include "velox/connector/IcebergPartitionProjection.h"
#include "velox/type/Timestamp.h"
#include "velox/vector/FlatVector.h"

namespace pangrova::velox::test {
namespace {
using namespace facebook::velox;

class IcebergPartitionProjectionTest : public ::testing::Test {
 protected:
  void SetUp() override { pool_ = pangrova::test::testLeafPool("IcebergPartitionProjectionTest"); }
  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
};

TEST_F(IcebergPartitionProjectionTest, RenamesUseIdsAndOldSpecsFillNewFieldsWithNull) {
  auto schema = folly::parseJson(R"({"type":"record","fields":[
    {"name":"old_region","field-id":1000,"type":["null","string"]},
    {"name":"old_total","field-id":1001,"type":["null","int"]}]})");
  std::vector<split::IcebergManifestPartitionSpec> specs{
      {0, {{"identity", "string", 1000, "region"}, {"identity", "long", 1001, "total"}}},
      {1, {{"identity", "string", 1000, "region"}, {"identity", "long", 1001, "total"},
          {"day", "date", 1002, "day"}}}};
  IcebergPartitionProjection projection(schema, 0, specs,
      ROW({"region", "total", "day"}, {VARCHAR(), BIGINT(), DATE()}));
  folly::dynamic first = folly::dynamic::object("old_region", "杭州")("old_total", 17);
  folly::dynamic second = folly::dynamic::object("old_region", nullptr)("old_total", nullptr);
  auto output = projection.project({&first, &second}, pool_.get());
  ASSERT_EQ(output->size(), 2);
  EXPECT_EQ(output->childAt(0)->asFlatVector<StringView>()->valueAt(0), "杭州");
  EXPECT_EQ(output->childAt(1)->asFlatVector<int64_t>()->valueAt(0), 17);
  EXPECT_TRUE(output->childAt(2)->isNullAt(0));
  EXPECT_TRUE(output->childAt(0)->isNullAt(1));
  EXPECT_TRUE(output->childAt(1)->isNullAt(1));
  EXPECT_TRUE(output->childAt(2)->isNullAt(1));
}

TEST_F(IcebergPartitionProjectionTest, PrimitiveLogicalTypesPreserveValuesAndPrecision) {
  auto schema = folly::parseJson(R"({"type":"record","fields":[
    {"name":"b","field-id":1000,"type":"boolean"},
    {"name":"f","field-id":1001,"type":"float"},
    {"name":"d","field-id":1002,"type":"double"},
    {"name":"l","field-id":1003,"type":"long"},
    {"name":"day","field-id":1004,"type":{"type":"int","logicalType":"date"}},
    {"name":"t","field-id":1005,"type":{"type":"long","logicalType":"time-micros"}},
    {"name":"ts","field-id":1006,"type":{"type":"long","logicalType":"timestamp-micros","adjust-to-utc":true}},
    {"name":"ns","field-id":1007,"type":{"type":"long","logicalType":"timestamp-nanos","adjust-to-utc":false}},
    {"name":"bin","field-id":1008,"type":"bytes"},
    {"name":"uuid","field-id":1009,"type":{"type":"fixed","size":16,"logicalType":"uuid"}},
    {"name":"dec","field-id":1010,"type":{"type":"fixed","size":2,"logicalType":"decimal","precision":4,"scale":2}}]})");
  std::vector<split::IcebergManifestPartitionSpec> specs{{0, {
      {"identity", "boolean", 1000, "b"}, {"identity", "double", 1001, "f"},
      {"identity", "double", 1002, "d"}, {"identity", "long", 1003, "l"},
      {"day", "date", 1004, "day"}, {"identity", "time", 1005, "t"},
      {"identity", "timestamptz", 1006, "ts"}, {"identity", "timestamp_ns", 1007, "ns"},
      {"identity", "binary", 1008, "bin"}, {"identity", "uuid", 1009, "uuid"},
      {"identity", "decimal(6, 2)", 1010, "dec"}}}};
  IcebergPartitionProjection projection(schema, 0, specs,
      ROW({"b", "f", "d", "l", "day", "t", "ts", "ns", "bin", "uuid", "dec"},
          {BOOLEAN(), DOUBLE(), DOUBLE(), BIGINT(), DATE(), capability::canonicalTimeType(6),
              TIMESTAMP(), TIMESTAMP(), VARBINARY(), VARBINARY(), DECIMAL(6, 2)}));
  const std::string binary("a\0b", 3), uuid(16, '\xff'), decimal("\xfb\x2e", 2);
  folly::dynamic row = folly::dynamic::object("b", true)("f", 1.25)("d", -2.5)("l", int64_t{-123})
      ("day", -1)("t", int64_t{86'399'999'999})("ts", -1)("ns", -1)
      ("bin", binary)("uuid", uuid)("dec", decimal);
  auto output = projection.project({&row}, pool_.get());
  EXPECT_TRUE(output->childAt(0)->asFlatVector<bool>()->valueAt(0));
  EXPECT_EQ(output->childAt(1)->asFlatVector<double>()->valueAt(0), 1.25);
  EXPECT_EQ(output->childAt(2)->asFlatVector<double>()->valueAt(0), -2.5);
  EXPECT_EQ(output->childAt(3)->asFlatVector<int64_t>()->valueAt(0), -123);
  EXPECT_EQ(output->childAt(4)->asFlatVector<int32_t>()->valueAt(0), -1);
  EXPECT_EQ(output->childAt(5)->asFlatVector<int64_t>()->valueAt(0), 86'399'999'999);
  EXPECT_EQ(output->childAt(6)->asFlatVector<Timestamp>()->valueAt(0), Timestamp::fromMicros(-1));
  EXPECT_EQ(output->childAt(7)->asFlatVector<Timestamp>()->valueAt(0), Timestamp::fromNanos(-1));
  EXPECT_EQ(output->childAt(8)->asFlatVector<StringView>()->valueAt(0), binary);
  EXPECT_EQ(output->childAt(9)->asFlatVector<StringView>()->valueAt(0), uuid);
  EXPECT_EQ(output->childAt(10)->asFlatVector<int64_t>()->valueAt(0), -1234);
  row["dec"] = std::string("\x27\x10", 2);
  EXPECT_THROW(projection.project({&row}, pool_.get()), std::runtime_error);
  row["dec"] = decimal;
  row["t"] = int64_t{86'400'000'000};
  EXPECT_THROW(projection.project({&row}, pool_.get()), std::runtime_error);
}

TEST_F(IcebergPartitionProjectionTest, RejectsWriterIdentityChangesAndInt32Overflow) {
  auto schema = folly::parseJson(R"({"type":"record","fields":[
    {"name":"old","field-id":1000,"type":"int"}]})");
  const std::vector<split::IcebergManifestPartitionSpec> specs{
      {0, {{"identity", "long", 1000, "renamed"}}}};
  auto type = ROW({"renamed"}, {BIGINT()});
  IcebergPartitionProjection projection(schema, 0, specs, type);
  folly::dynamic row = folly::dynamic::object("old", std::numeric_limits<int64_t>::max());
  EXPECT_THROW(projection.project({&row}, pool_.get()), std::runtime_error);
  schema["fields"][0]["field-id"] = 1001;
  EXPECT_THROW(IcebergPartitionProjection(schema, 0, specs, type), std::runtime_error);
}

TEST_F(IcebergPartitionProjectionTest, LongDecimalsRetainSigned128BitExtremesWithinPrecision) {
  auto schema = folly::parseJson(R"({"type":"record","fields":[
    {"name":"amount","field-id":1000,"type":{"type":"fixed","size":16,
      "logicalType":"decimal","precision":38,"scale":6}}]})");
  const std::vector<split::IcebergManifestPartitionSpec> specs{
      {0, {{"identity", "decimal(38, 6)", 1000, "amount"}}}};
  IcebergPartitionProjection projection(schema, 0, specs, ROW({"amount"}, {DECIMAL(38, 6)}));
  auto encode = [](int128_t value) {
    auto bits = std::bit_cast<__uint128_t>(value);
    std::string bytes(16, '\0');
    for (int i = 15; i >= 0; --i) {
      bytes[i] = static_cast<char>(bits & 0xff);
      bits >>= 8;
    }
    return bytes;
  };
  int128_t bound = 1;
  for (int i = 0; i < 38; ++i) bound *= 10;
  folly::dynamic positive = folly::dynamic::object("amount", encode(bound - 1));
  folly::dynamic negative = folly::dynamic::object("amount", encode(1 - bound));
  auto output = projection.project({&positive, &negative}, pool_.get());
  EXPECT_EQ(output->childAt(0)->asFlatVector<int128_t>()->valueAt(0), bound - 1);
  EXPECT_EQ(output->childAt(0)->asFlatVector<int128_t>()->valueAt(1), 1 - bound);
  positive["amount"] = encode(bound);
  EXPECT_THROW(projection.project({&positive}, pool_.get()), std::runtime_error);
}

TEST_F(IcebergPartitionProjectionTest, ExcludedFieldsDoNotAliasAReusedNameOrInventValues) {
  auto schema = folly::parseJson(R"({"type":"record","fields":[
    {"name":"previous","field-id":1000,"type":["null","long"]}]})");
  std::vector<split::IcebergManifestPartitionSpec> specs{
      {0, {{"void", "long", 1000, ""}}}, {1, {{"identity", "long", 1001, "current"}}}};
  auto type = ROW({"current"}, {BIGINT()});
  IcebergPartitionProjection projection(schema, 0, specs, type);
  folly::dynamic row = folly::dynamic::object("previous", nullptr);
  EXPECT_TRUE(projection.project({&row}, pool_.get())->childAt(0)->isNullAt(0));
  specs[0].fields[0].name = "current";
  EXPECT_THROW(IcebergPartitionProjection(schema, 0, specs, type), std::runtime_error);
}
}  // namespace
}  // namespace pangrova::velox::test
