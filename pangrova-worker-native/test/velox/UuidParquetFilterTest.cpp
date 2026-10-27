#include <gtest/gtest.h>

#include <array>
#include <cstring>
#include <limits>
#include <tuple>

#include "velox/connector/FilterDecomposer.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/functions/prestosql/types/UuidType.h"
#include "velox/type/DecimalUtil.h"
#include "velox/type/Filter.h"

namespace pangrova::velox::test {
namespace {
using namespace facebook::velox;

std::string bytes(int128_t value) {
  auto encoded = DecimalUtil::bigEndian(value);
  return std::string(reinterpret_cast<const char*>(&encoded), sizeof(encoded));
}

std::shared_ptr<const core::ITypedExpr> constant(int128_t value) {
  return std::make_shared<core::ConstantTypedExpr>(UUID(), variant(value));
}

std::shared_ptr<const core::ITypedExpr> comparison(
    const std::string& op, int128_t value, bool reversed = false) {
  auto field = std::make_shared<core::FieldAccessTypedExpr>(UUID(), "id");
  std::vector<core::TypedExprPtr> args = reversed
      ? std::vector<core::TypedExprPtr>{constant(value), field}
      : std::vector<core::TypedExprPtr>{field, constant(value)};
  return std::make_shared<core::CallTypedExpr>(BOOLEAN(), std::move(args), "" + op);
}

class UuidParquetFilterTest : public ::testing::TestWithParam<std::tuple<std::string, bool>> {};

TEST_P(UuidParquetFilterTest, PhysicalBytePredicatesMatchUnsignedUuidComparisonsAndStatistics) {
  auto [op, reversed] = GetParam();
  const std::array<int128_t, 4> values{
      0, int128_t{0x8000000000000000ULL},
      static_cast<int128_t>(uint128_t{1} << 127), -1};
  for (auto bound : values) {
    auto spec = std::make_shared<common::ScanSpec>("root");
    ASSERT_EQ(decomposeFilterToScanSpec(comparison(op, bound, reversed), spec,
        ROW({"id"}, {UUID()}), true), 1);
    const auto* filter = spec->childByName("id")->filter();
    ASSERT_NE(filter, nullptr);
    EXPECT_FALSE(filter->testNull());
    for (auto value : values) {
      auto wire = bytes(value);
      auto boundary = bytes(bound);
      int order = std::memcmp(wire.data(), boundary.data(), 16);
      if (reversed) order = -order;
      bool expected = op == "eq" ? order == 0 : op == "neq" ? order != 0
          : op == "lt" ? order < 0 : op == "lte" ? order <= 0
          : op == "gt" ? order > 0 : order >= 0;
      EXPECT_EQ(filter->testBytes(wire.data(), wire.size()), expected);
      EXPECT_EQ(filter->testBytesRange(wire, wire, false), expected);
    }
  }
}

INSTANTIATE_TEST_SUITE_P(Comparisons, UuidParquetFilterTest,
    ::testing::Combine(::testing::Values("eq", "neq", "lt", "lte", "gt", "gte"), ::testing::Bool()));

TEST(UuidParquetFilterBoundaryTest, LogicalHugeintReadersCannotReceiveUuidByteFilters) {
  auto spec = std::make_shared<common::ScanSpec>("root");
  EXPECT_EQ(decomposeFilterToScanSpec(comparison("eq", 0), spec, ROW({"id"}, {UUID()})), 0);
  EXPECT_EQ(decomposeFilterToScanSpec(comparison("eq", 0), spec, ROW({"id"}, {HUGEINT()}), true), 0);
}

TEST(UuidParquetFilterBoundaryTest, InListsAndBetweenUseCanonicalWireOrderAndRejectNulls) {
  auto spec = std::make_shared<common::ScanSpec>("root");
  auto in = std::make_shared<core::CallTypedExpr>(BOOLEAN(),
      std::vector<core::TypedExprPtr>{comparison("eq", 0), comparison("eq", -1)}, "or");
  ASSERT_EQ(decomposeFilterToScanSpec(in, spec, ROW({"id"}, {UUID()}), true), 1);
  auto* filter = spec->childByName("id")->filter();
  auto zero = bytes(0), all = bytes(-1), middle = bytes(1);
  EXPECT_TRUE(filter->testBytes(zero.data(), zero.size()));
  EXPECT_TRUE(filter->testBytes(all.data(), all.size()));
  EXPECT_FALSE(filter->testBytes(middle.data(), middle.size()));
  EXPECT_FALSE(filter->testNull());
  auto between = std::make_shared<core::CallTypedExpr>(BOOLEAN(),
      std::vector<core::TypedExprPtr>{std::make_shared<core::FieldAccessTypedExpr>(UUID(), "id"),
          constant(0), constant(-1)}, "between");
  ASSERT_EQ(decomposeFilterToScanSpec(between, spec, ROW({"id"}, {UUID()}), true), 1);
  filter = spec->childByName("id")->filter();
  EXPECT_TRUE(filter->testBytes(middle.data(), middle.size()));
  EXPECT_TRUE(filter->testBytesRange(zero, all, false));
  EXPECT_FALSE(filter->testNull());
}

TEST(FilterDecomposerBoundaryTest, TypedSmallIntegersRealsAndBinaryUseTheirActualVariantStorage) {
  const std::vector<std::pair<TypePtr, variant>> values{
      {TINYINT(), variant(int8_t{3})}, {SMALLINT(), variant(int16_t{3})},
      {INTEGER(), variant(int32_t{3})}, {BIGINT(), variant(int64_t{3})},
      {REAL(), variant(3.0f)}, {DOUBLE(), variant(3.0)},
      {VARBINARY(), variant::binary(std::string("a\0b", 3))}, {VARCHAR(), variant("abc")}};
  for (const auto& [type, value] : values) {
    auto spec = std::make_shared<common::ScanSpec>("root");
    auto call = std::make_shared<core::CallTypedExpr>(BOOLEAN(), std::vector<core::TypedExprPtr>{
        std::make_shared<core::FieldAccessTypedExpr>(type, "value"),
        std::make_shared<core::ConstantTypedExpr>(type, value)}, "eq");
    ASSERT_EQ(decomposeFilterToScanSpec(call, spec, ROW({"value"}, {type})), 1) << type->toString();
    auto* filter = spec->childByName("value")->filter();
    EXPECT_FALSE(filter->testNull());
    if (type->isReal() || type->isDouble()) EXPECT_TRUE(filter->testDouble(3));
    else if (type->isVarchar()) EXPECT_TRUE(filter->testBytes("abc", 3));
    else if (type->isVarbinary()) EXPECT_TRUE(filter->testBytes("a\0b", 3));
    else EXPECT_TRUE(filter->testInt64(3));
  }
}

TEST(FilterDecomposerBoundaryTest, ImpossibleStrictIntegerBoundsRejectAllRowsWithoutOverflow) {
  for (const auto& [op, value] : std::vector<std::pair<std::string, int64_t>>{
      {"lt", std::numeric_limits<int64_t>::min()}, {"gt", std::numeric_limits<int64_t>::max()}}) {
    auto spec = std::make_shared<common::ScanSpec>("root");
    auto call = std::make_shared<core::CallTypedExpr>(BOOLEAN(), std::vector<core::TypedExprPtr>{
        std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "value"),
        std::make_shared<core::ConstantTypedExpr>(BIGINT(), variant(value))}, "" + op);
    ASSERT_EQ(decomposeFilterToScanSpec(call, spec, ROW({"value"}, {BIGINT()})), 1);
    auto* filter = spec->childByName("value")->filter();
    EXPECT_FALSE(filter->testInt64(std::numeric_limits<int64_t>::min()));
    EXPECT_FALSE(filter->testInt64(std::numeric_limits<int64_t>::max()));
    EXPECT_FALSE(filter->testNull());
  }
}

TEST(FilterDecomposerBoundaryTest, NullPredicatesRequireAColumnInThePhysicalReaderSchema) {
  auto field = std::make_shared<core::FieldAccessTypedExpr>(VARCHAR(), "future_field");
  auto isNull = std::make_shared<core::CallTypedExpr>(BOOLEAN(),
      std::vector<core::TypedExprPtr>{field}, "is_null");
  auto isNotNull = std::make_shared<core::CallTypedExpr>(BOOLEAN(),
      std::vector<core::TypedExprPtr>{isNull}, "not");
  for (const auto& predicate : {isNull, isNotNull}) {
    auto missing = std::make_shared<common::ScanSpec>("root");
    EXPECT_EQ(decomposeFilterToScanSpec(predicate, missing, ROW({"id"}, {BIGINT()})), 0);
    EXPECT_TRUE(missing->children().empty());

    auto present = std::make_shared<common::ScanSpec>("root");
    ASSERT_EQ(decomposeFilterToScanSpec(predicate, present,
        ROW({"future_field"}, {VARCHAR()})), 1);
    const auto* filter = present->childByName("future_field")->filter();
    ASSERT_NE(filter, nullptr);
    EXPECT_EQ(filter->testNull(), predicate == isNull);
  }
}

}  // namespace
}  // namespace pangrova::velox::test
