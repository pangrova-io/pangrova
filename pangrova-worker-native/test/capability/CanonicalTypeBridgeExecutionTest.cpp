#include <gtest/gtest.h>

#include "capability/CanonicalPhysicalTypes.h"
#include "capability/FunctionTypeFormulaOps.h"
#include "google/protobuf/descriptor.h"
#include "velox/type/Type.h"

#include <limits>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace pangrova::capability {
namespace {

substrait::Type primitive(const std::string& name) {
  substrait::Type result;
  const auto* field = result.GetDescriptor()->FindFieldByName(name);
  if (!field || field->cpp_type() != google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
    throw std::runtime_error("Test type kind is absent from actual canonical descriptor");
  }
  auto* value = result.GetReflection()->MutableMessage(&result, field);
  const auto* nullable = value->GetDescriptor()->FindFieldByName("nullability");
  value->GetReflection()->SetEnumValue(value, nullable, substrait::Type::NULLABILITY_REQUIRED);
  return result;
}

TEST(CanonicalTypeBridgeExecutionTest, PrimitiveFactoriesMatchIndependentActualSdkTypes) {
  using namespace facebook::velox;
  const std::vector<std::pair<std::string, TypePtr>> expected{
      {"bool", BOOLEAN()}, {"i8", TINYINT()}, {"i16", SMALLINT()}, {"i32", INTEGER()},
      {"i64", BIGINT()}, {"fp32", REAL()}, {"fp64", DOUBLE()}, {"string", VARCHAR()},
      {"binary", VARBINARY()}, {"date", DATE()}, {"precision_timestamp", TIMESTAMP()},
      {"precision_timestamp_tz", TIMESTAMP()}, {"interval_year", INTERVAL_YEAR_MONTH()},
      {"interval_day", INTERVAL_DAY_TIME()}};
  for (const auto& [kind, type] : expected) {
    SCOPED_TRACE(kind);
    const auto actual = canonicalPhysicalType(primitive(kind));
    EXPECT_TRUE(actual->equivalent(*type));
    EXPECT_EQ(actual->isOrderable(), type->isOrderable());
    EXPECT_EQ(actual->isComparable(), type->isComparable());
    EXPECT_EQ(actual->isUnknown(), type->isUnknown());
  }
  for (const auto length : {1, 27, std::numeric_limits<int32_t>::max()}) {
    auto varchar = primitive("varchar");
    varchar.mutable_varchar()->set_length(length);
    auto character = primitive("fixed_char");
    character.mutable_fixed_char()->set_length(length);
    EXPECT_TRUE(canonicalPhysicalType(varchar)->equivalent(*VARCHAR()));
    EXPECT_TRUE(canonicalPhysicalType(character)->equivalent(*VARCHAR()));
  }
}

TEST(CanonicalTypeBridgeExecutionTest, CompleteDecimalDomainKeepsRuntimeEquivalenceAndExactParameters) {
  for (int precision = 1; precision <= 38; ++precision) {
    for (int scale = 0; scale <= precision; ++scale) {
      SCOPED_TRACE(std::to_string(precision) + "," + std::to_string(scale));
      const auto actual = canonicalDecimalType(precision, scale);
      const auto stock = facebook::velox::DECIMAL(precision, scale);
      EXPECT_TRUE(actual->equivalent(*stock));
      EXPECT_EQ(actual->kind(), stock->kind());
      ASSERT_EQ(actual->parameters().size(), 2);
      EXPECT_EQ(actual->parameters()[0].longLiteral, precision);
      EXPECT_EQ(actual->parameters()[1].longLiteral, scale);
    }
  }
  EXPECT_FALSE(canonicalDecimalType(12, 3)->equivalent(*canonicalDecimalType(18, 3)));
  EXPECT_EQ(canonicalDecimalType(12, 3)->kind(), canonicalDecimalType(18, 3)->kind());
  for (const auto precision : {-1, 0, 39, std::numeric_limits<int32_t>::max()}) {
    EXPECT_THROW(canonicalDecimalType(precision, 0), std::runtime_error);
  }
  for (const auto scale : {-1, 13, std::numeric_limits<int32_t>::max()}) {
    EXPECT_THROW(canonicalDecimalType(12, scale), std::runtime_error);
  }
}

TEST(CanonicalTypeBridgeExecutionTest, TimeAndNestedFactoriesMatchActualSdkTypesAndTraits) {
  using namespace facebook::velox;
  for (int precision = 0; precision <= 6; ++precision) {
    const auto expected = precision <= 3 ? TypePtr(TIME()) : TypePtr(TIME_MICRO_UTC());
    EXPECT_TRUE(canonicalTimeType(precision)->equivalent(*expected));
  }
  auto row = primitive("struct");
  *row.mutable_struct_()->add_types() = primitive("i64");
  *row.mutable_struct_()->add_types() = primitive("string");
  const auto expectedRow = ROW({"field_0", "field_1"}, {BIGINT(), VARCHAR()});
  EXPECT_TRUE(canonicalPhysicalType(row)->equivalent(*expectedRow));
  auto array = primitive("list");
  *array.mutable_list()->mutable_type() = row;
  EXPECT_TRUE(canonicalPhysicalType(array)->equivalent(*ARRAY(expectedRow)));
  auto map = primitive("map");
  *map.mutable_map()->mutable_key() = primitive("i64");
  *map.mutable_map()->mutable_value() = array;
  const auto actual = canonicalPhysicalType(map);
  EXPECT_TRUE(actual->equivalent(*MAP(BIGINT(), ARRAY(expectedRow))));
  EXPECT_FALSE(actual->isOrderable());
  EXPECT_TRUE(actual->isComparable());
  EXPECT_FALSE(actual->isUnknown());
  EXPECT_THROW(canonicalPhysicalType(primitive("list")), std::runtime_error);
  EXPECT_THROW(canonicalPhysicalType(primitive("map")), std::runtime_error);
}

TEST(CanonicalTypeBridgeExecutionTest, FormulaOperationsStayExactAndEvaluateConditionalOperands) {
  EXPECT_EQ(typeFormulaDivide(-7, 3), -2);
  EXPECT_EQ(typeFormulaRemainder(-7, 3), -1);
  EXPECT_THROW(typeFormulaAdd(std::numeric_limits<int64_t>::max(), 1), std::overflow_error);
  EXPECT_THROW(typeFormulaSubtract(std::numeric_limits<int64_t>::min(), 1), std::overflow_error);
  EXPECT_THROW(typeFormulaMultiply(std::numeric_limits<int64_t>::max(), 2), std::overflow_error);
  EXPECT_THROW(typeFormulaNegate(std::numeric_limits<int64_t>::min()), std::overflow_error);
  EXPECT_THROW(typeFormulaDivide(std::numeric_limits<int64_t>::min(), -1), std::overflow_error);
  EXPECT_THROW(typeFormulaRemainder(std::numeric_limits<int64_t>::min(), -1), std::overflow_error);
  EXPECT_THROW(typeFormulaConditional(0, typeFormulaDivide(1, 0), 7), std::overflow_error);
}

}  // namespace
}  // namespace pangrova::capability
