#include <gtest/gtest.h>

#include "capabilities/capability/CanonicalPhysicalTypes.h"
#include "StructuralContractIdentities.h"

namespace pangrova::capability {
namespace {

substrait::Type integerType() {
  substrait::Type result;
  result.mutable_i64()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  return result;
}

TEST(CanonicalPhysicalTypesTest, DecimalAndTimeRejectUnrepresentableParameters) {
  for (const auto precision : {0, 39}) {
    EXPECT_THROW(canonicalDecimalType(precision, 0), std::runtime_error);
  }
  EXPECT_THROW(canonicalDecimalType(38, -1), std::runtime_error);
  EXPECT_THROW(canonicalDecimalType(12, 13), std::runtime_error);
  EXPECT_TRUE(canonicalDecimalType(38, 18)->equivalent(*facebook::velox::DECIMAL(38, 18)));
  EXPECT_THROW(canonicalTimeType(-1), std::runtime_error);
  EXPECT_THROW(canonicalTimeType(7), std::runtime_error);
  EXPECT_TRUE(canonicalTimeType(3)->equivalent(*facebook::velox::TIME()));
  EXPECT_TRUE(canonicalTimeType(6)->equivalent(*facebook::velox::TIME_MICRO_UTC()));
}

TEST(CanonicalPhysicalTypesTest, NestedSchemaUsesCompleteDfsNames) {
  substrait::NamedStruct schema;
  auto* nested = schema.mutable_struct_()->add_types()->mutable_list()->mutable_type()->mutable_struct_();
  *nested->add_types() = integerType();
  schema.add_names("items");
  schema.add_names("id");
  const auto physical = canonicalPhysicalSchema(schema);
  EXPECT_EQ(physical->nameOf(0), "items");
  const auto child = std::dynamic_pointer_cast<const facebook::velox::RowType>(physical->childAt(0)->childAt(0));
  ASSERT_NE(child, nullptr);
  EXPECT_EQ(child->nameOf(0), "id");
  auto missing = schema;
  missing.mutable_names()->RemoveLast();
  EXPECT_THROW(canonicalPhysicalSchema(missing), std::runtime_error);
  schema.add_names("unused");
  EXPECT_THROW(canonicalPhysicalSchema(schema), std::runtime_error);
}

TEST(CanonicalPhysicalTypesTest, BoundedCharactersRequirePositiveDeclaredLengths) {
  substrait::Type varchar;
  varchar.mutable_varchar()->set_length(27);
  EXPECT_TRUE(canonicalPhysicalType(varchar)->equivalent(*facebook::velox::VARCHAR()));
  substrait::Type character;
  character.mutable_fixed_char()->set_length(7);
  EXPECT_TRUE(canonicalPhysicalType(character)->equivalent(*facebook::velox::VARCHAR()));
  varchar.mutable_varchar()->set_length(0);
  character.mutable_fixed_char()->set_length(-1);
  EXPECT_THROW(canonicalPhysicalType(varchar), std::runtime_error);
  EXPECT_THROW(canonicalPhysicalType(character), std::runtime_error);
}

TEST(CanonicalPhysicalTypesTest, PositionalRowsRemainValidWithoutNamedStruct) {
  substrait::Type type;
  *type.mutable_struct_()->add_types() = integerType();
  const auto row = std::dynamic_pointer_cast<const facebook::velox::RowType>(canonicalPhysicalType(type));
  ASSERT_NE(row, nullptr);
  EXPECT_EQ(row->nameOf(0), "field_0");
  EXPECT_TRUE(row->childAt(0)->equivalent(*facebook::velox::BIGINT()));
  EXPECT_THROW(canonicalPhysicalType(substrait::Type()), std::runtime_error);
  EXPECT_THROW(canonicalPhysicalSchema(substrait::NamedStruct()), std::runtime_error);
}

TEST(CanonicalPhysicalTypesTest, StructuralReferenceIsGeneratedFromTheCanonicalDeclaration) {
  EXPECT_STREQ(generated::pangrova_table_unnest.id, "pangrova.table.unnest");
  EXPECT_STREQ(generated::pangrova_table_unnest.version, "1.0.0");
  const std::array<uint8_t, 32> expected = {
      0xd3, 0xb0, 0x4e, 0xe2, 0x0b, 0x0a, 0xc3, 0x64,
      0xc0, 0x40, 0xe3, 0xe9, 0xa7, 0x75, 0x22, 0x1c,
      0xb5, 0xe6, 0x12, 0x6e, 0x63, 0x91, 0xd3, 0x9e,
      0x74, 0x9d, 0x51, 0xba, 0x37, 0x2c, 0xd1, 0xc6};
  EXPECT_EQ(generated::pangrova_table_unnest.signature, expected);
}

}  // namespace
}  // namespace pangrova::capability
