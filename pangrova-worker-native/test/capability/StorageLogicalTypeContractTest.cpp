#include "capability/LogicalTypeContract.h"

#include <arrow/type.h>
#include <gtest/gtest.h>

namespace pangrova::capability {
namespace {

protocol::v2::LogicalType vectorType() {
  protocol::v2::LogicalType logical;
  logical.set_canonical_sql("VECTOR(3,DOUBLE)");
  auto* list = logical.mutable_substrait_type()->mutable_list();
  list->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  list->mutable_type()->mutable_fp64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  auto* dimension = logical.add_constraints();
  dimension->set_name("dimension");
  dimension->set_integer_value(3);
  auto* element = logical.add_constraints();
  element->set_name("element_type");
  element->set_string_value("FLOAT64");
  return logical;
}

TEST(StorageLogicalTypeContractTest, ExactDoubleVectorSchemaAndEveryNullabilityAreRequired) {
  auto logical = vectorType();
  auto physical = arrow::field("embedding", arrow::fixed_size_list(arrow::field("element", arrow::float64(), true), 3), false);
  EXPECT_NO_THROW(validateStorageLogicalType(logical, *physical));
  EXPECT_THROW(validateStorageLogicalType(logical, *physical->WithNullable(true)), std::invalid_argument);
  EXPECT_THROW(validateStorageLogicalType(logical, *arrow::field("embedding",
      arrow::fixed_size_list(arrow::field("element", arrow::float64(), false), 3), false)), std::invalid_argument);
  EXPECT_THROW(validateStorageLogicalType(logical, *arrow::field("embedding",
      arrow::fixed_size_list(arrow::field("element", arrow::float32(), true), 3), false)), std::invalid_argument);
  EXPECT_THROW(validateStorageLogicalType(logical, *arrow::field("embedding",
      arrow::fixed_size_list(arrow::field("element", arrow::float64(), true), 4), false)), std::invalid_argument);
  logical.set_canonical_sql("VECTOR(3)");
  EXPECT_THROW(validateStorageLogicalType(logical, *physical), std::invalid_argument);
}

TEST(StorageLogicalTypeContractTest, NestedFieldNamesDimensionsAndDuplicateConstraintsCannotDrift) {
  const auto vector = vectorType();
  protocol::v2::LogicalType logical;
  logical.set_canonical_sql("ROW(\"quoted\"\"field\" VECTOR(3,DOUBLE))");
  auto* row = logical.mutable_substrait_type()->mutable_struct_();
  row->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  *row->add_types() = vector.substrait_type();
  auto* name = logical.add_constraints();
  name->set_name("field_name@0");
  name->set_string_value("quoted\"field");
  for (const auto& constraint : vector.constraints()) {
    auto* nested = logical.add_constraints();
    *nested = constraint;
    nested->set_name(constraint.name() + "@0");
  }
  const auto physical = arrow::field("records", arrow::struct_({arrow::field("quoted\"field",
      arrow::fixed_size_list(arrow::field("element", arrow::float64(), true), 3), false)}), true);
  EXPECT_NO_THROW(validateStorageLogicalType(logical, *physical));
  logical.mutable_constraints(0)->set_string_value("different");
  EXPECT_THROW(validateStorageLogicalType(logical, *physical), std::invalid_argument);
  logical.mutable_constraints(0)->set_string_value("quoted\"field");
  *logical.add_constraints() = logical.constraints(1);
  EXPECT_THROW(validateStorageLogicalType(logical, *physical), std::invalid_argument);
}

TEST(StorageLogicalTypeContractTest, DecimalScaleAndTimestampPrecisionAreNotPhysicalKindEquivalence) {
  protocol::v2::LogicalType decimal;
  decimal.set_canonical_sql("DECIMAL(38,17)");
  auto* declaration = decimal.mutable_substrait_type()->mutable_decimal();
  declaration->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  declaration->set_precision(38);
  declaration->set_scale(17);
  EXPECT_NO_THROW(validateStorageLogicalType(decimal, *arrow::field("amount", arrow::decimal128(38, 17), true)));
  EXPECT_THROW(validateStorageLogicalType(decimal, *arrow::field("amount", arrow::decimal128(38, 16), true)), std::invalid_argument);
  protocol::v2::LogicalType timestamp;
  timestamp.set_canonical_sql("TIMESTAMP(6)");
  timestamp.mutable_substrait_type()->mutable_precision_timestamp()->set_precision(6);
  timestamp.mutable_substrait_type()->mutable_precision_timestamp()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  EXPECT_NO_THROW(validateStorageLogicalType(timestamp, *arrow::field("created", arrow::timestamp(arrow::TimeUnit::MICRO), false)));
  EXPECT_THROW(validateStorageLogicalType(timestamp, *arrow::field("created", arrow::timestamp(arrow::TimeUnit::NANO), false)), std::invalid_argument);
  EXPECT_THROW(validateStorageLogicalType(timestamp, *arrow::field("created", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"), false)), std::invalid_argument);
}

TEST(StorageLogicalTypeContractTest, MapKeysRequireExplicitNullabilityAndUnknownVariationsAreRefused) {
  protocol::v2::LogicalType logical;
  logical.set_canonical_sql("MAP(VARCHAR(2147483647),BIGINT)");
  auto* map = logical.mutable_substrait_type()->mutable_map();
  map->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  map->mutable_key()->mutable_string()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  map->mutable_value()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  const auto physical = arrow::field("attributes", arrow::map(arrow::utf8(), arrow::int64()), true);
  EXPECT_NO_THROW(validateStorageLogicalType(logical, *physical));
  map->mutable_key()->mutable_string()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  EXPECT_THROW(validateStorageLogicalType(logical, *physical), std::invalid_argument);
  map->mutable_key()->mutable_string()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  map->mutable_value()->mutable_i64()->set_type_variation_reference(1);
  EXPECT_THROW(validateStorageLogicalType(logical, *physical), std::invalid_argument);
}

}  // namespace
}  // namespace pangrova::capability
