#include <gtest/gtest.h>

#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include "result/FlightResultServer.h"

namespace pangrova::result::test {
namespace {

protocol::v2::LogicalType logicalVector(uint32_t dimension, bool doublePrecision = false) {
  protocol::v2::LogicalType logical;
  logical.set_canonical_sql("VECTOR(" + std::to_string(dimension) + (doublePrecision ? ",DOUBLE)" : ")"));
  auto* list = logical.mutable_substrait_type()->mutable_list();
  list->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  if (doublePrecision) list->mutable_type()->mutable_fp64()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  else list->mutable_type()->mutable_fp32()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  auto* size = logical.add_constraints();
  size->set_name("dimension");
  size->set_integer_value(dimension);
  auto* precision = logical.add_constraints();
  precision->set_name("element_type");
  precision->set_string_value(doublePrecision ? "FLOAT64" : "FLOAT32");
  return logical;
}

std::shared_ptr<arrow::DataType> physicalVector(bool doublePrecision = false) {
  return arrow::list(arrow::field("element", doublePrecision ? arrow::float64() : arrow::float32(), false));
}

TEST(FlightResultColumnTypeTest, AnnotatesOnlyExactIndexedVectorColumns) {
  auto storage = arrow::schema({
      arrow::field("first_embedding", physicalVector()),
      arrow::field("ordinary_array", arrow::list(arrow::float32())),
      arrow::field("second_embedding", physicalVector())});

  auto result = detail::withResultColumnTypes(storage, {{0, 3, logicalVector(3)}, {2, 5, logicalVector(5)}});

  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_EQ((*result)->field(0)->metadata()->Get("ARROW:extension:name").ValueOrDie(),
            "pangrova.vector");
  EXPECT_EQ((*result)->field(0)->metadata()->Get("ARROW:extension:metadata").ValueOrDie(),
            "3");
  EXPECT_EQ((*result)->field(1)->metadata(), nullptr);
  EXPECT_EQ((*result)->field(2)->metadata()->Get("ARROW:extension:metadata").ValueOrDie(),
            "5");
}

TEST(FlightResultColumnTypeTest, RejectsSchemaAndPhysicalTypeMismatches) {
  auto storage = arrow::schema({
      arrow::field("embedding", physicalVector()),
      arrow::field("ordinary", physicalVector(true))});

  EXPECT_FALSE(detail::withResultColumnTypes(storage, {{2, 3, logicalVector(3)}}).ok());
  EXPECT_FALSE(detail::withResultColumnTypes(storage, {{1, 3, logicalVector(3)}}).ok());
  EXPECT_FALSE(detail::withResultColumnTypes(storage, {{0, 0, logicalVector(3)}}).ok());
  EXPECT_FALSE(detail::withResultColumnTypes(storage, {{0, 3, logicalVector(3)}, {0, 5, logicalVector(5)}}).ok());
  EXPECT_FALSE(detail::withResultColumnTypes(storage, {{0, 4, logicalVector(3)}}).ok());
}

TEST(FlightResultColumnTypeTest, DoublePrecisionRetainsPhysicalBuffersAndRequiredChildSchema) {
  auto storage = arrow::schema({arrow::field("embedding", physicalVector(true))});
  auto result = detail::withResultColumnTypes(storage, {{0, 3, logicalVector(3, true)}});
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_EQ((*result)->field(0)->type(), storage->field(0)->type());
  EXPECT_EQ((*result)->field(0)->metadata()->Get("ARROW:extension:metadata").ValueOrDie(), "3");
  auto nullableChild = arrow::schema({arrow::field("embedding", arrow::list(arrow::float64()))});
  EXPECT_FALSE(detail::withResultColumnTypes(nullableChild, {{0, 3, logicalVector(3, true)}}).ok());
}

}  // namespace
}  // namespace pangrova::result::test
