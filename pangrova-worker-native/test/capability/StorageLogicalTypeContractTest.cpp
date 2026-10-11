#include "capability/LogicalTypeContract.h"

#include <array>
#include <cstdint>
#include <stdexcept>
#include <string>

#include <arrow/type.h>
#include <arrow/array.h>
#include <arrow/builder.h>
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

TEST(StorageLogicalTypeContractTest, ResultVectorAcceptsBothListLayoutsAndPreservesActualDoubleValues) {
  auto logical = vectorType();
  const auto child = arrow::field("element", arrow::float64(), true);
  EXPECT_EQ(requireStorageVectorDimension(logical), 3U);
  EXPECT_NO_THROW(validateVectorResultField(logical, *arrow::field("result", arrow::list(child), false)));
  EXPECT_NO_THROW(validateVectorResultField(logical, *arrow::field("result", arrow::fixed_size_list(child, 3), false)));
  EXPECT_THROW(validateVectorResultField(logical, *arrow::field("result", arrow::fixed_size_list(child, 2), false)),
      std::invalid_argument);
  EXPECT_THROW(validateVectorResultField(logical, *arrow::field("result",
      arrow::list(arrow::field("element", arrow::float32(), true)), false)), std::invalid_argument);
  auto values = std::make_shared<arrow::DoubleBuilder>();
  arrow::ListBuilder rows(arrow::default_memory_pool(), values, arrow::list(child));
  ASSERT_TRUE(rows.Append().ok());
  ASSERT_TRUE(values->AppendValues({1.0000000000000002, -123456789.12345679, 0.25}).ok());
  auto array = rows.Finish().ValueOrDie();
  EXPECT_NO_THROW(validateVectorResultArray(logical, *array));
  const auto& decoded = static_cast<const arrow::DoubleArray&>(*static_cast<const arrow::ListArray&>(*array).values());
  EXPECT_DOUBLE_EQ(decoded.Value(0), 1.0000000000000002);
  EXPECT_DOUBLE_EQ(decoded.Value(1), -123456789.12345679);
}

TEST(StorageLogicalTypeContractTest, ResultValuesValidateVisibleLengthsAndBothNullabilityLevels) {
  auto logical = vectorType();
  auto values = std::make_shared<arrow::DoubleBuilder>();
  arrow::ListBuilder rows(arrow::default_memory_pool(), values,
      arrow::list(arrow::field("element", arrow::float64(), true)));
  ASSERT_TRUE(rows.Append().ok());
  ASSERT_TRUE(values->AppendValues({1.0, 2.0}).ok());
  auto wrongLength = rows.Finish().ValueOrDie();
  EXPECT_THROW(validateVectorResultArray(logical, *wrongLength), std::invalid_argument);
  ASSERT_TRUE(rows.AppendNull().ok());
  auto nullRow = rows.Finish().ValueOrDie();
  EXPECT_THROW(validateVectorResultArray(logical, *nullRow), std::invalid_argument);
  logical.mutable_substrait_type()->mutable_list()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  EXPECT_NO_THROW(validateVectorResultArray(logical, *nullRow));
  ASSERT_TRUE(rows.Append().ok());
  ASSERT_TRUE(values->Append(1.0).ok());
  ASSERT_TRUE(values->AppendNull().ok());
  ASSERT_TRUE(values->Append(3.0).ok());
  auto nullElement = rows.Finish().ValueOrDie();
  EXPECT_NO_THROW(validateVectorResultArray(logical, *nullElement));
  logical.mutable_substrait_type()->mutable_list()->mutable_type()->mutable_fp64()
      ->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  EXPECT_THROW(validateVectorResultArray(logical, *nullElement), std::invalid_argument);
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

struct TimestampStorageCase {
  int32_t precision;
  arrow::TimeUnit::type unit;
};

class TimestampStoragePrecisionTest : public testing::TestWithParam<TimestampStorageCase> {};

TEST_P(TimestampStoragePrecisionTest, ExactSdkUnitNullabilityAndUnzonedDeclarationAreRequired) {
  const auto& parameter = GetParam();
  protocol::v2::LogicalType logical;
  logical.set_canonical_sql("TIMESTAMP(" + std::to_string(parameter.precision) + ')');
  auto* declaration = logical.mutable_substrait_type()->mutable_precision_timestamp();
  declaration->set_precision(parameter.precision);
  const std::array units{arrow::TimeUnit::SECOND, arrow::TimeUnit::MILLI,
      arrow::TimeUnit::MICRO, arrow::TimeUnit::NANO};
  for (const bool nullable : std::array{false, true}) {
    SCOPED_TRACE(nullable ? "nullable timestamp" : "required timestamp");
    declaration->set_nullability(nullable ? substrait::Type::NULLABILITY_NULLABLE
                                         : substrait::Type::NULLABILITY_REQUIRED);
    const auto physical = arrow::field("created", arrow::timestamp(parameter.unit), nullable);
    EXPECT_NO_THROW(validateStorageLogicalType(logical, *physical));
    EXPECT_THROW(validateStorageLogicalType(logical, *physical->WithNullable(!nullable)),
        std::invalid_argument);
    EXPECT_THROW(validateStorageLogicalType(logical, *arrow::field("created",
        arrow::timestamp(parameter.unit, "UTC"), nullable)), std::invalid_argument);
    for (const auto unit : units) {
      if (unit == parameter.unit) continue;
      SCOPED_TRACE(static_cast<int>(unit));
      EXPECT_THROW(validateStorageLogicalType(logical,
          *arrow::field("created", arrow::timestamp(unit), nullable)), std::invalid_argument);
    }
  }
}

// These expectations are the actual Paimon 1.4.1 ArrowFieldTypeConversion
// timestamp precision boundaries, not the separate TIME storage unit rule.
INSTANTIATE_TEST_SUITE_P(PaimonSdkStorageUnits, TimestampStoragePrecisionTest,
    testing::Values(TimestampStorageCase{0, arrow::TimeUnit::SECOND},
        TimestampStorageCase{1, arrow::TimeUnit::MILLI},
        TimestampStorageCase{3, arrow::TimeUnit::MILLI},
        TimestampStorageCase{4, arrow::TimeUnit::MICRO},
        TimestampStorageCase{6, arrow::TimeUnit::MICRO},
        TimestampStorageCase{7, arrow::TimeUnit::NANO},
        TimestampStorageCase{9, arrow::TimeUnit::NANO}),
    [](const testing::TestParamInfo<TimestampStorageCase>& information) {
      return "Precision" + std::to_string(information.param.precision);
    });

TEST(StorageLogicalTypeContractTest, PrecisionZeroTimeRemainsMillisecondUnlikeTimestamp) {
  protocol::v2::LogicalType logical;
  logical.set_canonical_sql("TIME(0)");
  auto* declaration = logical.mutable_substrait_type()->mutable_precision_time();
  declaration->set_precision(0);
  declaration->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  EXPECT_NO_THROW(validateStorageLogicalType(logical,
      *arrow::field("clock", arrow::time32(arrow::TimeUnit::MILLI), false)));
  EXPECT_THROW(validateStorageLogicalType(logical,
      *arrow::field("clock", arrow::time32(arrow::TimeUnit::SECOND), false)), std::invalid_argument);
}

TEST(StorageLogicalTypeContractTest, PrecisionZeroZonedTimestampRequiresSecondAndAZone) {
  protocol::v2::LogicalType logical;
  logical.set_canonical_sql("TIMESTAMP_TZ(0)");
  auto* declaration = logical.mutable_substrait_type()->mutable_precision_timestamp_tz();
  declaration->set_precision(0);
  declaration->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  EXPECT_NO_THROW(validateStorageLogicalType(logical,
      *arrow::field("created", arrow::timestamp(arrow::TimeUnit::SECOND, "Asia/Shanghai"), false)));
  EXPECT_THROW(validateStorageLogicalType(logical,
      *arrow::field("created", arrow::timestamp(arrow::TimeUnit::MILLI, "Asia/Shanghai"), false)),
      std::invalid_argument);
  EXPECT_THROW(validateStorageLogicalType(logical,
      *arrow::field("created", arrow::timestamp(arrow::TimeUnit::SECOND), false)), std::invalid_argument);
  EXPECT_THROW(validateStorageLogicalType(logical,
      *arrow::field("created", arrow::timestamp(arrow::TimeUnit::SECOND, "Asia/Shanghai"), true)),
      std::invalid_argument);
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
