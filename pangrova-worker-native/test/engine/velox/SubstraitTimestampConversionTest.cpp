#include <gtest/gtest.h>
#include "CanonicalTestPlan.h"
#include "velox/expression/FunctionRegistrySnapshot.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "CanonicalOutputAssertions.h"
#include "CanonicalTaskExecution.h"
#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"
#include "velox/core/Expressions.h"
#include "velox/core/PlanNode.h"
#include "engine/velox/plan/VeloxPlanConverter.h"
#include "velox/type/Timestamp.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

namespace pangrova::velox::test {
namespace {

struct TimestampCase {
  int64_t value;
  int32_t precision;
  int64_t seconds;
  uint64_t nanos;
};

void setTimestampLiteral(
    substrait::Expression::Literal* literal,
    const TimestampCase& value,
    bool with_timezone) {
  if (with_timezone) {
    auto* timestamp = literal->mutable_precision_timestamp_tz();
    timestamp->set_value(value.value);
    timestamp->set_precision(value.precision);
  } else {
    auto* timestamp = literal->mutable_precision_timestamp();
    timestamp->set_value(value.value);
    timestamp->set_precision(value.precision);
  }
}

substrait::Rel timestampValues(
    const TimestampCase& value, bool with_timezone) {
  substrait::Rel rel;
  auto* read = rel.mutable_read();
  read->mutable_base_schema()->add_names("event_time");
  auto* type = read->mutable_base_schema()->mutable_struct_()->add_types();
  if (with_timezone) {
    type->mutable_precision_timestamp_tz()->set_precision(value.precision);
    type->mutable_precision_timestamp_tz()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  } else {
    type->mutable_precision_timestamp()->set_precision(value.precision);
    type->mutable_precision_timestamp()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  }
  setTimestampLiteral(
      read->mutable_virtual_table()->add_expressions()
          ->add_fields()->mutable_literal(),
      value, with_timezone);
  return rel;
}

std::vector<uint8_t> serializeTimestampPlan(const substrait::Rel& rel) {
  substrait::Plan plan;
  auto* root = plan.add_relations()->mutable_root();
  root->add_names("event_time");
  *root->mutable_input() = rel;
  const auto bytes = pangrova::test::canonicalTestPlan(plan).SerializeAsString();
  return {bytes.begin(), bytes.end()};
}

class SubstraitTimestampConversionTest
    : public ::testing::TestWithParam<TimestampCase> {};

TEST_P(SubstraitTimestampConversionTest, ValuesNormalizeSecondsAndNanos) {
  const auto& value = GetParam();
  for (bool with_timezone : {false, true}) {
    SCOPED_TRACE(with_timezone);
    const auto converter_registry =
        facebook::velox::exec::FunctionRegistrySnapshot::capture();
    VeloxPlanConverter converter(converter_registry);
    auto result = converter.convert(
        serializeTimestampPlan(timestampValues(value, with_timezone)), {});
    auto values = std::dynamic_pointer_cast<
        const facebook::velox::core::ValuesNode>(result);
    ASSERT_NE(values, nullptr);
    ASSERT_EQ(values->values().size(), 1);
    const auto timestamp = values->values().front()->childAt(0)
        ->asFlatVector<facebook::velox::Timestamp>()->valueAt(0);
    EXPECT_EQ(timestamp.getSeconds(), value.seconds);
    EXPECT_EQ(timestamp.getNanos(), value.nanos);
  }
}

TEST_P(SubstraitTimestampConversionTest, ProjectionUsesTheSameUnitContract) {
  pangrova::test::testMemoryManager();
  const auto& value = GetParam();
  for (bool with_timezone : {false, true}) {
    SCOPED_TRACE(with_timezone);
    substrait::Rel rel;
    auto* project = rel.mutable_project();
    *project->mutable_input() = timestampValues(value, with_timezone);
    setTimestampLiteral(project->add_expressions()->mutable_literal(),
                        value, with_timezone);
    project->mutable_common()->mutable_emit()->add_output_mapping(1);
    const auto converter_registry =
        facebook::velox::exec::FunctionRegistrySnapshot::capture();
    VeloxPlanConverter converter(converter_registry);
    auto result = converter.convert(serializeTimestampPlan(rel), {});
    auto projection = std::dynamic_pointer_cast<
        const facebook::velox::core::ProjectNode>(result);
    ASSERT_NE(projection, nullptr);
    ASSERT_EQ(projection->projections().size(), 1);
    expectCanonicalOutputField(converter, projection->outputType(), 0, "event_time");
    ASSERT_EQ(projection->sources().size(), 1);
    auto appended = std::dynamic_pointer_cast<
        const facebook::velox::core::ProjectNode>(projection->sources().front());
    ASSERT_NE(appended, nullptr);
    ASSERT_EQ(appended->projections().size(), 2);
    auto emitted = std::dynamic_pointer_cast<
        const facebook::velox::core::FieldAccessTypedExpr>(projection->projections().front());
    ASSERT_NE(emitted, nullptr);
    EXPECT_TRUE(emitted->inputs().empty());
    EXPECT_EQ(emitted->name(), appended->outputType()->nameOf(1));
    EXPECT_TRUE(emitted->type()->equivalent(*appended->outputType()->childAt(1)));
    auto constant = std::dynamic_pointer_cast<
        const facebook::velox::core::ConstantTypedExpr>(
        appended->projections().at(1));
    ASSERT_NE(constant, nullptr);
    const auto timestamp = constant->value().value<facebook::velox::Timestamp>();
    EXPECT_EQ(timestamp.getSeconds(), value.seconds);
    EXPECT_EQ(timestamp.getNanos(), value.nanos);
    const auto execution = executeCanonicalTask(result, converter_registry);
    ASSERT_EQ(execution.batches.size(), 1);
    ASSERT_EQ(execution.batches.front()->size(), 1);
    ASSERT_EQ(execution.batches.front()->childrenSize(), 1);
    facebook::velox::DecodedVector decoded(*execution.batches.front()->childAt(0));
    ASSERT_FALSE(decoded.isNullAt(0));
    const auto actual = decoded.valueAt<facebook::velox::Timestamp>(0);
    EXPECT_EQ(actual.getSeconds(), value.seconds);
    EXPECT_EQ(actual.getNanos(), value.nanos);
  }
}

INSTANTIATE_TEST_SUITE_P(
    PrecisionBoundaries,
    SubstraitTimestampConversionTest,
    ::testing::Values(
        TimestampCase{0, 6, 0, 0},
        TimestampCase{1735689600LL, 0, 1735689600, 0},
        TimestampCase{1735689600123LL, 3, 1735689600, 123000000},
        TimestampCase{1735689600123456LL, 6, 1735689600, 123456000},
        TimestampCase{1735689600123456789LL, 9, 1735689600, 123456789},
        TimestampCase{-1, 0, -1, 0},
        TimestampCase{-1, 3, -1, 999000000},
        TimestampCase{-1, 6, -1, 999999000},
        TimestampCase{-1, 9, -1, 999999999},
        TimestampCase{-1000001, 6, -2, 999999000},
        TimestampCase{std::numeric_limits<int64_t>::min(), 9,
                      -9223372037LL, 145224192},
        TimestampCase{std::numeric_limits<int64_t>::max(), 9,
                      9223372036LL, 854775807}));

TEST(SubstraitTimestampValidationTest, RejectsInvalidPrecision) {
  for (int32_t precision : {-1, 10, 13}) {
    for (bool with_timezone : {false, true}) {
      const auto converter_registry =
          facebook::velox::exec::FunctionRegistrySnapshot::capture();
      VeloxPlanConverter converter(converter_registry);
      EXPECT_THROW(converter.convert(serializeTimestampPlan(
          timestampValues(TimestampCase{1, precision, 0, 0}, with_timezone)), {}),
          std::runtime_error);
    }
  }
}

TEST(SubstraitTimestampValidationTest, RejectsNonTimestampValues) {
  auto rel = timestampValues(TimestampCase{1, 6, 0, 1000}, false);
  rel.mutable_read()->mutable_virtual_table()->mutable_expressions(0)
      ->mutable_fields(0)->mutable_literal()->set_i64(1);
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  EXPECT_THROW(converter.convert(serializeTimestampPlan(rel), {}),
               std::runtime_error);
}

TEST(SubstraitTimestampValidationTest, PreservesNullValues) {
  auto rel = timestampValues(TimestampCase{1, 6, 0, 1000}, false);
  const auto& type = rel.read().base_schema().struct_().types(0);
  auto* literal = rel.mutable_read()->mutable_virtual_table()
      ->mutable_expressions(0)->mutable_fields(0)->mutable_literal();
  *literal->mutable_null() = type;
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto result = converter.convert(serializeTimestampPlan(rel), {});
  auto values = std::dynamic_pointer_cast<
      const facebook::velox::core::ValuesNode>(result);
  ASSERT_NE(values, nullptr);
  EXPECT_TRUE(values->values().front()->childAt(0)->isNullAt(0));
}

TEST(SubstraitTimePrecisionTest, ValuesRescaleToNativeTimeStorage) {
  struct TimeCase {
    int32_t precision;
    int64_t value;
    int64_t expected;
  };
  for (const auto& value : {
           TimeCase{0, 45296, 45296000},
           TimeCase{3, 45296123, 45296123},
           TimeCase{6, 86399999999LL, 86399999999LL}}) {
    substrait::Rel rel;
    auto* read = rel.mutable_read();
    read->mutable_base_schema()->add_names("event_time");
    auto* declared = read->mutable_base_schema()->mutable_struct_()->add_types()->mutable_precision_time();
    declared->set_precision(value.precision);
    declared->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
    auto* literal = read->mutable_virtual_table()->add_expressions()
        ->add_fields()->mutable_literal()->mutable_precision_time();
    literal->set_precision(value.precision);
    literal->set_value(value.value);
    const auto converter_registry =
        facebook::velox::exec::FunctionRegistrySnapshot::capture();
    VeloxPlanConverter converter(converter_registry);
    auto result = converter.convert(serializeTimestampPlan(rel), {});
    auto values = std::dynamic_pointer_cast<
        const facebook::velox::core::ValuesNode>(result);
    ASSERT_NE(values, nullptr);
    EXPECT_EQ(values->values().front()->childAt(0)
                  ->asFlatVector<int64_t>()->valueAt(0), value.expected);
  }
}

TEST(SubstraitTimePrecisionTest, RejectsPrecisionBeyondNativeStorage) {
  substrait::Rel rel;
  auto* read = rel.mutable_read();
  read->mutable_base_schema()->add_names("event_time");
  auto* declared = read->mutable_base_schema()->mutable_struct_()->add_types()->mutable_precision_time();
  declared->set_precision(9);
  declared->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  auto* literal = read->mutable_virtual_table()->add_expressions()
      ->add_fields()->mutable_literal()->mutable_precision_time();
  literal->set_precision(9);
  literal->set_value(1);
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  EXPECT_THROW(converter.convert(serializeTimestampPlan(rel), {}),
               std::runtime_error);
}

}  // namespace
}  // namespace pangrova::velox::test
