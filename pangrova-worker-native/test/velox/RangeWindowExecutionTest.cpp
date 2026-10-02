// Canonical RANGE plans executed by the actual task; not distributed SQL evidence.
#include <gtest/gtest.h>

#include <limits>

#include "CanonicalTaskExecution.h"
#include "substrait/plan.pb.h"
#include "velox/plan/VeloxPlanConverter.h"
#include "velox/vector/DecodedVector.h"

namespace pangrova::velox::test {
namespace {
using namespace facebook::velox;
using Cell = std::optional<int64_t>;
using Rows = std::vector<std::vector<Cell>>;

struct RangeOptions {
  bool descending{false};
  bool nullsFirst{false};
  bool decimal{false};
  int64_t offset{1};
};

void field(substrait::Expression* expression, int ordinal) {
  expression->mutable_selection()->mutable_root_reference();
  expression->mutable_selection()->mutable_direct_reference()->mutable_struct_field()->set_field(ordinal);
}

void decimalValue(substrait::Expression_Literal* literal, int64_t value) {
  auto* decimal = literal->mutable_decimal();
  decimal->set_precision(12);
  decimal->set_scale(2);
  std::string bytes(16, value < 0 ? '\xff' : '\0');
  const auto bits = static_cast<uint64_t>(value);
  for (int ordinal = 0; ordinal < 8; ++ordinal) bytes[ordinal] = static_cast<char>(bits >> (ordinal * 8));
  decimal->set_value(std::move(bytes));
}

substrait::Plan rangePlan(const std::vector<Cell>& keys, RangeOptions options = {}) {
  substrait::Plan plan;
  auto* declaration = plan.add_extensions()->mutable_extension_function();
  declaration->set_function_anchor(0);
  declaration->set_name("sum");
  auto* root = plan.add_relations()->mutable_root();
  root->add_names("key");
  root->add_names("sum_result");
  auto* project = root->mutable_input()->mutable_project();
  auto* read = project->mutable_input()->mutable_read();
  auto* schema = read->mutable_base_schema();
  schema->add_names("key");
  schema->add_names("payload");
  auto* keyType = schema->mutable_struct_()->add_types();
  if (options.decimal) {
    keyType->mutable_decimal()->set_precision(12);
    keyType->mutable_decimal()->set_scale(2);
    keyType->mutable_decimal()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  } else keyType->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  schema->mutable_struct_()->add_types()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  for (size_t ordinal = 0; ordinal < keys.size(); ++ordinal) {
    auto* row = read->mutable_virtual_table()->add_expressions();
    auto* key = row->add_fields()->mutable_literal();
    if (!keys[ordinal]) key->mutable_null()->CopyFrom(*keyType);
    else {
      if (options.decimal) decimalValue(key, *keys[ordinal]);
      else key->set_i64(*keys[ordinal]);
      key->set_nullable(true);
    }
    row->add_fields()->mutable_literal()->set_i64((ordinal + 1) * 10);
  }
  auto* window = project->add_expressions()->mutable_window_function();
  window->set_function_reference(0);
  window->set_invocation(substrait::AggregateFunction::AGGREGATION_INVOCATION_ALL);
  window->set_phase(substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT);
  window->set_bounds_type(substrait::Expression_WindowFunction::BOUNDS_TYPE_RANGE);
  window->mutable_output_type()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  field(window->add_arguments()->mutable_value(), 1);
  auto* sort = window->add_sorts();
  field(sort->mutable_expr(), 0);
  if (options.descending) sort->set_direction(options.nullsFirst
      ? substrait::SortField::SORT_DIRECTION_DESC_NULLS_FIRST : substrait::SortField::SORT_DIRECTION_DESC_NULLS_LAST);
  else sort->set_direction(options.nullsFirst
      ? substrait::SortField::SORT_DIRECTION_ASC_NULLS_FIRST : substrait::SortField::SORT_DIRECTION_ASC_NULLS_LAST);
  window->mutable_lower_bound()->mutable_preceding()->set_offset(options.offset);
  window->mutable_upper_bound()->mutable_following()->set_offset(options.offset);
  project->mutable_common()->mutable_emit()->add_output_mapping(0);
  project->mutable_common()->mutable_emit()->add_output_mapping(2);
  return plan;
}

core::PlanNodePtr convert(const substrait::Plan& plan) {
  pangrova::test::detail::ensureRuntimeInitialized();
  VeloxPlanConverter converter;
  const auto encoded = plan.SerializeAsString();
  return converter.convert({encoded.begin(), encoded.end()}, {});
}

Rows execute(const substrait::Plan& plan) {
  const auto output = executeCanonicalTask(convert(plan));
  Rows rows;
  for (const auto& batch : output.batches) {
    DecodedVector keys(*batch->childAt(0));
    DecodedVector sums(*batch->childAt(1));
    for (vector_size_t row = 0; row < batch->size(); ++row) {
      rows.push_back({keys.isNullAt(row) ? Cell{} : Cell{keys.valueAt<int64_t>(row)},
          sums.isNullAt(row) ? Cell{} : Cell{sums.valueAt<int64_t>(row)}});
    }
  }
  return rows;
}

TEST(RangeWindowExecutionTest, RangeIncludesValuePeersNotNeighborRowsAndRetainsNullPeer) {
  EXPECT_EQ(execute(rangePlan({1, 1, 3, 5, std::nullopt})),
      (Rows{{1, 30}, {1, 30}, {3, 30}, {5, 40}, {std::nullopt, 50}}));
}

TEST(RangeWindowExecutionTest, DescendingNullsFirstUsesReversedArithmeticAndActualOrdering) {
  EXPECT_EQ(execute(rangePlan({1, 1, 3, 5, std::nullopt}, {true, true})),
      (Rows{{std::nullopt, 50}, {5, 40}, {3, 30}, {1, 30}, {1, 30}}));
}

TEST(RangeWindowExecutionTest, MaximumIntegerKeysUseAnExactExpandedComparisonDomain) {
  const auto maximum = std::numeric_limits<int64_t>::max();
  EXPECT_EQ(execute(rangePlan({maximum - 1, maximum})), (Rows{{maximum - 1, 30}, {maximum, 30}}));
  const auto minimum = std::numeric_limits<int64_t>::min();
  EXPECT_EQ(execute(rangePlan({minimum, 0, maximum}, {false, false, false, maximum})),
      (Rows{{minimum, 10}, {0, 50}, {maximum, 50}}));
}

TEST(RangeWindowExecutionTest, DecimalFractionalDigitsRemainExactInRangeComparison) {
  EXPECT_EQ(execute(rangePlan({123, 124, 250, std::nullopt}, {false, false, true})),
      (Rows{{123, 30}, {124, 30}, {250, 30}, {std::nullopt, 40}}));
}

TEST(RangeWindowExecutionTest, InvalidOffsetsAndMissingOrderingFailDuringConversion) {
  EXPECT_THROW(convert(rangePlan({1, 2}, {false, false, false, 0})), std::runtime_error);
  EXPECT_THROW(convert(rangePlan({1, 2}, {false, false, false, -1})), std::runtime_error);
  auto plan = rangePlan({1, 2});
  auto* window = plan.mutable_relations(0)->mutable_root()->mutable_input()->mutable_project()
      ->mutable_expressions(0)->mutable_window_function();
  window->clear_sorts();
  EXPECT_THROW(convert(plan), std::runtime_error);
}

TEST(RangeWindowExecutionTest, UnknownInvocationPhaseAndBoundsCannotFallBackToAllRows) {
  auto plan = rangePlan({1, 2});
  auto* window = plan.mutable_relations(0)->mutable_root()->mutable_input()->mutable_project()
      ->mutable_expressions(0)->mutable_window_function();
  window->set_invocation(static_cast<substrait::AggregateFunction::AggregationInvocation>(99));
  EXPECT_THROW(convert(plan), std::runtime_error);
  window->set_invocation(substrait::AggregateFunction::AGGREGATION_INVOCATION_ALL);
  window->set_phase(substrait::AGGREGATION_PHASE_UNSPECIFIED);
  EXPECT_THROW(convert(plan), std::runtime_error);
  window->set_phase(substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT);
  window->set_bounds_type(static_cast<substrait::Expression_WindowFunction::BoundsType>(99));
  EXPECT_THROW(convert(plan), std::runtime_error);
}
}  // namespace
}  // namespace pangrova::velox::test
