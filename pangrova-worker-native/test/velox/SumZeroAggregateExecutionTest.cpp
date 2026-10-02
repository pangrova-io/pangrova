#include <gtest/gtest.h>

#include "CanonicalTaskExecution.h"
#include "substrait/plan.pb.h"
#include "velox/plan/VeloxPlanConverter.h"
#include "velox/vector/DecodedVector.h"

namespace pangrova::velox::test {
namespace {
using namespace facebook::velox;
using Cell = std::optional<int64_t>;

substrait::Plan sumPlan(const std::vector<Cell>& values, bool partial = false) {
  substrait::Plan plan;
  auto* declaration = plan.add_extensions()->mutable_extension_function();
  declaration->set_function_anchor(0);
  declaration->set_name("sum0");
  auto* root = plan.add_relations()->mutable_root();
  root->add_names("result");
  auto* aggregate = root->mutable_input()->mutable_aggregate();
  auto* read = aggregate->mutable_input()->mutable_read();
  auto* schema = read->mutable_base_schema();
  schema->add_names("value");
  schema->mutable_struct_()->add_types()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  auto* table = read->mutable_virtual_table();
  for (const auto& value : values) {
    auto* literal = table->add_expressions()->add_fields()->mutable_literal();
    if (value) {
      literal->set_i64(*value);
      literal->set_nullable(true);
    } else literal->mutable_null()->CopyFrom(schema->struct_().types(0));
  }
  aggregate->add_groupings();
  auto* function = aggregate->add_measures()->mutable_measure();
  function->set_function_reference(0);
  function->set_phase(partial ? substrait::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE
      : substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT);
  function->set_invocation(substrait::AggregateFunction::AGGREGATION_INVOCATION_ALL);
  function->mutable_output_type()->mutable_i64()->set_nullability(partial
      ? substrait::Type::NULLABILITY_NULLABLE : substrait::Type::NULLABILITY_REQUIRED);
  auto* input = function->add_arguments()->mutable_value()->mutable_selection();
  input->mutable_root_reference();
  input->mutable_direct_reference()->mutable_struct_field()->set_field(0);
  return plan;
}

core::PlanNodePtr convert(const substrait::Plan& plan) {
  pangrova::test::detail::ensureRuntimeInitialized();
  VeloxPlanConverter converter;
  const auto encoded = plan.SerializeAsString();
  return converter.convert({encoded.begin(), encoded.end()}, {});
}

Cell execute(const core::PlanNodePtr& node) {
  const auto result = executeCanonicalTask(node);
  EXPECT_EQ(result.batches.size(), 1);
  if (result.batches.size() != 1 || result.batches.front()->size() != 1) {
    ADD_FAILURE() << "Global SUM0 must produce exactly one actual row";
    return std::nullopt;
  }
  DecodedVector value(*result.batches.front()->childAt(0));
  return value.isNullAt(0) ? Cell{} : Cell{value.valueAt<int64_t>(0)};
}

core::PlanNodePtr finalAggregate(core::PlanNodePtr partial) {
  core::AggregationNode::Aggregate aggregate;
  aggregate.rawInputTypes = {BIGINT()};
  auto state = std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), partial->outputType()->nameOf(0));
  aggregate.call = std::make_shared<core::CallTypedExpr>(BIGINT(), std::vector<core::TypedExprPtr>{state}, "sum0");
  auto gathered = core::LocalPartitionNode::gather("sum0-state-gather", {partial});
  return std::make_shared<core::AggregationNode>("sum0-final", core::AggregationNode::Step::kFinal,
      std::vector<core::FieldAccessTypedExprPtr>{}, std::vector<core::FieldAccessTypedExprPtr>{},
      std::vector<std::string>{"result"}, std::vector<core::AggregationNode::Aggregate>{aggregate},
      false, false, gathered);
}

TEST(SumZeroAggregateExecutionTest, EmptyAndNullInputsUseZeroButActualValuesRemainSum) {
  EXPECT_EQ(execute(convert(sumPlan({}))), 0);
  EXPECT_EQ(execute(convert(sumPlan({std::nullopt, std::nullopt}))), 0);
  EXPECT_EQ(execute(convert(sumPlan({1, std::nullopt, 2, -1}))), 2);
}

TEST(SumZeroAggregateExecutionTest, ImplicitFinalPhaseCannotBeExecutedAsRawSingleAggregation) {
  auto plan = sumPlan({1, 2});
  plan.mutable_relations(0)->mutable_root()->mutable_input()->mutable_aggregate()
      ->mutable_measures(0)->mutable_measure()->set_phase(substrait::AGGREGATION_PHASE_UNSPECIFIED);
  EXPECT_THROW(convert(plan), std::runtime_error);
}

TEST(SumZeroAggregateExecutionTest, PartialStateRetainsNullUntilActualFinalExtraction) {
  EXPECT_EQ(execute(convert(sumPlan({}, true))), std::nullopt);
  EXPECT_EQ(execute(convert(sumPlan({std::nullopt}, true))), std::nullopt);
  EXPECT_EQ(execute(finalAggregate(convert(sumPlan({}, true)))), 0);
  EXPECT_EQ(execute(finalAggregate(convert(sumPlan({std::nullopt}, true)))), 0);
  EXPECT_EQ(execute(finalAggregate(convert(sumPlan({1, std::nullopt, 2, -1}, true)))), 2);
}
}  // namespace
}  // namespace pangrova::velox::test
