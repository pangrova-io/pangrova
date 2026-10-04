#include <gtest/gtest.h>
#include "../CanonicalTestPlan.h"
#include "velox/expression/FunctionRegistrySnapshot.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "CanonicalTaskExecution.h"
#include "CanonicalPublishedTaskTest.h"
#include "substrait/plan.pb.h"
#include "velox/core/Expressions.h"
#include "velox/core/PlanNode.h"
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

core::PlanNodePtr convert(const substrait::Plan& plan,
    const std::shared_ptr<const exec::FunctionRegistrySnapshot>& registry) {
  VeloxPlanConverter converter(registry);
  const auto encoded = pangrova::test::canonicalTestPlan(plan).SerializeAsString();
  return converter.convert({encoded.begin(), encoded.end()}, {});
}

Cell execute(const core::PlanNodePtr& node,
    const std::shared_ptr<const exec::FunctionRegistrySnapshot>& registry) {
  const auto result = executeCanonicalTask(node, registry);
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
  pangrova::test::detail::ensureRuntimeInitialized();
  const auto registry = exec::FunctionRegistrySnapshot::capture();
  EXPECT_EQ(execute(convert(sumPlan({}), registry), registry), 0);
  EXPECT_EQ(execute(convert(sumPlan({std::nullopt, std::nullopt}), registry), registry), 0);
  EXPECT_EQ(execute(convert(sumPlan({1, std::nullopt, 2, -1}), registry), registry), 2);
}

TEST(SumZeroAggregateExecutionTest, ImplicitFinalPhaseCannotBeExecutedAsRawSingleAggregation) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const auto registry = exec::FunctionRegistrySnapshot::capture();
  auto plan = sumPlan({1, 2});
  plan.mutable_relations(0)->mutable_root()->mutable_input()->mutable_aggregate()
      ->mutable_measures(0)->mutable_measure()->set_phase(substrait::AGGREGATION_PHASE_UNSPECIFIED);
  EXPECT_THROW(convert(plan, registry), std::runtime_error);
}

TEST(SumZeroAggregateExecutionTest, PartialStateRetainsNullUntilActualFinalExtraction) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const auto registry = exec::FunctionRegistrySnapshot::capture();
  EXPECT_EQ(execute(convert(sumPlan({}, true), registry), registry), std::nullopt);
  EXPECT_EQ(execute(convert(sumPlan({std::nullopt}, true), registry), registry), std::nullopt);
  EXPECT_EQ(execute(finalAggregate(convert(sumPlan({}, true), registry)), registry), 0);
  EXPECT_EQ(execute(finalAggregate(convert(sumPlan({std::nullopt}, true), registry)), registry), 0);
  EXPECT_EQ(execute(finalAggregate(convert(sumPlan({1, std::nullopt, 2, -1}, true), registry)), registry), 2);
}

class DecimalSumAggregateExecutionTest : public CanonicalPublishedTaskTest {
 protected:
  using DecimalCell = std::optional<int128_t>;

  static substrait::Plan decimalSumPlan(const std::string& name,
      const std::vector<DecimalCell>& values, int precision, bool partial = false) {
    substrait::Plan plan;
    declare(plan, 0, name);
    auto* root = plan.add_relations()->mutable_root();
    root->add_names("result");
    auto* aggregate = root->mutable_input()->mutable_aggregate();
    auto* read = aggregate->mutable_input()->mutable_read();
    read->mutable_base_schema()->add_names("amount");
    *read->mutable_base_schema()->mutable_struct_()->add_types() = decimalType(precision, 2);
    read->mutable_virtual_table();
    for (const auto& value : values) {
      auto* literal = read->mutable_virtual_table()->add_expressions()->add_fields()->mutable_literal();
      if (value) *literal = decimalValue(precision, 2, *value);
      else *literal->mutable_null() = decimalType(precision, 2);
    }
    aggregate->add_groupings();
    auto* function = aggregate->add_measures()->mutable_measure();
    function->set_function_reference(0);
    function->set_phase(partial ? substrait::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE
        : substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT);
    function->set_invocation(substrait::AggregateFunction::AGGREGATION_INVOCATION_ALL);
    if (partial) function->mutable_output_type()->mutable_binary()->set_nullability(
        substrait::Type::NULLABILITY_NULLABLE);
    else *function->mutable_output_type() = decimalType(38, 2);
    *function->add_arguments()->mutable_value() = field(0);
    return plan;
  }

  static core::PlanNodePtr decimalFinal(core::PlanNodePtr partial,
      const std::string& name, int rawPrecision) {
    core::AggregationNode::Aggregate aggregate;
    // The actual raw type is retained, never reconstructed from VARBINARY.
    aggregate.rawInputTypes = {DECIMAL(rawPrecision, 2)};
    auto state = std::make_shared<core::FieldAccessTypedExpr>(VARBINARY(), partial->outputType()->nameOf(0));
    aggregate.call = std::make_shared<core::CallTypedExpr>(DECIMAL(38, 2),
        std::vector<core::TypedExprPtr>{state}, name);
    auto gathered = core::LocalPartitionNode::gather("decimal-state-gather", {partial});
    return std::make_shared<core::AggregationNode>("decimal-final", core::AggregationNode::Step::kFinal,
        std::vector<core::FieldAccessTypedExprPtr>{}, std::vector<core::FieldAccessTypedExprPtr>{},
        std::vector<std::string>{"result"}, std::vector<core::AggregationNode::Aggregate>{aggregate},
        false, false, gathered);
  }

  static DecimalCell decimalResult(const CanonicalTaskOutput& result) {
    EXPECT_EQ(result.batches.size(), 1);
    if (result.batches.size() != 1 || result.batches.front()->size() != 1) {
      ADD_FAILURE() << "Global decimal aggregate must emit exactly one actual row";
      return std::nullopt;
    }
    const auto& child = result.batches.front()->childAt(0);
    if (*child->type() != *DECIMAL(38, 2)) {
      ADD_FAILURE() << "DECIMAL SUM must retain its exact DECIMAL(38,2) result type";
      return std::nullopt;
    }
    DecodedVector value(*child);
    return value.isNullAt(0) ? DecimalCell{} : DecimalCell{value.valueAt<int128_t>(0)};
  }
};

TEST_F(DecimalSumAggregateExecutionTest, EmptyAndAllNullInputsPreserveSumVersusSumZeroSemantics) {
  for (const auto& name : {std::string{"sum"}, std::string{"sum0"}}) {
    SCOPED_TRACE(name);
    const DecimalCell empty = name == "sum" ? DecimalCell{} : DecimalCell{0};
    EXPECT_EQ(decimalResult(run(decimalSumPlan(name, {}, 15))), empty);
    EXPECT_EQ(decimalResult(run(decimalSumPlan(name, {std::nullopt, std::nullopt}, 15))), empty);
    EXPECT_EQ(decimalResult(run(decimalSumPlan(name, {100, std::nullopt, 225, -25}, 15))), DecimalCell{300});
  }
}

TEST_F(DecimalSumAggregateExecutionTest, ActualLongDecimalValueExceedsTheFormerNineteenDigitContract) {
  const int128_t nineteenDigitLimit = static_cast<int128_t>(1000000000000000000LL) * 10;
  for (const auto& name : {std::string{"sum"}, std::string{"sum0"}}) {
    SCOPED_TRACE(name);
    EXPECT_EQ(decimalResult(run(decimalSumPlan(name,
        {nineteenDigitLimit - 1, std::nullopt, 2}, 20))), DecimalCell{nineteenDigitLimit + 1});
  }
}

TEST_F(DecimalSumAggregateExecutionTest, ActualBinaryPartialStateAndOriginalRawTypeProduceExactFinalValues) {
  const int128_t large = static_cast<int128_t>(1000000000000000000LL) * 10;
  for (const auto& name : {std::string{"sum"}, std::string{"sum0"}}) {
    SCOPED_TRACE(name);
    const std::vector<std::vector<DecimalCell>> samples{
        {}, {std::nullopt}, {large - 1, std::nullopt, 2}};
    for (const auto& values : samples) {
      auto partial = convert(decimalSumPlan(name, values, 20, true));
      EXPECT_EQ(*partial->outputType()->childAt(0), *VARBINARY());
      {
        const auto state = executeCanonicalTask(partial, registry_);
        ASSERT_EQ(state.batches.size(), 1);
        ASSERT_EQ(state.batches.front()->size(), 1);
        EXPECT_EQ(*state.batches.front()->childAt(0)->type(), *VARBINARY());
      }
      const DecimalCell expected = values.size() == 3 ? DecimalCell{large + 1}
          : name == "sum" ? DecimalCell{} : DecimalCell{0};
      EXPECT_EQ(decimalResult(executeCanonicalTask(decimalFinal(partial, name, 20), registry_)), expected);
    }
  }
}

TEST_F(DecimalSumAggregateExecutionTest, MisdeclaredNineteenDigitReturnIsRejectedByTheActualAggregateBinding) {
  for (const auto& name : {std::string{"sum"}, std::string{"sum0"}}) {
    SCOPED_TRACE(name);
    auto plan = decimalSumPlan(name, {100}, 15);
    *plan.mutable_relations(0)->mutable_root()->mutable_input()->mutable_aggregate()
        ->mutable_measures(0)->mutable_measure()->mutable_output_type() = decimalType(19, 2);
    rejectContract(plan, "aggregate output contract mismatch: " + name);
  }
}
}  // namespace
}  // namespace pangrova::velox::test
