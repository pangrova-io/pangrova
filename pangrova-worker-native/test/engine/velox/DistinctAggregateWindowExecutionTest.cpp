// Canonical plan -> production converter -> actual Velox Task values.
// Not public SQL/JNI/distributed end-to-end evidence.
#include <gtest/gtest.h>
#include "CanonicalTestPlan.h"
#include "velox/expression/FunctionRegistrySnapshot.h"

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <vector>

#include "TestMemoryHelper.h"
#include "substrait/plan.pb.h"
#include "velox/core/PlanFragment.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/Task.h"
#include "engine/velox/plan/VeloxPlanConverter.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"

namespace pangrova::velox::test {
namespace {
using namespace facebook::velox;
using NullableLong = std::optional<int64_t>;

void field(substrait::Expression* expression, int ordinal) {
  expression->mutable_selection()->mutable_root_reference();
  expression->mutable_selection()->mutable_direct_reference()->mutable_struct_field()->set_field(ordinal);
}

substrait::Plan windows(bool sliding, bool constants = false) {
  substrait::Plan plan;
  for (int ordinal = 0; ordinal < 2; ++ordinal) {
    auto* declaration = plan.add_extensions()->mutable_extension_function();
    declaration->set_function_anchor(ordinal);
    declaration->set_name(ordinal == 0 ? "count" : "sum");
  }
  auto* root = plan.add_relations()->mutable_root();
  root->add_names("count_result");
  root->add_names("sum_result");
  auto* project = root->mutable_input()->mutable_project();
  auto* read = project->mutable_input()->mutable_read();
  auto* schema = read->mutable_base_schema();
  schema->add_names("id");
  schema->add_names("value");
  schema->mutable_struct_()->add_types()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  schema->mutable_struct_()->add_types()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  const std::vector<NullableLong> values{std::nullopt, 1, 1, 2, std::nullopt, 2, 3};
  for (size_t ordinal = 0; ordinal < values.size(); ++ordinal) {
    auto* row = read->mutable_virtual_table()->add_expressions();
    row->add_fields()->mutable_literal()->set_i64(ordinal + 1);
    auto* value = row->add_fields()->mutable_literal();
    if (values[ordinal]) {
      value->set_i64(*values[ordinal]);
      value->set_nullable(true);
    } else value->mutable_null()->CopyFrom(schema->struct_().types(1));
  }
  for (int ordinal = 0; ordinal < 2; ++ordinal) {
    auto* window = project->add_expressions()->mutable_window_function();
    window->set_function_reference(ordinal);
    window->set_invocation(substrait::AggregateFunction::AGGREGATION_INVOCATION_DISTINCT);
    window->set_phase(substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT);
    window->set_bounds_type(substrait::Expression_WindowFunction::BOUNDS_TYPE_ROWS);
    window->mutable_output_type()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
    auto* argument = window->add_arguments()->mutable_value();
    if (constants) argument->mutable_literal()->set_i64(7);
    else field(argument, 1);
    auto* sort = window->add_sorts();
    field(sort->mutable_expr(), 0);
    sort->set_direction(substrait::SortField::SORT_DIRECTION_ASC_NULLS_LAST);
    if (sliding) window->mutable_lower_bound()->mutable_preceding()->set_offset(1);
    else window->mutable_lower_bound()->mutable_unbounded();
    window->mutable_upper_bound()->mutable_current_row();
    project->mutable_common()->mutable_emit()->add_output_mapping(ordinal + 2);
  }
  return plan;
}

std::vector<std::vector<NullableLong>> execute(const substrait::Plan& plan) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  const auto encoded = pangrova::test::canonicalTestPlan(plan).SerializeAsString();
  auto node = converter.convert({encoded.begin(), encoded.end()}, {});
  auto task = exec::Task::create("distinct-window-values", core::PlanFragment{node}, 0,
      core::QueryCtx::create(pangrova::jni::globalExecutor().get(),
          core::QueryConfig({{"require_function_registry_snapshot", "true"}}, converter_registry)), exec::Task::ExecutionMode::kSerial);
  EXPECT_TRUE(task->supportSerialExecutionMode());
  std::vector<std::vector<NullableLong>> output;
  while (auto batch = task->next()) {
    std::vector<DecodedVector> columns(batch->childrenSize());
    for (size_t ordinal = 0; ordinal < columns.size(); ++ordinal) columns[ordinal].decode(*batch->childAt(ordinal));
    for (vector_size_t row = 0; row < batch->size(); ++row) {
      std::vector<NullableLong> values;
      for (const auto& column : columns) {
        values.emplace_back(column.isNullAt(row) ? std::nullopt : NullableLong{column.valueAt<int64_t>(row)});
      }
      output.push_back(std::move(values));
    }
  }
  EXPECT_FALSE(task->error()) << task->errorMessage();
  EXPECT_EQ(task->state(), exec::TaskState::kFinished);
  EXPECT_EQ(task->numRunningDrivers(), 0);
  return output;
}

TEST(DistinctAggregateWindowExecutionTest, PrefixFramesPreserveDuplicatesNullsAndActualSum) {
  EXPECT_EQ(execute(windows(false)), (std::vector<std::vector<NullableLong>>{
      {0, std::nullopt}, {1, 1}, {1, 1}, {2, 3}, {2, 3}, {2, 3}, {3, 6}}));
}

TEST(DistinctAggregateWindowExecutionTest, SlidingFramesRemoveValuesThatLeaveTheActualFrame) {
  EXPECT_EQ(execute(windows(true)), (std::vector<std::vector<NullableLong>>{
      {0, std::nullopt}, {1, 1}, {1, 1}, {2, 3}, {1, 2}, {1, 2}, {2, 5}}));
}

TEST(DistinctAggregateWindowExecutionTest, ConstantTupleIsDeduplicatedInsteadOfCountingRows) {
  EXPECT_EQ(execute(windows(true, true)), (std::vector<std::vector<NullableLong>>{
      {1, 7}, {1, 7}, {1, 7}, {1, 7}, {1, 7}, {1, 7}, {1, 7}}));
}

TEST(DistinctAggregateWindowExecutionTest, EmptyFramesUseTheActualAggregateIdentity) {
  auto plan = windows(true);
  for (auto& expression : *plan.mutable_relations(0)->mutable_root()->mutable_input()->mutable_project()->mutable_expressions()) {
    expression.mutable_window_function()->mutable_lower_bound()->mutable_following()->set_offset(1);
  }
  EXPECT_EQ(execute(plan), (std::vector<std::vector<NullableLong>>(7, {0, std::nullopt})));
}

TEST(DistinctAggregateWindowExecutionTest, SumZeroKeepsZeroOnEmptyAndNullFramesWithActualDistinctValues) {
  auto plan = windows(false);
  plan.mutable_extensions(1)->mutable_extension_function()->set_name("sum0");
  EXPECT_EQ(execute(plan), (std::vector<std::vector<NullableLong>>{
      {0, 0}, {1, 1}, {1, 1}, {2, 3}, {2, 3}, {2, 3}, {3, 6}}));
  for (auto& expression : *plan.mutable_relations(0)->mutable_root()->mutable_input()->mutable_project()->mutable_expressions()) {
    expression.mutable_window_function()->mutable_lower_bound()->mutable_following()->set_offset(1);
    expression.mutable_window_function()->mutable_upper_bound()->mutable_current_row();
  }
  EXPECT_EQ(execute(plan), (std::vector<std::vector<NullableLong>>(7, {0, 0})));
}

TEST(DistinctAggregateWindowExecutionTest, NullTreatmentChangesActualFirstValueResults) {
  auto plan = windows(false);
  plan.mutable_extensions(0)->mutable_extension_function()->set_name("first_value");
  auto* window = plan.mutable_relations(0)->mutable_root()->mutable_input()->mutable_project()
      ->mutable_expressions(0)->mutable_window_function();
  window->set_invocation(substrait::AggregateFunction::AGGREGATION_INVOCATION_ALL);
  const auto respect = execute(plan);
  for (const auto& row : respect) EXPECT_EQ(row[0], std::nullopt);
  auto* option = window->add_options();
  option->set_name("null_treatment");
  option->add_preference("IGNORE_NULLS");
  const auto ignore = execute(plan);
  ASSERT_EQ(ignore.size(), 7);
  EXPECT_EQ(ignore[0][0], std::nullopt);
  for (size_t ordinal = 1; ordinal < ignore.size(); ++ordinal) EXPECT_EQ(ignore[ordinal][0], 1);
}
}  // namespace
}  // namespace pangrova::velox::test
