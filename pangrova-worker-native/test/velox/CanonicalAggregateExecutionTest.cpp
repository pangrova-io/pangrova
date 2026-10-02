// Native component value/type checks; this is not public SQL/JNI E2E.
#include <gtest/gtest.h>

#include <algorithm>
#include <optional>

#include "CanonicalTaskExecution.h"
#include "substrait/plan.pb.h"
#include "velox/plan/VeloxPlanConverter.h"
#include "velox/vector/DecodedVector.h"

namespace pangrova::velox::test {
namespace {
using namespace facebook::velox;
using Cell = std::optional<int64_t>;
using Rows = std::vector<std::vector<Cell>>;

substrait::Expression field(int ordinal, int child = -1) {
  substrait::Expression result;
  auto* reference = result.mutable_selection();
  reference->mutable_root_reference();
  auto* segment = reference->mutable_direct_reference()->mutable_struct_field();
  segment->set_field(ordinal);
  if (child >= 0) segment->mutable_child()->mutable_struct_field()->set_field(child);
  return result;
}

substrait::Plan groupingPlan(bool multipleSets, bool empty) {
  substrait::Plan plan;
  auto* function = plan.add_extensions()->mutable_extension_function();
  function->set_function_anchor(1);
  function->set_name("sum:i64");
  auto* root = plan.add_relations()->mutable_root();
  for (int i = 0; i < (multipleSets ? 4 : 3); ++i) root->add_names("result");
  auto* aggregate = root->mutable_input()->mutable_aggregate();
  auto* read = aggregate->mutable_input()->mutable_read();
  auto* schema = read->mutable_base_schema();
  for (const char* name : {"id", "record", "key", "value", "enabled"}) schema->add_names(name);
  schema->mutable_struct_()->add_types()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  auto* rowType = schema->mutable_struct_()->add_types()->mutable_struct_();
  rowType->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  rowType->add_types()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  rowType->add_types()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  schema->mutable_struct_()->add_types()->mutable_bool_()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  read->mutable_virtual_table();
  if (!empty) {
    const Rows rows{{7, 1, 5, 1}, {7, 1, 7, 0}, {8, 2, 11, std::nullopt}, {8, 2, std::nullopt, 1}};
    for (const auto& row : rows) {
      auto* record = read->mutable_virtual_table()->add_expressions();
      record->add_fields()->mutable_literal()->set_i64(*row[0]);
      auto* nested = record->add_fields()->mutable_literal()->mutable_struct_();
      nested->add_fields()->set_i64(*row[1]);
      auto* value = nested->add_fields();
      if (row[2]) value->set_i64(*row[2]);
      else value->mutable_null()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
      auto* mask = record->add_fields()->mutable_literal();
      if (row[3]) mask->set_boolean(*row[3] != 0);
      else mask->mutable_null()->mutable_bool_()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
    }
  }
  auto* cast = aggregate->add_grouping_expressions()->mutable_cast();
  cast->mutable_input()->CopyFrom(field(0));
  cast->mutable_type()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  cast->set_failure_behavior(substrait::Expression::Cast::FAILURE_BEHAVIOR_THROW_EXCEPTION);
  aggregate->add_grouping_expressions()->CopyFrom(field(1, 0));
  auto* grouping = aggregate->add_groupings();
  grouping->add_expression_references(1);
  grouping->add_expression_references(0);
  if (multipleSets) aggregate->add_groupings();
  auto* measure = aggregate->add_measures();
  measure->mutable_filter()->CopyFrom(field(2));
  auto* call = measure->mutable_measure();
  call->set_function_reference(1);
  call->set_phase(substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT);
  call->add_arguments()->mutable_value()->CopyFrom(field(1, 1));
  call->mutable_output_type()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  return plan;
}

class CanonicalAggregateExecutionTest : public ::testing::Test {
 protected:
  void SetUp() override { pangrova::test::testMemoryManager(); }

  Rows execute(const substrait::Plan& plan) {
    const auto encoded = plan.SerializeAsString();
    VeloxPlanConverter converter;
    const auto node = converter.convert({encoded.begin(), encoded.end()}, {});
    EXPECT_EQ(node->outputType()->size(), converter.resultColumnLabels().size());
    const auto result = executeCanonicalTask(node);
    Rows rows;
    for (const auto& batch : result.batches) {
      for (vector_size_t row = 0; row < batch->size(); ++row) {
        std::vector<Cell> values;
        for (const auto& child : batch->children()) {
          DecodedVector decoded(*child);
          values.push_back(decoded.isNullAt(row) ? Cell{} : Cell{child->type()->isInteger()
              ? int64_t(decoded.valueAt<int32_t>(row)) : decoded.valueAt<int64_t>(row)});
        }
        rows.push_back(std::move(values));
      }
    }
    std::sort(rows.begin(), rows.end());
    return rows;
  }
};

TEST_F(CanonicalAggregateExecutionTest, ComputedAndNestedKeysUseDeclarationOrderAndFilteredNestedArguments) {
  EXPECT_EQ(execute(groupingPlan(false, false)), (Rows{{7, 1, 5}, {8, 2, std::nullopt}}));
}

TEST_F(CanonicalAggregateExecutionTest, GroupingSetsReuseOriginalMaskAndArgumentsAndPreserveSetIdentity) {
  EXPECT_EQ(execute(groupingPlan(true, false)), (Rows{
      {std::nullopt, std::nullopt, 5, 1}, {7, 1, 5, 0}, {8, 2, std::nullopt, 0}}));
  EXPECT_EQ(execute(groupingPlan(true, true)), (Rows{{std::nullopt, std::nullopt, std::nullopt, 1}}));
}
}  // namespace
}  // namespace pangrova::velox::test
