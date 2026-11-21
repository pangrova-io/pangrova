// Canonical proto -> production conversion -> actual Velox Task, not SQL/JNI E2E.
#include <gtest/gtest.h>
#include "CanonicalTestPlan.h"
#include "velox/expression/FunctionRegistrySnapshot.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

#include "TestMemoryHelper.h"
#include "substrait/plan.pb.h"
#include "CanonicalTaskExecution.h"
#include "engine/velox/plan/CanonicalOrdinalIdentity.h"
#include "engine/velox/plan/VeloxPlanConverter.h"
#include "engine/velox/runner/SingleMatchJoinGuard.h"
#include "velox/vector/DecodedVector.h"

namespace pangrova::velox::test {
namespace {
using namespace facebook::velox;
using Cell = std::optional<int64_t>;
using Rows = std::vector<std::vector<Cell>>;

substrait::Expression field(int index) {
  substrait::Expression expression;
  expression.mutable_selection()->mutable_root_reference();
  expression.mutable_selection()->mutable_direct_reference()->mutable_struct_field()->set_field(index);
  return expression;
}

substrait::Rel values(const Rows& rows, int columns = 1) {
  substrait::Rel relation;
  auto* read = relation.mutable_read();
  auto* schema = read->mutable_base_schema();
  schema->mutable_struct_();
  for (int i = 0; i < columns; ++i) {
    schema->add_names("duplicate_label");
    schema->mutable_struct_()->add_types()->mutable_i64()->set_nullability(
        substrait::Type::NULLABILITY_NULLABLE);
  }
  read->mutable_virtual_table();
  for (const auto& row : rows) {
    auto* record = read->mutable_virtual_table()->add_expressions();
    for (const auto& cell : row) {
      auto* literal = record->add_fields()->mutable_literal();
      if (cell) literal->set_i64(*cell);
      else literal->mutable_null()->mutable_i64()->set_nullability(
          substrait::Type::NULLABILITY_NULLABLE);
    }
  }
  return relation;
}

substrait::Expression equality(int left = 0, int right = 1) {
  substrait::Expression expression;
  auto* function = expression.mutable_scalar_function();
  function->set_function_reference(1);
  function->mutable_output_type()->mutable_bool_()->set_nullability(
      substrait::Type::NULLABILITY_NULLABLE);
  function->add_arguments()->mutable_value()->CopyFrom(field(left));
  function->add_arguments()->mutable_value()->CopyFrom(field(right));
  return expression;
}

substrait::Plan plan(const substrait::Rel& relation, int columns) {
  substrait::Plan result;
  auto* function = result.add_extensions()->mutable_extension_function();
  function->set_function_anchor(1);
  function->set_name("equal");
  auto* root = result.add_relations()->mutable_root();
  root->mutable_input()->CopyFrom(relation);
  for (int i = 0; i < columns; ++i) root->add_names("duplicate_label");
  return result;
}

template <typename Relation>
void key(Relation* relation, int left = 0, int right = 0,
         substrait::ComparisonJoinKey::SimpleComparisonType comparison =
             substrait::ComparisonJoinKey::SIMPLE_COMPARISON_TYPE_EQ) {
  auto* key = relation->add_keys();
  key->mutable_left()->CopyFrom(field(left).selection());
  key->mutable_right()->CopyFrom(field(right).selection());
  key->mutable_comparison()->set_simple(comparison);
}

class CanonicalJoinExecutionTest : public ::testing::Test {
 protected:
  void SetUp() override { pool_ = pangrova::test::testLeafPool("canonical-join-oracle"); }

  Rows execute(const substrait::Plan& plan) {
    const auto encoded = pangrova::test::canonicalTestPlan(plan).SerializeAsString();
    const auto converter_registry =
        facebook::velox::exec::FunctionRegistrySnapshot::capture();
    VeloxPlanConverter converter(converter_registry);
    auto node = converter.convert({encoded.begin(), encoded.end()}, {});
    EXPECT_EQ(converter.resultColumnLabels().size(), node->outputType()->size());
    for (size_t i = 0; i < node->outputType()->size(); ++i) {
      EXPECT_EQ(node->outputType()->nameOf(i), physicalFieldAlias(i));
      EXPECT_EQ(converter.resultColumnLabels()[i], "duplicate_label");
    }
    const auto execution = executeCanonicalTask(node, converter_registry);
    Rows result;
    for (const auto& batch : execution.batches) {
      for (vector_size_t row = 0; row < batch->size(); ++row) {
        std::vector<Cell> cells;
        for (const auto& child : batch->children()) {
          DecodedVector decoded(*child);
          cells.push_back(decoded.isNullAt(row) ? Cell{} : Cell(child->type()->isBoolean()
              ? int64_t(decoded.valueAt<bool>(row)) : decoded.valueAt<int64_t>(row)));
        }
        result.push_back(std::move(cells));
      }
    }
    validateSingleMatchJoinStats(execution.task->taskStats(), converter.singleMatchJoinRequirements());
    std::sort(result.begin(), result.end());
    return result;
  }

  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
};

TEST_F(CanonicalJoinExecutionTest, OuterPostFilterIsAfterNullExtensionForEveryCanonicalJoin) {
  for (int operatorKind = 0; operatorKind < 3; ++operatorKind) {
    substrait::Rel relation;
    if (operatorKind == 0) {
      auto* join = relation.mutable_join();
      join->set_type(substrait::JoinRel::JOIN_TYPE_LEFT);
      join->mutable_left()->CopyFrom(values({{1}}));
      join->mutable_right()->CopyFrom(values({{2}}));
      join->mutable_expression()->CopyFrom(equality());
      join->mutable_post_join_filter()->mutable_literal()->set_boolean(false);
    } else if (operatorKind == 1) {
      auto* join = relation.mutable_hash_join();
      join->set_type(substrait::HashJoinRel::JOIN_TYPE_LEFT);
      join->mutable_left()->CopyFrom(values({{1}}));
      join->mutable_right()->CopyFrom(values({{2}}));
      key(join);
      join->mutable_post_join_filter()->mutable_literal()->set_boolean(false);
    } else {
      auto* join = relation.mutable_merge_join();
      join->set_type(substrait::MergeJoinRel::JOIN_TYPE_LEFT);
      join->mutable_left()->CopyFrom(values({{1}}));
      join->mutable_right()->CopyFrom(values({{2}}));
      key(join);
      join->mutable_post_join_filter()->mutable_literal()->set_boolean(false);
    }
    EXPECT_TRUE(execute(plan(relation, 2)).empty());
  }
}

TEST_F(CanonicalJoinExecutionTest, HashBuildLeftPreservesLogicalOutputAndMixedNullKeySemantics) {
  substrait::Rel relation;
  auto* join = relation.mutable_hash_join();
  join->set_type(substrait::HashJoinRel::JOIN_TYPE_LEFT);
  join->set_build_input(substrait::HashJoinRel::BUILD_INPUT_LEFT);
  join->mutable_left()->CopyFrom(values({{1, std::nullopt}, {std::nullopt, 7}}, 2));
  join->mutable_right()->CopyFrom(values({{1, std::nullopt}, {std::nullopt, 7}}, 2));
  key(join, 0, 0);
  key(join, 1, 1, substrait::ComparisonJoinKey::SIMPLE_COMPARISON_TYPE_IS_NOT_DISTINCT_FROM);
  EXPECT_EQ(execute(plan(relation, 4)), (Rows{
      {std::nullopt, 7, std::nullopt, std::nullopt},
      {1, std::nullopt, 1, std::nullopt}}));
}

TEST_F(CanonicalJoinExecutionTest, MarkRetainsUnknownUnlessAnyPartnerIsTrueAndEmptyIsFalse) {
  for (bool rightMark : {false, true}) {
    for (const auto& partners : {Rows{{std::nullopt}}, Rows{{std::nullopt}, {1}}, Rows{}}) {
      substrait::Rel relation;
      auto* join = relation.mutable_nested_loop_join();
      join->set_type(rightMark ? substrait::NestedLoopJoinRel::JOIN_TYPE_RIGHT_MARK
          : substrait::NestedLoopJoinRel::JOIN_TYPE_LEFT_MARK);
      join->mutable_left()->CopyFrom(values(rightMark ? partners : Rows{{1}, {2}}));
      join->mutable_right()->CopyFrom(values(rightMark ? Rows{{1}, {2}} : partners));
      join->mutable_expression()->CopyFrom(equality());
      Rows expected{{1, partners.empty() ? Cell{0} : (partners.size() == 2 ? Cell{1} : Cell{})},
          {2, partners.empty() ? Cell{0} : Cell{}}};
      EXPECT_EQ(execute(plan(relation, 2)), expected);
    }
  }
}

TEST_F(CanonicalJoinExecutionTest, NonEquiMarkPreservesUnknownOnEitherLogicalSide) {
  for (bool rightMark : {false, true}) {
    for (const auto& partners : {Rows{{std::nullopt}, {1}}, Rows{{2}, {3}}, Rows{}}) {
      substrait::Rel relation;
      auto* join = relation.mutable_nested_loop_join();
      join->set_type(rightMark ? substrait::NestedLoopJoinRel::JOIN_TYPE_RIGHT_MARK
          : substrait::NestedLoopJoinRel::JOIN_TYPE_LEFT_MARK);
      const Rows preserved{{std::nullopt}, {1}, {2}};
      join->mutable_left()->CopyFrom(values(rightMark ? partners : preserved));
      join->mutable_right()->CopyFrom(values(rightMark ? preserved : partners));
      join->mutable_expression()->CopyFrom(equality(rightMark ? 1 : 0, rightMark ? 0 : 1));
      auto taskPlan = plan(relation, 2);
      taskPlan.mutable_extensions(0)->mutable_extension_function()->set_name("gt");
      const Rows expected = partners.empty() ? Rows{{std::nullopt, 0}, {1, 0}, {2, 0}}
          : (partners.front().front() ? Rows{{std::nullopt, std::nullopt}, {1, 0}, {2, 0}}
              : Rows{{std::nullopt, std::nullopt}, {1, std::nullopt}, {2, 1}});
      EXPECT_EQ(execute(taskPlan), expected);
    }
  }
}

TEST_F(CanonicalJoinExecutionTest, NonEquiSemiAndAntiPreserveSideAndExcludeUnknownMatches) {
  for (bool right : {false, true}) {
    for (bool anti : {false, true}) {
      substrait::Rel relation;
      auto* join = relation.mutable_nested_loop_join();
      join->set_type(right ? (anti ? substrait::NestedLoopJoinRel::JOIN_TYPE_RIGHT_ANTI
          : substrait::NestedLoopJoinRel::JOIN_TYPE_RIGHT_SEMI)
          : (anti ? substrait::NestedLoopJoinRel::JOIN_TYPE_LEFT_ANTI
          : substrait::NestedLoopJoinRel::JOIN_TYPE_LEFT_SEMI));
      join->mutable_left()->CopyFrom(values(right ? Rows{{1}, {std::nullopt}} : Rows{{1}, {2}}));
      join->mutable_right()->CopyFrom(values(right ? Rows{{1}, {2}} : Rows{{1}, {std::nullopt}}));
      join->mutable_expression()->CopyFrom(equality());
      EXPECT_EQ(execute(plan(relation, 1)), (anti ? Rows{{2}} : Rows{{1}}));
    }
  }
}

TEST_F(CanonicalJoinExecutionTest, SingleMatchProofUsesActualPreservedBuildOrProbeRows) {
  for (bool buildLeft : {false, true}) {
    substrait::Rel relation;
    auto* join = relation.mutable_hash_join();
    join->set_type(substrait::HashJoinRel::JOIN_TYPE_LEFT_SINGLE);
    join->set_build_input(buildLeft ? substrait::HashJoinRel::BUILD_INPUT_LEFT
        : substrait::HashJoinRel::BUILD_INPUT_RIGHT);
    join->mutable_left()->CopyFrom(values({{1}, {2}}));
    join->mutable_right()->CopyFrom(values({{1}}));
    key(join);
    EXPECT_EQ(execute(plan(relation, 2)), (Rows{{1, 1}, {2, std::nullopt}}));
    join->mutable_right()->CopyFrom(values({{1}, {1}}));
    EXPECT_THROW(execute(plan(relation, 2)), std::runtime_error);
  }
}

TEST_F(CanonicalJoinExecutionTest, JoinEmitKeepsRepeatedOrdinalsAndZeroColumnRowCount) {
  substrait::Rel relation;
  auto* join = relation.mutable_cross();
  join->mutable_left()->CopyFrom(values({{1}, {2}}));
  join->mutable_right()->CopyFrom(values({{7}}));
  auto* emit = join->mutable_common()->mutable_emit();
  emit->add_output_mapping(1);
  emit->add_output_mapping(0);
  emit->add_output_mapping(1);
  EXPECT_EQ(execute(plan(relation, 3)), (Rows{{7, 1, 7}, {7, 2, 7}}));
  emit->clear_output_mapping();
  EXPECT_EQ(execute(plan(relation, 0)), (Rows{{}, {}}));
}

TEST_F(CanonicalJoinExecutionTest, NonBooleanPostFilterIsRejectedWithoutExecution) {
  substrait::Rel relation;
  auto* join = relation.mutable_join();
  join->set_type(substrait::JoinRel::JOIN_TYPE_LEFT);
  join->mutable_left()->CopyFrom(values({{1}}));
  join->mutable_right()->CopyFrom(values({{1}}));
  join->mutable_expression()->CopyFrom(equality());
  join->mutable_post_join_filter()->mutable_literal()->set_i64(1);
  const auto encoded = pangrova::test::canonicalTestPlan(plan(relation, 2)).SerializeAsString();
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  EXPECT_THROW(converter.convert({encoded.begin(), encoded.end()}, {}), std::runtime_error);
}

TEST_F(CanonicalJoinExecutionTest, CanonicalSetAlgebraDistinguishesAnyAllAndMultiplicity) {
  const Rows primary{{1}, {2}, {2}, {3}, {3}, {3}, {4}, {std::nullopt}, {std::nullopt}};
  const Rows secondary1{{1}, {2}, {3}, {3}, {std::nullopt}};
  const Rows secondary2{{2}, {3}, {3}, {std::nullopt}};
  const std::vector<std::pair<substrait::SetRel::SetOp, Rows>> oracles{
      {substrait::SetRel::SET_OP_UNION_DISTINCT, {{std::nullopt}, {1}, {2}, {3}, {4}}},
      {substrait::SetRel::SET_OP_MINUS_PRIMARY, {{4}}},
      {substrait::SetRel::SET_OP_MINUS_PRIMARY_ALL, {{4}}},
      {substrait::SetRel::SET_OP_MINUS_MULTISET, {{1}, {4}}},
      {substrait::SetRel::SET_OP_INTERSECTION_PRIMARY, {{std::nullopt}, {1}, {2}, {3}}},
      {substrait::SetRel::SET_OP_INTERSECTION_MULTISET, {{std::nullopt}, {2}, {3}}},
      {substrait::SetRel::SET_OP_INTERSECTION_MULTISET_ALL, {{std::nullopt}, {2}, {3}, {3}}}};
  for (const auto& [operation, expected] : oracles) {
    substrait::Rel relation;
    auto* set = relation.mutable_set();
    set->set_op(operation);
    set->add_inputs()->CopyFrom(values(primary));
    set->add_inputs()->CopyFrom(values(secondary1));
    set->add_inputs()->CopyFrom(values(secondary2));
    EXPECT_EQ(execute(plan(relation, 1)), expected) << operation;
  }
}

TEST_F(CanonicalJoinExecutionTest, ZeroColumnSetDistinguishesEmptyAndMultiplicity) {
  for (const auto operation : {substrait::SetRel::SET_OP_UNION_DISTINCT,
           substrait::SetRel::SET_OP_MINUS_PRIMARY_ALL,
           substrait::SetRel::SET_OP_INTERSECTION_MULTISET_ALL}) {
    substrait::Rel relation;
    auto* set = relation.mutable_set();
    set->set_op(operation);
    set->add_inputs()->CopyFrom(values({{}, {}, {}}, 0));
    set->add_inputs()->CopyFrom(values({{}}, 0));
    const size_t count = operation == substrait::SetRel::SET_OP_MINUS_PRIMARY_ALL ? 2 : 1;
    EXPECT_EQ(execute(plan(relation, 0)), Rows(count));
    set->mutable_inputs(0)->CopyFrom(values({}, 0));
    set->mutable_inputs(1)->CopyFrom(values({}, 0));
    EXPECT_TRUE(execute(plan(relation, 0)).empty());
  }
}
}  // namespace
}  // namespace pangrova::velox::test
