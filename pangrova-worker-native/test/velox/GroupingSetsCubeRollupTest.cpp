// GroupingSetsCubeRollupTest: Verifies that CUBE and ROLLUP patterns
// (encoded as multiple grouping sets in Substrait AggregateRel) are
// correctly handled by convertMultiGroupingAggregate via GroupIdNode.
//
// CUBE(a, b, c) produces 2^3 = 8 grouping sets.
// ROLLUP(a, b, c) produces N+1 = 4 prefix subsets.
// Both are just specific patterns of arbitrary grouping set combinations.
//
// **Validates: Requirements 8.3**
#include <gtest/gtest.h>
#include <set>

#include "velox/plan/VeloxPlanConverter.h"

#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"

namespace pangrova::velox::test {
namespace {

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

void addI64Column(substrait::NamedStruct* schema, const std::string& name) {
  schema->add_names(name);
  auto* field = schema->mutable_struct_()->add_types();
  field->mutable_i64();
}

void setFieldRef(substrait::Expression* expr, int field_index) {
  auto* ref = expr->mutable_selection();
  ref->mutable_root_reference();
  ref->mutable_direct_reference()->mutable_struct_field()->set_field(
      field_index);
}

// Build a plan with an AggregateRel that has multiple grouping sets.
// Input: TableScan with columns [col_0, col_1, ..., col_{num_cols-1}, agg_col].
// Grouping sets: provided as vectors of column indices.
// Measure: sum(agg_col).
std::vector<uint8_t> buildMultiGroupingSetPlan(
    int num_grouping_cols,
    const std::vector<std::vector<int>>& grouping_sets) {
  substrait::Plan plan;

  // Register "sum" function.
  auto* ext = plan.add_extensions();
  auto* ext_func = ext->mutable_extension_function();
  ext_func->set_function_anchor(1);
  ext_func->set_name("sum:i64");

  auto* plan_rel = plan.add_relations();
  auto* root = plan_rel->mutable_root();
  std::set<int> referenced;
  for (const auto& grouping : grouping_sets) referenced.insert(grouping.begin(), grouping.end());
  std::vector<int> groupingOrdinals(num_grouping_cols, -1);
  int groupingOrdinal = 0;
  for (const auto column : referenced) {
    groupingOrdinals.at(column) = groupingOrdinal++;
    root->add_names("col_" + std::to_string(column));
  }
  root->add_names("sum");
  if (grouping_sets.size() > 1) root->add_names("grouping_set_index");

  auto* agg = root->mutable_input()->mutable_aggregate();

  // Input: TableScan with num_grouping_cols + 1 columns.
  auto* input_read = agg->mutable_input()->mutable_read();
  input_read->mutable_named_table()->add_names("data");
  auto* schema = input_read->mutable_base_schema();
  for (int i = 0; i < num_grouping_cols; ++i) {
    addI64Column(schema, "col_" + std::to_string(i));
  }
  addI64Column(schema, "agg_col");

  for (const auto column : referenced) {
    setFieldRef(agg->add_grouping_expressions(), column);
  }

  // Add grouping sets.
  for (const auto& gs : grouping_sets) {
    auto* grouping = agg->add_groupings();
    for (int col_idx : gs) {
      grouping->add_expression_references(groupingOrdinals.at(col_idx));
    }
  }

  // Measure: sum(agg_col) — last column.
  auto* measure = agg->add_measures();
  auto* agg_func = measure->mutable_measure();
  agg_func->set_function_reference(1);
  agg_func->set_phase(substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT);
  auto* arg = agg_func->add_arguments();
  setFieldRef(arg->mutable_value(), num_grouping_cols);
  agg_func->mutable_output_type()->mutable_i64();

  std::string bytes;
  plan.SerializeToString(&bytes);
  return {bytes.begin(), bytes.end()};
}

// Generate CUBE grouping sets for N columns (2^N subsets).
std::vector<std::vector<int>> generateCubeGroupingSets(int num_cols) {
  std::vector<std::vector<int>> result;
  int total = 1 << num_cols;  // 2^N
  for (int mask = total - 1; mask >= 0; --mask) {
    std::vector<int> subset;
    for (int i = 0; i < num_cols; ++i) {
      if (mask & (1 << i)) {
        subset.push_back(i);
      }
    }
    result.push_back(std::move(subset));
  }
  return result;
}

// Generate ROLLUP grouping sets for N columns (N+1 prefix subsets).
// ROLLUP(a, b, c) = {a,b,c}, {a,b}, {a}, {}
std::vector<std::vector<int>> generateRollupGroupingSets(int num_cols) {
  std::vector<std::vector<int>> result;
  for (int len = num_cols; len >= 0; --len) {
    std::vector<int> prefix;
    for (int i = 0; i < len; ++i) {
      prefix.push_back(i);
    }
    result.push_back(std::move(prefix));
  }
  return result;
}

// Find a node by name in the plan tree (DFS).
std::shared_ptr<const facebook::velox::core::PlanNode> findNode(
    const std::shared_ptr<const facebook::velox::core::PlanNode>& root,
    const std::string& name) {
  if (!root) return nullptr;
  if (root->name() == name) return root;
  for (const auto& child : root->sources()) {
    auto found = findNode(child, name);
    if (found) return found;
  }
  return nullptr;
}

// ---------------------------------------------------------------------------
// Test: CUBE(a, b) produces correct GroupIdNode with 4 grouping sets
// ---------------------------------------------------------------------------

TEST(GroupingSetsCubeRollupTest, CubeTwoColumns) {
  // CUBE(col_0, col_1) = {col_0, col_1}, {col_0}, {col_1}, {}
  auto grouping_sets = generateCubeGroupingSets(2);
  ASSERT_EQ(grouping_sets.size(), 4u);

  auto plan_bytes = buildMultiGroupingSetPlan(2, grouping_sets);
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);

  // Should produce GroupIdNode in the plan tree.
  auto gid_node = findNode(result, "GroupId");
  ASSERT_NE(gid_node, nullptr) << "GroupIdNode not found in plan tree";

  // GroupIdNode's declared internal discriminator is BIGINT. Its physical
  // alias must not be confused with the public INTEGER grouping-set index.
  auto group_id = std::dynamic_pointer_cast<const facebook::velox::core::GroupIdNode>(gid_node);
  ASSERT_NE(group_id, nullptr);
  auto gid_output = gid_node->outputType();
  ASSERT_GT(gid_output->size(), 0);
  EXPECT_EQ(gid_output->nameOf(gid_output->size() - 1), group_id->groupIdName());
  EXPECT_TRUE(gid_output->childAt(gid_output->size() - 1)->isBigint());

  // Should have Aggregation node above GroupIdNode.
  auto agg_node = findNode(result, "Aggregation");
  ASSERT_NE(agg_node, nullptr) << "AggregationNode not found in plan tree";

  // Final output should have grouping keys + aggregate (no groupId).
  auto final_output = result->outputType();
  bool final_has_group_id = false;
  for (size_t i = 0; i < final_output->size(); ++i) {
    if (final_output->nameOf(i) == "groupId") {
      final_has_group_id = true;
      break;
    }
  }
  EXPECT_FALSE(final_has_group_id)
      << "Final output should not contain 'groupId' column";
  ASSERT_EQ(final_output->size(), 4);
  EXPECT_TRUE(final_output->childAt(3)->isInteger());
  EXPECT_EQ(converter.resultColumnLabels()[3], "grouping_set_index");
}

// ---------------------------------------------------------------------------
// Test: CUBE(a, b, c) produces correct GroupIdNode with 8 grouping sets
// ---------------------------------------------------------------------------

TEST(GroupingSetsCubeRollupTest, CubeThreeColumns) {
  // CUBE(col_0, col_1, col_2) = 2^3 = 8 grouping sets.
  auto grouping_sets = generateCubeGroupingSets(3);
  ASSERT_EQ(grouping_sets.size(), 8u);

  auto plan_bytes = buildMultiGroupingSetPlan(3, grouping_sets);
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);

  // Should produce GroupIdNode.
  auto gid_node = findNode(result, "GroupId");
  ASSERT_NE(gid_node, nullptr) << "GroupIdNode not found for CUBE(3 cols)";

  // Verify output schema: should have all 3 grouping keys + aggregate.
  auto final_output = result->outputType();
  EXPECT_GE(final_output->size(), 4u)
      << "Expected at least 3 keys + 1 aggregate in output";
}

// ---------------------------------------------------------------------------
// Test: ROLLUP(a, b, c) produces correct GroupIdNode with 4 grouping sets
// ---------------------------------------------------------------------------

TEST(GroupingSetsCubeRollupTest, RollupThreeColumns) {
  // ROLLUP(col_0, col_1, col_2) = {col_0,col_1,col_2}, {col_0,col_1},
  //                                 {col_0}, {}
  auto grouping_sets = generateRollupGroupingSets(3);
  ASSERT_EQ(grouping_sets.size(), 4u);

  auto plan_bytes = buildMultiGroupingSetPlan(3, grouping_sets);
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);

  // Should produce GroupIdNode.
  auto gid_node = findNode(result, "GroupId");
  ASSERT_NE(gid_node, nullptr) << "GroupIdNode not found for ROLLUP(3 cols)";

  // Verify Aggregation node exists.
  auto agg_node = findNode(result, "Aggregation");
  ASSERT_NE(agg_node, nullptr) << "AggregationNode not found for ROLLUP";

  // Final output should have all 3 grouping keys + aggregate.
  auto final_output = result->outputType();
  EXPECT_GE(final_output->size(), 4u)
      << "Expected at least 3 keys + 1 aggregate in output";
}

// ---------------------------------------------------------------------------
// Test: ROLLUP(a, b) produces correct GroupIdNode with 3 grouping sets
// ---------------------------------------------------------------------------

TEST(GroupingSetsCubeRollupTest, RollupTwoColumns) {
  // ROLLUP(col_0, col_1) = {col_0, col_1}, {col_0}, {}
  auto grouping_sets = generateRollupGroupingSets(2);
  ASSERT_EQ(grouping_sets.size(), 3u);

  auto plan_bytes = buildMultiGroupingSetPlan(2, grouping_sets);
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);

  // Should produce GroupIdNode.
  auto gid_node = findNode(result, "GroupId");
  ASSERT_NE(gid_node, nullptr) << "GroupIdNode not found for ROLLUP(2 cols)";
}

// ---------------------------------------------------------------------------
// Test: Empty grouping set (grand total) is handled correctly
// ---------------------------------------------------------------------------

TEST(GroupingSetsCubeRollupTest, EmptyGroupingSetGrandTotal) {
  // GROUPING SETS ({col_0, col_1}, {})
  // The empty set {} represents the grand total.
  std::vector<std::vector<int>> grouping_sets = {{0, 1}, {}};

  auto plan_bytes = buildMultiGroupingSetPlan(2, grouping_sets);
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);

  // Should produce GroupIdNode even with empty grouping set.
  auto gid_node = findNode(result, "GroupId");
  ASSERT_NE(gid_node, nullptr)
      << "GroupIdNode not found for plan with empty grouping set";
}

// ---------------------------------------------------------------------------
// Test: Single-column CUBE (2 grouping sets: {a}, {})
// ---------------------------------------------------------------------------

TEST(GroupingSetsCubeRollupTest, CubeSingleColumn) {
  // CUBE(col_0) = {col_0}, {}
  auto grouping_sets = generateCubeGroupingSets(1);
  ASSERT_EQ(grouping_sets.size(), 2u);

  auto plan_bytes = buildMultiGroupingSetPlan(1, grouping_sets);
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);

  // Should produce GroupIdNode.
  auto gid_node = findNode(result, "GroupId");
  ASSERT_NE(gid_node, nullptr) << "GroupIdNode not found for CUBE(1 col)";
}

// ---------------------------------------------------------------------------
// Test: Arbitrary non-standard grouping sets (not CUBE or ROLLUP)
// ---------------------------------------------------------------------------

TEST(GroupingSetsCubeRollupTest, ArbitraryGroupingSets) {
  // GROUPING SETS ({col_0, col_2}, {col_1, col_2}, {col_0})
  // This is neither CUBE nor ROLLUP — just arbitrary combinations.
  std::vector<std::vector<int>> grouping_sets = {{0, 2}, {1, 2}, {0}};

  auto plan_bytes = buildMultiGroupingSetPlan(3, grouping_sets);
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);

  // Should produce GroupIdNode for arbitrary grouping sets.
  auto gid_node = findNode(result, "GroupId");
  ASSERT_NE(gid_node, nullptr)
      << "GroupIdNode not found for arbitrary grouping sets";

  // Final output should include all referenced keys (col_0, col_1, col_2)
  // + aggregate.
  auto final_output = result->outputType();
  EXPECT_GE(final_output->size(), 4u)
      << "Expected at least 3 keys + 1 aggregate in output";
}

// ---------------------------------------------------------------------------
// Test: CUBE(a, b, c, d) produces 16 grouping sets (stress test)
// ---------------------------------------------------------------------------

TEST(GroupingSetsCubeRollupTest, CubeFourColumnsStress) {
  // CUBE(col_0, col_1, col_2, col_3) = 2^4 = 16 grouping sets.
  auto grouping_sets = generateCubeGroupingSets(4);
  ASSERT_EQ(grouping_sets.size(), 16u);

  auto plan_bytes = buildMultiGroupingSetPlan(4, grouping_sets);
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);

  // Should produce GroupIdNode.
  auto gid_node = findNode(result, "GroupId");
  ASSERT_NE(gid_node, nullptr) << "GroupIdNode not found for CUBE(4 cols)";

  // Verify output schema: should have all 4 grouping keys + aggregate.
  auto final_output = result->outputType();
  EXPECT_GE(final_output->size(), 5u)
      << "Expected at least 4 keys + 1 aggregate in output";
}

// ---------------------------------------------------------------------------
// Test: ROLLUP(a, b, c, d) produces 5 grouping sets
// ---------------------------------------------------------------------------

TEST(GroupingSetsCubeRollupTest, RollupFourColumns) {
  // ROLLUP(col_0, col_1, col_2, col_3) = 5 prefix subsets.
  auto grouping_sets = generateRollupGroupingSets(4);
  ASSERT_EQ(grouping_sets.size(), 5u);

  auto plan_bytes = buildMultiGroupingSetPlan(4, grouping_sets);
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);

  // Should produce GroupIdNode.
  auto gid_node = findNode(result, "GroupId");
  ASSERT_NE(gid_node, nullptr) << "GroupIdNode not found for ROLLUP(4 cols)";

  // Verify output schema: should have all 4 grouping keys + aggregate.
  auto final_output = result->outputType();
  EXPECT_GE(final_output->size(), 5u)
      << "Expected at least 4 keys + 1 aggregate in output";
}

}  // namespace
}  // namespace pangrova::velox::test
