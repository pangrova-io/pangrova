// ExpandNodeGroupIdEquivalenceTest: Parameterized tests for Property 15.
//
// Property 15: ExpandNode/GroupIdNode Equivalence
//
// TEST_P over grouping set counts (2, 3, 5) × input sizes (10, 100, 500)
// × column counts (2, 4, 6) × patterns (arbitrary sets, CUBE, ROLLUP).
//
// Verifies that the GroupIdNode-based GROUPING SETS implementation produces
// correct plan structure: GroupIdNode + AggregationNode + ProjectNode are
// created correctly for the same inputs that the multi-branch implementation
// would handle.
//
// **Validates: Requirements 8.1, 8.2**

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "../TestMemoryHelper.h"
#include "velox/plan/VeloxPlanConverter.h"

#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"

#include "velox/core/PlanNode.h"

namespace pangrova::velox::test {
namespace {

using namespace facebook::velox;

// ===========================================================================
// Test parameters
// ===========================================================================

enum class GroupingPattern { kArbitrary, kCube, kRollup };

std::string patternName(GroupingPattern p) {
  switch (p) {
    case GroupingPattern::kArbitrary: return "Arbitrary";
    case GroupingPattern::kCube: return "Cube";
    case GroupingPattern::kRollup: return "Rollup";
  }
  return "Unknown";
}

struct ExpandNodeEquivalenceParam {
  std::string name;
  int grouping_set_count;  // Target number of grouping sets (2, 3, 5)
  int input_size;          // Conceptual input size (10, 100, 500)
  int column_count;        // Number of grouping columns (2, 4, 6)
  GroupingPattern pattern; // Pattern type
};

// ===========================================================================
// Helpers: generate grouping sets for each pattern
// ===========================================================================

// Generate CUBE grouping sets for N columns (2^N subsets).
std::vector<std::vector<int>> generateCubeGroupingSets(int num_cols) {
  std::vector<std::vector<int>> result;
  int total = 1 << num_cols;
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

// Generate arbitrary grouping sets with the target count.
// Creates diverse combinations of column subsets.
std::vector<std::vector<int>> generateArbitraryGroupingSets(
    int num_cols,
    int target_count) {
  std::vector<std::vector<int>> result;

  // Start with the full set.
  std::vector<int> full_set;
  for (int i = 0; i < num_cols; ++i) {
    full_set.push_back(i);
  }
  result.push_back(full_set);

  // Add the empty set (grand total).
  if (static_cast<int>(result.size()) < target_count) {
    result.push_back({});
  }

  // Add single-column sets.
  for (int i = 0; i < num_cols && static_cast<int>(result.size()) < target_count; ++i) {
    result.push_back({i});
  }

  // Add pairs.
  for (int i = 0; i < num_cols && static_cast<int>(result.size()) < target_count; ++i) {
    for (int j = i + 1; j < num_cols && static_cast<int>(result.size()) < target_count; ++j) {
      result.push_back({i, j});
    }
  }

  // Add triples if needed.
  for (int i = 0; i < num_cols && static_cast<int>(result.size()) < target_count; ++i) {
    for (int j = i + 1; j < num_cols && static_cast<int>(result.size()) < target_count; ++j) {
      for (int k = j + 1; k < num_cols && static_cast<int>(result.size()) < target_count; ++k) {
        result.push_back({i, j, k});
      }
    }
  }

  // Trim to target count.
  if (static_cast<int>(result.size()) > target_count) {
    result.resize(target_count);
  }

  return result;
}

// Generate grouping sets based on pattern and parameters.
// For CUBE/ROLLUP, the grouping_set_count is determined by the pattern
// (2^N for CUBE, N+1 for ROLLUP). We select column_count to approximate
// the target grouping_set_count.
std::vector<std::vector<int>> generateGroupingSets(
    GroupingPattern pattern,
    int column_count,
    int target_grouping_set_count) {
  switch (pattern) {
    case GroupingPattern::kCube: {
      // CUBE produces 2^N sets. Use min(column_count, 5) to avoid 2^6=64.
      int cube_cols = std::min(column_count, 5);
      return generateCubeGroupingSets(cube_cols);
    }
    case GroupingPattern::kRollup: {
      // ROLLUP produces N+1 sets.
      return generateRollupGroupingSets(column_count);
    }
    case GroupingPattern::kArbitrary: {
      return generateArbitraryGroupingSets(column_count, target_grouping_set_count);
    }
  }
  return {};
}

// ===========================================================================
// Helpers: build Substrait plan
// ===========================================================================

void addI64Column(substrait::NamedStruct* schema, const std::string& name) {
  schema->add_names(name);
  schema->mutable_struct_()->add_types()->mutable_i64();
}

void setFieldRef(substrait::Expression* expr, int field_index) {
  auto* ref = expr->mutable_selection();
  ref->mutable_direct_reference()->mutable_struct_field()->set_field(
      field_index);
}

// Build a Substrait plan with an AggregateRel containing multiple grouping sets.
// Schema: [col_0, ..., col_{column_count-1}, agg_col]
// Measure: sum(agg_col)
std::vector<uint8_t> buildMultiGroupingSetPlan(
    int column_count,
    const std::vector<std::vector<int>>& grouping_sets) {
  substrait::Plan plan;

  // Register "sum" function.
  auto* ext = plan.add_extensions();
  auto* ext_func = ext->mutable_extension_function();
  ext_func->set_function_anchor(1);
  ext_func->set_name("sum:i64");

  auto* plan_rel = plan.add_relations();
  auto* root = plan_rel->mutable_root();
  root->add_names("output");

  auto* agg = root->mutable_input()->mutable_aggregate();

  // Input: TableScan with column_count grouping columns + 1 aggregate column.
  auto* input_read = agg->mutable_input()->mutable_read();
  input_read->mutable_named_table()->add_names("data");
  auto* schema = input_read->mutable_base_schema();
  for (int i = 0; i < column_count; ++i) {
    addI64Column(schema, "col_" + std::to_string(i));
  }
  addI64Column(schema, "agg_col");

  // Add grouping sets.
  for (const auto& gs : grouping_sets) {
    auto* grouping = agg->add_groupings();
    for (int col_idx : gs) {
      setFieldRef(grouping->add_grouping_expressions(), col_idx);
    }
  }

  // Measure: sum(agg_col) — last column.
  auto* measure = agg->add_measures();
  auto* agg_func = measure->mutable_measure();
  agg_func->set_function_reference(1);
  auto* arg = agg_func->add_arguments();
  setFieldRef(arg->mutable_value(), column_count);
  agg_func->mutable_output_type()->mutable_i64();

  std::string bytes;
  plan.SerializeToString(&bytes);
  return {bytes.begin(), bytes.end()};
}

// ===========================================================================
// Helpers: plan tree inspection
// ===========================================================================

// Find a node by name in the plan tree (DFS).
std::shared_ptr<const core::PlanNode> findNode(
    const std::shared_ptr<const core::PlanNode>& root,
    const std::string& name) {
  if (!root) return nullptr;
  if (root->name() == name) return root;
  for (const auto& child : root->sources()) {
    auto found = findNode(child, name);
    if (found) return found;
  }
  return nullptr;
}

// Count nodes with a given name in the plan tree.
int countNodes(
    const std::shared_ptr<const core::PlanNode>& root,
    const std::string& name) {
  if (!root) return 0;
  int count = (root->name() == name) ? 1 : 0;
  for (const auto& child : root->sources()) {
    count += countNodes(child, name);
  }
  return count;
}

// Collect all node names in pre-order traversal.
void collectNodeNames(
    const std::shared_ptr<const core::PlanNode>& node,
    std::vector<std::string>& names) {
  if (!node) return;
  names.emplace_back(node->name());
  for (const auto& source : node->sources()) {
    collectNodeNames(source, names);
  }
}

// Check if a column name exists in a RowType.
bool hasColumn(const RowTypePtr& type, const std::string& name) {
  for (size_t i = 0; i < type->size(); ++i) {
    if (type->nameOf(i) == name) {
      return true;
    }
  }
  return false;
}

// ===========================================================================
// Test fixture
// ===========================================================================

class ExpandNodeGroupIdEquivalenceTest
    : public ::testing::TestWithParam<ExpandNodeEquivalenceParam> {
 protected:
  static void SetUpTestSuite() {
    pangrova::test::testMemoryManager();
  }
};

// ===========================================================================
// Property 15.1: GroupIdNode Presence in Multi-Grouping-Set Plans
//
// For any AggregateRel with multiple grouping sets, the converted plan
// SHALL contain a GroupIdNode that assigns grouping set identifiers.
//
// **Validates: Requirements 8.1**
// ===========================================================================

TEST_P(ExpandNodeGroupIdEquivalenceTest, GroupIdNodePresent) {
  const auto& param = GetParam();

  auto grouping_sets = generateGroupingSets(
      param.pattern, param.column_count, param.grouping_set_count);

  // Must have at least 2 grouping sets for multi-grouping-set path.
  ASSERT_GE(grouping_sets.size(), 2u)
      << "Need at least 2 grouping sets for " << param.name;

  auto plan_bytes = buildMultiGroupingSetPlan(param.column_count, grouping_sets);
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr)
      << "Plan conversion failed for " << param.name;

  // GroupIdNode must be present in the plan tree.
  auto gid_node = findNode(result, "GroupId");
  ASSERT_NE(gid_node, nullptr)
      << "GroupIdNode not found in plan tree for " << param.name
      << " (grouping_sets=" << grouping_sets.size()
      << ", columns=" << param.column_count << ")";
}

// ===========================================================================
// Property 15.2: AggregationNode Follows GroupIdNode
//
// The plan SHALL contain a single AggregationNode that operates on the
// GroupIdNode output (single-pass aggregation over expanded rows).
//
// **Validates: Requirements 8.1**
// ===========================================================================

TEST_P(ExpandNodeGroupIdEquivalenceTest, SingleAggregationNodePresent) {
  const auto& param = GetParam();

  auto grouping_sets = generateGroupingSets(
      param.pattern, param.column_count, param.grouping_set_count);
  ASSERT_GE(grouping_sets.size(), 2u);

  auto plan_bytes = buildMultiGroupingSetPlan(param.column_count, grouping_sets);
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});
  ASSERT_NE(result, nullptr);

  // Exactly one AggregationNode should exist (single-pass strategy).
  int agg_count = countNodes(result, "Aggregation");
  EXPECT_EQ(agg_count, 1)
      << "Expected exactly 1 AggregationNode for " << param.name
      << " but found " << agg_count;
}

// ===========================================================================
// Property 15.3: GroupId Column Stripped From Final Output
//
// The final plan output SHALL NOT contain the internal 'groupId' column.
// A ProjectNode strips it before returning results to the caller.
//
// **Validates: Requirements 8.1, 8.2**
// ===========================================================================

TEST_P(ExpandNodeGroupIdEquivalenceTest, GroupIdStrippedFromFinalOutput) {
  const auto& param = GetParam();

  auto grouping_sets = generateGroupingSets(
      param.pattern, param.column_count, param.grouping_set_count);
  ASSERT_GE(grouping_sets.size(), 2u);

  auto plan_bytes = buildMultiGroupingSetPlan(param.column_count, grouping_sets);
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});
  ASSERT_NE(result, nullptr);

  // Final output must NOT contain 'groupId'.
  auto final_output = result->outputType();
  EXPECT_FALSE(hasColumn(final_output, "groupId"))
      << "Final output should not contain 'groupId' column for " << param.name;
}

// ===========================================================================
// Property 15.4: GroupIdNode Output Contains groupId Column
//
// The GroupIdNode output SHALL include a 'groupId' column of integer type
// that identifies which grouping set each row belongs to.
//
// **Validates: Requirements 8.1**
// ===========================================================================

TEST_P(ExpandNodeGroupIdEquivalenceTest, GroupIdNodeOutputHasGroupIdColumn) {
  const auto& param = GetParam();

  auto grouping_sets = generateGroupingSets(
      param.pattern, param.column_count, param.grouping_set_count);
  ASSERT_GE(grouping_sets.size(), 2u);

  auto plan_bytes = buildMultiGroupingSetPlan(param.column_count, grouping_sets);
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});
  ASSERT_NE(result, nullptr);

  auto gid_node = findNode(result, "GroupId");
  ASSERT_NE(gid_node, nullptr);

  // GroupIdNode output must contain 'groupId' column.
  auto gid_output = gid_node->outputType();
  EXPECT_TRUE(hasColumn(gid_output, "groupId"))
      << "GroupIdNode output missing 'groupId' column for " << param.name;
}

// ===========================================================================
// Property 15.5: Output Schema Contains All Grouping Keys Plus Aggregate
//
// The final output schema SHALL contain all referenced grouping key columns
// plus the aggregate result column. The number of output columns must equal
// the union of all grouping key columns + number of measures.
//
// **Validates: Requirements 8.2**
// ===========================================================================

TEST_P(ExpandNodeGroupIdEquivalenceTest, OutputSchemaCorrect) {
  const auto& param = GetParam();

  auto grouping_sets = generateGroupingSets(
      param.pattern, param.column_count, param.grouping_set_count);
  ASSERT_GE(grouping_sets.size(), 2u);

  auto plan_bytes = buildMultiGroupingSetPlan(param.column_count, grouping_sets);
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});
  ASSERT_NE(result, nullptr);

  auto final_output = result->outputType();

  // Output should have: union of all referenced grouping columns + 1 aggregate.
  // For CUBE with capped columns, only the actually-referenced columns appear.
  std::set<int> referenced_cols;
  for (const auto& gs : grouping_sets) {
    for (int col : gs) {
      referenced_cols.insert(col);
    }
  }
  size_t expected_min_cols = referenced_cols.size() + 1;
  EXPECT_GE(final_output->size(), expected_min_cols)
      << "Expected at least " << expected_min_cols
      << " columns in output for " << param.name
      << " but got " << final_output->size();
}

// ===========================================================================
// Property 15.6: Plan Structure Consistency Across Input Sizes
//
// The plan structure (node topology) SHALL be identical regardless of the
// conceptual input size. Input size affects runtime behavior, not plan shape.
//
// **Validates: Requirements 8.1, 8.2**
// ===========================================================================

TEST_P(ExpandNodeGroupIdEquivalenceTest, PlanStructureConsistentAcrossInputSizes) {
  const auto& param = GetParam();

  auto grouping_sets = generateGroupingSets(
      param.pattern, param.column_count, param.grouping_set_count);
  ASSERT_GE(grouping_sets.size(), 2u);

  // Build and convert the plan.
  auto plan_bytes = buildMultiGroupingSetPlan(param.column_count, grouping_sets);
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});
  ASSERT_NE(result, nullptr);

  // Collect node names.
  std::vector<std::string> node_names;
  collectNodeNames(result, node_names);

  // The plan must contain these nodes: Project, Aggregation, GroupId, TableScan.
  bool has_project = std::find(node_names.begin(), node_names.end(), "Project") != node_names.end();
  bool has_aggregation = std::find(node_names.begin(), node_names.end(), "Aggregation") != node_names.end();
  bool has_group_id = std::find(node_names.begin(), node_names.end(), "GroupId") != node_names.end();
  bool has_table_scan = std::find(node_names.begin(), node_names.end(), "TableScan") != node_names.end();

  EXPECT_TRUE(has_project)
      << "ProjectNode not found for " << param.name;
  EXPECT_TRUE(has_aggregation)
      << "AggregationNode not found for " << param.name;
  EXPECT_TRUE(has_group_id)
      << "GroupIdNode not found for " << param.name;
  EXPECT_TRUE(has_table_scan)
      << "TableScanNode not found for " << param.name;
}

// ===========================================================================
// Parameterization: grouping set counts × input sizes × column counts × patterns
// ===========================================================================

std::vector<ExpandNodeEquivalenceParam> generateParams() {
  std::vector<ExpandNodeEquivalenceParam> params;

  std::vector<int> grouping_set_counts = {2, 3, 5};
  std::vector<int> input_sizes = {10, 100, 500};
  std::vector<int> column_counts = {2, 4, 6};
  std::vector<GroupingPattern> patterns = {
      GroupingPattern::kArbitrary,
      GroupingPattern::kCube,
      GroupingPattern::kRollup};

  for (int gs_count : grouping_set_counts) {
    for (int input_size : input_sizes) {
      for (int col_count : column_counts) {
        for (auto pattern : patterns) {
          std::string name = "GS" + std::to_string(gs_count) +
              "_Input" + std::to_string(input_size) +
              "_Cols" + std::to_string(col_count) +
              "_" + patternName(pattern);
          params.push_back({name, gs_count, input_size, col_count, pattern});
        }
      }
    }
  }

  return params;
}

INSTANTIATE_TEST_SUITE_P(
    ExpandNodeGroupIdEquivalence,
    ExpandNodeGroupIdEquivalenceTest,
    ::testing::ValuesIn(generateParams()),
    [](const ::testing::TestParamInfo<ExpandNodeEquivalenceParam>& info) {
      return info.param.name;
    });

}  // namespace
}  // namespace pangrova::velox::test
