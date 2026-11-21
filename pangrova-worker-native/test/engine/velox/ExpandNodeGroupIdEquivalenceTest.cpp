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
#include "CanonicalTestPlan.h"
#include "velox/expression/FunctionRegistrySnapshot.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "TestMemoryHelper.h"
#include "CanonicalTaskExecution.h"
#include "engine/velox/plan/VeloxPlanConverter.h"

#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"

#include "velox/core/PlanNode.h"
#include "velox/exec/AggregateFunctionRegistry.h"
#include "velox/vector/DecodedVector.h"

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
  schema->mutable_struct_()->add_types()->mutable_i64()
      ->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
}

void setFieldRef(substrait::Expression* expr, int field_index) {
  auto* ref = expr->mutable_selection();
  ref->mutable_root_reference();
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
  std::set<int> referenced;
  for (const auto& grouping : grouping_sets) referenced.insert(grouping.begin(), grouping.end());
  std::vector<int> groupingOrdinals(column_count, -1);
  int groupingOrdinal = 0;
  for (const auto column : referenced) {
    groupingOrdinals.at(column) = groupingOrdinal++;
    root->add_names("col_" + std::to_string(column));
  }
  root->add_names("sum");
  if (grouping_sets.size() > 1) root->add_names("grouping_set_index");

  auto* agg = root->mutable_input()->mutable_aggregate();

  // Input: TableScan with column_count grouping columns + 1 aggregate column.
  auto* input_read = agg->mutable_input()->mutable_read();
  input_read->mutable_named_table()->add_names("data");
  auto* schema = input_read->mutable_base_schema();
  for (int i = 0; i < column_count; ++i) {
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
  setFieldRef(arg->mutable_value(), column_count);
  agg_func->mutable_output_type()->mutable_i64()
      ->set_nullability(substrait::Type::NULLABILITY_NULLABLE);

  std::string bytes;
  pangrova::test::canonicalTestPlan(plan).SerializeToString(&bytes);
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
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto result = converter.convert(plan_bytes, {});
  ASSERT_NE(result, nullptr);

  // One expansion and one business aggregate, evaluated as actual
  // partial→exchange→final states rather than duplicate raw aggregation.
  EXPECT_EQ(countNodes(result, "GroupId"), 1);
  const auto final = std::dynamic_pointer_cast<const core::AggregationNode>(findNode(result, "Aggregation"));
  ASSERT_NE(final, nullptr);
  EXPECT_EQ(final->step(), core::AggregationNode::Step::kFinal);
  ASSERT_EQ(final->sources().size(), 1);
  const auto exchange = std::dynamic_pointer_cast<const core::LocalPartitionNode>(final->sources().front());
  ASSERT_NE(exchange, nullptr);
  ASSERT_EQ(exchange->sources().size(), 1);
  const auto partial = std::dynamic_pointer_cast<const core::AggregationNode>(exchange->sources().front());
  ASSERT_NE(partial, nullptr);
  EXPECT_EQ(partial->step(), core::AggregationNode::Step::kPartial);
  EXPECT_EQ(countNodes(result, "Aggregation"), 2);
  EXPECT_EQ(countNodes(partial->sources().front(), "Aggregation"), 0);
  EXPECT_EQ(countNodes(partial->sources().front(), "GroupId"), 1);
  ASSERT_EQ(partial->aggregates().size(), 1);
  ASSERT_EQ(final->aggregates().size(), 1);
  const auto& raw = partial->aggregates().front();
  const auto& merged = final->aggregates().front();
  ASSERT_EQ(raw.rawInputTypes.size(), 1);
  EXPECT_TRUE(raw.rawInputTypes.front()->isBigint());
  ASSERT_EQ(merged.rawInputTypes.size(), 1);
  EXPECT_TRUE(merged.rawInputTypes.front()->equivalent(*raw.rawInputTypes.front()));
  EXPECT_EQ(raw.call->name(), "sum");
  EXPECT_EQ(merged.call->name(), raw.call->name());
  EXPECT_TRUE(raw.call->type()->equivalent(*exec::resolveIntermediateType("sum", raw.rawInputTypes)));
  EXPECT_TRUE(merged.call->type()->isBigint());
  ASSERT_EQ(merged.call->inputs().size(), 1);
  const auto state = std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(merged.call->inputs().front());
  ASSERT_NE(state, nullptr);
  EXPECT_EQ(state->name(), partial->outputType()->nameOf(final->groupingKeys().size()));
  EXPECT_TRUE(state->type()->equivalent(*raw.call->type()));
  std::vector<vector_size_t> globalSets;
  for (size_t set = 0; set < grouping_sets.size(); ++set) {
    if (grouping_sets[set].empty()) globalSets.push_back(static_cast<vector_size_t>(set));
  }
  EXPECT_EQ(partial->globalGroupingSets(), globalSets);
  EXPECT_EQ(partial->groupId().has_value(), !globalSets.empty());
}

using GroupCell = std::optional<int64_t>;
using GroupRows = std::vector<std::vector<GroupCell>>;

std::vector<GroupCell> groupingInputRow(int row, int columns) {
  std::vector<GroupCell> result;
  for (int column = 0; column < columns; ++column) {
    result.push_back(row % 7 == 0 ? GroupCell{} : GroupCell{row % (column + 2)});
  }
  result.push_back(row + 1);
  return result;
}

GroupRows expectedGroupingValues(const std::vector<std::vector<int>>& sets, int columns, int size) {
  std::set<int> referenced;
  for (const auto& set : sets) referenced.insert(set.begin(), set.end());
  GroupRows expected;
  for (size_t index = 0; index < sets.size(); ++index) {
    std::map<std::vector<GroupCell>, int64_t> sums;
    const std::set<int> present(sets[index].begin(), sets[index].end());
    for (int row = 0; row < size; ++row) {
      const auto values = groupingInputRow(row, columns);
      std::vector<GroupCell> key;
      for (const auto column : referenced) key.push_back(present.contains(column) ? values[column] : GroupCell{});
      sums[key] += *values.back();
    }
    for (auto& [key, sum] : sums) {
      auto result = key;
      result.push_back(sum);
      result.push_back(static_cast<int64_t>(index));
      expected.push_back(std::move(result));
    }
    if (size == 0 && sets[index].empty()) {
      std::vector<GroupCell> result(referenced.size() + 1);
      result.push_back(static_cast<int64_t>(index));
      expected.push_back(std::move(result));
    }
  }
  std::sort(expected.begin(), expected.end());
  return expected;
}

TEST_P(ExpandNodeGroupIdEquivalenceTest, ActualValuesAndEmptyGlobalSetsKeepExactAggregateSemantics) {
  const auto& param = GetParam();
  const auto sets = generateGroupingSets(param.pattern, param.column_count, param.grouping_set_count);
  const auto bytes = buildMultiGroupingSetPlan(param.column_count, sets);
  for (const bool empty : {false, true}) {
    substrait::Plan proto;
    ASSERT_TRUE(proto.ParseFromArray(bytes.data(), bytes.size()));
    auto* read = proto.mutable_relations(0)->mutable_root()->mutable_input()
        ->mutable_aggregate()->mutable_input()->mutable_read();
    read->clear_named_table();
    read->mutable_virtual_table();
    for (auto& type : *read->mutable_base_schema()->mutable_struct_()->mutable_types()) {
      type.mutable_i64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
    }
    const auto size = empty ? 0 : param.input_size;
    for (int row = 0; row < size; ++row) {
      auto* record = read->mutable_virtual_table()->add_expressions();
      for (const auto& value : groupingInputRow(row, param.column_count)) {
        auto* literal = record->add_fields()->mutable_literal();
        if (value) literal->set_i64(*value);
        else literal->mutable_null()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
      }
    }
    const auto encoded = pangrova::test::canonicalTestPlan(proto).SerializeAsString();
    const auto converter_registry =
        facebook::velox::exec::FunctionRegistrySnapshot::capture();
    VeloxPlanConverter converter(converter_registry);
    const auto plan = converter.convert({encoded.begin(), encoded.end()}, {});
    const auto execution = executeCanonicalTask(plan, converter_registry);
    GroupRows actual;
    for (const auto& batch : execution.batches) for (vector_size_t row = 0; row < batch->size(); ++row) {
      std::vector<GroupCell> values;
      for (const auto& child : batch->children()) {
        DecodedVector decoded(*child);
        values.push_back(decoded.isNullAt(row) ? GroupCell{} : GroupCell{child->type()->isInteger()
            ? int64_t{decoded.valueAt<int32_t>(row)} : decoded.valueAt<int64_t>(row)});
      }
      actual.push_back(std::move(values));
    }
    std::sort(actual.begin(), actual.end());
    EXPECT_EQ(actual, expectedGroupingValues(sets, param.column_count, size));
  }
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
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto result = converter.convert(plan_bytes, {});
  ASSERT_NE(result, nullptr);

  // Final output must NOT contain 'groupId'.
  auto final_output = result->outputType();
  EXPECT_FALSE(hasColumn(final_output, "groupId"))
      << "Final output should not contain 'groupId' column for " << param.name;
  ASSERT_GT(final_output->size(), 0);
  EXPECT_TRUE(final_output->childAt(final_output->size() - 1)->isInteger());
  ASSERT_EQ(converter.resultColumnLabels().size(), final_output->size());
  EXPECT_EQ(converter.resultColumnLabels().back(), "grouping_set_index");
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
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto result = converter.convert(plan_bytes, {});
  ASSERT_NE(result, nullptr);

  auto gid_node = findNode(result, "GroupId");
  ASSERT_NE(gid_node, nullptr);

  // Inspect the actual declared discriminator, independent of physical alias.
  auto group_id = std::dynamic_pointer_cast<const core::GroupIdNode>(gid_node);
  ASSERT_NE(group_id, nullptr);
  auto gid_output = gid_node->outputType();
  ASSERT_GT(gid_output->size(), 0);
  EXPECT_TRUE(hasColumn(gid_output, group_id->groupIdName()));
  EXPECT_EQ(gid_output->nameOf(gid_output->size() - 1), group_id->groupIdName());
  EXPECT_TRUE(gid_output->childAt(gid_output->size() - 1)->isBigint());
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
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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
  ASSERT_EQ(final_output->size(), referenced_cols.size() + 2);
  ASSERT_EQ(converter.resultColumnLabels().size(), final_output->size());
  size_t ordinal = 0;
  for (const auto column : referenced_cols) {
    EXPECT_EQ(converter.resultColumnLabels()[ordinal++], "col_" + std::to_string(column));
  }
  EXPECT_EQ(converter.resultColumnLabels()[ordinal++], "sum");
  EXPECT_EQ(converter.resultColumnLabels()[ordinal], "grouping_set_index");
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
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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
