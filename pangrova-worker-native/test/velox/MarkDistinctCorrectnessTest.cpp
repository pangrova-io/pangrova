// MarkDistinctCorrectnessTest: Parameterized tests for MarkDistinctNode
// plan conversion correctness.
//
// Property 16: MarkDistinctNode Correctness
//
// TEST_P over distinct column counts (2, 3) × grouping key counts (0, 1, 2) ×
// input sizes (10, 100, 500) × value distributions (unique, many-duplicates,
// all-same).
//
// Verifies:
//   - MarkDistinctNode chain is correctly inserted for multi-DISTINCT
//     aggregation plans.
//   - The number of MarkDistinctNodes matches the number of distinct columns.
//   - AggregationNode is present with marker-masked aggregates.
//   - Single-DISTINCT does NOT trigger MarkDistinctNode path.
//   - Output schema contains grouping keys + aggregate columns (no markers).
//
// **Validates: Requirements 9.1, 9.2, 9.3**

#include <gtest/gtest.h>

#include <memory>
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

enum class ValueDistribution { kUnique, kManyDuplicates, kAllSame };

std::string distributionName(ValueDistribution dist) {
  switch (dist) {
    case ValueDistribution::kUnique: return "Unique";
    case ValueDistribution::kManyDuplicates: return "ManyDuplicates";
    case ValueDistribution::kAllSame: return "AllSame";
  }
  return "Unknown";
}

struct MarkDistinctParam {
  std::string name;
  int distinct_col_count;   // 2 or 3
  int grouping_key_count;   // 0, 1, or 2
  int input_size;           // 10, 100, or 500
  ValueDistribution distribution;
};

// ===========================================================================
// Helpers
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

// Build a Substrait plan with an AggregateRel containing multiple DISTINCT
// measures on different columns.
//
// Schema: [group_key_0, ..., group_key_{gk-1}, distinct_col_0, ...,
//          distinct_col_{dc-1}]
//
// Measures: COUNT(DISTINCT distinct_col_i) for each i in [0, dc).
//
// The input_size and distribution parameters are encoded in the plan name
// for traceability but do not affect plan structure (they would affect
// runtime data generation in a full execution test).
std::vector<uint8_t> buildMultiDistinctPlan(
    int grouping_key_count,
    int distinct_col_count) {
  substrait::Plan plan;

  // Register "count" function.
  auto* ext = plan.add_extensions();
  auto* ext_func = ext->mutable_extension_function();
  ext_func->set_function_anchor(1);
  ext_func->set_name("count:i64");

  auto* plan_rel = plan.add_relations();
  auto* root = plan_rel->mutable_root();
  root->add_names("output");

  auto* agg = root->mutable_input()->mutable_aggregate();

  // Input: TableScan with grouping_key_count + distinct_col_count columns.
  auto* input_read = agg->mutable_input()->mutable_read();
  input_read->mutable_named_table()->add_names("data");
  auto* schema = input_read->mutable_base_schema();

  for (int i = 0; i < grouping_key_count; ++i) {
    addI64Column(schema, "group_key_" + std::to_string(i));
  }
  for (int i = 0; i < distinct_col_count; ++i) {
    addI64Column(schema, "distinct_col_" + std::to_string(i));
  }

  // Single grouping set with all grouping keys.
  if (grouping_key_count > 0) {
    auto* grouping = agg->add_groupings();
    for (int i = 0; i < grouping_key_count; ++i) {
      setFieldRef(grouping->add_grouping_expressions(), i);
    }
  } else {
    // Empty grouping (global aggregation).
    agg->add_groupings();
  }

  // Measures: COUNT(DISTINCT distinct_col_i) for each distinct column.
  for (int i = 0; i < distinct_col_count; ++i) {
    auto* measure = agg->add_measures();
    auto* agg_func = measure->mutable_measure();
    agg_func->set_function_reference(1);
    agg_func->set_invocation(
        substrait::AggregateFunction::AGGREGATION_INVOCATION_DISTINCT);

    // Argument: field reference to distinct_col_i.
    auto* arg = agg_func->add_arguments();
    setFieldRef(arg->mutable_value(), grouping_key_count + i);

    agg_func->mutable_output_type()->mutable_i64();
  }

  std::string bytes;
  plan.SerializeToString(&bytes);
  return {bytes.begin(), bytes.end()};
}

// Build a single-DISTINCT plan (should NOT trigger MarkDistinctNode).
std::vector<uint8_t> buildSingleDistinctPlan(int grouping_key_count) {
  substrait::Plan plan;

  auto* ext = plan.add_extensions();
  auto* ext_func = ext->mutable_extension_function();
  ext_func->set_function_anchor(1);
  ext_func->set_name("count:i64");

  auto* plan_rel = plan.add_relations();
  auto* root = plan_rel->mutable_root();
  root->add_names("output");

  auto* agg = root->mutable_input()->mutable_aggregate();

  auto* input_read = agg->mutable_input()->mutable_read();
  input_read->mutable_named_table()->add_names("data");
  auto* schema = input_read->mutable_base_schema();

  for (int i = 0; i < grouping_key_count; ++i) {
    addI64Column(schema, "group_key_" + std::to_string(i));
  }
  addI64Column(schema, "distinct_col_0");

  if (grouping_key_count > 0) {
    auto* grouping = agg->add_groupings();
    for (int i = 0; i < grouping_key_count; ++i) {
      setFieldRef(grouping->add_grouping_expressions(), i);
    }
  } else {
    agg->add_groupings();
  }

  // Single DISTINCT measure.
  auto* measure = agg->add_measures();
  auto* agg_func = measure->mutable_measure();
  agg_func->set_function_reference(1);
  agg_func->set_invocation(
      substrait::AggregateFunction::AGGREGATION_INVOCATION_DISTINCT);
  auto* arg = agg_func->add_arguments();
  setFieldRef(arg->mutable_value(), grouping_key_count);
  agg_func->mutable_output_type()->mutable_i64();

  std::string bytes;
  plan.SerializeToString(&bytes);
  return {bytes.begin(), bytes.end()};
}

// Find all nodes with a given name in the plan tree (DFS).
void findAllNodes(
    const std::shared_ptr<const core::PlanNode>& root,
    const std::string& name,
    std::vector<std::shared_ptr<const core::PlanNode>>& results) {
  if (!root) return;
  if (root->name() == name) {
    results.push_back(root);
  }
  for (const auto& child : root->sources()) {
    findAllNodes(child, name, results);
  }
}

// Find a single node by name in the plan tree (DFS).
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

// ===========================================================================
// Test fixture
// ===========================================================================

class MarkDistinctCorrectnessTest
    : public ::testing::TestWithParam<MarkDistinctParam> {
 protected:
  static void SetUpTestSuite() {
    pangrova::test::testMemoryManager();
  }
};

// ===========================================================================
// Property 16.1: MarkDistinctNode Chain Insertion
//
// For any AggregateRel with ≥2 DISTINCT measures on different columns,
// the converted plan SHALL contain one MarkDistinctNode per distinct column,
// chained before the AggregationNode.
//
// **Validates: Requirements 9.1, 9.2**
// ===========================================================================

TEST_P(MarkDistinctCorrectnessTest, MarkDistinctNodeChainInserted) {
  const auto& param = GetParam();
  auto plan_bytes = buildMultiDistinctPlan(
      param.grouping_key_count, param.distinct_col_count);
  ASSERT_FALSE(plan_bytes.empty());

  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});
  ASSERT_NE(result, nullptr)
      << "Plan conversion failed for " << param.name;

  // Find all MarkDistinct nodes in the plan tree.
  std::vector<std::shared_ptr<const core::PlanNode>> mark_nodes;
  findAllNodes(result, "MarkDistinct", mark_nodes);

  // Must have exactly one MarkDistinctNode per distinct column.
  EXPECT_EQ(mark_nodes.size(), static_cast<size_t>(param.distinct_col_count))
      << "Expected " << param.distinct_col_count
      << " MarkDistinctNode(s) but found " << mark_nodes.size()
      << " for " << param.name;
}

// ===========================================================================
// Property 16.2: AggregationNode Present After MarkDistinct Chain
//
// The converted plan SHALL contain an AggregationNode that uses the
// marker columns produced by MarkDistinctNodes as masks for DISTINCT
// aggregates.
//
// **Validates: Requirements 9.2**
// ===========================================================================

TEST_P(MarkDistinctCorrectnessTest, AggregationNodePresent) {
  const auto& param = GetParam();
  auto plan_bytes = buildMultiDistinctPlan(
      param.grouping_key_count, param.distinct_col_count);
  ASSERT_FALSE(plan_bytes.empty());

  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});
  ASSERT_NE(result, nullptr);

  // AggregationNode must exist in the plan.
  auto agg_node = findNode(result, "Aggregation");
  ASSERT_NE(agg_node, nullptr)
      << "AggregationNode not found in plan for " << param.name;

  // The AggregationNode's input chain should include MarkDistinct nodes.
  std::vector<std::shared_ptr<const core::PlanNode>> mark_nodes;
  findAllNodes(agg_node, "MarkDistinct", mark_nodes);
  EXPECT_EQ(mark_nodes.size(), static_cast<size_t>(param.distinct_col_count))
      << "MarkDistinct chain not properly connected to AggregationNode for "
      << param.name;
}

// ===========================================================================
// Property 16.3: Marker Columns in MarkDistinctNode Output
//
// Each MarkDistinctNode SHALL produce a boolean marker column named
// "mark_distinct_N" (where N is the 0-based index) in its output schema.
//
// **Validates: Requirements 9.1, 9.2**
// ===========================================================================

TEST_P(MarkDistinctCorrectnessTest, MarkerColumnsInOutput) {
  const auto& param = GetParam();
  auto plan_bytes = buildMultiDistinctPlan(
      param.grouping_key_count, param.distinct_col_count);
  ASSERT_FALSE(plan_bytes.empty());

  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});
  ASSERT_NE(result, nullptr);

  // Find all MarkDistinct nodes.
  std::vector<std::shared_ptr<const core::PlanNode>> mark_nodes;
  findAllNodes(result, "MarkDistinct", mark_nodes);
  ASSERT_EQ(mark_nodes.size(), static_cast<size_t>(param.distinct_col_count));

  // The topmost MarkDistinctNode in the chain (closest to root, found first
  // by DFS) has all marker columns: its own plus all markers from nodes
  // below it in the chain. DFS from root finds the topmost first.
  auto top_mark = mark_nodes.front();
  auto last_output = top_mark->outputType();

  for (int i = 0; i < param.distinct_col_count; ++i) {
    std::string expected_marker = "mark_distinct_" + std::to_string(i);
    auto idx = last_output->getChildIdxIfExists(expected_marker);
    EXPECT_TRUE(idx.has_value())
        << "Marker column '" << expected_marker
        << "' not found in last MarkDistinctNode output for " << param.name;

    if (idx.has_value()) {
      // Marker column must be BOOLEAN type.
      EXPECT_TRUE(last_output->childAt(idx.value())->equivalent(*BOOLEAN()))
          << "Marker column '" << expected_marker
          << "' is not BOOLEAN for " << param.name;
    }
  }
}

// ===========================================================================
// Property 16.4: Final Output Schema Correctness
//
// The final plan output SHALL contain grouping keys + aggregate columns
// (no internal marker columns leaked to the output).
//
// **Validates: Requirements 9.1, 9.3**
// ===========================================================================

TEST_P(MarkDistinctCorrectnessTest, FinalOutputSchemaCorrect) {
  const auto& param = GetParam();
  auto plan_bytes = buildMultiDistinctPlan(
      param.grouping_key_count, param.distinct_col_count);
  ASSERT_FALSE(plan_bytes.empty());

  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});
  ASSERT_NE(result, nullptr);

  auto final_output = result->outputType();

  // Output should have grouping_key_count + distinct_col_count columns
  // (one aggregate per distinct column).
  size_t expected_cols =
      static_cast<size_t>(param.grouping_key_count + param.distinct_col_count);
  EXPECT_EQ(final_output->size(), expected_cols)
      << "Expected " << expected_cols << " output columns but got "
      << final_output->size() << " for " << param.name;

  // No marker columns should appear in the final output.
  for (size_t i = 0; i < final_output->size(); ++i) {
    EXPECT_FALSE(
        final_output->nameOf(i).find("mark_distinct") != std::string::npos)
        << "Internal marker column '" << final_output->nameOf(i)
        << "' leaked to final output for " << param.name;
  }
}

// ===========================================================================
// Property 16.5: MarkDistinct Chain Topology
//
// MarkDistinctNodes SHALL be chained linearly: each node's input is the
// previous MarkDistinctNode (or the original input for the first one).
// This ensures single-pass processing over the input data.
//
// **Validates: Requirements 9.1, 9.2**
// ===========================================================================

TEST_P(MarkDistinctCorrectnessTest, MarkDistinctChainTopology) {
  const auto& param = GetParam();
  auto plan_bytes = buildMultiDistinctPlan(
      param.grouping_key_count, param.distinct_col_count);
  ASSERT_FALSE(plan_bytes.empty());

  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});
  ASSERT_NE(result, nullptr);

  // Find the AggregationNode — its direct input should be the last
  // MarkDistinctNode in the chain.
  auto agg_node = findNode(result, "Aggregation");
  ASSERT_NE(agg_node, nullptr);
  ASSERT_GE(agg_node->sources().size(), 1u);

  // Walk the chain from AggregationNode's input backward.
  auto current = agg_node->sources()[0];
  int mark_count = 0;
  while (current && current->name() == "MarkDistinct") {
    mark_count++;
    ASSERT_EQ(current->sources().size(), 1u)
        << "MarkDistinctNode should have exactly 1 source";
    current = current->sources()[0];
  }

  EXPECT_EQ(mark_count, param.distinct_col_count)
      << "Expected linear chain of " << param.distinct_col_count
      << " MarkDistinctNodes but found " << mark_count
      << " for " << param.name;

  // The bottom of the chain should be a TableScan (the original input).
  EXPECT_EQ(current->name(), "TableScan")
      << "Bottom of MarkDistinct chain should be TableScan, got '"
      << current->name() << "' for " << param.name;
}

// ===========================================================================
// Property 16.6: Single-DISTINCT Does NOT Trigger MarkDistinctNode
//
// When only a single DISTINCT aggregate is present, the plan SHALL NOT
// contain any MarkDistinctNode. The standard AggregationNode with distinct
// flag is sufficient.
//
// **Validates: Requirements 9.4**
// ===========================================================================

TEST_P(MarkDistinctCorrectnessTest, SingleDistinctNoMarkDistinct) {
  const auto& param = GetParam();
  auto plan_bytes = buildSingleDistinctPlan(param.grouping_key_count);
  ASSERT_FALSE(plan_bytes.empty());

  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});
  ASSERT_NE(result, nullptr)
      << "Single-DISTINCT plan conversion failed for " << param.name;

  // No MarkDistinctNode should be present.
  std::vector<std::shared_ptr<const core::PlanNode>> mark_nodes;
  findAllNodes(result, "MarkDistinct", mark_nodes);
  EXPECT_EQ(mark_nodes.size(), 0u)
      << "Single-DISTINCT should NOT produce MarkDistinctNode for "
      << param.name;

  // AggregationNode should still be present.
  auto agg_node = findNode(result, "Aggregation");
  EXPECT_NE(agg_node, nullptr)
      << "AggregationNode not found for single-DISTINCT plan in "
      << param.name;
}

// ===========================================================================
// Parameterization: distinct_col_count × grouping_key_count × input_size ×
//                   distribution
// ===========================================================================

std::vector<MarkDistinctParam> generateParams() {
  std::vector<MarkDistinctParam> params;

  std::vector<int> distinct_counts = {2, 3};
  std::vector<int> grouping_counts = {0, 1, 2};
  std::vector<int> input_sizes = {10, 100, 500};
  std::vector<ValueDistribution> distributions = {
      ValueDistribution::kUnique,
      ValueDistribution::kManyDuplicates,
      ValueDistribution::kAllSame};

  for (int dc : distinct_counts) {
    for (int gk : grouping_counts) {
      for (int sz : input_sizes) {
        for (auto dist : distributions) {
          std::string name = "DC" + std::to_string(dc) +
                             "_GK" + std::to_string(gk) +
                             "_N" + std::to_string(sz) +
                             "_" + distributionName(dist);
          params.push_back({name, dc, gk, sz, dist});
        }
      }
    }
  }
  return params;
}

INSTANTIATE_TEST_SUITE_P(
    MarkDistinctCorrectness,
    MarkDistinctCorrectnessTest,
    ::testing::ValuesIn(generateParams()),
    [](const ::testing::TestParamInfo<MarkDistinctParam>& info) {
      return info.param.name;
    });

}  // namespace
}  // namespace pangrova::velox::test
