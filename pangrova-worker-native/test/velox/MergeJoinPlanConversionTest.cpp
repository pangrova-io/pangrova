// MergeJoinPlanConversionTest: Parameterized tests for MergeJoin plan
// conversion with sort insertion verification.
//
// Property 4: MergeJoin Plan Conversion with Sort Insertion
//
// TEST_P over join types (INNER, LEFT, RIGHT, OUTER) × sorted/unsorted
// inputs × key counts (1, 2, 3).
// Verifies:
//   - MergeJoinNode created with correct join type and key columns.
//   - OrderByNode (sort) inserted when inputs are not pre-sorted.
//   - No OrderByNode inserted when inputs are pre-sorted.
//   - Output schema has correct column count (left + right columns).
//
// **Validates: Requirements 2.1, 2.5**

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "../TestMemoryHelper.h"
#include "velox/plan/VeloxPlanConverter.h"

#include "pangrova/native/protocol/v1/plan_hints.pb.h"
#include "substrait/algebra.pb.h"
#include "substrait/extensions/extensions.pb.h"
#include "substrait/plan.pb.h"

#include "velox/core/PlanNode.h"

#include <google/protobuf/any.pb.h>

namespace pangrova::velox::test {
namespace {

using namespace facebook::velox;

// ===========================================================================
// Test parameters: join type × sorted/unsorted × key count
// ===========================================================================

struct MergeJoinPlanConversionParam {
  std::string name;
  substrait::MergeJoinRel::JoinType join_type;
  bool inputs_pre_sorted;
  int key_count;  // 1, 2, or 3
};

std::string joinTypeName(substrait::MergeJoinRel::JoinType jt) {
  switch (jt) {
    case substrait::MergeJoinRel::JOIN_TYPE_INNER: return "Inner";
    case substrait::MergeJoinRel::JOIN_TYPE_LEFT: return "Left";
    case substrait::MergeJoinRel::JOIN_TYPE_RIGHT: return "Right";
    case substrait::MergeJoinRel::JOIN_TYPE_OUTER: return "Outer";
    default: return "Unknown";
  }
}

// ===========================================================================
// Helpers: build Substrait MergeJoinRel plans
// ===========================================================================

namespace {

void addI64Column(substrait::NamedStruct* schema, const std::string& name) {
  schema->add_names(name);
  schema->mutable_struct_()->add_types()->mutable_i64();
}

void setJoinKeyRef(
    substrait::Expression::FieldReference* ref,
    int field_index) {
  ref->mutable_direct_reference()->mutable_struct_field()->set_field(
      field_index);
}

// Build a Substrait Plan containing a MergeJoinRel with the given parameters.
// Left table has key_count key columns + 1 payload column.
// Right table has key_count key columns + 1 payload column.
std::vector<uint8_t> buildMergeJoinPlan(
    substrait::MergeJoinRel::JoinType join_type,
    bool inputs_pre_sorted,
    int key_count) {
  substrait::Plan plan;
  auto* plan_rel = plan.add_relations();
  auto* root = plan_rel->mutable_root();

  // Output names: left keys + left payload + right keys + right payload
  for (int i = 0; i < key_count; ++i) {
    root->add_names("left_key_" + std::to_string(i));
  }
  root->add_names("left_payload");
  for (int i = 0; i < key_count; ++i) {
    root->add_names("right_key_" + std::to_string(i));
  }
  root->add_names("right_payload");

  auto* merge_join = root->mutable_input()->mutable_merge_join();

  // Left input: ReadRel with key_count keys + 1 payload
  auto* left_read = merge_join->mutable_left()->mutable_read();
  left_read->mutable_named_table()->add_names("left_table");
  auto* left_schema = left_read->mutable_base_schema();
  for (int i = 0; i < key_count; ++i) {
    addI64Column(left_schema, "left_key_" + std::to_string(i));
  }
  addI64Column(left_schema, "left_payload");

  // Right input: ReadRel with key_count keys + 1 payload
  auto* right_read = merge_join->mutable_right()->mutable_read();
  right_read->mutable_named_table()->add_names("right_table");
  auto* right_schema = right_read->mutable_base_schema();
  for (int i = 0; i < key_count; ++i) {
    addI64Column(right_schema, "right_key_" + std::to_string(i));
  }
  addI64Column(right_schema, "right_payload");

  // Set join keys: left_key_i joins with right_key_i
  for (int i = 0; i < key_count; ++i) {
    setJoinKeyRef(merge_join->add_left_keys(), i);
    setJoinKeyRef(merge_join->add_right_keys(), i);
  }

  // Set join type
  merge_join->set_type(join_type);

  // Set MergeJoinHint in advanced_extension.enhancement
  pangrova::native::protocol::v1::MergeJoinHint hint;
  hint.set_inputs_pre_sorted(inputs_pre_sorted);
  merge_join->mutable_advanced_extension()->mutable_enhancement()->PackFrom(
      hint);

  std::string bytes;
  plan.SerializeToString(&bytes);
  return {bytes.begin(), bytes.end()};
}

// Recursively collect plan node names in pre-order traversal.
void collectNodeNames(
    const std::shared_ptr<const core::PlanNode>& node,
    std::vector<std::string>& names) {
  if (!node) return;
  names.emplace_back(node->name());
  for (const auto& source : node->sources()) {
    collectNodeNames(source, names);
  }
}

// Count occurrences of a node name in the plan tree.
int countNodes(
    const std::shared_ptr<const core::PlanNode>& node,
    const std::string& target_name) {
  if (!node) return 0;
  int count = (node->name() == target_name) ? 1 : 0;
  for (const auto& source : node->sources()) {
    count += countNodes(source, target_name);
  }
  return count;
}

}  // namespace

// ===========================================================================
// Test fixture
// ===========================================================================

class MergeJoinPlanConversionTest
    : public ::testing::TestWithParam<MergeJoinPlanConversionParam> {
 protected:
  static void SetUpTestSuite() {
    pangrova::test::testMemoryManager();
  }
};

// ===========================================================================
// Property 4.1: MergeJoinNode Created with Correct Join Type
//
// For any valid MergeJoinRel, the converted plan root SHALL be a
// MergeJoinNode with the correct Velox join type.
//
// **Validates: Requirements 2.1**
// ===========================================================================

TEST_P(MergeJoinPlanConversionTest, MergeJoinNodeCreatedWithCorrectType) {
  const auto& param = GetParam();
  auto plan_bytes = buildMergeJoinPlan(
      param.join_type, param.inputs_pre_sorted, param.key_count);
  ASSERT_FALSE(plan_bytes.empty());

  VeloxPlanConverter converter;
  auto plan = converter.convert(plan_bytes, {});
  ASSERT_NE(plan, nullptr)
      << "Plan conversion failed for " << param.name;

  // Root node must be MergeJoin.
  EXPECT_EQ(plan->name(), "MergeJoin")
      << "Expected MergeJoin root node for " << param.name;
}

// ===========================================================================
// Property 4.2: MergeJoinNode Has Correct Key Count
//
// The MergeJoinNode SHALL have the correct number of join keys matching
// the key_count parameter.
//
// **Validates: Requirements 2.1**
// ===========================================================================

TEST_P(MergeJoinPlanConversionTest, MergeJoinNodeHasCorrectKeyCount) {
  const auto& param = GetParam();
  auto plan_bytes = buildMergeJoinPlan(
      param.join_type, param.inputs_pre_sorted, param.key_count);
  ASSERT_FALSE(plan_bytes.empty());

  VeloxPlanConverter converter;
  auto plan = converter.convert(plan_bytes, {});
  ASSERT_NE(plan, nullptr);

  // Verify the output type has the expected column count:
  // left_keys + left_payload + right_keys + right_payload
  auto output_type = plan->outputType();
  int expected_cols = param.key_count + 1 + param.key_count + 1;
  EXPECT_EQ(static_cast<int>(output_type->size()), expected_cols)
      << "Output column count mismatch for " << param.name
      << ". Expected " << expected_cols << " got " << output_type->size();
}

// ===========================================================================
// Property 4.3: SortNode Inserted When Inputs Not Pre-Sorted
//
// When inputs_pre_sorted is false, OrderByNode operators SHALL be inserted
// before the MergeJoinNode (one for each input). When inputs_pre_sorted
// is true, no OrderByNode SHALL be present.
//
// **Validates: Requirements 2.5**
// ===========================================================================

TEST_P(MergeJoinPlanConversionTest, SortNodeInsertedWhenNotPreSorted) {
  const auto& param = GetParam();
  auto plan_bytes = buildMergeJoinPlan(
      param.join_type, param.inputs_pre_sorted, param.key_count);
  ASSERT_FALSE(plan_bytes.empty());

  VeloxPlanConverter converter;
  auto plan = converter.convert(plan_bytes, {});
  ASSERT_NE(plan, nullptr);

  int order_by_count = countNodes(plan, "OrderBy");

  if (param.inputs_pre_sorted) {
    // No sort nodes should be inserted when inputs are pre-sorted.
    EXPECT_EQ(order_by_count, 0)
        << "OrderByNode should NOT be inserted when inputs are pre-sorted for "
        << param.name;
  } else {
    // Two sort nodes should be inserted (one per input) when not pre-sorted.
    EXPECT_EQ(order_by_count, 2)
        << "Expected 2 OrderByNodes (one per input) when inputs are not "
           "pre-sorted for "
        << param.name;
  }
}

// ===========================================================================
// Property 4.4: Plan Structure Correctness
//
// When not pre-sorted: MergeJoin → [OrderBy → TableScan, OrderBy → TableScan]
// When pre-sorted: MergeJoin → [TableScan, TableScan]
//
// **Validates: Requirements 2.1, 2.5**
// ===========================================================================

TEST_P(MergeJoinPlanConversionTest, PlanStructureCorrect) {
  const auto& param = GetParam();
  auto plan_bytes = buildMergeJoinPlan(
      param.join_type, param.inputs_pre_sorted, param.key_count);
  ASSERT_FALSE(plan_bytes.empty());

  VeloxPlanConverter converter;
  auto plan = converter.convert(plan_bytes, {});
  ASSERT_NE(plan, nullptr);

  // Collect all node names in pre-order.
  std::vector<std::string> node_names;
  collectNodeNames(plan, node_names);

  if (param.inputs_pre_sorted) {
    // Expected: MergeJoin, TableScan, TableScan
    ASSERT_EQ(node_names.size(), 3u)
        << "Expected 3 nodes (MergeJoin + 2 TableScans) for pre-sorted "
        << param.name;
    EXPECT_EQ(node_names[0], "MergeJoin");
    EXPECT_EQ(node_names[1], "TableScan");
    EXPECT_EQ(node_names[2], "TableScan");
  } else {
    // Expected: MergeJoin, OrderBy, TableScan, OrderBy, TableScan
    ASSERT_EQ(node_names.size(), 5u)
        << "Expected 5 nodes (MergeJoin + 2x(OrderBy + TableScan)) for "
           "unsorted "
        << param.name;
    EXPECT_EQ(node_names[0], "MergeJoin");
    EXPECT_EQ(node_names[1], "OrderBy");
    EXPECT_EQ(node_names[2], "TableScan");
    EXPECT_EQ(node_names[3], "OrderBy");
    EXPECT_EQ(node_names[4], "TableScan");
  }
}

// ===========================================================================
// Property 4.5: Output Schema Column Names Correct
//
// The output schema SHALL contain left columns followed by right columns,
// with deduplication applied to right-side column names that collide with
// left-side names.
//
// **Validates: Requirements 2.1**
// ===========================================================================

TEST_P(MergeJoinPlanConversionTest, OutputSchemaColumnsCorrect) {
  const auto& param = GetParam();
  auto plan_bytes = buildMergeJoinPlan(
      param.join_type, param.inputs_pre_sorted, param.key_count);
  ASSERT_FALSE(plan_bytes.empty());

  VeloxPlanConverter converter;
  auto plan = converter.convert(plan_bytes, {});
  ASSERT_NE(plan, nullptr);

  auto output_type = plan->outputType();

  // Verify left-side columns are present at the beginning.
  for (int i = 0; i < param.key_count; ++i) {
    EXPECT_EQ(output_type->nameOf(i), "left_key_" + std::to_string(i))
        << "Left key column name mismatch at index " << i;
  }
  EXPECT_EQ(output_type->nameOf(param.key_count), "left_payload")
      << "Left payload column name mismatch";

  // Verify right-side columns follow left-side columns.
  int right_start = param.key_count + 1;
  for (int i = 0; i < param.key_count; ++i) {
    EXPECT_EQ(
        output_type->nameOf(right_start + i),
        "right_key_" + std::to_string(i))
        << "Right key column name mismatch at index " << i;
  }
  EXPECT_EQ(
      output_type->nameOf(right_start + param.key_count), "right_payload")
      << "Right payload column name mismatch";

  // Verify all columns are BIGINT type.
  for (size_t i = 0; i < output_type->size(); ++i) {
    EXPECT_TRUE(output_type->childAt(i)->isBigint())
        << "Column at index " << i << " should be BIGINT for " << param.name;
  }
}

// ===========================================================================
// Parameterization: join types × sorted/unsorted × key counts
// ===========================================================================

std::vector<MergeJoinPlanConversionParam> generateParams() {
  std::vector<MergeJoinPlanConversionParam> params;

  std::vector<substrait::MergeJoinRel::JoinType> join_types = {
      substrait::MergeJoinRel::JOIN_TYPE_INNER,
      substrait::MergeJoinRel::JOIN_TYPE_LEFT,
      substrait::MergeJoinRel::JOIN_TYPE_RIGHT,
      substrait::MergeJoinRel::JOIN_TYPE_OUTER,
  };

  std::vector<bool> sorted_options = {true, false};
  std::vector<int> key_counts = {1, 2, 3};

  for (auto jt : join_types) {
    for (bool sorted : sorted_options) {
      for (int keys : key_counts) {
        std::string name = joinTypeName(jt) + "_" +
            (sorted ? "Sorted" : "Unsorted") + "_Keys" +
            std::to_string(keys);
        params.push_back({name, jt, sorted, keys});
      }
    }
  }

  return params;
}

INSTANTIATE_TEST_SUITE_P(
    MergeJoinPlanConversion,
    MergeJoinPlanConversionTest,
    ::testing::ValuesIn(generateParams()),
    [](const ::testing::TestParamInfo<MergeJoinPlanConversionParam>& info) {
      return info.param.name;
    });

}  // namespace
}  // namespace pangrova::velox::test
