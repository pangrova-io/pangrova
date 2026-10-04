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
#include "../CanonicalTestPlan.h"
#include "velox/expression/FunctionRegistrySnapshot.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "../TestMemoryHelper.h"
#include "CanonicalTaskExecution.h"
#include "CanonicalOutputAssertions.h"
#include "velox/plan/VeloxPlanConverter.h"

#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"

#include "velox/core/PlanNode.h"
#include "velox/vector/DecodedVector.h"

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
  schema->mutable_struct_()->add_types()->mutable_i64()
      ->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
}

void setJoinKeyRef(
    substrait::Expression::FieldReference* ref,
    int field_index) {
  ref->mutable_root_reference();
  ref->mutable_direct_reference()->mutable_struct_field()->set_field(
      field_index);
}

void addStandardJoinKey(
    substrait::MergeJoinRel* join,
    int left_index,
    int right_index) {
  auto* key = join->add_keys();
  setJoinKeyRef(key->mutable_left(), left_index);
  setJoinKeyRef(key->mutable_right(), right_index);
  key->mutable_comparison()->set_simple(
      substrait::ComparisonJoinKey::SIMPLE_COMPARISON_TYPE_EQ);
}

substrait::ReadRel* addMergeJoinInput(
    substrait::Rel* input,
    bool already_sorted,
    int keyCount) {
  if (!already_sorted) {
    return input->mutable_read();
  }
  auto* sort = input->mutable_sort();
  for (int key = 0; key < keyCount; ++key) {
    auto* sort_field = sort->add_sorts();
    setJoinKeyRef(sort_field->mutable_expr()->mutable_selection(), key);
    sort_field->set_direction(substrait::SortField::SORT_DIRECTION_ASC_NULLS_FIRST);
  }
  return sort->mutable_input()->mutable_read();
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
  auto* left_read = addMergeJoinInput(
      merge_join->mutable_left(), inputs_pre_sorted, key_count);
  left_read->mutable_named_table()->add_names("left_table");
  auto* left_schema = left_read->mutable_base_schema();
  for (int i = 0; i < key_count; ++i) {
    addI64Column(left_schema, "left_key_" + std::to_string(i));
  }
  addI64Column(left_schema, "left_payload");

  // Right input: ReadRel with key_count keys + 1 payload
  auto* right_read = addMergeJoinInput(
      merge_join->mutable_right(), inputs_pre_sorted, key_count);
  right_read->mutable_named_table()->add_names("right_table");
  auto* right_schema = right_read->mutable_base_schema();
  for (int i = 0; i < key_count; ++i) {
    addI64Column(right_schema, "right_key_" + std::to_string(i));
  }
  addI64Column(right_schema, "right_payload");

  // Set join keys: left_key_i joins with right_key_i
  for (int i = 0; i < key_count; ++i) {
    addStandardJoinKey(merge_join, i, i);
  }

  // Set join type
  merge_join->set_type(join_type);

  std::string bytes;
  pangrova::test::canonicalTestPlan(plan).SerializeToString(&bytes);
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

using JoinCell = std::optional<int64_t>;
using JoinRows = std::vector<std::vector<JoinCell>>;

JoinRows mergeInputValues(bool left, int keys) {
  const std::vector<JoinCell> values = left ? std::vector<JoinCell>{std::nullopt, 1, 2, 1}
                                          : std::vector<JoinCell>{1, std::nullopt, 3, 1};
  JoinRows rows;
  for (size_t row = 0; row < values.size(); ++row) {
    std::vector<JoinCell> result;
    for (int key = 0; key < keys; ++key) result.push_back(values[row] ? JoinCell{*values[row] + key} : JoinCell{});
    result.push_back((left ? 10 : 20) + static_cast<int64_t>(row));
    rows.push_back(std::move(result));
  }
  return rows;
}

substrait::Plan actualMergePlan(const MergeJoinPlanConversionParam& param) {
  const auto bytes = buildMergeJoinPlan(param.join_type, param.inputs_pre_sorted, param.key_count);
  substrait::Plan plan;
  if (!plan.ParseFromArray(bytes.data(), bytes.size())) throw std::runtime_error("Invalid merge test plan");
  auto* merge = plan.mutable_relations(0)->mutable_root()->mutable_input()->mutable_merge_join();
  for (bool left : {true, false}) {
    auto* input = left ? merge->mutable_left() : merge->mutable_right();
    auto* read = param.inputs_pre_sorted ? input->mutable_sort()->mutable_input()->mutable_read()
                                        : input->mutable_read();
    read->clear_named_table();
    for (auto& type : *read->mutable_base_schema()->mutable_struct_()->mutable_types()) {
      type.mutable_i64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
    }
    for (const auto& row : mergeInputValues(left, param.key_count)) {
      auto* record = read->mutable_virtual_table()->add_expressions();
      for (const auto& value : row) {
        auto* literal = record->add_fields()->mutable_literal();
        if (value) literal->set_i64(*value);
        else literal->mutable_null()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
      }
    }
  }
  return plan;
}

JoinRows expectedMergeRows(const MergeJoinPlanConversionParam& param) {
  const auto left = mergeInputValues(true, param.key_count), right = mergeInputValues(false, param.key_count);
  JoinRows rows;
  std::vector<bool> matchedRight(right.size(), false);
  for (const auto& probe : left) {
    bool matched = false;
    for (size_t i = 0; i < right.size(); ++i) {
      bool equal = true;
      for (int key = 0; key < param.key_count; ++key) equal &= probe[key] && right[i][key] && probe[key] == right[i][key];
      if (!equal) continue;
      auto row = probe;
      row.insert(row.end(), right[i].begin(), right[i].end());
      rows.push_back(std::move(row));
      matched = matchedRight[i] = true;
    }
    if (!matched && (param.join_type == substrait::MergeJoinRel::JOIN_TYPE_LEFT ||
                    param.join_type == substrait::MergeJoinRel::JOIN_TYPE_OUTER)) {
      auto row = probe;
      row.resize(2 * (param.key_count + 1));
      rows.push_back(std::move(row));
    }
  }
  if (param.join_type == substrait::MergeJoinRel::JOIN_TYPE_RIGHT ||
      param.join_type == substrait::MergeJoinRel::JOIN_TYPE_OUTER) {
    for (size_t i = 0; i < right.size(); ++i) if (!matchedRight[i]) {
      std::vector<JoinCell> row(param.key_count + 1);
      row.insert(row.end(), right[i].begin(), right[i].end());
      rows.push_back(std::move(row));
    }
  }
  std::sort(rows.begin(), rows.end());
  return rows;
}

JoinRows executeMergeRows(const core::PlanNodePtr& plan, const FunctionRegistryOwner& registry) {
  const auto execution = executeCanonicalTask(plan, registry);
  JoinRows rows;
  for (const auto& batch : execution.batches) {
    for (vector_size_t row = 0; row < batch->size(); ++row) {
      std::vector<JoinCell> values;
      for (const auto& child : batch->children()) {
        DecodedVector decoded(*child);
        values.push_back(decoded.isNullAt(row) ? JoinCell{} : JoinCell{decoded.valueAt<int64_t>(row)});
      }
      rows.push_back(std::move(values));
    }
  }
  std::sort(rows.begin(), rows.end());
  return rows;
}

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

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto plan = converter.convert(plan_bytes, {});
  ASSERT_NE(plan, nullptr);

  int order_by_count = countNodes(plan, "OrderBy");

  // Sorted inputs carry two actual SortRels; neither may be duplicated.
  // Unsorted inputs have no producer SortRel; two must be inserted.
  EXPECT_EQ(order_by_count, 2) << param.name;
  const auto merge = std::dynamic_pointer_cast<const core::MergeJoinNode>(plan);
  ASSERT_NE(merge, nullptr);
  for (const auto& source : merge->sources()) {
    EXPECT_EQ(source->name(), param.inputs_pre_sorted ? "Project" : "OrderBy");
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

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto plan = converter.convert(plan_bytes, {});
  ASSERT_NE(plan, nullptr);

  // Collect all node names in pre-order.
  std::vector<std::string> node_names;
  collectNodeNames(plan, node_names);

  if (param.inputs_pre_sorted) {
    // The right input rebases its local ordinals into the combined schema.
    EXPECT_EQ(node_names, (std::vector<std::string>{"MergeJoin", "Project", "OrderBy", "TableScan",
        "Project", "Project", "OrderBy", "TableScan"}));
  } else {
    EXPECT_EQ(node_names, (std::vector<std::string>{"MergeJoin", "OrderBy", "Project", "TableScan",
        "OrderBy", "Project", "Project", "TableScan"}));
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

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto plan = converter.convert(plan_bytes, {});
  ASSERT_NE(plan, nullptr);

  auto output_type = plan->outputType();

  // Verify left-side columns are present at the beginning.
  for (int i = 0; i < param.key_count; ++i) {
    expectCanonicalOutputField(converter, output_type, i, "left_key_" + std::to_string(i));
  }
  expectCanonicalOutputField(converter, output_type, param.key_count, "left_payload");

  // Verify right-side columns follow left-side columns.
  int right_start = param.key_count + 1;
  for (int i = 0; i < param.key_count; ++i) {
    expectCanonicalOutputField(converter, output_type, right_start + i,
        "right_key_" + std::to_string(i));
  }
  expectCanonicalOutputField(converter, output_type, right_start + param.key_count, "right_payload");

  // Verify all columns are BIGINT type.
  for (size_t i = 0; i < output_type->size(); ++i) {
    EXPECT_TRUE(output_type->childAt(i)->isBigint())
        << "Column at index " << i << " should be BIGINT for " << param.name;
  }
}

TEST_P(MergeJoinPlanConversionTest, ActualValuesPreserveDuplicatesNullsAndOuterRows) {
  const auto& param = GetParam();
  const auto serialized = pangrova::test::canonicalTestPlan(actualMergePlan(param)).SerializeAsString();
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  const auto plan = converter.convert({serialized.begin(), serialized.end()}, {});
  EXPECT_EQ(countNodes(plan, "OrderBy"), 2);
  EXPECT_EQ(executeMergeRows(plan, converter_registry), expectedMergeRows(param));
}

TEST(MergeJoinOrderingNegativeTest, WrongDirectionNullOrderAndIncompletePrefixRequireNewSorts) {
  pangrova::test::testMemoryManager();
  const MergeJoinPlanConversionParam param{"actual-order-contract", substrait::MergeJoinRel::JOIN_TYPE_OUTER, true, 2};
  for (int invalid = 0; invalid < 3; ++invalid) {
    auto proto = actualMergePlan(param);
    auto* merge = proto.mutable_relations(0)->mutable_root()->mutable_input()->mutable_merge_join();
    for (auto* input : {merge->mutable_left(), merge->mutable_right()}) {
      auto* sort = input->mutable_sort();
      if (invalid == 2) sort->mutable_sorts()->RemoveLast();
      else for (auto& key : *sort->mutable_sorts()) key.set_direction(invalid == 0
          ? substrait::SortField::SORT_DIRECTION_DESC_NULLS_FIRST
          : substrait::SortField::SORT_DIRECTION_ASC_NULLS_LAST);
    }
    const auto serialized = pangrova::test::canonicalTestPlan(proto).SerializeAsString();
    const auto converter_registry =
        facebook::velox::exec::FunctionRegistrySnapshot::capture();
    VeloxPlanConverter converter(converter_registry);
    const auto plan = converter.convert({serialized.begin(), serialized.end()}, {});
    EXPECT_EQ(countNodes(plan, "OrderBy"), 4);
    EXPECT_EQ(executeMergeRows(plan, converter_registry), expectedMergeRows(param));
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
