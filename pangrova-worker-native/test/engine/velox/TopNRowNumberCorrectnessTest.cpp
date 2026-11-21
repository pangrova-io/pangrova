// TopNRowNumberCorrectnessTest: Parameterized tests for TopNRowNumberNode
// pattern detection and correctness.
//
// Property 14 (TopN subset): TopNRowNumberNode Correctness
//
// TEST_P over partition key counts (1, 2) × order key counts (1, 2) ×
// limit values (1, 5, 50) × input sizes (10, 100, 1000).
// Verifies:
//   - TopNRowNumberNode pattern detection: row_number() + filter `rn <= K`
//     is correctly detected and converted to TopNRowNumberNode.
//   - TopNRowNumberNode has the correct limit value K.
//   - TopNRowNumberNode has the correct partition keys and sorting keys.
//   - Plan structure: TopNRowNumberNode replaces RowNumberNode + FilterNode.
//
// **Validates: Requirements 7.2, 7.3**

#include <gtest/gtest.h>
#include "CanonicalTestPlan.h"
#include "velox/expression/FunctionRegistrySnapshot.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "TestMemoryHelper.h"
#include "CanonicalOutputAssertions.h"
#include "CanonicalTaskExecution.h"
#include "engine/velox/plan/VeloxPlanConverter.h"

#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"

#include "velox/core/PlanNode.h"
#include "velox/vector/DecodedVector.h"

namespace pangrova::velox::test {
namespace {

using namespace facebook::velox;

// ===========================================================================
// Test parameters: partition key count × order key count × limit × input size
// ===========================================================================

struct TopNRowNumberParam {
  std::string name;
  int partition_key_count;  // 1 or 2
  int order_key_count;      // 1 or 2
  int32_t limit;            // 1, 5, or 50
  int input_size;           // 10, 100, or 1000; actual Values oracle below.
};

// ===========================================================================
// Helpers: build Substrait plans with row_number() + filter (rn <= K)
// ===========================================================================

namespace {

void addI64Column(substrait::NamedStruct* schema, const std::string& name) {
  schema->add_names(name);
  schema->mutable_struct_()->add_types()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
}

void setFieldRef(substrait::Expression* expr, int field_index) {
  auto* ref = expr->mutable_selection();
  ref->mutable_root_reference();
  ref->mutable_direct_reference()->mutable_struct_field()->set_field(
      field_index);
}

std::vector<int64_t> inputRow(int row, int partitionKeys, int orderKeys, int size) {
  std::vector<int64_t> values;
  for (int key = 0; key < partitionKeys; ++key) values.push_back(key == 0 ? row % 3 : (row / 3) % 2);
  for (int key = 0; key < orderKeys; ++key) values.push_back(size - row + key);
  values.push_back(row);
  return values;
}

// Build a Substrait Plan containing:
//   FilterRel(condition: row_number_col <= limit)
//     └── ConsistentPartitionWindowRel(row_number(), partition_by, order_by)
//          └── ReadRel(table with partition_keys + order_keys + payload)
//
// The plan exercises the TopNRowNumberNode pattern detection in convertFilterRel.
std::vector<uint8_t> buildTopNRowNumberPlan(
    int partition_key_count,
    int order_key_count,
    int64_t limit,
    int inputSize = 0) {
  substrait::Plan plan;

  // Register functions:
  //   anchor 0 → row_number (window function)
  //   anchor 1 → lte (comparison for filter)
  auto* ext_rn = plan.add_extensions();
  ext_rn->mutable_extension_function()->set_function_anchor(0);
  ext_rn->mutable_extension_function()->set_name("row_number:");

  auto* ext_lte = plan.add_extensions();
  ext_lte->mutable_extension_function()->set_function_anchor(1);
  ext_lte->mutable_extension_function()->set_name("lte:i64_i64");

  auto* plan_rel = plan.add_relations();
  auto* root = plan_rel->mutable_root();

  // Output names: partition_keys + order_keys + payload + row_number
  for (int i = 0; i < partition_key_count; ++i) {
    root->add_names("pk_" + std::to_string(i));
  }
  for (int i = 0; i < order_key_count; ++i) {
    root->add_names("ok_" + std::to_string(i));
  }
  root->add_names("payload");
  root->add_names("row_number");

  // Build: FilterRel → WindowRel → ReadRel
  auto* filter_rel = root->mutable_input()->mutable_filter();

  // --- WindowRel (ConsistentPartitionWindowRel) ---
  auto* window_rel = filter_rel->mutable_input()->mutable_window();

  // Input: ReadRel with partition_keys + order_keys + payload
  auto* read_rel = window_rel->mutable_input()->mutable_read();
  read_rel->mutable_named_table()->add_names("input_table");
  auto* schema = read_rel->mutable_base_schema();
  for (int i = 0; i < partition_key_count; ++i) {
    addI64Column(schema, "pk_" + std::to_string(i));
  }
  for (int i = 0; i < order_key_count; ++i) {
    addI64Column(schema, "ok_" + std::to_string(i));
  }
  addI64Column(schema, "payload");
  if (inputSize > 0) {
    read_rel->clear_named_table();
    for (int row = 0; row < inputSize; ++row) {
      auto* record = read_rel->mutable_virtual_table()->add_expressions();
      for (const auto value : inputRow(row, partition_key_count, order_key_count, inputSize)) {
        record->add_fields()->mutable_literal()->set_i64(value);
      }
    }
  }

  // Partition expressions: field references to partition key columns
  for (int i = 0; i < partition_key_count; ++i) {
    setFieldRef(window_rel->add_partition_expressions(), i);
  }

  // Sort fields: field references to order key columns
  for (int i = 0; i < order_key_count; ++i) {
    auto* sort_field = window_rel->add_sorts();
    setFieldRef(sort_field->mutable_expr(), partition_key_count + i);
    sort_field->set_direction(
        substrait::SortField::SORT_DIRECTION_ASC_NULLS_LAST);
  }

  // Window function: row_number() with default frame (no bounds set).
  // Not setting bounds → isDefaultRowsFrame returns true (first branch).
  auto* wf = window_rel->add_window_functions();
  wf->set_phase(substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT);
  wf->set_bounds_type(substrait::Expression_WindowFunction::BOUNDS_TYPE_ROWS);
  wf->set_function_reference(0);  // row_number
  wf->mutable_output_type()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);

  // --- FilterRel condition: row_number_col <= limit ---
  // The row_number column is the last column in the window output.
  // Window output schema: [pk_0, ..., pk_N-1, ok_0, ..., ok_M-1, payload, row_number]
  int rn_col_index = partition_key_count + order_key_count + 1;  // last column

  auto* cond = filter_rel->mutable_condition()->mutable_scalar_function();
  cond->set_function_reference(1);  // lte
  cond->mutable_output_type()->mutable_bool_()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);

  // Left arg: field reference to row_number column
  setFieldRef(cond->add_arguments()->mutable_value(), rn_col_index);

  // Right arg: literal integer (limit)
  cond->add_arguments()->mutable_value()->mutable_literal()->set_i64(limit);

  std::string bytes;
  pangrova::test::canonicalTestPlan(plan).SerializeToString(&bytes);
  return {bytes.begin(), bytes.end()};
}

// Recursively find a node by name in the plan tree.
std::shared_ptr<const core::PlanNode> findNode(
    const std::shared_ptr<const core::PlanNode>& node,
    const std::string& target_name) {
  if (!node) return nullptr;
  if (node->name() == target_name) return node;
  for (const auto& source : node->sources()) {
    auto found = findNode(source, target_name);
    if (found) return found;
  }
  return nullptr;
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

class TopNRowNumberCorrectnessTest
    : public ::testing::TestWithParam<TopNRowNumberParam> {
 protected:
  static void SetUpTestSuite() {
    pangrova::test::testMemoryManager();
  }
};

// ===========================================================================
// Property 14.1: TopNRowNumberNode Pattern Detected
//
// For any valid row_number() + filter(rn <= K) pattern with ORDER BY,
// the converter SHALL produce a TopNRowNumberNode instead of a
// RowNumberNode + FilterNode.
//
// **Validates: Requirements 7.2**
// ===========================================================================

TEST_P(TopNRowNumberCorrectnessTest, TopNRowNumberNodePatternDetected) {
  const auto& param = GetParam();
  auto plan_bytes = buildTopNRowNumberPlan(
      param.partition_key_count, param.order_key_count, param.limit);
  ASSERT_FALSE(plan_bytes.empty());

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto plan = converter.convert(plan_bytes, {});
  ASSERT_NE(plan, nullptr)
      << "Plan conversion failed for " << param.name;

  // TopNRowNumberNode must exist in the plan tree.
  auto topn_node = findNode(plan, "TopNRowNumber");
  EXPECT_NE(topn_node, nullptr)
      << "Expected TopNRowNumberNode in plan for " << param.name
      << ". Pattern detection should convert row_number() + filter(rn <= K) "
         "to TopNRowNumberNode.";
}

// ===========================================================================
// Property 14.2: TopNRowNumberNode Has Correct Limit
//
// The TopNRowNumberNode SHALL have limit = K matching the filter constant.
//
// **Validates: Requirements 7.2**
// ===========================================================================

TEST_P(TopNRowNumberCorrectnessTest, TopNRowNumberNodeHasCorrectLimit) {
  const auto& param = GetParam();
  auto plan_bytes = buildTopNRowNumberPlan(
      param.partition_key_count, param.order_key_count, param.limit);
  ASSERT_FALSE(plan_bytes.empty());

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto plan = converter.convert(plan_bytes, {});
  ASSERT_NE(plan, nullptr);

  auto topn_node = findNode(plan, "TopNRowNumber");
  ASSERT_NE(topn_node, nullptr)
      << "TopNRowNumberNode not found for " << param.name;

  // Cast to TopNRowNumberNode to access limit.
  auto topn = std::dynamic_pointer_cast<
      const facebook::velox::core::TopNRowNumberNode>(topn_node);
  ASSERT_NE(topn, nullptr)
      << "Failed to cast to TopNRowNumberNode for " << param.name;

  EXPECT_EQ(topn->limit(), param.limit)
      << "TopNRowNumberNode limit mismatch for " << param.name
      << ". Expected " << param.limit << " got " << topn->limit();
}

// ===========================================================================
// Property 14.3: TopNRowNumberNode Has Correct Partition Keys
//
// The TopNRowNumberNode SHALL have the correct number of partition keys
// matching the partition_key_count parameter.
//
// **Validates: Requirements 7.2, 7.3**
// ===========================================================================

TEST_P(TopNRowNumberCorrectnessTest, TopNRowNumberNodeHasCorrectPartitionKeys) {
  const auto& param = GetParam();
  auto plan_bytes = buildTopNRowNumberPlan(
      param.partition_key_count, param.order_key_count, param.limit);
  ASSERT_FALSE(plan_bytes.empty());

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto plan = converter.convert(plan_bytes, {});
  ASSERT_NE(plan, nullptr);

  auto topn_node = findNode(plan, "TopNRowNumber");
  ASSERT_NE(topn_node, nullptr)
      << "TopNRowNumberNode not found for " << param.name;

  auto topn = std::dynamic_pointer_cast<
      const facebook::velox::core::TopNRowNumberNode>(topn_node);
  ASSERT_NE(topn, nullptr);

  EXPECT_EQ(
      static_cast<int>(topn->partitionKeys().size()),
      param.partition_key_count)
      << "Partition key count mismatch for " << param.name;

  // Verify partition key names.
  for (int i = 0; i < param.partition_key_count; ++i) {
    expectCanonicalOutputField(converter, plan->outputType(), i, "pk_" + std::to_string(i));
    EXPECT_EQ(topn->partitionKeys()[i]->name(), physicalFieldAlias(i))
        << "Partition key name mismatch at index " << i
        << " for " << param.name;
  }
}

// ===========================================================================
// Property 14.4: TopNRowNumberNode Has Correct Sorting Keys
//
// The TopNRowNumberNode SHALL have the correct number of sorting keys
// matching the order_key_count parameter.
//
// **Validates: Requirements 7.2, 7.3**
// ===========================================================================

TEST_P(TopNRowNumberCorrectnessTest, TopNRowNumberNodeHasCorrectSortingKeys) {
  const auto& param = GetParam();
  auto plan_bytes = buildTopNRowNumberPlan(
      param.partition_key_count, param.order_key_count, param.limit);
  ASSERT_FALSE(plan_bytes.empty());

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto plan = converter.convert(plan_bytes, {});
  ASSERT_NE(plan, nullptr);

  auto topn_node = findNode(plan, "TopNRowNumber");
  ASSERT_NE(topn_node, nullptr)
      << "TopNRowNumberNode not found for " << param.name;

  auto topn = std::dynamic_pointer_cast<
      const facebook::velox::core::TopNRowNumberNode>(topn_node);
  ASSERT_NE(topn, nullptr);

  EXPECT_EQ(
      static_cast<int>(topn->sortingKeys().size()),
      param.order_key_count)
      << "Sorting key count mismatch for " << param.name;

  // Verify sorting key names.
  for (int i = 0; i < param.order_key_count; ++i) {
    expectCanonicalOutputField(converter, plan->outputType(),
        param.partition_key_count + i, "ok_" + std::to_string(i));
    EXPECT_EQ(
        topn->sortingKeys()[i]->name(),
        physicalFieldAlias(param.partition_key_count + i))
        << "Sorting key name mismatch at index " << i
        << " for " << param.name;
  }

  // Verify sorting orders are all ASC NULLS LAST.
  EXPECT_EQ(
      static_cast<int>(topn->sortingOrders().size()),
      param.order_key_count)
      << "Sorting order count mismatch for " << param.name;
  for (const auto& order : topn->sortingOrders()) {
    EXPECT_TRUE(order.isAscending());
    EXPECT_FALSE(order.isNullsFirst());
  }
}

// ===========================================================================
// Property 14.5: No RowNumberNode or FilterNode in Plan
//
// When TopNRowNumberNode pattern is detected, the plan SHALL NOT contain
// a separate RowNumberNode or FilterNode — they are replaced by the
// single TopNRowNumberNode.
//
// **Validates: Requirements 7.2**
// ===========================================================================

TEST_P(TopNRowNumberCorrectnessTest, NoRowNumberOrFilterNodeInPlan) {
  const auto& param = GetParam();
  auto plan_bytes = buildTopNRowNumberPlan(
      param.partition_key_count, param.order_key_count, param.limit);
  ASSERT_FALSE(plan_bytes.empty());

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto plan = converter.convert(plan_bytes, {});
  ASSERT_NE(plan, nullptr);

  // Verify TopNRowNumberNode exists.
  ASSERT_NE(findNode(plan, "TopNRowNumber"), nullptr)
      << "TopNRowNumberNode not found for " << param.name;

  // RowNumberNode should NOT exist — it was replaced by TopNRowNumberNode.
  int rn_count = countNodes(plan, "RowNumber");
  EXPECT_EQ(rn_count, 0)
      << "RowNumberNode should not exist when TopNRowNumberNode pattern is "
         "detected for " << param.name;

  // FilterNode should NOT exist — the filter was absorbed into TopNRowNumberNode.
  int filter_count = countNodes(plan, "Filter");
  EXPECT_EQ(filter_count, 0)
      << "FilterNode should not exist when TopNRowNumberNode pattern is "
         "detected for " << param.name;
}

// ===========================================================================
// Property 14.6: TopNRowNumberNode Source is TableScan
//
// The TopNRowNumberNode's source SHALL be the original input (TableScan),
// not the RowNumberNode. This verifies the plan is correctly restructured.
//
// **Validates: Requirements 7.2, 7.3**
// ===========================================================================

TEST_P(TopNRowNumberCorrectnessTest, TopNRowNumberNodeSourceIsTableScan) {
  const auto& param = GetParam();
  auto plan_bytes = buildTopNRowNumberPlan(
      param.partition_key_count, param.order_key_count, param.limit);
  ASSERT_FALSE(plan_bytes.empty());

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto plan = converter.convert(plan_bytes, {});
  ASSERT_NE(plan, nullptr);

  auto topn_node = findNode(plan, "TopNRowNumber");
  ASSERT_NE(topn_node, nullptr);

  // TopNRowNumberNode should have exactly one source.
  ASSERT_EQ(topn_node->sources().size(), 1u)
      << "TopNRowNumberNode should have exactly one source for " << param.name;

  // The source should be a TableScan (the original input to the window).
  // Note: there may be intermediate nodes (LocalPartition gather) depending
  // on the converter's parallelism handling.
  auto source = topn_node->sources()[0];
  ASSERT_NE(source, nullptr);

  // The source chain should eventually reach a TableScan.
  auto scan = findNode(source, "TableScan");
  EXPECT_NE(scan, nullptr)
      << "TopNRowNumberNode source chain should contain a TableScan for "
      << param.name;
}

TEST_P(TopNRowNumberCorrectnessTest, ActualValuesKeepExactPartitionTopKAndOrdinals) {
  const auto& param = GetParam();
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  const auto plan = converter.convert(buildTopNRowNumberPlan(param.partition_key_count,
      param.order_key_count, param.limit, param.input_size), {});
  ASSERT_NE(findNode(plan, "TopNRowNumber"), nullptr);
  EXPECT_EQ(countNodes(plan, "RowNumber"), 0);
  EXPECT_EQ(countNodes(plan, "Filter"), 0);
  EXPECT_EQ(countNodes(plan, "OrderBy"), 0);
  const auto values = std::dynamic_pointer_cast<const core::ValuesNode>(findNode(plan, "Values"));
  ASSERT_NE(values, nullptr);
  ASSERT_EQ(values->values().size(), 1);
  EXPECT_EQ(values->values().front()->size(), param.input_size);
  const auto execution = executeCanonicalTask(plan, converter_registry);
  std::vector<std::vector<int64_t>> actual, expected;
  for (const auto& batch : execution.batches) {
    for (vector_size_t row = 0; row < batch->size(); ++row) {
      std::vector<int64_t> result;
      for (const auto& child : batch->children()) {
        DecodedVector decoded(*child);
        ASSERT_FALSE(decoded.isNullAt(row));
        result.push_back(decoded.valueAt<int64_t>(row));
      }
      actual.push_back(std::move(result));
    }
  }
  std::map<std::vector<int64_t>, int64_t> counts;
  for (int row = param.input_size - 1; row >= 0; --row) {
    auto result = inputRow(row, param.partition_key_count, param.order_key_count, param.input_size);
    const std::vector<int64_t> key(result.begin(), result.begin() + param.partition_key_count);
    const auto number = ++counts[key];
    if (number > param.limit) continue;
    result.push_back(number);
    expected.push_back(std::move(result));
  }
  std::sort(actual.begin(), actual.end());
  std::sort(expected.begin(), expected.end());
  EXPECT_EQ(actual, expected);
  ASSERT_EQ(plan->outputType()->size(), param.partition_key_count + param.order_key_count + 2);
  expectCanonicalOutputField(converter, plan->outputType(), plan->outputType()->size() - 1, "row_number");
}

TEST(TopNRowNumberNegativeTest, StrictMinimumIntegerDoesNotOverflowOrRewrite) {
  pangrova::test::testMemoryManager();
  auto bytes = buildTopNRowNumberPlan(1, 1, std::numeric_limits<int64_t>::min(), 10);
  substrait::Plan proto;
  ASSERT_TRUE(proto.ParseFromArray(bytes.data(), bytes.size()));
  proto.mutable_extensions(1)->mutable_extension_function()->set_name("lt:i64_i64");
  const auto serialized = pangrova::test::canonicalTestPlan(proto).SerializeAsString();
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  const auto plan = converter.convert({serialized.begin(), serialized.end()}, {});
  EXPECT_EQ(findNode(plan, "TopNRowNumber"), nullptr);
  EXPECT_EQ(countNodes(plan, "Filter"), 1);
  const auto execution = executeCanonicalTask(plan, converter_registry);
  size_t rows = 0;
  for (const auto& batch : execution.batches) rows += batch->size();
  EXPECT_EQ(rows, 0);
}

TEST(TopNRowNumberNegativeTest, ComputedMarkerCannotBeAssumedToHaveTheOriginalOrdinal) {
  pangrova::test::testMemoryManager();
  const auto bytes = buildTopNRowNumberPlan(1, 1, 1, 10);
  substrait::Plan proto;
  ASSERT_TRUE(proto.ParseFromArray(bytes.data(), bytes.size()));
  auto* filter = proto.mutable_relations(0)->mutable_root()->mutable_input()->mutable_filter();
  const auto window = filter->input();
  auto* project = filter->mutable_input()->mutable_project();
  *project->mutable_input() = window;
  auto* add = proto.add_extensions()->mutable_extension_function();
  add->set_function_anchor(2);
  add->set_name("add:i64_i64");
  const auto& addDefinition = pangrova::test::canonical_plan_detail::definition(
      pangrova::protocol::v2::FUNCTION_KIND_SCALAR, "add");
  uint32_t urnAnchor = 0;
  for (const auto& urn : proto.extension_urns()) urnAnchor = std::max(urnAnchor, urn.extension_urn_anchor() + 1);
  auto* arithmeticUrn = proto.add_extension_urns();
  arithmeticUrn->set_extension_urn_anchor(urnAnchor);
  arithmeticUrn->set_urn(addDefinition.ref().urn());
  add->set_extension_urn_reference(urnAnchor);
  auto* call = project->add_expressions()->mutable_scalar_function();
  call->set_function_reference(2);
  call->mutable_output_type()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  setFieldRef(call->add_arguments()->mutable_value(), 3);
  call->add_arguments()->mutable_value()->mutable_literal()->set_i64(10);
  for (int ordinal : {0, 1, 2, 4}) project->mutable_common()->mutable_emit()->add_output_mapping(ordinal);
  const auto serialized = pangrova::test::canonicalTestPlan(proto).SerializeAsString();
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  const auto plan = converter.convert({serialized.begin(), serialized.end()}, {});
  EXPECT_EQ(findNode(plan, "TopNRowNumber"), nullptr);
  EXPECT_EQ(countNodes(plan, "Filter"), 1);
  const auto execution = executeCanonicalTask(plan, converter_registry);
  size_t rows = 0;
  for (const auto& batch : execution.batches) rows += batch->size();
  EXPECT_EQ(rows, 0);
}

TEST(TopNRowNumberNegativeTest, DuplicateProjectionKeepsBothDeclaredMarkersWithoutRewrite) {
  pangrova::test::testMemoryManager();
  const auto bytes = buildTopNRowNumberPlan(1, 1, 1, 10);
  substrait::Plan proto;
  ASSERT_TRUE(proto.ParseFromArray(bytes.data(), bytes.size()));
  auto* root = proto.mutable_relations(0)->mutable_root();
  root->add_names("marker_copy");
  auto* filter = root->mutable_input()->mutable_filter();
  const auto window = filter->input();
  auto* project = filter->mutable_input()->mutable_project();
  *project->mutable_input() = window;
  for (int ordinal : {0, 1, 2, 3, 3}) project->mutable_common()->mutable_emit()->add_output_mapping(ordinal);
  const auto serialized = pangrova::test::canonicalTestPlan(proto).SerializeAsString();
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  const auto plan = converter.convert({serialized.begin(), serialized.end()}, {});
  EXPECT_EQ(findNode(plan, "TopNRowNumber"), nullptr);
  EXPECT_EQ(countNodes(plan, "Filter"), 1);
  const auto execution = executeCanonicalTask(plan, converter_registry);
  size_t rows = 0;
  for (const auto& batch : execution.batches) {
    ASSERT_EQ(batch->childrenSize(), 5);
    DecodedVector number(*batch->childAt(3)), copy(*batch->childAt(4));
    for (vector_size_t row = 0; row < batch->size(); ++row) {
      EXPECT_EQ(number.valueAt<int64_t>(row), 1);
      EXPECT_EQ(copy.valueAt<int64_t>(row), number.valueAt<int64_t>(row));
    }
    rows += batch->size();
  }
  EXPECT_EQ(rows, 3);
  expectCanonicalOutputField(converter, plan->outputType(), 4, "marker_copy");
}

// ===========================================================================
// Parameterization: partition key counts × order key counts × limits × sizes
// ===========================================================================

std::vector<TopNRowNumberParam> generateParams() {
  std::vector<TopNRowNumberParam> params;

  std::vector<int> partition_key_counts = {1, 2};
  std::vector<int> order_key_counts = {1, 2};
  std::vector<int32_t> limits = {1, 5, 50};
  std::vector<int> input_sizes = {10, 100, 1000};

  for (int pk : partition_key_counts) {
    for (int ok : order_key_counts) {
      for (int32_t limit : limits) {
        for (int size : input_sizes) {
          std::string name = "PK" + std::to_string(pk) +
              "_OK" + std::to_string(ok) +
              "_Limit" + std::to_string(limit) +
              "_Size" + std::to_string(size);
          params.push_back({name, pk, ok, limit, size});
        }
      }
    }
  }

  return params;
}

INSTANTIATE_TEST_SUITE_P(
    TopNRowNumberCorrectness,
    TopNRowNumberCorrectnessTest,
    ::testing::ValuesIn(generateParams()),
    [](const ::testing::TestParamInfo<TopNRowNumberParam>& info) {
      return info.param.name;
    });

}  // namespace
}  // namespace pangrova::velox::test
