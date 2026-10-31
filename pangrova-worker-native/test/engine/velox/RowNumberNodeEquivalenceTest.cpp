// RowNumberNodeEquivalenceTest: Parameterized tests for RowNumberNode output
// equivalence verification.
//
// Property 14: RowNumberNode Output Equivalence
//
// TEST_P over partition key counts (0, 1, 3) × input sizes (10, 100, 1000) ×
// data distributions (uniform, skewed, all-same-partition).
// Verifies:
//   - ConsistentPartitionWindowRel with row_number() converts to RowNumberNode
//     (not WindowNode) when the pattern matches.
//   - RowNumberNode output schema matches expected structure (input columns +
//     row_number column appended).
//   - Plan structure preserves Project → RowNumber → LocalPartition →
//     Project → TableScan, including ordinal-only output restoration.
//   - WindowNode fallback is used when pattern does NOT match (multiple window
//     functions or non-default frame).
//
// **Validates: Requirements 7.1, 7.3**

#include <gtest/gtest.h>
#include "CanonicalTestPlan.h"
#include "velox/expression/FunctionRegistrySnapshot.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "TestMemoryHelper.h"
#include "CanonicalOutputAssertions.h"
#include "CanonicalTaskExecution.h"
#include "engine/velox/plan/VeloxPlanConverter.h"

#include "substrait/algebra.pb.h"
#include "substrait/extensions/extensions.pb.h"
#include "substrait/plan.pb.h"

#include "velox/core/PlanNode.h"
#include "velox/vector/DecodedVector.h"

namespace pangrova::velox::test {
namespace {

using namespace facebook::velox;

// ===========================================================================
// Test parameters: partition key count × input size × data distribution
// ===========================================================================

enum class DataDistribution { kUniform, kSkewed, kAllSamePartition };

std::string distributionName(DataDistribution dist) {
  switch (dist) {
    case DataDistribution::kUniform: return "Uniform";
    case DataDistribution::kSkewed: return "Skewed";
    case DataDistribution::kAllSamePartition: return "AllSamePartition";
  }
  return "Unknown";
}

struct RowNumberEquivalenceParam {
  std::string name;
  int partition_key_count;  // 0, 1, or 3
  int input_size;           // 10, 100, or 1000
  DataDistribution distribution;
};

// ===========================================================================
// Helpers: build Substrait plans with ConsistentPartitionWindowRel
// ===========================================================================

namespace {

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

std::vector<uint8_t> serializePlan(const substrait::Plan& plan) {
  std::string bytes;
  pangrova::test::canonicalTestPlan(plan).SerializeToString(&bytes);
  return {bytes.begin(), bytes.end()};
}

// Build a Substrait Plan containing a ConsistentPartitionWindowRel with a
// single row_number() function (eligible for RowNumberNode optimization).
// The input is a ReadRel with partition_key_count key columns + 1 value column.
std::vector<uint8_t> buildRowNumberPlan(
    int partition_key_count,
    int input_size,
    DataDistribution distribution,
    bool actualValues = false) {
  substrait::Plan plan;

  // Register row_number function extension.
  auto* ext = plan.add_extensions();
  ext->mutable_extension_function()->set_function_anchor(0);
  ext->mutable_extension_function()->set_name("row_number:");

  auto* plan_rel = plan.add_relations();
  auto* root = plan_rel->mutable_root();

  // Output names: partition keys + value + row_number
  for (int i = 0; i < partition_key_count; ++i) {
    root->add_names("partition_key_" + std::to_string(i));
  }
  root->add_names("value");
  root->add_names("row_number");

  // Build ConsistentPartitionWindowRel.
  auto* window_rel = root->mutable_input()->mutable_window();

  // Input: ReadRel with partition keys + value column.
  auto* read = window_rel->mutable_input()->mutable_read();
  read->mutable_named_table()->add_names("input_table");
  auto* schema = read->mutable_base_schema();
  for (int i = 0; i < partition_key_count; ++i) {
    addI64Column(schema, "partition_key_" + std::to_string(i));
  }
  addI64Column(schema, "value");
  if (actualValues) {
    read->clear_named_table();
    for (int row = 0; row < input_size; ++row) {
      auto* record = read->mutable_virtual_table()->add_expressions();
      const auto key = distribution == DataDistribution::kAllSamePartition ? 7 :
          distribution == DataDistribution::kSkewed && row < input_size * 9 / 10 ? 0 : row % 5;
      for (int i = 0; i < partition_key_count; ++i) record->add_fields()->mutable_literal()->set_i64(key + i);
      record->add_fields()->mutable_literal()->set_i64(row);
    }
  }

  // Partition expressions: reference each partition key column.
  for (int i = 0; i < partition_key_count; ++i) {
    setFieldRef(window_rel->add_partition_expressions(), i);
  }

  // No sorting keys (row_number with PARTITION BY only, no ORDER BY).

  // Single window function: row_number() with no arguments, default frame.
  auto* wf = window_rel->add_window_functions();
  wf->set_phase(substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT);
  wf->set_bounds_type(substrait::Expression_WindowFunction::BOUNDS_TYPE_ROWS);
  wf->set_function_reference(0);
  wf->mutable_output_type()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  // No arguments (row_number takes none).
  // No bounds set → isDefaultRowsFrame returns true.

  return serializePlan(plan);
}

// Build a Substrait Plan with a WindowRel that has TWO window functions
// (row_number + rank), which should NOT trigger RowNumberNode optimization.
std::vector<uint8_t> buildMultiWindowFunctionPlan(int partition_key_count) {
  substrait::Plan plan;

  // Register row_number and rank function extensions.
  auto* ext1 = plan.add_extensions();
  ext1->mutable_extension_function()->set_function_anchor(0);
  ext1->mutable_extension_function()->set_name("row_number:");

  auto* ext2 = plan.add_extensions();
  ext2->mutable_extension_function()->set_function_anchor(1);
  ext2->mutable_extension_function()->set_name("rank:");

  auto* plan_rel = plan.add_relations();
  auto* root = plan_rel->mutable_root();

  for (int i = 0; i < partition_key_count; ++i) {
    root->add_names("partition_key_" + std::to_string(i));
  }
  root->add_names("value");
  root->add_names("row_number");
  root->add_names("rank");

  auto* window_rel = root->mutable_input()->mutable_window();

  auto* read = window_rel->mutable_input()->mutable_read();
  read->mutable_named_table()->add_names("input_table");
  auto* schema = read->mutable_base_schema();
  for (int i = 0; i < partition_key_count; ++i) {
    addI64Column(schema, "partition_key_" + std::to_string(i));
  }
  addI64Column(schema, "value");

  for (int i = 0; i < partition_key_count; ++i) {
    setFieldRef(window_rel->add_partition_expressions(), i);
  }

  // Two window functions: row_number() and rank().
  auto* wf1 = window_rel->add_window_functions();
  wf1->set_phase(substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT);
  wf1->set_bounds_type(substrait::Expression_WindowFunction::BOUNDS_TYPE_ROWS);
  wf1->set_function_reference(0);
  wf1->mutable_output_type()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);

  auto* wf2 = window_rel->add_window_functions();
  wf2->set_phase(substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT);
  wf2->set_bounds_type(substrait::Expression_WindowFunction::BOUNDS_TYPE_ROWS);
  wf2->set_function_reference(1);
  wf2->mutable_output_type()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);

  return serializePlan(plan);
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

class RowNumberNodeEquivalenceTest
    : public ::testing::TestWithParam<RowNumberEquivalenceParam> {
 protected:
  static void SetUpTestSuite() {
    pangrova::test::testMemoryManager();
  }
};

// ===========================================================================
// Property 14.1: RowNumberNode Created Instead of WindowNode
//
// For any ConsistentPartitionWindowRel containing only a single row_number()
// with no custom frame, the converter SHALL contain a RowNumberNode (not a
// WindowNode). Its exact final projection preserves every declared ordinal.
// This verifies the pattern detection works correctly across
// all partition key counts and data distributions.
//
// **Validates: Requirements 7.1, 7.3**
// ===========================================================================

TEST_P(RowNumberNodeEquivalenceTest, RowNumberNodeCreatedInsteadOfWindowNode) {
  const auto& param = GetParam();
  auto plan_bytes = buildRowNumberPlan(
      param.partition_key_count, param.input_size, param.distribution);
  ASSERT_FALSE(plan_bytes.empty());

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto plan = converter.convert(plan_bytes, {});
  ASSERT_NE(plan, nullptr)
      << "Plan conversion failed for " << param.name;

  const auto output = std::dynamic_pointer_cast<const core::ProjectNode>(plan);
  ASSERT_NE(output, nullptr);
  ASSERT_EQ(output->sources().size(), 1);
  const auto rowNumber = std::dynamic_pointer_cast<const core::RowNumberNode>(output->sources().front());
  ASSERT_NE(rowNumber, nullptr);
  EXPECT_EQ(rowNumber->name(), "RowNumber")
      << "Expected RowNumber node for " << param.name
      << " but got " << plan->name();

  // No WindowNode should be present in the plan.
  EXPECT_EQ(countNodes(plan, "Window"), 0)
      << "WindowNode should NOT be present when RowNumberNode is used for "
      << param.name;
}

// ===========================================================================
// Property 14.2: RowNumberNode Output Schema Correct
//
// The RowNumberNode output schema SHALL contain all input columns plus the
// row_number column appended by Velox's RowNumberNode (generateRowNumber=true).
// The total column count must be partition_key_count + 1 (value) + 1 (row_number).
//
// Note: Velox's RowNumberNode with generateRowNumber=true appends a "row_number"
// column to the input schema in its outputType(). The Substrait root names
// include the row_number column, and when sizes match, the converter applies
// a rename ProjectNode. When sizes don't match (e.g., Velox version difference),
// the plan root is the RowNumberNode directly.
//
// This test verifies the output schema is structurally correct regardless of
// whether a rename ProjectNode is applied.
//
// **Validates: Requirements 7.1, 7.3**
// ===========================================================================

TEST_P(RowNumberNodeEquivalenceTest, OutputSchemaCorrect) {
  const auto& param = GetParam();
  auto plan_bytes = buildRowNumberPlan(
      param.partition_key_count, param.input_size, param.distribution);
  ASSERT_FALSE(plan_bytes.empty());

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto plan = converter.convert(plan_bytes, {});
  ASSERT_NE(plan, nullptr);

  auto output_type = plan->outputType();
  // RowNumberNode with generateRowNumber=true appends a row_number column.
  // The output has: partition keys + value + row_number.
  // However, the Substrait root rename ProjectNode is only applied when
  // output_type->size() == root.names_size(). Verify the output contains
  // at minimum the input columns (partition keys + value).
  int min_expected_cols = param.partition_key_count + 1;
  int max_expected_cols = param.partition_key_count + 1 + 1;  // with row_number
  ASSERT_GE(static_cast<int>(output_type->size()), min_expected_cols)
      << "Output column count too small for " << param.name
      << ". Expected at least " << min_expected_cols
      << " got " << output_type->size();
  ASSERT_LE(static_cast<int>(output_type->size()), max_expected_cols)
      << "Output column count too large for " << param.name
      << ". Expected at most " << max_expected_cols
      << " got " << output_type->size();

  // Verify all columns are BIGINT type (partition keys, value, and optionally
  // row_number are all i64 in our test schema).
  for (size_t i = 0; i < output_type->size(); ++i) {
    EXPECT_TRUE(output_type->childAt(i)->isBigint())
        << "Column at index " << i << " should be BIGINT for " << param.name;
  }
}

// ===========================================================================
// Property 14.3: Plan Structure Correctness
//
// The converted plan SHALL preserve the exact typed preparation, partitioning
// and output-restoration structure without unrelated intermediate nodes.
//
// **Validates: Requirements 7.1**
// ===========================================================================

TEST_P(RowNumberNodeEquivalenceTest, PlanStructureCorrect) {
  const auto& param = GetParam();
  auto plan_bytes = buildRowNumberPlan(
      param.partition_key_count, param.input_size, param.distribution);
  ASSERT_FALSE(plan_bytes.empty());

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto plan = converter.convert(plan_bytes, {});
  ASSERT_NE(plan, nullptr);

  // Preparation keeps row identity, and the real LocalPartition routes every
  // equal key to one row-number driver. The last projection is ordinal-only.
  std::vector<std::string> node_names;
  collectNodeNames(plan, node_names);
  EXPECT_EQ(node_names, (std::vector<std::string>{"Project", "RowNumber", "LocalPartition", "Project", "TableScan"}));
  const auto output = std::dynamic_pointer_cast<const core::ProjectNode>(plan);
  ASSERT_NE(output, nullptr);
  ASSERT_EQ(output->sources().size(), 1);
  expectOrdinalIdentityProjection(output);
  const auto rowNumber = std::dynamic_pointer_cast<const core::RowNumberNode>(output->sources().front());
  ASSERT_NE(rowNumber, nullptr);
  ASSERT_EQ(rowNumber->sources().size(), 1);
  const auto partition = std::dynamic_pointer_cast<const core::LocalPartitionNode>(rowNumber->sources().front());
  ASSERT_NE(partition, nullptr);
  ASSERT_EQ(partition->sources().size(), 1);
  const auto prepared = std::dynamic_pointer_cast<const core::ProjectNode>(partition->sources().front());
  ASSERT_NE(prepared, nullptr);
  ASSERT_EQ(prepared->sources().size(), 1);
  EXPECT_NE(std::dynamic_pointer_cast<const core::TableScanNode>(prepared->sources().front()), nullptr);
  expectOrdinalIdentityProjection(prepared);
}

// ===========================================================================
// Property 14.4: RowNumberNode Partition Key Count Matches Input
//
// The RowNumberNode SHALL have the correct number of partition keys matching
// the partition_key_count parameter. When partition_key_count is 0, the
// RowNumberNode should have no partition keys (global row numbering).
// Verified by checking the output type contains at least the input columns.
//
// **Validates: Requirements 7.1, 7.3**
// ===========================================================================

TEST_P(RowNumberNodeEquivalenceTest, PartitionKeyCountMatchesInput) {
  const auto& param = GetParam();
  auto plan_bytes = buildRowNumberPlan(
      param.partition_key_count, param.input_size, param.distribution);
  ASSERT_FALSE(plan_bytes.empty());

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto plan = converter.convert(plan_bytes, {});
  ASSERT_NE(plan, nullptr);

  const auto output = std::dynamic_pointer_cast<const core::ProjectNode>(plan);
  ASSERT_NE(output, nullptr);
  ASSERT_EQ(output->sources().size(), 1);
  const auto rowNumber = std::dynamic_pointer_cast<const core::RowNumberNode>(output->sources().front());
  ASSERT_NE(rowNumber, nullptr);
  EXPECT_TRUE(rowNumber->generateRowNumber());
  ASSERT_EQ(rowNumber->partitionKeys().size(), param.partition_key_count);
  ASSERT_EQ(plan->outputType()->size(), param.partition_key_count + 2);
  for (int key = 0; key < param.partition_key_count; ++key) {
    EXPECT_EQ(rowNumber->partitionKeys().at(key)->name(), physicalFieldAlias(key));
    expectCanonicalOutputField(converter, plan->outputType(), key, "partition_key_" + std::to_string(key));
  }
  expectCanonicalOutputField(converter, plan->outputType(), param.partition_key_count, "value");
  expectCanonicalOutputField(converter, plan->outputType(), param.partition_key_count + 1, "row_number");
}

TEST_P(RowNumberNodeEquivalenceTest, ActualValuesPreserveEveryPayloadAndNumberEachPartition) {
  const auto& param = GetParam();
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  const auto plan = converter.convert(buildRowNumberPlan(param.partition_key_count,
      param.input_size, param.distribution, true), {});
  const auto execution = executeCanonicalTask(plan, converter_registry);
  std::map<std::vector<int64_t>, std::vector<int64_t>> numbers;
  std::set<int64_t> payloads;
  for (const auto& batch : execution.batches) {
    ASSERT_EQ(batch->childrenSize(), param.partition_key_count + 2);
    for (vector_size_t row = 0; row < batch->size(); ++row) {
      std::vector<int64_t> key;
      for (int i = 0; i < param.partition_key_count; ++i) {
        DecodedVector decoded(*batch->childAt(i));
        ASSERT_FALSE(decoded.isNullAt(row));
        key.push_back(decoded.valueAt<int64_t>(row));
      }
      DecodedVector payload(*batch->childAt(param.partition_key_count));
      DecodedVector number(*batch->childAt(param.partition_key_count + 1));
      ASSERT_FALSE(payload.isNullAt(row));
      ASSERT_FALSE(number.isNullAt(row));
      const auto value = payload.valueAt<int64_t>(row);
      ASSERT_TRUE(payloads.insert(value).second);
      ASSERT_GE(value, 0);
      ASSERT_LT(value, param.input_size);
      const auto expected = param.distribution == DataDistribution::kAllSamePartition ? 7 :
          param.distribution == DataDistribution::kSkewed && value < param.input_size * 9 / 10 ? 0 : value % 5;
      for (int i = 0; i < param.partition_key_count; ++i) EXPECT_EQ(key.at(i), expected + i);
      numbers[key].push_back(number.valueAt<int64_t>(row));
    }
  }
  EXPECT_EQ(payloads.size(), param.input_size);
  for (auto& [key, values] : numbers) {
    std::sort(values.begin(), values.end());
    for (size_t i = 0; i < values.size(); ++i) EXPECT_EQ(values.at(i), i + 1);
  }
}

// ===========================================================================
// Property 14.5: WindowNode Fallback When Pattern Does Not Match
//
// When the ConsistentPartitionWindowRel contains multiple window functions
// (not just row_number alone), the converter SHALL fall back to WindowNode
// instead of RowNumberNode. This verifies Requirement 7.4 (fallback).
//
// Note: The plan root may be a ProjectNode (rename alias) wrapping the
// WindowNode, since the converter applies root name aliases when the
// output column count matches root.names_size().
//
// **Validates: Requirements 7.1, 7.3 (by verifying fallback preserves
// correctness for non-matching patterns)**
// ===========================================================================

TEST_P(RowNumberNodeEquivalenceTest, WindowNodeFallbackForMultipleFunctions) {
  const auto& param = GetParam();
  // Build a plan with TWO window functions (row_number + rank).
  auto plan_bytes = buildMultiWindowFunctionPlan(param.partition_key_count);
  ASSERT_FALSE(plan_bytes.empty());

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto plan = converter.convert(plan_bytes, {});
  ASSERT_NE(plan, nullptr)
      << "Plan conversion failed for multi-function fallback " << param.name;

  // A WindowNode must be present in the plan tree (may be wrapped by a
  // ProjectNode for root name aliasing).
  EXPECT_GT(countNodes(plan, "Window"), 0)
      << "WindowNode should be present for multi-function case " << param.name;

  // No RowNumberNode should be present — the pattern doesn't match.
  EXPECT_EQ(countNodes(plan, "RowNumber"), 0)
      << "RowNumberNode should NOT be present for multi-function case "
      << param.name;
}

// ===========================================================================
// Parameterization: partition key counts × input sizes × distributions
// ===========================================================================

std::vector<RowNumberEquivalenceParam> generateParams() {
  std::vector<RowNumberEquivalenceParam> params;

  std::vector<int> partition_key_counts = {0, 1, 3};
  std::vector<int> input_sizes = {10, 100, 1000};
  std::vector<DataDistribution> distributions = {
      DataDistribution::kUniform,
      DataDistribution::kSkewed,
      DataDistribution::kAllSamePartition,
  };

  for (int keys : partition_key_counts) {
    for (int size : input_sizes) {
      for (auto dist : distributions) {
        std::string name = "Keys" + std::to_string(keys) + "_Size" +
            std::to_string(size) + "_" + distributionName(dist);
        params.push_back({name, keys, size, dist});
      }
    }
  }

  return params;
}

INSTANTIATE_TEST_SUITE_P(
    RowNumberNodeEquivalence,
    RowNumberNodeEquivalenceTest,
    ::testing::ValuesIn(generateParams()),
    [](const ::testing::TestParamInfo<RowNumberEquivalenceParam>& info) {
      return info.param.name;
    });

}  // namespace
}  // namespace pangrova::velox::test
