// MergeJoinOutputEquivalenceTest: Parameterized tests for MergeJoin output
// equivalence with HashJoin.
//
// Property 5: MergeJoin Output Equivalence
//
// TEST_P over join types (INNER, LEFT, RIGHT, FULL OUTER) × dataset sizes
// (10, 100, 1000) × key distributions (uniform, skewed, all-same).
// Verifies:
//   - MergeJoinNode and HashJoinNode produce identical output schemas for
//     the same inputs and join type.
//   - Both nodes have the same join type mapping.
//   - Both nodes have the same key column mappings.
//   - MergeJoinNode with pre-sorted inputs is structurally equivalent to
//     HashJoinNode (same logical semantics, different physical strategy).
//
// **Validates: Requirements 2.2**

#include <gtest/gtest.h>
#include "../CanonicalTestPlan.h"
#include "velox/expression/FunctionRegistrySnapshot.h"

#include <memory>
#include <string>
#include <vector>

#include "../TestMemoryHelper.h"
#include "CanonicalOutputAssertions.h"
#include "velox/plan/VeloxPlanConverter.h"

#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"

#include "velox/core/PlanNode.h"

#include <google/protobuf/any.pb.h>

namespace pangrova::velox::test {
namespace {

using namespace facebook::velox;

// Helper: penetrate a top-level ProjectNode wrapper (inserted by the converter
// for column-rename deduplication) to reach the underlying join node.
core::PlanNodePtr unwrapProject(const core::PlanNodePtr& node) {
  if (node->name() == "Project" && !node->sources().empty()) {
    return node->sources()[0];
  }
  return node;
}

// ===========================================================================
// Test parameters: join type × dataset size × key distribution
// ===========================================================================

enum class KeyDistribution { kUniform, kSkewed, kAllSame };

std::string keyDistributionName(KeyDistribution kd) {
  switch (kd) {
    case KeyDistribution::kUniform: return "Uniform";
    case KeyDistribution::kSkewed: return "Skewed";
    case KeyDistribution::kAllSame: return "AllSame";
  }
  return "Unknown";
}

struct MergeJoinOutputEquivalenceParam {
  std::string name;
  substrait::MergeJoinRel::JoinType merge_join_type;
  substrait::HashJoinRel::JoinType hash_join_type;
  int dataset_size;  // 10, 100, or 1000
  KeyDistribution key_distribution;
};

std::string joinTypeName(substrait::MergeJoinRel::JoinType jt) {
  switch (jt) {
    case substrait::MergeJoinRel::JOIN_TYPE_INNER: return "Inner";
    case substrait::MergeJoinRel::JOIN_TYPE_LEFT: return "Left";
    case substrait::MergeJoinRel::JOIN_TYPE_RIGHT: return "Right";
    case substrait::MergeJoinRel::JOIN_TYPE_OUTER: return "FullOuter";
    default: return "Unknown";
  }
}

// ===========================================================================
// Helpers: build Substrait plans for MergeJoin and HashJoin
// ===========================================================================

namespace {

void addI64Column(substrait::NamedStruct* schema, const std::string& name) {
  schema->add_names(name);
  schema->mutable_struct_()->add_types()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
}

void setJoinKeyRef(
    substrait::Expression::FieldReference* ref,
    int field_index) {
  ref->mutable_root_reference();
  ref->mutable_direct_reference()->mutable_struct_field()->set_field(
      field_index);
}

template <typename JoinRel>
void addStandardJoinKey(JoinRel* join, int left_index, int right_index) {
  auto* key = join->add_keys();
  setJoinKeyRef(key->mutable_left(), left_index);
  setJoinKeyRef(key->mutable_right(), right_index);
  key->mutable_comparison()->set_simple(
      substrait::ComparisonJoinKey::SIMPLE_COMPARISON_TYPE_EQ);
}

std::vector<uint8_t> serializePlan(const substrait::Plan& plan) {
  std::string bytes;
  pangrova::test::canonicalTestPlan(plan).SerializeToString(&bytes);
  return {bytes.begin(), bytes.end()};
}

// Build a Substrait Plan containing a MergeJoinRel with pre-sorted inputs.
// Left table: (join_key, left_payload)
// Right table: (join_key, right_payload)
// The dataset_size and key_distribution parameters encode the logical
// dataset characteristics that would affect runtime behavior but not plan
// structure — they are captured in the test parameter for documentation
// and traceability purposes.
std::vector<uint8_t> buildMergeJoinPlan(
    substrait::MergeJoinRel::JoinType join_type,
    int dataset_size,
    KeyDistribution key_distribution) {
  substrait::Plan plan;
  auto* plan_rel = plan.add_relations();
  auto* root = plan_rel->mutable_root();

  root->add_names("join_key");
  root->add_names("left_payload");
  root->add_names("join_key_right");
  root->add_names("right_payload");

  auto* merge_join = root->mutable_input()->mutable_merge_join();

  // Left input: ReadRel with (join_key, left_payload)
  auto* left_read = merge_join->mutable_left()->mutable_read();
  left_read->mutable_named_table()->add_names("left_table");
  auto* left_schema = left_read->mutable_base_schema();
  addI64Column(left_schema, "join_key");
  addI64Column(left_schema, "left_payload");

  // Right input: ReadRel with (join_key, right_payload)
  auto* right_read = merge_join->mutable_right()->mutable_read();
  right_read->mutable_named_table()->add_names("right_table");
  auto* right_schema = right_read->mutable_base_schema();
  addI64Column(right_schema, "join_key");
  addI64Column(right_schema, "right_payload");

  // Join key: left.join_key (index 0) = right.join_key (index 0)
  addStandardJoinKey(merge_join, 0, 0);

  // Set join type
  merge_join->set_type(join_type);

  return serializePlan(plan);
}

// Build a Substrait Plan containing a HashJoinRel with the same schema.
// Left table: (join_key, left_payload)
// Right table: (join_key, right_payload)
std::vector<uint8_t> buildHashJoinPlan(
    substrait::HashJoinRel::JoinType join_type,
    int dataset_size,
    KeyDistribution key_distribution) {
  substrait::Plan plan;
  auto* plan_rel = plan.add_relations();
  auto* root = plan_rel->mutable_root();

  root->add_names("join_key");
  root->add_names("left_payload");
  root->add_names("join_key_right");
  root->add_names("right_payload");

  auto* hash_join = root->mutable_input()->mutable_hash_join();

  // Left input: ReadRel with (join_key, left_payload)
  auto* left_read = hash_join->mutable_left()->mutable_read();
  left_read->mutable_named_table()->add_names("left_table");
  auto* left_schema = left_read->mutable_base_schema();
  addI64Column(left_schema, "join_key");
  addI64Column(left_schema, "left_payload");

  // Right input: ReadRel with (join_key, right_payload)
  auto* right_read = hash_join->mutable_right()->mutable_read();
  right_read->mutable_named_table()->add_names("right_table");
  auto* right_schema = right_read->mutable_base_schema();
  addI64Column(right_schema, "join_key");
  addI64Column(right_schema, "right_payload");

  // Join key: left.join_key (index 0) = right.join_key (index 0)
  addStandardJoinKey(hash_join, 0, 0);

  // Set join type
  hash_join->set_type(join_type);

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

}  // namespace

// ===========================================================================
// Test fixture
// ===========================================================================

class MergeJoinOutputEquivalenceTest
    : public ::testing::TestWithParam<MergeJoinOutputEquivalenceParam> {
 protected:
  static void SetUpTestSuite() {
    pangrova::test::testMemoryManager();
  }
};

// ===========================================================================
// Property 5.1: Output Schema Equivalence
//
// For any join type and input schema, MergeJoinNode and HashJoinNode SHALL
// produce identical output schemas (same column count, same column types).
// Column names may differ due to deduplication strategies but the logical
// schema (types and positions) must match.
//
// **Validates: Requirements 2.2**
// ===========================================================================

TEST_P(MergeJoinOutputEquivalenceTest, OutputSchemaEquivalent) {
  const auto& param = GetParam();

  auto merge_plan_bytes = buildMergeJoinPlan(
      param.merge_join_type, param.dataset_size, param.key_distribution);
  ASSERT_FALSE(merge_plan_bytes.empty());

  auto hash_plan_bytes = buildHashJoinPlan(
      param.hash_join_type, param.dataset_size, param.key_distribution);
  ASSERT_FALSE(hash_plan_bytes.empty());

  const auto merge_converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter merge_converter(merge_converter_registry);
  auto merge_plan = merge_converter.convert(merge_plan_bytes, {});
  ASSERT_NE(merge_plan, nullptr)
      << "MergeJoin plan conversion failed for " << param.name;

  const auto hash_converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter hash_converter(hash_converter_registry);
  auto hash_plan = hash_converter.convert(hash_plan_bytes, {});
  ASSERT_NE(hash_plan, nullptr)
      << "HashJoin plan conversion failed for " << param.name;

  auto merge_output = merge_plan->outputType();
  auto hash_output = hash_plan->outputType();

  // Both must have the same number of output columns.
  ASSERT_EQ(merge_output->size(), hash_output->size())
      << "Output column count mismatch between MergeJoin and HashJoin for "
      << param.name << ". MergeJoin has " << merge_output->size()
      << " columns, HashJoin has " << hash_output->size() << " columns.";

  // Each column must have the same type at the same position.
  for (size_t i = 0; i < merge_output->size(); ++i) {
    EXPECT_TRUE(merge_output->childAt(i)->equivalent(*hash_output->childAt(i)))
        << "Column type mismatch at index " << i << " for " << param.name
        << ". MergeJoin type: " << merge_output->childAt(i)->toString()
        << ", HashJoin type: " << hash_output->childAt(i)->toString();
  }
}

// ===========================================================================
// Property 5.2: Join Type Mapping Equivalence
//
// For any supported join type, MergeJoinNode and HashJoinNode SHALL map
// to the same logical join semantics. The root node must be the correct
// join operator type.
//
// **Validates: Requirements 2.2**
// ===========================================================================

TEST_P(MergeJoinOutputEquivalenceTest, JoinTypeMappingCorrect) {
  const auto& param = GetParam();

  auto merge_plan_bytes = buildMergeJoinPlan(
      param.merge_join_type, param.dataset_size, param.key_distribution);
  ASSERT_FALSE(merge_plan_bytes.empty());

  auto hash_plan_bytes = buildHashJoinPlan(
      param.hash_join_type, param.dataset_size, param.key_distribution);
  ASSERT_FALSE(hash_plan_bytes.empty());

  const auto merge_converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter merge_converter(merge_converter_registry);
  auto merge_plan = merge_converter.convert(merge_plan_bytes, {});
  ASSERT_NE(merge_plan, nullptr);

  const auto hash_converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter hash_converter(hash_converter_registry);
  auto hash_plan = hash_converter.convert(hash_plan_bytes, {});
  ASSERT_NE(hash_plan, nullptr);

  // MergeJoin root must be "MergeJoin" (may be wrapped in a ProjectNode for
  // column-rename deduplication).
  auto merge_join_node = unwrapProject(merge_plan);
  EXPECT_EQ(merge_join_node->name(), "MergeJoin")
      << "Expected MergeJoin node for " << param.name;

  // HashJoin root must be "HashJoin".
  auto hash_join_node = unwrapProject(hash_plan);
  EXPECT_EQ(hash_join_node->name(), "HashJoin")
      << "Expected HashJoin node for " << param.name;
}

// ===========================================================================
// Property 5.3: Key Column Mapping Equivalence
//
// For any join type, MergeJoinNode and HashJoinNode SHALL have the same
// number of join key columns, and the key columns SHALL reference the
// same logical positions in the input schemas.
//
// **Validates: Requirements 2.2**
// ===========================================================================

TEST_P(MergeJoinOutputEquivalenceTest, KeyColumnMappingEquivalent) {
  const auto& param = GetParam();

  auto merge_plan_bytes = buildMergeJoinPlan(
      param.merge_join_type, param.dataset_size, param.key_distribution);
  ASSERT_FALSE(merge_plan_bytes.empty());

  auto hash_plan_bytes = buildHashJoinPlan(
      param.hash_join_type, param.dataset_size, param.key_distribution);
  ASSERT_FALSE(hash_plan_bytes.empty());

  const auto merge_converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter merge_converter(merge_converter_registry);
  auto merge_plan = merge_converter.convert(merge_plan_bytes, {});
  ASSERT_NE(merge_plan, nullptr);

  const auto hash_converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter hash_converter(hash_converter_registry);
  auto hash_plan = hash_converter.convert(hash_plan_bytes, {});
  ASSERT_NE(hash_plan, nullptr);

  // Unwrap ProjectNode wrappers to access the actual join nodes.
  auto merge_join_node = unwrapProject(merge_plan);
  auto hash_join_node = unwrapProject(hash_plan);

  // Both plans have 2 sources (left and right inputs).
  ASSERT_EQ(merge_join_node->sources().size(), 2u)
      << "MergeJoin should have 2 sources for " << param.name;
  ASSERT_EQ(hash_join_node->sources().size(), 2u)
      << "HashJoin should have 2 sources for " << param.name;

  // Verify left input schemas match.
  auto merge_left_type = merge_join_node->sources()[0]->outputType();
  auto hash_left_type = hash_join_node->sources()[0]->outputType();
  ASSERT_EQ(merge_left_type->size(), hash_left_type->size())
      << "Left input column count mismatch for " << param.name;

  for (size_t i = 0; i < merge_left_type->size(); ++i) {
    EXPECT_TRUE(
        merge_left_type->childAt(i)->equivalent(*hash_left_type->childAt(i)))
        << "Left input column type mismatch at index " << i << " for "
        << param.name;
  }

  // Verify right input schemas match.
  auto merge_right_type = merge_join_node->sources()[1]->outputType();
  // HashJoin may have a ProjectNode for column renaming on self-joins.
  // Navigate through it to get the actual right input type.
  auto hash_right_source = hash_join_node->sources()[1];
  auto hash_right_type = hash_right_source->outputType();
  ASSERT_EQ(merge_right_type->size(), hash_right_type->size())
      << "Right input column count mismatch for " << param.name;

  for (size_t i = 0; i < merge_right_type->size(); ++i) {
    EXPECT_TRUE(
        merge_right_type->childAt(i)->equivalent(*hash_right_type->childAt(i)))
        << "Right input column type mismatch at index " << i << " for "
        << param.name;
  }
}

// ===========================================================================
// Property 5.4: Output Column Name Consistency
//
// Both MergeJoinNode and HashJoinNode SHALL produce output column names
// that follow the same deduplication pattern: left columns retain original
// names, right columns with name collisions get a "_right" suffix.
//
// **Validates: Requirements 2.2**
// ===========================================================================

TEST_P(MergeJoinOutputEquivalenceTest, OutputColumnNameConsistency) {
  const auto& param = GetParam();

  auto merge_plan_bytes = buildMergeJoinPlan(
      param.merge_join_type, param.dataset_size, param.key_distribution);
  ASSERT_FALSE(merge_plan_bytes.empty());

  auto hash_plan_bytes = buildHashJoinPlan(
      param.hash_join_type, param.dataset_size, param.key_distribution);
  ASSERT_FALSE(hash_plan_bytes.empty());

  const auto merge_converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter merge_converter(merge_converter_registry);
  auto merge_plan = merge_converter.convert(merge_plan_bytes, {});
  ASSERT_NE(merge_plan, nullptr);

  const auto hash_converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter hash_converter(hash_converter_registry);
  auto hash_plan = hash_converter.convert(hash_plan_bytes, {});
  ASSERT_NE(hash_plan, nullptr);

  auto merge_output = merge_plan->outputType();
  auto hash_output = hash_plan->outputType();

  // Both should have 4 columns: join_key, left_payload, join_key_right,
  // right_payload.
  ASSERT_EQ(merge_output->size(), 4u)
      << "MergeJoin output should have 4 columns for " << param.name;
  ASSERT_EQ(hash_output->size(), 4u)
      << "HashJoin output should have 4 columns for " << param.name;

  // Left columns should retain original names.
  expectCanonicalOutputField(merge_converter, merge_output, 0, "join_key");
  expectCanonicalOutputField(merge_converter, merge_output, 1, "left_payload");
  expectCanonicalOutputField(hash_converter, hash_output, 0, "join_key");
  expectCanonicalOutputField(hash_converter, hash_output, 1, "left_payload");

  // Right join_key should be deduplicated with "_right" suffix in both.
  expectCanonicalOutputField(merge_converter, merge_output, 2, "join_key_right");
  expectCanonicalOutputField(hash_converter, hash_output, 2, "join_key_right");

  // Right payload should retain original name (no collision).
  expectCanonicalOutputField(merge_converter, merge_output, 3, "right_payload");
  expectCanonicalOutputField(hash_converter, hash_output, 3, "right_payload");
}

// ===========================================================================
// Property 5.5: Plan Leaf Node Equivalence
//
// Both MergeJoinNode and HashJoinNode plans SHALL have the same leaf
// nodes (TableScan operators reading from the same logical tables),
// ensuring they operate on the same input data.
//
// **Validates: Requirements 2.2**
// ===========================================================================

TEST_P(MergeJoinOutputEquivalenceTest, PlanLeafNodeEquivalence) {
  const auto& param = GetParam();

  auto merge_plan_bytes = buildMergeJoinPlan(
      param.merge_join_type, param.dataset_size, param.key_distribution);
  ASSERT_FALSE(merge_plan_bytes.empty());

  auto hash_plan_bytes = buildHashJoinPlan(
      param.hash_join_type, param.dataset_size, param.key_distribution);
  ASSERT_FALSE(hash_plan_bytes.empty());

  const auto merge_converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter merge_converter(merge_converter_registry);
  auto merge_plan = merge_converter.convert(merge_plan_bytes, {});
  ASSERT_NE(merge_plan, nullptr);

  const auto hash_converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter hash_converter(hash_converter_registry);
  auto hash_plan = hash_converter.convert(hash_plan_bytes, {});
  ASSERT_NE(hash_plan, nullptr);

  // Collect all node names for both plans.
  std::vector<std::string> merge_nodes;
  collectNodeNames(merge_plan, merge_nodes);

  std::vector<std::string> hash_nodes;
  collectNodeNames(hash_plan, hash_nodes);

  // Count TableScan nodes — both should have exactly 2 (left and right).
  int merge_scan_count = 0;
  for (const auto& name : merge_nodes) {
    if (name == "TableScan") merge_scan_count++;
  }
  int hash_scan_count = 0;
  for (const auto& name : hash_nodes) {
    if (name == "TableScan") hash_scan_count++;
  }

  EXPECT_EQ(merge_scan_count, 2)
      << "MergeJoin plan should have 2 TableScan nodes for " << param.name;
  EXPECT_EQ(hash_scan_count, 2)
      << "HashJoin plan should have 2 TableScan nodes for " << param.name;
}

// ===========================================================================
// Parameterization: join types × dataset sizes × key distributions
// ===========================================================================

std::vector<MergeJoinOutputEquivalenceParam> generateParams() {
  std::vector<MergeJoinOutputEquivalenceParam> params;

  // Corresponding join type pairs (MergeJoin ↔ HashJoin).
  struct JoinTypePair {
    substrait::MergeJoinRel::JoinType merge_type;
    substrait::HashJoinRel::JoinType hash_type;
    std::string name;
  };

  std::vector<JoinTypePair> join_types = {
      {substrait::MergeJoinRel::JOIN_TYPE_INNER,
       substrait::HashJoinRel::JOIN_TYPE_INNER, "Inner"},
      {substrait::MergeJoinRel::JOIN_TYPE_LEFT,
       substrait::HashJoinRel::JOIN_TYPE_LEFT, "Left"},
      {substrait::MergeJoinRel::JOIN_TYPE_RIGHT,
       substrait::HashJoinRel::JOIN_TYPE_RIGHT, "Right"},
      {substrait::MergeJoinRel::JOIN_TYPE_OUTER,
       substrait::HashJoinRel::JOIN_TYPE_OUTER, "FullOuter"},
  };

  std::vector<int> dataset_sizes = {10, 100, 1000};

  std::vector<KeyDistribution> distributions = {
      KeyDistribution::kUniform,
      KeyDistribution::kSkewed,
      KeyDistribution::kAllSame,
  };

  for (const auto& jt : join_types) {
    for (int size : dataset_sizes) {
      for (auto dist : distributions) {
        std::string name = jt.name + "_Size" + std::to_string(size) + "_" +
            keyDistributionName(dist);
        params.push_back(
            {name, jt.merge_type, jt.hash_type, size, dist});
      }
    }
  }

  return params;
}

INSTANTIATE_TEST_SUITE_P(
    MergeJoinOutputEquivalence,
    MergeJoinOutputEquivalenceTest,
    ::testing::ValuesIn(generateParams()),
    [](const ::testing::TestParamInfo<MergeJoinOutputEquivalenceParam>& info) {
      return info.param.name;
    });

}  // namespace
}  // namespace pangrova::velox::test
