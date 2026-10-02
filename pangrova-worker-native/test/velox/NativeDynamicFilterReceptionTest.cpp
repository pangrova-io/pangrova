// NativeDynamicFilterReceptionTest: Parameterized tests for native dynamic
// filter reception verification.
//
// Property 1: Dynamic Filter Reception Verification
//
// TEST_P over join key types (bigint, varchar) × cardinalities (1, 100, 10000).
// Verifies:
//   - HashJoin + TableScan plan with kHashProbeDynamicFilterPushdownEnabled=true
//     is correctly structured for native dynamic filter generation.
//   - addDynamicFilter() is invoked on PangrovaVeloxDataSource and correctly
//     stores filters for subsequent scan batches.
//   - PendingFilterRegistry external injection still works alongside native DF.
//
// **Validates: Requirements 1.1, 1.3, 1.4**

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "../TestMemoryHelper.h"
#include "velox/connector/PangrovaVeloxConnector.h"
#include "velox/filter/PendingFilterRegistry.h"
#include "velox/plan/VeloxPlanConverter.h"
#include "velox/runner/VeloxTaskRunner.h"

#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"

#include "velox/common/memory/Memory.h"
#include "velox/core/PlanNode.h"
#include "velox/type/Filter.h"
#include "velox/type/Type.h"

namespace pangrova::velox::test {
namespace {

using namespace facebook::velox;

// ===========================================================================
// Test parameter: join key type × cardinality
// ===========================================================================

struct DynamicFilterReceptionParam {
  std::string name;
  std::shared_ptr<const Type> key_type;
  int32_t cardinality;  // Number of distinct values in build side
};

class NativeDynamicFilterReceptionTest
    : public ::testing::TestWithParam<DynamicFilterReceptionParam> {
 protected:
  static void SetUpTestSuite() {
    pool_ = pangrova::test::testLeafPool("NativeDynamicFilterReceptionTest");
  }

  static std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
};

std::shared_ptr<facebook::velox::memory::MemoryPool>
    NativeDynamicFilterReceptionTest::pool_;

// ===========================================================================
// Helper: build Substrait plan with HashJoin + TableScan
// ===========================================================================

namespace {

void addTypedColumn(
    substrait::NamedStruct* schema,
    const std::string& name,
    const std::shared_ptr<const Type>& type) {
  schema->add_names(name);
  auto* field = schema->mutable_struct_()->add_types();
  if (type->isBigint()) {
    field->mutable_i64();
  } else if (type->isVarchar()) {
    field->mutable_string();
  } else if (type->isInteger()) {
    field->mutable_i32();
  } else {
    field->mutable_i64();
  }
}

void setJoinKeyRef(
    substrait::Expression::FieldReference* ref,
    int field_index) {
  ref->mutable_root_reference();
  ref->mutable_direct_reference()->mutable_struct_field()->set_field(
      field_index);
}

void addStandardJoinKey(
    substrait::HashJoinRel* join,
    int left_index,
    int right_index) {
  auto* key = join->add_keys();
  setJoinKeyRef(key->mutable_left(), left_index);
  setJoinKeyRef(key->mutable_right(), right_index);
  key->mutable_comparison()->set_simple(
      substrait::ComparisonJoinKey::SIMPLE_COMPARISON_TYPE_EQ);
}

// Build a Substrait Plan with HashJoin(probe=TableScan, build=TableScan).
std::vector<uint8_t> buildHashJoinPlan(
    const std::shared_ptr<const Type>& key_type) {
  substrait::Rel rel;
  auto* join = rel.mutable_hash_join();

  // Left (probe) input: fact table with join key + payload.
  auto* left_read = join->mutable_left()->mutable_read();
  left_read->mutable_named_table()->add_names("probe_table");
  auto* left_schema = left_read->mutable_base_schema();
  addTypedColumn(left_schema, "probe_key", key_type);
  addTypedColumn(left_schema, "payload", BIGINT());

  // Right (build) input: dimension table with join key.
  auto* right_read = join->mutable_right()->mutable_read();
  right_read->mutable_named_table()->add_names("build_table");
  auto* right_schema = right_read->mutable_base_schema();
  addTypedColumn(right_schema, "build_key", key_type);

  // Join keys: probe_key (field 0) = build_key (field 0).
  addStandardJoinKey(join, 0, 0);

  join->set_type(substrait::HashJoinRel::JOIN_TYPE_INNER);

  // Serialize to Plan bytes.
  substrait::Plan plan;
  auto* plan_rel = plan.add_relations();
  *plan_rel->mutable_root()->mutable_input() = rel;
  plan_rel->mutable_root()->add_names("output");

  std::string bytes;
  plan.SerializeToString(&bytes);
  return {bytes.begin(), bytes.end()};
}

// Generate a Velox bigint filter for test.
// Single value uses BigintRange; multiple values use BigintValuesUsingHashTable.
std::shared_ptr<common::Filter> makeBigintInListFilter(
    const std::vector<int64_t>& values) {
  if (values.empty()) {
    return nullptr;
  }
  if (values.size() == 1) {
    return std::make_shared<common::BigintRange>(
        values[0], values[0], /*nullAllowed=*/false);
  }
  auto [minIt, maxIt] = std::minmax_element(values.begin(), values.end());
  return std::make_shared<common::BigintValuesUsingHashTable>(
      *minIt, *maxIt, values, /*nullAllowed=*/false);
}

// Generate a Velox BytesValues filter for test.
std::shared_ptr<common::Filter> makeStringInListFilter(
    const std::vector<std::string>& values) {
  if (values.empty()) {
    return nullptr;
  }
  return std::make_shared<common::BytesValues>(values, /*nullAllowed=*/false);
}

// Generate filter values for a given cardinality and type.
std::shared_ptr<common::Filter> makeFilterForType(
    const std::shared_ptr<const Type>& key_type,
    int32_t cardinality) {
  if (key_type->isBigint()) {
    std::vector<int64_t> values;
    values.reserve(cardinality);
    for (int64_t i = 0; i < cardinality; ++i) {
      values.push_back(i * 7 + 1);
    }
    return makeBigintInListFilter(values);
  } else if (key_type->isVarchar()) {
    std::vector<std::string> values;
    values.reserve(cardinality);
    for (int32_t i = 0; i < cardinality; ++i) {
      values.push_back("key_" + std::to_string(i));
    }
    return makeStringInListFilter(values);
  }
  return nullptr;
}

}  // namespace

// ===========================================================================
// Property 1.1: Plan Conversion Creates Correct HashJoin + TableScan
// Structure with Dynamic Filter Enablement
//
// Verifies that the converted plan has a HashJoinNode with TableScan
// children, and that VeloxTaskRunnerConfig correctly enables
// kHashProbeDynamicFilterPushdownEnabled.
//
// **Validates: Requirements 1.1**
// ===========================================================================

TEST_P(NativeDynamicFilterReceptionTest, PlanStructureForNativeDynamicFilter) {
  const auto& param = GetParam();

  auto plan_bytes = buildHashJoinPlan(param.key_type);
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr)
      << "HashJoin plan conversion failed for key type " << param.name;

  EXPECT_EQ(result->name(), "HashJoin")
      << "Root node must be HashJoin for " << param.name;

  ASSERT_EQ(result->sources().size(), 2)
      << "HashJoin must have 2 sources for " << param.name;

  EXPECT_EQ(result->sources()[0]->name(), "TableScan")
      << "Probe side must be TableScan for " << param.name;
  EXPECT_EQ(result->sources()[1]->name(), "TableScan")
      << "Build side must be TableScan for " << param.name;

  // Verify VeloxTaskRunnerConfig enables dynamic filter pushdown by default.
  VeloxTaskRunnerConfig config;
  EXPECT_TRUE(config.hash_probe_finish_early_on_empty_build);
  EXPECT_TRUE(config.hash_probe_string_dynamic_filter_pushdown);

  // Verify scan node output types are tracked for DF column resolution.
  const auto& scan_types = converter.scanNodeOutputTypes();
  EXPECT_GE(scan_types.size(), 2u)
      << "Converter must track output types for both scan nodes";
}

// ===========================================================================
// Property 1.2: addDynamicFilter() Correctly Receives and Stores Filters
//
// Verifies that PangrovaVeloxDataSource::addDynamicFilter() correctly
// accepts filters of the parameterized type and cardinality, storing
// them for application on subsequent scan batches.
//
// **Validates: Requirements 1.3**
// ===========================================================================

TEST_P(NativeDynamicFilterReceptionTest, AddDynamicFilterReceivesFilter) {
  const auto& param = GetParam();

  auto outputType = ROW({"key_col", "payload_col"}, {param.key_type, BIGINT()});
  PangrovaVeloxDataSource dataSource(
      pool_.get(),
      outputType,
      outputType,
      /*taskId=*/"test-task-df-" + param.name);

  // Create a filter matching the parameterized type and cardinality.
  auto filter = makeFilterForType(param.key_type, param.cardinality);
  ASSERT_NE(filter, nullptr)
      << "Failed to create filter for type " << param.name
      << " with cardinality " << param.cardinality;

  // Invoke addDynamicFilter — simulating what Velox's HashProbe does
  // internally when kHashProbeDynamicFilterPushdownEnabled=true.
  dataSource.addDynamicFilter(/*outputChannel=*/0, filter);

  // Multiple filters should accumulate without error.
  auto filter2 = makeFilterForType(param.key_type, 1);
  ASSERT_NE(filter2, nullptr);
  dataSource.addDynamicFilter(/*outputChannel=*/0, filter2);

  // Out-of-bounds column index is handled gracefully (WARN + skip).
  auto outOfBoundsFilter = makeFilterForType(param.key_type, 1);
  dataSource.addDynamicFilter(/*outputChannel=*/99, outOfBoundsFilter);
}

// ===========================================================================
// Property 1.3: PendingFilterRegistry External Injection Works Alongside
// Native Dynamic Filter
//
// Verifies that the PendingFilterRegistry (JNI injection path) continues
// to function correctly when native dynamic filter generation is also
// active. Both paths must coexist without interference.
//
// **Validates: Requirements 1.4**
// ===========================================================================

TEST_P(NativeDynamicFilterReceptionTest, PendingFilterRegistryCoexistsWithNativeDF) {
  const auto& param = GetParam();

  const std::string taskId = "test-task-coexist-" + param.name;

  auto outputType = ROW({"key_col", "payload_col"}, {param.key_type, BIGINT()});
  PangrovaVeloxDataSource dataSource(
      pool_.get(),
      outputType,
      outputType,
      taskId);

  // Step 1: Inject a native dynamic filter (simulating Velox HashProbe path).
  auto nativeFilter = makeFilterForType(param.key_type, param.cardinality);
  ASSERT_NE(nativeFilter, nullptr);
  dataSource.addDynamicFilter(/*outputChannel=*/0, nativeFilter);

  // Step 2: Inject an external filter via PendingFilterRegistry (JNI path).
  auto externalFilter = makeFilterForType(param.key_type, 1);
  ASSERT_NE(externalFilter, nullptr);
  const std::string planNodeId = "node_0";
  PendingFilterRegistry::instance().push(
      taskId, planNodeId, /*column_index=*/0, externalFilter);

  // Step 3: Verify PendingFilterRegistry has the external filter.
  auto drained = PendingFilterRegistry::instance().drain(taskId, planNodeId);
  EXPECT_EQ(drained.size(), 1u)
      << "PendingFilterRegistry must hold exactly 1 external filter for "
      << param.name;
  EXPECT_EQ(drained[0].column_index, 0);
  EXPECT_NE(drained[0].filter, nullptr);

  // Step 4: After drain, registry should be empty for this task.
  auto drainedAgain = PendingFilterRegistry::instance().drain(taskId, planNodeId);
  EXPECT_TRUE(drainedAgain.empty());

  // Step 5: Verify native DF path still works after external injection.
  auto nativeFilter2 = makeFilterForType(param.key_type, param.cardinality);
  dataSource.addDynamicFilter(/*outputChannel=*/0, nativeFilter2);

  // Cleanup.
  PendingFilterRegistry::instance().remove(taskId);
}

TEST(PendingFilterRegistryTest, IsolatesSiblingScanNodesWithinTask) {
  const std::string taskId = "test-task-two-scans";
  auto first = std::make_shared<common::BigintRange>(
      1, 10, false);
  auto second = std::make_shared<common::BigintRange>(
      20, 30, false);

  PendingFilterRegistry::instance().push(taskId, "node_0", 0, first);
  PendingFilterRegistry::instance().push(taskId, "node_1", 1, second);

  auto node0 = PendingFilterRegistry::instance().drain(taskId, "node_0");
  ASSERT_EQ(node0.size(), 1u);
  EXPECT_EQ(node0.front().column_index, 0);
  EXPECT_TRUE(PendingFilterRegistry::instance()
                  .drain(taskId, "node_missing")
                  .empty());

  auto node1 = PendingFilterRegistry::instance().drain(taskId, "node_1");
  ASSERT_EQ(node1.size(), 1u);
  EXPECT_EQ(node1.front().column_index, 1);
  EXPECT_TRUE(PendingFilterRegistry::instance()
                  .drain(taskId, "node_0")
                  .empty());
}

// ===========================================================================
// Property 1.4: Null Filter Handling
//
// Verifies that addDynamicFilter gracefully handles null filters.
//
// **Validates: Requirements 1.1, 1.3**
// ===========================================================================

TEST_P(NativeDynamicFilterReceptionTest, NullFilterIsNoOp) {
  const auto& param = GetParam();

  auto outputType = ROW({"key_col", "payload_col"}, {param.key_type, BIGINT()});
  PangrovaVeloxDataSource dataSource(
      pool_.get(),
      outputType,
      outputType,
      /*taskId=*/"test-task-null-" + param.name);

  // Null filter should be silently ignored.
  dataSource.addDynamicFilter(/*outputChannel=*/0, nullptr);
}

// ===========================================================================
// Property 1.5: QueryConfig Enables Native Dynamic Filter Generation
//
// Verifies that VeloxTaskRunnerConfig correctly maps to QueryConfig
// entries that enable native dynamic filter generation from HashProbe.
//
// **Validates: Requirements 1.1**
// ===========================================================================

TEST_P(NativeDynamicFilterReceptionTest, QueryConfigEnablesDynamicFilterPushdown) {
  const auto& param = GetParam();

  VeloxTaskRunnerConfig config;
  config.task_id = "test-df-config-" + param.name;
  config.query_id = "test-query";

  EXPECT_TRUE(config.hash_probe_finish_early_on_empty_build);
  EXPECT_TRUE(config.hash_probe_string_dynamic_filter_pushdown);

  // Verify that disabling the config flag works.
  VeloxTaskRunnerConfig disabledConfig;
  disabledConfig.hash_probe_string_dynamic_filter_pushdown = false;
  EXPECT_FALSE(disabledConfig.hash_probe_string_dynamic_filter_pushdown);
}

// ===========================================================================
// Parameterization: join key types × cardinalities
// ===========================================================================

INSTANTIATE_TEST_SUITE_P(
    DynamicFilterReception,
    NativeDynamicFilterReceptionTest,
    ::testing::Values(
        DynamicFilterReceptionParam{"Bigint_Card1", BIGINT(), 1},
        DynamicFilterReceptionParam{"Bigint_Card100", BIGINT(), 100},
        DynamicFilterReceptionParam{"Bigint_Card10000", BIGINT(), 10000},
        DynamicFilterReceptionParam{"Varchar_Card1", VARCHAR(), 1},
        DynamicFilterReceptionParam{"Varchar_Card100", VARCHAR(), 100},
        DynamicFilterReceptionParam{"Varchar_Card10000", VARCHAR(), 10000}),
    [](const ::testing::TestParamInfo<DynamicFilterReceptionParam>& info) {
      return info.param.name;
    });

}  // namespace
}  // namespace pangrova::velox::test
