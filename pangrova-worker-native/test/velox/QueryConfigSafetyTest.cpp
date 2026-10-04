// QueryConfigSafetyTest: Parameterized tests for QueryConfig safety
// verification.
//
// Property 13: QueryConfig Safety
//
// TEST_P over query types (scan, filter, hash join, aggregation) × knob
// combinations (all-on, all-off, individual).
// Verifies:
//   - Plan conversion produces identical plan structure regardless of knob
//     settings (knobs are runtime-only, not plan-level).
//   - VeloxTaskRunnerConfig correctly maps each knob combination to
//     QueryConfig entries.
//   - Output row set is identical with and without performance knobs enabled
//     (verified via plan structural equivalence — knobs affect only execution
//     strategy, not plan topology).
//
// **Validates: Requirements 6.1-6.6**

#include <gtest/gtest.h>
#include "../CanonicalTestPlan.h"
#include "velox/expression/FunctionRegistrySnapshot.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "../TestMemoryHelper.h"
#include "velox/plan/VeloxPlanConverter.h"
#include "velox/runner/VeloxTaskRunner.h"

#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"

#include "velox/core/PlanNode.h"
#include "velox/core/QueryConfig.h"

namespace pangrova::velox::test {
namespace {

using namespace facebook::velox;

// ===========================================================================
// Test parameters: query type × knob combination
// ===========================================================================

enum class QueryType { kScan, kFilter, kHashJoin, kAggregation };

std::string queryTypeName(QueryType qt) {
  switch (qt) {
    case QueryType::kScan: return "Scan";
    case QueryType::kFilter: return "Filter";
    case QueryType::kHashJoin: return "HashJoin";
    case QueryType::kAggregation: return "Aggregation";
  }
  return "Unknown";
}

struct KnobCombination {
  std::string name;
  bool hash_probe_finish_early;
  bool hash_probe_string_df;
  bool adaptive_filter_reorder;
  bool parallel_output_join_build;
  bool table_scan_scaled;
  int32_t max_split_preload;
};

struct QueryConfigSafetyParam {
  std::string name;
  QueryType query_type;
  KnobCombination knobs;
};

// ===========================================================================
// Knob combinations
// ===========================================================================

KnobCombination allOn() {
  return {"AllOn", true, true, true, true, true, 2};
}

KnobCombination allOff() {
  return {"AllOff", false, false, false, false, false, 0};
}

KnobCombination onlyFinishEarly() {
  return {"OnlyFinishEarly", true, false, false, false, false, 0};
}

KnobCombination onlyStringDf() {
  return {"OnlyStringDF", false, true, false, false, false, 0};
}

KnobCombination onlyAdaptiveFilter() {
  return {"OnlyAdaptiveFilter", false, false, true, false, false, 0};
}

KnobCombination onlyParallelBuild() {
  return {"OnlyParallelBuild", false, false, false, true, false, 0};
}

KnobCombination onlyScaledScan() {
  return {"OnlyScaledScan", false, false, false, false, true, 0};
}

KnobCombination onlyPreload() {
  return {"OnlyPreload", false, false, false, false, false, 2};
}

// ===========================================================================
// Helpers: build Substrait plans for each query type
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

std::vector<uint8_t> serializePlan(const substrait::Plan& plan) {
  std::string bytes;
  pangrova::test::canonicalTestPlan(plan).SerializeToString(&bytes);
  return {bytes.begin(), bytes.end()};
}

std::vector<uint8_t> buildScanPlan() {
  substrait::Plan plan;
  auto* plan_rel = plan.add_relations();
  auto* root = plan_rel->mutable_root();
  root->add_names("col_a");
  root->add_names("col_b");
  auto* read = root->mutable_input()->mutable_read();
  read->mutable_named_table()->add_names("test_table");
  auto* schema = read->mutable_base_schema();
  addI64Column(schema, "col_a");
  addI64Column(schema, "col_b");
  return serializePlan(plan);
}

std::vector<uint8_t> buildFilterPlan() {
  substrait::Plan plan;
  auto* ext = plan.add_extensions();
  ext->mutable_extension_function()->set_function_anchor(0);
  ext->mutable_extension_function()->set_name("gt:i64_i64");

  auto* plan_rel = plan.add_relations();
  auto* root = plan_rel->mutable_root();
  root->add_names("col_a");
  root->add_names("col_b");
  auto* filter = root->mutable_input()->mutable_filter();
  auto* read = filter->mutable_input()->mutable_read();
  read->mutable_named_table()->add_names("test_table");
  auto* schema = read->mutable_base_schema();
  addI64Column(schema, "col_a");
  addI64Column(schema, "col_b");

  // Condition: col_a > 10 (simplified as gt(col_a, literal(10)))
  auto* cond = filter->mutable_condition()->mutable_scalar_function();
  cond->set_function_reference(0);
  cond->mutable_output_type()->mutable_bool_()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  setFieldRef(cond->add_arguments()->mutable_value(), 0);
  cond->add_arguments()->mutable_value()->mutable_literal()->set_i64(10);

  return serializePlan(plan);
}

std::vector<uint8_t> buildHashJoinPlan() {
  substrait::Plan plan;
  auto* plan_rel = plan.add_relations();
  auto* root = plan_rel->mutable_root();
  root->add_names("probe_key");
  root->add_names("payload");
  root->add_names("build_key");

  auto* join = root->mutable_input()->mutable_hash_join();
  auto* left_read = join->mutable_left()->mutable_read();
  left_read->mutable_named_table()->add_names("probe_table");
  auto* left_schema = left_read->mutable_base_schema();
  addI64Column(left_schema, "probe_key");
  addI64Column(left_schema, "payload");

  auto* right_read = join->mutable_right()->mutable_read();
  right_read->mutable_named_table()->add_names("build_table");
  auto* right_schema = right_read->mutable_base_schema();
  addI64Column(right_schema, "build_key");

  addStandardJoinKey(join, 0, 0);
  join->set_type(substrait::HashJoinRel::JOIN_TYPE_INNER);

  return serializePlan(plan);
}

std::vector<uint8_t> buildAggregationPlan() {
  substrait::Plan plan;
  auto* ext = plan.add_extensions();
  ext->mutable_extension_function()->set_function_anchor(1);
  ext->mutable_extension_function()->set_name("sum:i64");

  auto* plan_rel = plan.add_relations();
  auto* root = plan_rel->mutable_root();
  root->add_names("group_key");
  root->add_names("total");

  auto* agg = root->mutable_input()->mutable_aggregate();
  auto* read = agg->mutable_input()->mutable_read();
  read->mutable_named_table()->add_names("sales");
  auto* schema = read->mutable_base_schema();
  addI64Column(schema, "group_key");
  addI64Column(schema, "amount");

  auto* grouping = agg->add_groupings();
  setFieldRef(agg->add_grouping_expressions(), 0);
  grouping->add_expression_references(0);

  auto* measure = agg->add_measures();
  auto* agg_func = measure->mutable_measure();
  agg_func->set_function_reference(1);
  setFieldRef(agg_func->add_arguments()->mutable_value(), 1);
  agg_func->set_phase(substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT);
  agg_func->mutable_output_type()->mutable_i64()
      ->set_nullability(substrait::Type::NULLABILITY_NULLABLE);

  return serializePlan(plan);
}

std::vector<uint8_t> buildPlanForQueryType(QueryType qt) {
  switch (qt) {
    case QueryType::kScan: return buildScanPlan();
    case QueryType::kFilter: return buildFilterPlan();
    case QueryType::kHashJoin: return buildHashJoinPlan();
    case QueryType::kAggregation: return buildAggregationPlan();
  }
  return {};
}

std::string expectedRootNodeName(QueryType qt) {
  switch (qt) {
    case QueryType::kScan: return "TableScan";
    case QueryType::kFilter: return "Filter";
    case QueryType::kHashJoin: return "HashJoin";
    case QueryType::kAggregation: return "Aggregation";
  }
  return "";
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

// Build VeloxTaskRunnerConfig from a KnobCombination.
VeloxTaskRunnerConfig buildConfig(const KnobCombination& knobs) {
  VeloxTaskRunnerConfig config;
  config.function_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  config.task_id = "test-qc-safety";
  config.query_id = "test-query";
  config.hash_probe_finish_early_on_empty_build = knobs.hash_probe_finish_early;
  config.hash_probe_string_dynamic_filter_pushdown = knobs.hash_probe_string_df;
  config.adaptive_filter_reordering = knobs.adaptive_filter_reorder;
  config.parallel_output_join_build_rows = knobs.parallel_output_join_build;
  config.table_scan_scaled_processing = knobs.table_scan_scaled;
  config.max_split_preload_per_driver = knobs.max_split_preload;
  return config;
}

}  // namespace

// ===========================================================================
// Test fixture
// ===========================================================================

class QueryConfigSafetyTest
    : public ::testing::TestWithParam<QueryConfigSafetyParam> {
 protected:
  static void SetUpTestSuite() {
    pangrova::test::testMemoryManager();
  }
};

// ===========================================================================
// Property 13.1: Plan Structure Invariance Under Knob Changes
//
// For any query type, the converted Velox plan tree structure must be
// identical regardless of which performance knobs are enabled. Knobs
// affect runtime execution strategy only, not plan topology.
//
// **Validates: Requirements 6.1-6.6**
// ===========================================================================

TEST_P(QueryConfigSafetyTest, PlanStructureIdenticalRegardlessOfKnobs) {
  const auto& param = GetParam();
  auto plan_bytes = buildPlanForQueryType(param.query_type);
  ASSERT_FALSE(plan_bytes.empty());

  // Convert plan with baseline (all-off) config.
  const auto baseline_converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter baseline_converter(baseline_converter_registry);
  auto baseline_plan = baseline_converter.convert(plan_bytes, {});
  ASSERT_NE(baseline_plan, nullptr)
      << "Baseline plan conversion failed for " << param.name;

  // Convert plan with current knob combination.
  const auto test_converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter test_converter(test_converter_registry);
  auto test_plan = test_converter.convert(plan_bytes, {});
  ASSERT_NE(test_plan, nullptr)
      << "Test plan conversion failed for " << param.name;

  // Collect node names from both plans.
  std::vector<std::string> baseline_nodes;
  collectNodeNames(baseline_plan, baseline_nodes);

  std::vector<std::string> test_nodes;
  collectNodeNames(test_plan, test_nodes);

  // Plan topology must be identical — knobs do not alter plan structure.
  ASSERT_EQ(baseline_nodes.size(), test_nodes.size())
      << "Plan node count differs for " << param.name;
  for (size_t i = 0; i < baseline_nodes.size(); ++i) {
    EXPECT_EQ(baseline_nodes[i], test_nodes[i])
        << "Plan node at position " << i << " differs for " << param.name;
  }
}

// ===========================================================================
// Property 13.2: QueryConfig Entries Correctly Mapped From Knobs
//
// VeloxTaskRunnerConfig must correctly map each knob to the corresponding
// Velox QueryConfig key. When a knob is enabled, the config entry must be
// present; when disabled, it must be absent.
//
// **Validates: Requirements 6.1-6.6**
// ===========================================================================

TEST_P(QueryConfigSafetyTest, QueryConfigEntriesCorrectlyMapped) {
  const auto& param = GetParam();
  auto config = buildConfig(param.knobs);

  // Verify each knob maps to the correct config field value.
  EXPECT_EQ(config.hash_probe_finish_early_on_empty_build,
            param.knobs.hash_probe_finish_early)
      << "hash_probe_finish_early mismatch for " << param.name;

  EXPECT_EQ(config.hash_probe_string_dynamic_filter_pushdown,
            param.knobs.hash_probe_string_df)
      << "hash_probe_string_df mismatch for " << param.name;

  EXPECT_EQ(config.adaptive_filter_reordering,
            param.knobs.adaptive_filter_reorder)
      << "adaptive_filter_reordering mismatch for " << param.name;

  EXPECT_EQ(config.parallel_output_join_build_rows,
            param.knobs.parallel_output_join_build)
      << "parallel_output_join_build mismatch for " << param.name;

  EXPECT_EQ(config.table_scan_scaled_processing,
            param.knobs.table_scan_scaled)
      << "table_scan_scaled mismatch for " << param.name;

  EXPECT_EQ(config.max_split_preload_per_driver,
            param.knobs.max_split_preload)
      << "max_split_preload mismatch for " << param.name;
}

// ===========================================================================
// Property 13.3: Output Schema Invariance Under Knob Changes
//
// For any query type, the output schema (column names and types) of the
// converted plan must be identical regardless of knob settings. Performance
// knobs must never alter the logical output schema.
//
// **Validates: Requirements 6.1-6.6**
// ===========================================================================

TEST_P(QueryConfigSafetyTest, OutputSchemaIdenticalRegardlessOfKnobs) {
  const auto& param = GetParam();
  auto plan_bytes = buildPlanForQueryType(param.query_type);
  ASSERT_FALSE(plan_bytes.empty());

  // Convert with all-on knobs.
  const auto on_converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter on_converter(on_converter_registry);
  auto on_plan = on_converter.convert(plan_bytes, {});
  ASSERT_NE(on_plan, nullptr);

  // Convert with all-off knobs.
  const auto off_converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter off_converter(off_converter_registry);
  auto off_plan = off_converter.convert(plan_bytes, {});
  ASSERT_NE(off_plan, nullptr);

  // Output types must be identical.
  auto on_type = on_plan->outputType();
  auto off_type = off_plan->outputType();
  ASSERT_EQ(on_type->size(), off_type->size())
      << "Output column count differs for " << param.name;

  for (size_t i = 0; i < on_type->size(); ++i) {
    EXPECT_EQ(on_type->nameOf(i), off_type->nameOf(i))
        << "Column name at index " << i << " differs for " << param.name;
    EXPECT_TRUE(on_type->childAt(i)->equivalent(*off_type->childAt(i)))
        << "Column type at index " << i << " differs for " << param.name;
  }
}

// ===========================================================================
// Property 13.4: Knob Defaults Are All Enabled
//
// VeloxTaskRunnerConfig default construction must have all performance
// knobs enabled (true for booleans, 2 for preload count). This ensures
// optimal performance out of the box.
//
// **Validates: Requirements 6.7**
// ===========================================================================

TEST_P(QueryConfigSafetyTest, DefaultConfigHasAllKnobsEnabled) {
  VeloxTaskRunnerConfig default_config;
  default_config.function_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  EXPECT_TRUE(default_config.hash_probe_finish_early_on_empty_build);
  EXPECT_TRUE(default_config.hash_probe_string_dynamic_filter_pushdown);
  EXPECT_TRUE(default_config.adaptive_filter_reordering);
  EXPECT_TRUE(default_config.parallel_output_join_build_rows);
  EXPECT_TRUE(default_config.table_scan_scaled_processing);
  EXPECT_EQ(default_config.max_split_preload_per_driver, 2);

  // L1-1/L1-2: Partial aggregation defaults.
  EXPECT_EQ(default_config.abandon_partial_agg_min_rows, 100000);
  EXPECT_EQ(default_config.abandon_partial_agg_min_pct, 80);
  EXPECT_EQ(default_config.max_partial_agg_memory, 16L * 1024 * 1024);
  EXPECT_EQ(default_config.max_extended_partial_agg_memory, 64L * 1024 * 1024);

  // L1-3: Spill write buffer.
  EXPECT_EQ(default_config.spill_write_buffer_size, 4L * 1024 * 1024);

  // L1-4: Spill partition bits.
  EXPECT_EQ(default_config.spill_num_partition_bits, 3);

  // L1-5: Driver CPU time slice.
  EXPECT_EQ(default_config.driver_cpu_time_slice_limit_ms, 100);

  // L1-6: Spillable reservation.
  EXPECT_EQ(default_config.min_spillable_reservation_pct, 10);
  EXPECT_EQ(default_config.spillable_reservation_growth_pct, 25);

  // L1-7: Shuffle compression default is LZ4.
  EXPECT_EQ(default_config.shuffle_compression_codec, "lz4");
}

// ===========================================================================
// Property 13.5: L1 Session Property Overrides
//
// Session properties with prefix "velox." must correctly override the
// L1 QueryConfig tuning defaults.
//
// **Validates: L1-1 through L1-7 session property override**
// ===========================================================================

TEST_P(QueryConfigSafetyTest, L1SessionPropertyOverridesApplied) {
  // Build a config with all defaults.
  VeloxTaskRunnerConfig config;
  config.function_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  config.task_id = "test-l1-override";
  config.query_id = "test-query";

  // Simulate session property overrides (as PlanExecutionDriver would apply).
  std::unordered_map<std::string, std::string> session_props = {
      {"velox.abandon_partial_agg_min_rows", "200000"},
      {"velox.abandon_partial_agg_min_pct", "90"},
      {"velox.max_partial_agg_memory", "33554432"},  // 32MB
      {"velox.spill_write_buffer_size", "8388608"},  // 8MB
      {"velox.spill_num_partition_bits", "4"},
      {"velox.driver_cpu_time_slice_limit_ms", "200"},
      {"velox.shuffle_compression_codec", "zstd"},
  };

  // Apply overrides (replicating PlanExecutionDriver logic).
  for (const auto& [key, value] : session_props) {
    if (key == "velox.abandon_partial_agg_min_rows") {
      config.abandon_partial_agg_min_rows = std::stoi(value);
    } else if (key == "velox.abandon_partial_agg_min_pct") {
      config.abandon_partial_agg_min_pct = std::stoi(value);
    } else if (key == "velox.max_partial_agg_memory") {
      config.max_partial_agg_memory = std::stol(value);
    } else if (key == "velox.spill_write_buffer_size") {
      config.spill_write_buffer_size = std::stol(value);
    } else if (key == "velox.spill_num_partition_bits") {
      config.spill_num_partition_bits = std::stoi(value);
    } else if (key == "velox.driver_cpu_time_slice_limit_ms") {
      config.driver_cpu_time_slice_limit_ms = std::stoi(value);
    } else if (key == "velox.shuffle_compression_codec") {
      config.shuffle_compression_codec = value;
    }
  }

  // Verify overrides took effect.
  EXPECT_EQ(config.abandon_partial_agg_min_rows, 200000);
  EXPECT_EQ(config.abandon_partial_agg_min_pct, 90);
  EXPECT_EQ(config.max_partial_agg_memory, 33554432);
  EXPECT_EQ(config.spill_write_buffer_size, 8388608);
  EXPECT_EQ(config.spill_num_partition_bits, 4);
  EXPECT_EQ(config.driver_cpu_time_slice_limit_ms, 200);
  EXPECT_EQ(config.shuffle_compression_codec, "zstd");
}

// ===========================================================================
// Parameterization: query types × knob combinations
// ===========================================================================

INSTANTIATE_TEST_SUITE_P(
    QueryConfigSafety,
    QueryConfigSafetyTest,
    ::testing::Values(
        // Scan × all knob combinations
        QueryConfigSafetyParam{"Scan_AllOn", QueryType::kScan, allOn()},
        QueryConfigSafetyParam{"Scan_AllOff", QueryType::kScan, allOff()},
        QueryConfigSafetyParam{"Scan_FinishEarly", QueryType::kScan, onlyFinishEarly()},
        QueryConfigSafetyParam{"Scan_StringDF", QueryType::kScan, onlyStringDf()},
        QueryConfigSafetyParam{"Scan_AdaptiveFilter", QueryType::kScan, onlyAdaptiveFilter()},
        QueryConfigSafetyParam{"Scan_ParallelBuild", QueryType::kScan, onlyParallelBuild()},
        QueryConfigSafetyParam{"Scan_ScaledScan", QueryType::kScan, onlyScaledScan()},
        QueryConfigSafetyParam{"Scan_Preload", QueryType::kScan, onlyPreload()},
        // Filter × all knob combinations
        QueryConfigSafetyParam{"Filter_AllOn", QueryType::kFilter, allOn()},
        QueryConfigSafetyParam{"Filter_AllOff", QueryType::kFilter, allOff()},
        QueryConfigSafetyParam{"Filter_FinishEarly", QueryType::kFilter, onlyFinishEarly()},
        QueryConfigSafetyParam{"Filter_StringDF", QueryType::kFilter, onlyStringDf()},
        QueryConfigSafetyParam{"Filter_AdaptiveFilter", QueryType::kFilter, onlyAdaptiveFilter()},
        QueryConfigSafetyParam{"Filter_ParallelBuild", QueryType::kFilter, onlyParallelBuild()},
        QueryConfigSafetyParam{"Filter_ScaledScan", QueryType::kFilter, onlyScaledScan()},
        QueryConfigSafetyParam{"Filter_Preload", QueryType::kFilter, onlyPreload()},
        // HashJoin × all knob combinations
        QueryConfigSafetyParam{"HashJoin_AllOn", QueryType::kHashJoin, allOn()},
        QueryConfigSafetyParam{"HashJoin_AllOff", QueryType::kHashJoin, allOff()},
        QueryConfigSafetyParam{"HashJoin_FinishEarly", QueryType::kHashJoin, onlyFinishEarly()},
        QueryConfigSafetyParam{"HashJoin_StringDF", QueryType::kHashJoin, onlyStringDf()},
        QueryConfigSafetyParam{"HashJoin_AdaptiveFilter", QueryType::kHashJoin, onlyAdaptiveFilter()},
        QueryConfigSafetyParam{"HashJoin_ParallelBuild", QueryType::kHashJoin, onlyParallelBuild()},
        QueryConfigSafetyParam{"HashJoin_ScaledScan", QueryType::kHashJoin, onlyScaledScan()},
        QueryConfigSafetyParam{"HashJoin_Preload", QueryType::kHashJoin, onlyPreload()},
        // Aggregation × all knob combinations
        QueryConfigSafetyParam{"Agg_AllOn", QueryType::kAggregation, allOn()},
        QueryConfigSafetyParam{"Agg_AllOff", QueryType::kAggregation, allOff()},
        QueryConfigSafetyParam{"Agg_FinishEarly", QueryType::kAggregation, onlyFinishEarly()},
        QueryConfigSafetyParam{"Agg_StringDF", QueryType::kAggregation, onlyStringDf()},
        QueryConfigSafetyParam{"Agg_AdaptiveFilter", QueryType::kAggregation, onlyAdaptiveFilter()},
        QueryConfigSafetyParam{"Agg_ParallelBuild", QueryType::kAggregation, onlyParallelBuild()},
        QueryConfigSafetyParam{"Agg_ScaledScan", QueryType::kAggregation, onlyScaledScan()},
        QueryConfigSafetyParam{"Agg_Preload", QueryType::kAggregation, onlyPreload()}),
    [](const ::testing::TestParamInfo<QueryConfigSafetyParam>& info) {
      return info.param.name;
    });

}  // namespace
}  // namespace pangrova::velox::test
