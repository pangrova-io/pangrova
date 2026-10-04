#include "executor/driver/PlanExecutionConfiguration.h"

#include <gtest/gtest.h>
#include <stdexcept>
#include <string>
#include <vector>

#include "../CanonicalTestPlan.h"

#include "executor/driver/PlanExecutionDriver.h"
#include "executor/driver/TaskExecutor.h"
#include "velox/plan/VeloxPlanConverter.h"
#include "velox/expression/FunctionRegistrySnapshot.h"

namespace pangrova::executor {

TEST(PlanExecutionConfigurationTest, SessionDriverOverrideIsAppliedBeforeDriverSelection) {
  TaskExecutorConfig task;
  task.function_registry = facebook::velox::exec::FunctionRegistrySnapshot::capture();
  task.resource_limit.max_threads = 3;
  task.session_properties["velox.max_drivers"] = "7";
  velox::VeloxPlanConverter converter(task.function_registry);
  const auto runner = makeRunnerConfig(task, converter, {});
  EXPECT_EQ(runner.max_drivers_override, 7);
  EXPECT_EQ(runner.max_drivers, 7);
  task.session_properties["velox.max_drivers"] = "1";
  EXPECT_EQ(makeRunnerConfig(task, converter, {}).max_drivers, 2);
  task.session_properties.erase("velox.max_drivers");
  EXPECT_EQ(makeRunnerConfig(task, converter, {}).max_drivers, 3);
}

TEST(PlanExecutionConfigurationTest, ActualLimitsAuthorityAndTuningValuesArePreserved) {
  TaskExecutorConfig task;
  task.function_registry = facebook::velox::exec::FunctionRegistrySnapshot::capture();
  task.task_id = "runner-config";
  task.query_id = "runner-query";
  task.tenant_id = "runner-tenant";
  task.resource_limit.max_provider_items = 7;
  task.resource_limit.max_provider_attempts = 11;
  task.resource_limit.max_provider_request_bytes = 31;
  task.resource_limit.max_provider_response_bytes = 37;
  task.resource_limit.max_memory_bytes = 64 * 1024 * 1024;
  task.session_properties = {{"velox.adaptive_filter_reordering", "false"},
      {"velox.spill_num_partition_bits", "5"},
      {"velox.max_partial_agg_memory", "67108864"},
      {"velox.shuffle_compression_codec", "none"},
      {"velox.preserve_dictionary_encoding", "false"}};
  task.shuffle_compression_codec = "zstd";
  task.shuffle_batch_size = 3;
  velox::VeloxPlanConverter converter(task.function_registry);
  WorkerLocalSpillPolicy spill{true, "/task-local-spill", 4096};
  const auto runner = makeRunnerConfig(task, converter, spill);
  EXPECT_EQ(runner.function_registry, task.function_registry);
  EXPECT_EQ(runner.task_id, task.task_id);
  EXPECT_EQ(runner.query_id, task.query_id);
  EXPECT_EQ(runner.tenant_id, task.tenant_id);
  EXPECT_EQ(runner.max_provider_items, 7);
  EXPECT_EQ(runner.max_provider_attempts, 11);
  EXPECT_EQ(runner.max_provider_request_bytes, 31);
  EXPECT_EQ(runner.max_provider_response_bytes, 37);
  EXPECT_EQ(runner.max_memory_bytes, task.resource_limit.max_memory_bytes);
  EXPECT_TRUE(runner.spill_enabled);
  EXPECT_EQ(runner.spill_directory, spill.directory);
  EXPECT_EQ(runner.max_spill_bytes, spill.max_bytes);
  EXPECT_FALSE(runner.adaptive_filter_reordering);
  EXPECT_FALSE(runner.preserve_dictionary_encoding);
  EXPECT_EQ(runner.spill_num_partition_bits, 5);
  EXPECT_EQ(runner.max_partial_agg_memory, 67108864);
  EXPECT_EQ(runner.shuffle_compression_codec, "zstd");
  EXPECT_EQ(runner.preferred_output_batch_rows, 3);
}

TEST(PlanExecutionConfigurationTest, SerializedOutputThresholdUsesExactGrantNotSessionOverride) {
  TaskExecutorConfig task;
  task.function_registry = facebook::velox::exec::FunctionRegistrySnapshot::capture();
  task.is_result_task = true;
  task.resource_limit.max_shuffle_buffer_bytes = 1'048'576;
  task.session_properties["velox.max_output_buffer_size"] = "1073741824";
  task.session_properties["max_output_buffer_size"] = "1073741824";
  velox::VeloxPlanConverter converter(task.function_registry);
  EXPECT_EQ(makeRunnerConfig(task, converter, {}).serialized_output_buffer_threshold_bytes, 1'048'576);
  task.resource_limit.max_shuffle_buffer_bytes = 0;
  EXPECT_THROW(makeRunnerConfig(task, converter, {}), std::runtime_error);
  task.is_result_task = false;
  EXPECT_EQ(makeRunnerConfig(task, converter, {}).serialized_output_buffer_threshold_bytes, 0);
}

TEST(PlanExecutionConfigurationTest, ConvertedWireBindingsRemainOwnedAfterConverterReuse) {
  const auto bytes = [](const std::string& logicalName) {
    substrait::Plan plan;
    auto* root = plan.add_relations()->mutable_root();
    root->add_names("client_presentation");
    auto* read = root->mutable_input()->mutable_read();
    read->mutable_base_schema()->add_names(logicalName);
    read->mutable_base_schema()->mutable_struct_()->add_types()->mutable_i64()->set_nullability(
        substrait::Type::NULLABILITY_REQUIRED);
    read->mutable_virtual_table()->add_expressions()->add_fields()->mutable_literal()->set_i64(42);
    std::string wire;
    pangrova::test::canonicalTestPlan(plan).SerializeToString(&wire);
    return std::vector<uint8_t>(wire.begin(), wire.end());
  };
  TaskExecutorConfig task;
  task.function_registry = facebook::velox::exec::FunctionRegistrySnapshot::capture();
  task.is_result_task = true;
  task.resource_limit.max_shuffle_buffer_bytes = 1'048'576;
  velox::VeloxPlanConverter converter(task.function_registry);
  const auto firstOutput = converter.convert(bytes("first_logical"), {}, true);
  const auto first = makeRunnerConfig(task, converter, {});
  ASSERT_EQ(first.serde_schema_bindings.size(), 1);
  ASSERT_NE(first.canonical_output_schema, nullptr);
  EXPECT_EQ(first.serde_schema_bindings.at(firstOutput->id()).SerializeAsString(),
      first.canonical_output_schema->SerializeAsString());
  EXPECT_EQ(first.canonical_output_schema->names(0), "first_logical");
  EXPECT_EQ(first.result_column_labels, (std::vector<std::string>{"client_presentation"}));
  converter.convert(bytes("second_logical"), {}, true);
  const auto second = makeRunnerConfig(task, converter, {});
  EXPECT_EQ(second.canonical_output_schema->names(0), "second_logical");
  EXPECT_EQ(first.canonical_output_schema->names(0), "first_logical");
  EXPECT_EQ(first.serde_schema_bindings.at(firstOutput->id()).names(0), "first_logical");
  EXPECT_EQ(first.function_registry, task.function_registry);
  EXPECT_EQ(first.serialized_output_buffer_threshold_bytes, 1'048'576);
}

}  // namespace pangrova::executor
