#include "executor/driver/PlanExecutionConfiguration.h"

#include <gtest/gtest.h>

#include "executor/driver/PlanExecutionDriver.h"
#include "executor/driver/TaskExecutor.h"
#include "velox/plan/VeloxPlanConverter.h"

namespace pangrova::executor {

TEST(PlanExecutionConfigurationTest, SessionDriverOverrideIsAppliedBeforeDriverSelection) {
  TaskExecutorConfig task;
  task.resource_limit.max_threads = 3;
  task.session_properties["velox.max_drivers"] = "7";
  velox::VeloxPlanConverter converter;
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
  task.task_id = "runner-config";
  task.query_id = "runner-query";
  task.tenant_id = "runner-tenant";
  task.resource_limit.max_provider_items = 7;
  task.resource_limit.max_provider_attempts = 11;
  task.resource_limit.max_provider_request_bytes = 31;
  task.resource_limit.max_provider_response_bytes = 37;
  task.session_properties = {{"velox.adaptive_filter_reordering", "false"},
      {"velox.spill_num_partition_bits", "5"},
      {"velox.max_partial_agg_memory", "67108864"},
      {"velox.shuffle_compression_codec", "none"},
      {"velox.preserve_dictionary_encoding", "false"}};
  velox::VeloxPlanConverter converter;
  WorkerLocalSpillPolicy spill{true, "/task-local-spill", 4096};
  const auto runner = makeRunnerConfig(task, converter, spill);
  EXPECT_EQ(runner.task_id, task.task_id);
  EXPECT_EQ(runner.query_id, task.query_id);
  EXPECT_EQ(runner.tenant_id, task.tenant_id);
  EXPECT_EQ(runner.max_provider_items, 7);
  EXPECT_EQ(runner.max_provider_attempts, 11);
  EXPECT_EQ(runner.max_provider_request_bytes, 31);
  EXPECT_EQ(runner.max_provider_response_bytes, 37);
  EXPECT_TRUE(runner.spill_enabled);
  EXPECT_EQ(runner.spill_directory, spill.directory);
  EXPECT_EQ(runner.max_spill_bytes, spill.max_bytes);
  EXPECT_FALSE(runner.adaptive_filter_reordering);
  EXPECT_FALSE(runner.preserve_dictionary_encoding);
  EXPECT_EQ(runner.spill_num_partition_bits, 5);
  EXPECT_EQ(runner.max_partial_agg_memory, 67108864);
  EXPECT_EQ(runner.shuffle_compression_codec, "none");
}

}  // namespace pangrova::executor
