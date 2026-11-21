// VeloxIntegrationTest: Tests Velox integration layer.
// Velox is always available (REQUIRED dependency). Tests verify:
//   - Velox adapter classes compile and are functional.
//   - TaskExecutor uses Velox as the sole execution path.
#include <gtest/gtest.h>
#include "velox/expression/FunctionRegistrySnapshot.h"
#include "TestMemoryHelper.h"
#include <stdexcept>
#include <memory>
#include <vector>

#include "engine/executor/driver/TaskExecutor.h"
#include "engine/executor/queue/OutputQueue.h"
#include "velox/core/PlanNode.h"
#include "velox/connectors/ConnectorRegistry.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "engine/velox/connector/PangrovaVeloxConnector.h"
#include "engine/velox/memory/VeloxMemoryGovernor.h"
#include "engine/velox/plan/VeloxPlanConverter.h"
#include "engine/velox/runner/VeloxTaskRunner.h"

namespace pangrova::velox::test {
namespace {

// --- Availability checks ---

TEST(VeloxAvailabilityTest, ConnectorAdapterIsAvailable) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const auto connector = facebook::velox::connector::ConnectorRegistry::tryGet(
      PangrovaVeloxConnector::kConnectorId);
  ASSERT_NE(connector, nullptr);
  EXPECT_EQ(connector->connectorId(), PangrovaVeloxConnector::kConnectorId);
  EXPECT_NE(std::dynamic_pointer_cast<PangrovaVeloxConnector>(connector), nullptr);
}

TEST(VeloxAvailabilityTest, PlanConverterIsAlwaysAvailable) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  EXPECT_EQ(converter.convert({}, {}), nullptr);
  EXPECT_THROW(converter.convert({0xff, 0xfe, 0xfd}, {}), std::runtime_error);
}

TEST(VeloxAvailabilityTest, TaskRunnerClassIsAvailable) {
  auto pool = pangrova::test::testLeafPool("runner-admission-oracle");
  using namespace facebook::velox;
  auto values = BaseVector::create<RowVector>(ROW({"v"}, {BIGINT()}), 1, pool.get());
  auto plan = std::make_shared<core::ValuesNode>("runner-values", std::vector<RowVectorPtr>{values});
  executor::OutputQueue queue(1);
  VeloxTaskRunnerConfig config;
  config.function_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  config.task_id = "test-task";
  config.query_id = "test-query";
  config.max_drivers = 1;
  EXPECT_EQ(config.max_drivers, 1);
  EXPECT_NO_THROW(VeloxTaskRunner(config, plan, {}, &queue));
  config.function_registry.reset();
  EXPECT_THROW(VeloxTaskRunner(config, plan, {}, &queue), std::invalid_argument);
}

TEST(VeloxAvailabilityTest, MemoryBridgeIsAvailable) {
  auto root = pangrova::test::testRootPool("velox-integration-allocation-root");
  auto pool = root->addLeafChild("velox-integration-allocation");
  ASSERT_TRUE(pool->trackUsage());
  ASSERT_NE(pangrova::jni::globalVeloxMemoryGovernor(), nullptr);
  auto* allocation = pool->allocate(1024);
  ASSERT_NE(allocation, nullptr);
  EXPECT_GE(pool->usedBytes(), 1024);
  pool->free(allocation, 1024);
  EXPECT_EQ(pool->usedBytes(), 0);
  EXPECT_EQ(root->usedBytes(), 0);
}

// --- TaskExecutor uses Velox as sole execution path ---

TEST(VeloxExecutionTest, TaskExecutorConstructsWithVeloxPath) {
  executor::TaskExecutorConfig config;
  config.function_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  config.task_id = "test-task-velox";
  config.query_id = "test-query-1";
  config.substrait_plan = {};  // Empty plan.
  config.output_queue_capacity = 4;

  executor::TaskExecutor executor(config);
  // Executor should be constructable with Velox as the sole path.
  EXPECT_EQ(executor.state(), executor::ExecutorState::PENDING);
  EXPECT_EQ(executor.taskId(), "test-task-velox");
}

// --- Compile-time verification ---

TEST(VeloxCompileGuardTest, VeloxClassesCompile) {
  EXPECT_THROW(VeloxPlanConverter(nullptr), std::invalid_argument);
  VeloxTaskRunnerConfig config;
  EXPECT_THROW(VeloxTaskRunner(config, nullptr, {}, nullptr), std::invalid_argument);
}

}  // namespace
}  // namespace pangrova::velox::test
