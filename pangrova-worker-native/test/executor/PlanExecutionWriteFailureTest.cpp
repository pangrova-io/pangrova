#include <gtest/gtest.h>
#include "velox/expression/FunctionRegistrySnapshot.h"

#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <thread>
#include <vector>

#include "../TestMemoryHelper.h"
#include "executor/driver/TaskExecutor.h"
#include "substrait/plan.pb.h"

namespace pangrova::executor::test {
namespace {

std::vector<uint8_t> writeSourcePlan() {
  substrait::Plan plan;
  auto* root = plan.add_relations()->mutable_root();
  root->add_names("value");
  auto* read = root->mutable_input()->mutable_read();
  auto* schema = read->mutable_base_schema();
  schema->add_names("value");
  schema->mutable_struct_()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  schema->mutable_struct_()->add_types()->mutable_i64()->set_nullability(
      substrait::Type::NULLABILITY_REQUIRED);
  for (int64_t value = 0; value < 16; ++value) {
    read->mutable_virtual_table()->add_expressions()->add_fields()
        ->mutable_literal()->set_i64(value);
  }
  std::vector<uint8_t> serialized(plan.ByteSizeLong());
  if (!plan.SerializeToArray(serialized.data(), static_cast<int>(serialized.size()))) {
    throw std::runtime_error("Actual write source plan serialization failed");
  }
  return serialized;
}

TEST(PlanExecutionWriteFailureTest, MissingActualMetadataChannelFailsAndJoinsConsumer) {
  pangrova::test::testMemoryManager();
  TaskExecutorConfig config;
  config.function_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  config.query_id = "actual-write-channel-query";
  config.task_id = "actual-write-channel-task";
  config.substrait_plan = writeSourcePlan();
  config.output_queue_capacity = 1;
  config.queue_poll_timeout = std::chrono::milliseconds(10);
  config.write_config.target_table = "lakehouse.public.sink";
  config.write_config.write_mode = "INSERT";
  config.write_config.file_format = split::FileFormatType::PARQUET;
  TaskExecutor executor(config);
  executor.start();
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (executor.state() == ExecutorState::RUNNING &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  ASSERT_EQ(executor.state(), ExecutorState::FAILED);
  EXPECT_NE(executor.errorMessage().find("registered NativeWriter"), std::string::npos);
  EXPECT_TRUE(executor.outputQueue()->isClosed());
  EXPECT_NO_THROW(executor.release());
  EXPECT_EQ(executor.state(), ExecutorState::FAILED);
  EXPECT_TRUE(executor.hasFinalTaskStats());
  EXPECT_FALSE(executor.collectTaskStats().empty());
  EXPECT_NO_THROW(executor.release());
}

}  // namespace
}  // namespace pangrova::executor::test
