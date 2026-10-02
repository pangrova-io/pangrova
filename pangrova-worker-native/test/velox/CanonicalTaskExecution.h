#pragma once

#include <atomic>
#include <gtest/gtest.h>

#include "../TestMemoryHelper.h"
#include "velox/core/PlanFragment.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/Task.h"

namespace pangrova::velox::test {
struct CanonicalTaskOutput {
  std::shared_ptr<facebook::velox::exec::Task> task;
  std::vector<facebook::velox::RowVectorPtr> batches;
};

// Keeps the real task, vectors and their pools alive throughout value checks.
inline CanonicalTaskOutput executeCanonicalTask(
    const facebook::velox::core::PlanNodePtr& node) {
  static std::atomic<uint64_t> id{0};
  using namespace facebook::velox;
  CanonicalTaskOutput result;
  result.task = exec::Task::create("canonical-task-oracle-" + std::to_string(id.fetch_add(1)),
      core::PlanFragment{node}, 0, core::QueryCtx::create(pangrova::jni::globalExecutor().get()),
      exec::Task::ExecutionMode::kSerial);
  EXPECT_TRUE(result.task->supportSerialExecutionMode());
  while (auto batch = result.task->next()) result.batches.push_back(std::move(batch));
  EXPECT_FALSE(result.task->error()) << result.task->errorMessage();
  EXPECT_EQ(result.task->state(), exec::TaskState::kFinished);
  EXPECT_EQ(result.task->numRunningDrivers(), 0);
  return result;
}
}  // namespace pangrova::velox::test
