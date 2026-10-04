#pragma once

#include <atomic>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../TestMemoryHelper.h"
#include "velox/core/PlanFragment.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/Task.h"
#include "velox/expression/FunctionRegistrySnapshot.h"
#include "velox/vector/ComplexVector.h"

namespace pangrova::velox::test {
using FunctionRegistryOwner =
    std::shared_ptr<const facebook::velox::exec::FunctionRegistrySnapshot>;
struct CanonicalTaskOutput {
  std::shared_ptr<facebook::velox::exec::Task> task;
  std::vector<facebook::velox::RowVectorPtr> batches;
};

// Keeps the real task, vectors and their pools alive throughout value checks.
inline CanonicalTaskOutput executeCanonicalTask(
    const facebook::velox::core::PlanNodePtr& node,
    const FunctionRegistryOwner& registry) {
  static std::atomic<uint64_t> id{0};
  using namespace facebook::velox;
  CanonicalTaskOutput result;
  core::QueryConfig config({{"require_function_registry_snapshot", "true"}}, registry);
  result.task = exec::Task::create("canonical-task-oracle-" + std::to_string(id.fetch_add(1)),
      core::PlanFragment{node}, 0,
      core::QueryCtx::create(pangrova::jni::globalExecutor().get(), std::move(config)),
      exec::Task::ExecutionMode::kSerial);
  EXPECT_TRUE(result.task->supportSerialExecutionMode());
  while (auto batch = result.task->next()) result.batches.push_back(std::move(batch));
  EXPECT_FALSE(result.task->error()) << result.task->errorMessage();
  EXPECT_EQ(result.task->state(), exec::TaskState::kFinished);
  EXPECT_EQ(result.task->numRunningDrivers(), 0);
  return result;
}
}  // namespace pangrova::velox::test
