#include "executor/driver/TaskLifecycle.h"

#include <gtest/gtest.h>
#include <thread>
#include <vector>

namespace pangrova::executor {

TEST(TaskLifecycleTest, CancelledCleanupErrorDoesNotRewriteTerminalState) {
  TaskLifecycle lifecycle;
  ASSERT_TRUE(lifecycle.transitionTo(ExecutorState::RUNNING));
  ASSERT_TRUE(lifecycle.transitionTo(ExecutorState::CANCELLED));
  lifecycle.appendError("Dataset encoders failed during cancellation");
  EXPECT_EQ(lifecycle.state(), ExecutorState::CANCELLED);
  EXPECT_EQ(lifecycle.errorMessage(), "Dataset encoders failed during cancellation");
  EXPECT_FALSE(lifecycle.transitionTo(ExecutorState::FAILED));
  EXPECT_FALSE(lifecycle.transitionTo(ExecutorState::FINISHED));
}

TEST(TaskLifecycleTest, FailedCleanupRetainsOriginalErrorAndStack) {
  TaskLifecycle lifecycle;
  lifecycle.notifyTaskFailed("Actual operator rejected its input");
  ASSERT_EQ(lifecycle.state(), ExecutorState::FAILED);
  const auto stack = lifecycle.nativeStackTrace();
  lifecycle.appendError("Writer stage cleanup failed");
  lifecycle.appendError("");
  EXPECT_EQ(lifecycle.errorMessage(),
            "Actual operator rejected its input; Writer stage cleanup failed");
  EXPECT_EQ(lifecycle.nativeStackTrace(), stack);
  EXPECT_EQ(lifecycle.state(), ExecutorState::FAILED);
}

TEST(TaskLifecycleTest, ConcurrentCleanupDiagnosticsAreNotLost) {
  TaskLifecycle lifecycle;
  ASSERT_TRUE(lifecycle.transitionTo(ExecutorState::CANCELLED));
  std::vector<std::thread> writers;
  for (int ordinal = 0; ordinal < 8; ++ordinal) {
    writers.emplace_back([&lifecycle, ordinal] {
      lifecycle.appendError("cleanup_owner_" + std::to_string(ordinal));
    });
  }
  for (auto& writer : writers) writer.join();
  const auto diagnostic = lifecycle.errorMessage();
  for (int ordinal = 0; ordinal < 8; ++ordinal) {
    const auto owner = "cleanup_owner_" + std::to_string(ordinal);
    const auto first = diagnostic.find(owner);
    ASSERT_NE(first, std::string::npos);
    EXPECT_EQ(diagnostic.find(owner, first + owner.size()), std::string::npos);
  }
  EXPECT_EQ(lifecycle.state(), ExecutorState::CANCELLED);
}

}  // namespace pangrova::executor
