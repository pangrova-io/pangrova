#include "engine/executor/driver/TaskLifecycle.h"

#include <gtest/gtest.h>
#include <future>
#include <thread>
#include <vector>

namespace pangrova::executor {

TEST(TaskLifecycleTest, WaitWakesOnlyOnTerminalStateAndLateReaderSeesIt) {
  using namespace std::chrono_literals;
  TaskLifecycle lifecycle;
  ASSERT_TRUE(lifecycle.transitionTo(ExecutorState::RUNNING));
  std::promise<void> reached_wait;
  auto entered = reached_wait.get_future();
  auto observed = std::async(std::launch::async, [&] {
    reached_wait.set_value();
    return lifecycle.awaitTerminal(5s);
  });
  ASSERT_EQ(entered.wait_for(1s), std::future_status::ready);
  ASSERT_TRUE(lifecycle.transitionTo(ExecutorState::FLUSHING));
  EXPECT_EQ(observed.wait_for(10ms), std::future_status::timeout);
  ASSERT_TRUE(lifecycle.transitionTo(ExecutorState::FINISHED));
  ASSERT_EQ(observed.wait_for(1s), std::future_status::ready);
  EXPECT_EQ(observed.get(), ExecutorState::FINISHED);
  EXPECT_EQ(lifecycle.awaitTerminal(0ms), ExecutorState::FINISHED);
}

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

TEST(TaskLifecycleTest, ProducerFailurePrecedesResultStreamRevocation) {
  TaskLifecycle lifecycle;
  ASSERT_TRUE(lifecycle.transitionTo(ExecutorState::RUNNING));
  protocol::v2::ExecutionError producer;
  producer.set_error_class(protocol::v2::ERROR_CLASS_INTERNAL);
  producer.set_code("NATIVE_TASK_FAILED");
  producer.set_message("division by zero");
  producer.set_retryable(false);
  producer.set_failed_component("native-executor");
  lifecycle.recordExecutionError(producer);

  protocol::v2::ExecutionError stream;
  stream.set_error_class(protocol::v2::ERROR_CLASS_RESULT_VALIDATION);
  stream.set_code("RESULT_STREAM_FAILED");
  stream.set_message("Result authority was revoked");
  stream.set_retryable(false);
  stream.set_failed_component("native-result-stream");
  lifecycle.recordExecutionError(stream);
  lifecycle.notifyTaskFailed(stream.message());

  const auto state = lifecycle.snapshot();
  ASSERT_TRUE(state.error.has_value());
  EXPECT_EQ(state.error->code(), "NATIVE_TASK_FAILED");
  EXPECT_EQ(state.error->message(), "division by zero");
  EXPECT_EQ(state.state, ExecutorState::FAILED);
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
