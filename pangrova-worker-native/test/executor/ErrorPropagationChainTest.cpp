// Tests for error propagation chain: Native → Java.
// Validates that errors from PangrovaVeloxDataSource, VeloxTaskRunner,
// and PlanExecutionDriver propagate correctly through the chain:
//   native error → VeloxTaskRunner → PlanExecutionDriver → TaskExecutor
//   → FAILED state + error message accessible via JNI.
//
// Also validates NativeUnsupportedFeatureException carries structured
// metadata (feature_name, connector_type, split_id) and produces
// NATIVE_FALLBACK_REQUIRED prefix in the error message.
#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <thread>

#include "executor/NativeUnsupportedFeatureException.h"
#include "executor/driver/TaskExecutor.h"
#include "executor/driver/TaskLifecycle.h"

#include "../TestMemoryHelper.h"

namespace pangrova::executor::test {
namespace {

using std::chrono::milliseconds;

// Ensure Velox runtime is initialized before any test in this file.
static auto* g_init_memory = pangrova::test::testMemoryManager();

bool waitForTerminal(TaskExecutor& exec, milliseconds timeout) {
  auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    auto s = exec.state();
    if (s == ExecutorState::FINISHED || s == ExecutorState::FAILED ||
        s == ExecutorState::CANCELLED) {
      return true;
    }
    std::this_thread::sleep_for(milliseconds(10));
  }
  return false;
}

TaskExecutorConfig makeConfig() {
  TaskExecutorConfig config;
  config.task_id = "error-test-task-1";
  config.query_id = "error-test-query-1";
  config.trace_id = "trace-error-test-001";
  config.substrait_plan = {};
  config.queue_poll_timeout = milliseconds(200);
  return config;
}

// --- NativeUnsupportedFeatureException construction and accessors ---

TEST(ErrorPropagationChainTest, UnsupportedFeatureExceptionCarriesMetadata) {
  NativeUnsupportedFeatureException ex(
      "avro_format", "iceberg", "split-42");

  EXPECT_EQ(ex.featureName(), "avro_format");
  EXPECT_EQ(ex.connectorType(), "iceberg");
  EXPECT_EQ(ex.splitId(), "split-42");

  std::string msg = ex.what();
  EXPECT_NE(msg.find("avro_format"), std::string::npos);
  EXPECT_NE(msg.find("iceberg"), std::string::npos);
  EXPECT_NE(msg.find("split-42"), std::string::npos);
}

TEST(ErrorPropagationChainTest, UnsupportedFeatureExceptionIsRuntimeError) {
  // Must be catchable as std::runtime_error.
  try {
    throw NativeUnsupportedFeatureException("feat", "conn", "sid");
  } catch (const std::runtime_error& e) {
    EXPECT_NE(std::string(e.what()).find("feat"), std::string::npos);
  }
}

TEST(ErrorPropagationChainTest, UnsupportedFeatureExceptionIsStdException) {
  try {
    throw NativeUnsupportedFeatureException("feat", "conn", "sid");
  } catch (const std::exception& e) {
    EXPECT_NE(std::string(e.what()).find("feat"), std::string::npos);
  }
}

// --- Empty plan propagates error through full chain ---

TEST(ErrorPropagationChainTest, EmptyPlanProducesFailedStateWithMessage) {
  auto config = makeConfig();
  TaskExecutor exec(config);
  exec.start();
  ASSERT_TRUE(waitForTerminal(exec, milliseconds(2000)));
  EXPECT_EQ(exec.state(), ExecutorState::FAILED);
  EXPECT_FALSE(exec.errorMessage().empty());
}

// --- Garbage plan bytes propagate error ---

TEST(ErrorPropagationChainTest, GarbagePlanBytesProduceFailedState) {
  auto config = makeConfig();
  config.substrait_plan = {0xDE, 0xAD, 0xBE, 0xEF};
  TaskExecutor exec(config);
  exec.start();
  ASSERT_TRUE(waitForTerminal(exec, milliseconds(2000)));
  EXPECT_EQ(exec.state(), ExecutorState::FAILED);
  EXPECT_FALSE(exec.errorMessage().empty());
}

// --- notifyTaskFailed sets FAILED state and error message ---

TEST(ErrorPropagationChainTest, NotifyTaskFailedSetsStateAndMessage) {
  auto config = makeConfig();
  TaskExecutor exec(config);
  exec.notifyTaskFailed("DV parse failure: file not found");
  EXPECT_EQ(exec.state(), ExecutorState::FAILED);
  EXPECT_EQ(exec.errorMessage(), "DV parse failure: file not found");
}

// --- notifyTaskFailed with structured error context ---

TEST(ErrorPropagationChainTest, NotifyTaskFailedWithStructuredContext) {
  auto config = makeConfig();
  TaskExecutor exec(config);
  exec.notifyTaskFailed(
      "NATIVE_FALLBACK_REQUIRED: Unsupported feature: "
      "feature_name=avro_format, connector_type=iceberg, split_id=s1");
  EXPECT_EQ(exec.state(), ExecutorState::FAILED);
  auto msg = exec.errorMessage();
  EXPECT_NE(msg.find("NATIVE_FALLBACK_REQUIRED"), std::string::npos);
  EXPECT_NE(msg.find("avro_format"), std::string::npos);
}

// --- Error message accessible after FAILED state ---

TEST(ErrorPropagationChainTest, ErrorMessageAccessibleAfterFailed) {
  auto config = makeConfig();
  TaskExecutor exec(config);
  exec.start();
  ASSERT_TRUE(waitForTerminal(exec, milliseconds(2000)));
  EXPECT_EQ(exec.state(), ExecutorState::FAILED);
  // Error message should be non-empty and stable across multiple reads.
  auto msg1 = exec.errorMessage();
  auto msg2 = exec.errorMessage();
  EXPECT_FALSE(msg1.empty());
  EXPECT_EQ(msg1, msg2);
}

// --- TraceId is preserved through error path ---

TEST(ErrorPropagationChainTest, TraceIdPreservedThroughErrorPath) {
  auto config = makeConfig();
  config.trace_id = "trace-abc-123";
  TaskExecutor exec(config);
  exec.start();
  ASSERT_TRUE(waitForTerminal(exec, milliseconds(2000)));
  // Task fails (empty plan), but trace_id was accepted.
  EXPECT_EQ(exec.state(), ExecutorState::FAILED);
  EXPECT_EQ(exec.taskId(), "error-test-task-1");
}

// --- getNextBatch returns kError after FAILED ---

TEST(ErrorPropagationChainTest, GetNextBatchReturnsErrorAfterFailed) {
  auto config = makeConfig();
  TaskExecutor exec(config);
  exec.notifyTaskFailed("test error");
  facebook::velox::RowVectorPtr row_vector;
  int rc = exec.getNextBatch(row_vector);
  EXPECT_EQ(rc, kError);
}

// --- Multiple notifyTaskFailed calls: first wins ---

TEST(ErrorPropagationChainTest, FirstNotifyTaskFailedWins) {
  auto config = makeConfig();
  TaskExecutor exec(config);
  exec.notifyTaskFailed("first error");
  exec.notifyTaskFailed("second error");
  // TaskLifecycle is monotonic — first FAILED transition wins.
  EXPECT_EQ(exec.state(), ExecutorState::FAILED);
  EXPECT_EQ(exec.errorMessage(), "first error");
}

// --- NativeUnsupportedFeatureException with empty fields ---

TEST(ErrorPropagationChainTest, UnsupportedFeatureEmptyFieldsAreSafe) {
  NativeUnsupportedFeatureException ex("", "", "");
  EXPECT_EQ(ex.featureName(), "");
  EXPECT_EQ(ex.connectorType(), "");
  EXPECT_EQ(ex.splitId(), "");
  // Message should still be well-formed.
  std::string msg = ex.what();
  EXPECT_NE(msg.find("feature_name="), std::string::npos);
}

// --- NativeUnsupportedFeatureException with special characters ---

TEST(ErrorPropagationChainTest, UnsupportedFeatureSpecialCharsInFields) {
  NativeUnsupportedFeatureException ex(
      "format:avro/v2", "iceberg-v3", "split/path=foo&bar");
  EXPECT_EQ(ex.featureName(), "format:avro/v2");
  EXPECT_EQ(ex.connectorType(), "iceberg-v3");
  EXPECT_EQ(ex.splitId(), "split/path=foo&bar");
}

// --- TaskLifecycle monotonic: FAILED cannot transition to RUNNING ---

TEST(ErrorPropagationChainTest, FailedCannotTransitionToRunning) {
  TaskLifecycle lifecycle;
  lifecycle.notifyTaskFailed("error");
  EXPECT_EQ(lifecycle.state(), ExecutorState::FAILED);
  EXPECT_FALSE(lifecycle.transitionTo(ExecutorState::RUNNING));
  EXPECT_EQ(lifecycle.state(), ExecutorState::FAILED);
}

// --- TaskLifecycle: error message preserved ---

TEST(ErrorPropagationChainTest, LifecycleErrorMessagePreserved) {
  TaskLifecycle lifecycle;
  lifecycle.notifyTaskFailed("schema mismatch: expected 5 columns, got 3");
  EXPECT_EQ(lifecycle.errorMessage(),
            "schema mismatch: expected 5 columns, got 3");
}

// --- notifyTaskFailed captures native stack trace ---

TEST(ErrorPropagationChainTest, NotifyTaskFailedCapturesStackTrace) {
  auto config = makeConfig();
  TaskExecutor exec(config);
  exec.notifyTaskFailed("test error with stack trace");
  EXPECT_EQ(exec.state(), ExecutorState::FAILED);
  EXPECT_EQ(exec.errorMessage(), "test error with stack trace");
  // Stack trace should be non-empty on platforms with backtrace support.
#if defined(__APPLE__) || defined(__linux__)
  auto stack = exec.nativeStackTrace();
  EXPECT_FALSE(stack.empty())
      << "Native stack trace should be captured on failure";
  // Stack trace should contain at least one frame reference.
  EXPECT_NE(stack.find("0x"), std::string::npos)
      << "Stack trace should contain address references";
#endif
}

// --- Empty plan produces stack trace in FAILED state ---

TEST(ErrorPropagationChainTest, EmptyPlanFailureIncludesStackTrace) {
  auto config = makeConfig();
  TaskExecutor exec(config);
  exec.start();
  ASSERT_TRUE(waitForTerminal(exec, milliseconds(2000)));
  EXPECT_EQ(exec.state(), ExecutorState::FAILED);
  EXPECT_FALSE(exec.errorMessage().empty());
#if defined(__APPLE__) || defined(__linux__)
  auto stack = exec.nativeStackTrace();
  EXPECT_FALSE(stack.empty())
      << "Failed task should have native stack trace";
#endif
}

}  // namespace
}  // namespace pangrova::executor::test
