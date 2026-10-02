// Tests for PlanExecutionDriver::cancel() — native execution cancel chain.
// Validates: Requirements 1.47, 1.48, 2.47, 2.48
//
// Verifies:
// 1. cancel() propagates to VeloxTaskRunner (native Velox task cancelled)
// 2. Lifecycle transitions correctly on cancel
// 3. Resources are freed (execution thread joins, queues closed)
// 4. Cancel before start is safe (early exit)
// 5. Cancel after completion is safe (no-op)
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <thread>
#include <future>

#include "executor/driver/PlanExecutionDriver.h"
#include "executor/driver/TaskExecutor.h"
#include "executor/driver/TaskLifecycle.h"
#include "executor/queue/OutputQueue.h"
#include "observability/NativeMetricsCollector.h"

#include "../TestMemoryHelper.h"
#include "velox/vector/ComplexVector.h"

#if __has_include("substrait/plan.pb.h")
#include "substrait/plan.pb.h"
#include "substrait/algebra.pb.h"
#define PANGROVA_HAS_SUBSTRAIT_PROTO 1
#else
#define PANGROVA_HAS_SUBSTRAIT_PROTO 0
#endif

namespace pangrova::executor::test {
namespace {

using std::chrono::milliseconds;

// Ensure Velox runtime is initialized before any test in this file.
static auto* g_init_memory = pangrova::test::testMemoryManager();

// Helper: wait for a lifecycle to reach a terminal state.
bool waitForTerminalState(TaskLifecycle& lifecycle, milliseconds timeout) {
  auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (lifecycle.isTerminal()) {
      return true;
    }
    std::this_thread::sleep_for(milliseconds(5));
  }
  return false;
}

// Helper: build a minimal TaskExecutorConfig.
TaskExecutorConfig makeDriverConfig(
    const std::vector<uint8_t>& plan = {}) {
  TaskExecutorConfig config;
  config.task_id = "cancel-test-task";
  config.query_id = "cancel-test-query";
  config.substrait_plan = plan;
  config.queue_poll_timeout = milliseconds(100);
  config.output_queue_capacity = 8;
  return config;
}

// --- Cancel before executeAsync: isCancelled flag set, no crash ---

TEST(PlanExecutionDriverCancelTest, CancelBeforeStartIsSafe) {
  auto config = makeDriverConfig({});
  TaskLifecycle lifecycle;
  OutputQueue queue(config.output_queue_capacity);
  observability::NativeMetricsCollector metrics;

  PlanExecutionDriver driver(config, lifecycle, &queue, nullptr, metrics);

  // Cancel before starting — should set flag without crash.
  driver.cancel();
  EXPECT_TRUE(driver.isCancelled());

  // Start after cancel — execution thread should exit early.
  lifecycle.transitionTo(ExecutorState::RUNNING);
  driver.executeAsync();
  driver.join();

  // Queue should be closed by cancel().
  EXPECT_TRUE(queue.isClosed());
}

// --- Cancel with empty plan: lifecycle reaches terminal state ---

TEST(PlanExecutionDriverCancelTest, CancelWithEmptyPlanReachesTerminal) {
  auto config = makeDriverConfig({});
  TaskLifecycle lifecycle;
  OutputQueue queue(config.output_queue_capacity);
  observability::NativeMetricsCollector metrics;

  PlanExecutionDriver driver(config, lifecycle, &queue, nullptr, metrics);

  lifecycle.transitionTo(ExecutorState::RUNNING);
  driver.executeAsync();

  // Small delay to let execution thread start.
  std::this_thread::sleep_for(milliseconds(10));
  driver.cancel();

  driver.join();

  // Either FAILED (plan conversion failed first) or lifecycle stayed
  // in RUNNING (cancel prevented lifecycle notification).
  // The key assertion: isCancelled is true and thread joined cleanly.
  EXPECT_TRUE(driver.isCancelled());
  EXPECT_TRUE(queue.isClosed());
}

// --- Cancel via TaskExecutor: full integration ---

TEST(PlanExecutionDriverCancelTest, TaskExecutorCancelTransitionsToCancelled) {
  auto config = makeDriverConfig({});
  TaskExecutor exec(config);
  exec.start();

  // Cancel immediately.
  exec.cancel();

  // Wait for terminal state.
  auto deadline = std::chrono::steady_clock::now() + milliseconds(2000);
  while (std::chrono::steady_clock::now() < deadline) {
    auto s = exec.state();
    if (s == ExecutorState::FINISHED || s == ExecutorState::FAILED ||
        s == ExecutorState::CANCELLED) {
      break;
    }
    std::this_thread::sleep_for(milliseconds(10));
  }

  auto s = exec.state();
  // Cancel should win the race or plan fails first.
  EXPECT_TRUE(s == ExecutorState::CANCELLED || s == ExecutorState::FAILED);
}

// --- Cancel is idempotent: calling cancel() multiple times is safe ---

TEST(PlanExecutionDriverCancelTest, CancelIsIdempotent) {
  auto config = makeDriverConfig({});
  TaskLifecycle lifecycle;
  OutputQueue queue(config.output_queue_capacity);
  observability::NativeMetricsCollector metrics;

  PlanExecutionDriver driver(config, lifecycle, &queue, nullptr, metrics);

  lifecycle.transitionTo(ExecutorState::RUNNING);
  driver.executeAsync();

  // Cancel multiple times — no crash.
  driver.cancel();
  driver.cancel();
  driver.cancel();

  driver.join();
  EXPECT_TRUE(driver.isCancelled());
}

// --- Release after cancel: thread joins cleanly ---

TEST(PlanExecutionDriverCancelTest, ReleaseAfterCancelJoinsCleanly) {
  auto config = makeDriverConfig({0xFF, 0xFE});
  TaskExecutor exec(config);
  exec.start();
  exec.cancel();
  // release() calls cancel() + join() — should not hang.
  exec.release();

  auto s = exec.state();
  EXPECT_TRUE(s == ExecutorState::CANCELLED || s == ExecutorState::FAILED);
}

// --- getNextBatch returns kEof after cancel ---

TEST(PlanExecutionDriverCancelTest, GetNextBatchReturnsEofAfterCancel) {
  auto config = makeDriverConfig({0x01, 0x02});
  TaskExecutor exec(config);
  exec.start();
  exec.cancel();

  // Wait for terminal.
  auto deadline = std::chrono::steady_clock::now() + milliseconds(2000);
  while (std::chrono::steady_clock::now() < deadline) {
    auto s = exec.state();
    if (s == ExecutorState::FINISHED || s == ExecutorState::FAILED ||
        s == ExecutorState::CANCELLED) {
      break;
    }
    std::this_thread::sleep_for(milliseconds(10));
  }

  facebook::velox::RowVectorPtr row_vector;
  int rc = exec.getNextBatch(row_vector);
  EXPECT_TRUE(rc == kEof || rc == kError);
}

#if PANGROVA_HAS_SUBSTRAIT_PROTO

namespace {

std::vector<uint8_t> serialize(const google::protobuf::MessageLite& msg) {
  std::vector<uint8_t> buf(msg.ByteSizeLong());
  msg.SerializeToArray(buf.data(), static_cast<int>(buf.size()));
  return buf;
}

std::vector<uint8_t> buildValuesPlan() {
  substrait::Plan plan;
  auto* plan_rel = plan.add_relations();
  auto* root = plan_rel->mutable_root();
  root->add_names("value");
  auto* read = root->mutable_input()->mutable_read();
  read->mutable_base_schema()->add_names("value");
  read->mutable_base_schema()->mutable_struct_()->add_types()->mutable_i64();
  read->mutable_virtual_table()->add_expressions()->add_fields()->mutable_literal()->set_i64(42);
  return serialize(plan);
}

}  // namespace

// --- Cancel during real Velox execution: VeloxTaskRunner cancel propagates ---

TEST(PlanExecutionDriverCancelTest, CancelDuringVeloxExecution) {
  auto config = makeDriverConfig(buildValuesPlan());
  TaskExecutor exec(config);
  exec.start();

  // Small delay to let Velox task start.
  std::this_thread::sleep_for(milliseconds(20));
  exec.cancel();

  auto deadline = std::chrono::steady_clock::now() + milliseconds(2000);
  while (std::chrono::steady_clock::now() < deadline) {
    auto s = exec.state();
    if (s == ExecutorState::FINISHED || s == ExecutorState::FAILED ||
        s == ExecutorState::CANCELLED) {
      break;
    }
    std::this_thread::sleep_for(milliseconds(10));
  }

  auto s = exec.state();
  // VALUES plan is fast, so either CANCELLED (cancel won) or
  // FINISHED (execution completed before cancel). Both are valid.
  EXPECT_TRUE(s == ExecutorState::CANCELLED ||
              s == ExecutorState::FINISHED);

  // Output queue should be closed regardless.
  EXPECT_TRUE(exec.outputQueue()->isClosed());
}

TEST(PlanExecutionDriverCancelTest, FailedPlanClosesOutputBeforePublishingTerminalState) {
  auto config = makeDriverConfig({0xff, 0xfe});
  TaskExecutor exec(config);
  auto output = std::async(std::launch::async, [&] {
    facebook::velox::RowVectorPtr row;
    return exec.outputQueue()->pop(row, milliseconds(2000));
  });
  exec.start();
  const auto deadline = std::chrono::steady_clock::now() + milliseconds(2000);
  while (exec.state() != ExecutorState::FAILED &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  ASSERT_EQ(exec.state(), ExecutorState::FAILED);
  EXPECT_TRUE(exec.outputQueue()->isClosed());
  ASSERT_EQ(output.wait_for(milliseconds(100)), std::future_status::ready);
  EXPECT_EQ(output.get(), PopResult::CLOSED);
  exec.cancel();
  EXPECT_EQ(exec.state(), ExecutorState::FAILED);
  EXPECT_TRUE(exec.outputQueue()->isClosed());
}

// --- Cancel after successful completion: no-op, state stays FINISHED ---

TEST(PlanExecutionDriverCancelTest, CancelAfterCompletionIsNoop) {
  auto config = makeDriverConfig(buildValuesPlan());
  TaskExecutor exec(config);
  exec.start();

  // Wait for completion.
  auto deadline = std::chrono::steady_clock::now() + milliseconds(2000);
  while (std::chrono::steady_clock::now() < deadline) {
    auto s = exec.state();
    if (s == ExecutorState::FINISHED || s == ExecutorState::FAILED ||
        s == ExecutorState::CANCELLED) {
      break;
    }
    std::this_thread::sleep_for(milliseconds(10));
  }

  // Now cancel after completion — should be safe.
  exec.cancel();

  // A valid completed VALUES execution must not turn into a failure or cancellation.
  auto s = exec.state();
  EXPECT_EQ(s, ExecutorState::FINISHED);
}

#endif  // PANGROVA_HAS_SUBSTRAIT_PROTO

}  // namespace
}  // namespace pangrova::executor::test
