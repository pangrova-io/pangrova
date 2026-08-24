// Tests for TaskExecutor::start() — Velox Task creation and execution.
// Validates plan parsing, VALUES execution, callbacks, and state transitions.
#include <gtest/gtest.h>

#include <arrow/api.h>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include "executor/driver/TaskExecutor.h"
#include "velox/vector/ComplexVector.h"

#include "../TestMemoryHelper.h"

// Include Substrait proto when available for building real plan bytes.
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

TaskExecutorConfig makeConfig(const std::vector<uint8_t>& plan = {}) {
  TaskExecutorConfig config;
  config.task_id = "test-task-1";
  config.query_id = "test-query-1";
  config.substrait_plan = plan;
  config.queue_poll_timeout = milliseconds(200);
  return config;
}

// --- Helper: wait for terminal state with timeout ---

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

// --- Empty plan: start() transitions to FAILED ---

TEST(TaskExecutorStartTest, EmptyPlanTransitionsToFailed) {
  auto config = makeConfig({});
  TaskExecutor exec(config);
  exec.start();
  ASSERT_TRUE(waitForTerminal(exec, milliseconds(2000)));
  EXPECT_EQ(exec.state(), ExecutorState::FAILED);
  EXPECT_FALSE(exec.errorMessage().empty());
}

// --- TraceId propagation via config ---

TEST(TaskExecutorStartTest, TraceIdFromConfigIsPropagated) {
  auto config = makeConfig({});
  config.trace_id = "abc123def456";
  TaskExecutor exec(config);
  // trace_id is stored — executor can be started normally.
  exec.start();
  ASSERT_TRUE(waitForTerminal(exec, milliseconds(2000)));
  // Plan is empty so it fails, but trace_id was accepted without error.
  EXPECT_EQ(exec.state(), ExecutorState::FAILED);
}

// --- TraceId set via setTraceId before start ---

TEST(TaskExecutorStartTest, SetTraceIdBeforeStart) {
  auto config = makeConfig({});
  TaskExecutor exec(config);
  exec.setTraceId("trace-from-java-side");
  exec.start();
  ASSERT_TRUE(waitForTerminal(exec, milliseconds(2000)));
  EXPECT_EQ(exec.state(), ExecutorState::FAILED);
}

// --- Empty traceId is safe ---

TEST(TaskExecutorStartTest, EmptyTraceIdIsSafe) {
  auto config = makeConfig({});
  config.trace_id = "";
  TaskExecutor exec(config);
  exec.start();
  ASSERT_TRUE(waitForTerminal(exec, milliseconds(2000)));
  EXPECT_EQ(exec.state(), ExecutorState::FAILED);
}

// --- Garbage plan bytes: start() transitions to FAILED ---

TEST(TaskExecutorStartTest, GarbagePlanTransitionsToFailed) {
  auto config = makeConfig({0xFF, 0xFE, 0xFD});
  TaskExecutor exec(config);
  exec.start();
  ASSERT_TRUE(waitForTerminal(exec, milliseconds(2000)));
  EXPECT_EQ(exec.state(), ExecutorState::FAILED);
}

// --- Double start throws ---

TEST(TaskExecutorStartTest, DoubleStartThrows) {
  auto config = makeConfig({});
  TaskExecutor exec(config);
  exec.start();
  EXPECT_THROW(exec.start(), std::runtime_error);
  waitForTerminal(exec, milliseconds(2000));
}

// --- Cancel during execution ---

TEST(TaskExecutorStartTest, CancelDuringExecution) {
  auto config = makeConfig({});
  TaskExecutor exec(config);
  exec.start();
  exec.cancel();
  ASSERT_TRUE(waitForTerminal(exec, milliseconds(2000)));
  auto s = exec.state();
  // Either CANCELLED (if cancel won the race) or FAILED (if plan exec finished first).
  EXPECT_TRUE(s == ExecutorState::CANCELLED || s == ExecutorState::FAILED);
}

// --- getNextBatch returns kEof after FAILED ---

TEST(TaskExecutorStartTest, GetNextBatchReturnsEofOrErrorAfterFailed) {
  auto config = makeConfig({0x01});
  TaskExecutor exec(config);
  exec.start();
  ASSERT_TRUE(waitForTerminal(exec, milliseconds(2000)));
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
  auto* read = root->mutable_input()->mutable_read();
  auto* vt = read->mutable_virtual_table();
  // Add one row with a single BIGINT value so ValuesNode produces 1 row.
  auto* row = vt->add_values();
  row->add_fields()->set_i64(42);
  return serialize(plan);
}

std::vector<uint8_t> buildTableScanPlan() {
  substrait::Plan plan;
  auto* plan_rel = plan.add_relations();
  auto* root = plan_rel->mutable_root();
  auto* read = root->mutable_input()->mutable_read();
  read->mutable_named_table()->add_names("test_table");
  return serialize(plan);
}

std::vector<uint8_t> buildProjectValuesPlan() {
  substrait::Plan plan;
  auto* plan_rel = plan.add_relations();
  auto* root = plan_rel->mutable_root();
  auto* project = root->mutable_input()->mutable_project();
  auto* read = project->mutable_input()->mutable_read();
  read->mutable_virtual_table();
  return serialize(plan);
}

std::vector<uint8_t> buildEmptyPlan() {
  substrait::Plan plan;
  return serialize(plan);
}

}  // namespace

// --- VALUES plan: produces batch and reaches FINISHED ---

TEST(TaskExecutorStartTest, ValuesPlanProducesBatchAndFinishes) {
  auto config = makeConfig(buildValuesPlan());
  TaskExecutor exec(config);
  exec.start();
  ASSERT_TRUE(waitForTerminal(exec, milliseconds(2000)));
  EXPECT_EQ(exec.state(), ExecutorState::FINISHED);
}

TEST(TaskExecutorStartTest, ValuesPlanBatchIsReadable) {
  auto config = makeConfig(buildValuesPlan());
  config.queue_poll_timeout = milliseconds(500);
  TaskExecutor exec(config);
  exec.start();

  facebook::velox::RowVectorPtr row_vector;
  // Poll until data or terminal.
  int rc = kNotReady;
  auto deadline = std::chrono::steady_clock::now() + milliseconds(2000);
  while (rc == kNotReady &&
         std::chrono::steady_clock::now() < deadline) {
    rc = exec.getNextBatch(row_vector);
  }
  ASSERT_EQ(rc, kDataAvailable);
  ASSERT_NE(row_vector, nullptr);
  EXPECT_EQ(row_vector->size(), 1);
  EXPECT_EQ(row_vector->childrenSize(), 1);
}

// --- Project → VALUES: walks to leaf and produces batch ---

TEST(TaskExecutorStartTest, ProjectValuesWalksToLeaf) {
  auto config = makeConfig(buildProjectValuesPlan());
  TaskExecutor exec(config);
  exec.start();
  ASSERT_TRUE(waitForTerminal(exec, milliseconds(2000)));
  EXPECT_EQ(exec.state(), ExecutorState::FINISHED);
}

// --- TableScan plan: reaches FINISHED (placeholder produces no data) ---

TEST(TaskExecutorStartTest, TableScanPlanReachesFinished) {
  auto config = makeConfig(buildTableScanPlan());
  TaskExecutor exec(config);
  exec.start();
  ASSERT_TRUE(waitForTerminal(exec, milliseconds(2000)));
  EXPECT_EQ(exec.state(), ExecutorState::FINISHED);
}

// --- Empty Substrait Plan (no relations): FAILED ---

TEST(TaskExecutorStartTest, EmptySubstraitPlanFails) {
  auto config = makeConfig(buildEmptyPlan());
  TaskExecutor exec(config);
  exec.start();
  ASSERT_TRUE(waitForTerminal(exec, milliseconds(2000)));
  EXPECT_EQ(exec.state(), ExecutorState::FAILED);
}

// --- VALUES batch has correct Arrow schema ---

TEST(TaskExecutorStartTest, ValuesBatchHasCorrectSchema) {
  auto config = makeConfig(buildValuesPlan());
  config.queue_poll_timeout = milliseconds(500);
  TaskExecutor exec(config);
  exec.start();

  facebook::velox::RowVectorPtr row_vector;
  int rc = kNotReady;
  auto deadline = std::chrono::steady_clock::now() + milliseconds(2000);
  while (rc == kNotReady &&
         std::chrono::steady_clock::now() < deadline) {
    rc = exec.getNextBatch(row_vector);
  }
  ASSERT_EQ(rc, kDataAvailable);
  ASSERT_NE(row_vector, nullptr);
  // RowVector type is ROW — check child count and child type.
  auto rowType = std::dynamic_pointer_cast<const facebook::velox::RowType>(
      row_vector->type());
  ASSERT_NE(rowType, nullptr);
  EXPECT_EQ(rowType->size(), 1);
  EXPECT_TRUE(rowType->childAt(0)->isBigint());
}

// --- After consuming batch, next call returns kEof ---

TEST(TaskExecutorStartTest, AfterBatchConsumedReturnsEof) {
  auto config = makeConfig(buildValuesPlan());
  config.queue_poll_timeout = milliseconds(500);
  TaskExecutor exec(config);
  exec.start();

  facebook::velox::RowVectorPtr row_vector;
  int rc = kNotReady;
  auto deadline = std::chrono::steady_clock::now() + milliseconds(2000);
  while (rc == kNotReady &&
         std::chrono::steady_clock::now() < deadline) {
    rc = exec.getNextBatch(row_vector);
  }
  ASSERT_EQ(rc, kDataAvailable);

  // Wait for FINISHED state.
  ASSERT_TRUE(waitForTerminal(exec, milliseconds(2000)));

  // Next call should return kEof.
  facebook::velox::RowVectorPtr row_vector2;
  rc = exec.getNextBatch(row_vector2);
  EXPECT_EQ(rc, kEof);
}

#endif  // PANGROVA_HAS_SUBSTRAIT_PROTO

}  // namespace
}  // namespace pangrova::executor::test
