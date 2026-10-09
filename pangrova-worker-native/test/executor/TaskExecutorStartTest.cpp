// Tests for TaskExecutor::start() — Velox Task creation and execution.
// Validates plan parsing, VALUES execution, callbacks, and state transitions.
#include <gtest/gtest.h>
#include "velox/expression/FunctionRegistrySnapshot.h"

#include <arrow/api.h>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>
#include <future>
#include <folly/ScopeGuard.h>
#include <folly/executors/CPUThreadPoolExecutor.h>

#include "cache/SemanticResultCache.h"
#include "executor/driver/TaskExecutor.h"
#include "velox/runtime/NativeEngineLifecycle.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"

#include "../TestMemoryHelper.h"

// Include Substrait proto when available for building real plan bytes.
#if __has_include("substrait/plan.pb.h")
#include "substrait/plan.pb.h"
#include "substrait/algebra.pb.h"
#include "pangrova/protocol/v2/result.pb.h"
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
  config.function_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
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

class ResultStartupGateTest : public ::testing::TestWithParam<bool> {};

TEST_P(ResultStartupGateTest, WaitsForPreparationWithoutBlockingActualDeadline) {
  auto config = makeConfig({});
  config.is_result_task = true;
  if (GetParam()) {
    config.deadline_epoch_ms = std::chrono::duration_cast<milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count() + 2500;
  }
  TaskExecutor task(config);
  std::future<void> start;
  auto pool = std::dynamic_pointer_cast<folly::CPUThreadPoolExecutor>(
      jni::NativeEngineLifecycle::instance().executor());
  ASSERT_NE(pool, nullptr);
  const auto count = pool->numThreads();
  ASSERT_GT(count, 0);
  std::promise<void> unblock;
  auto blocked = unblock.get_future().share();
  std::promise<void> allEntered;
  auto entered = allEntered.get_future();
  std::atomic<size_t> remaining{count};
  auto releaseWorkers = folly::makeGuard([&] { unblock.set_value(); });
  for (size_t worker = 0; worker < count; ++worker) {
    pool->add([&, blocked] {
      if (remaining.fetch_sub(1) == 1) allEntered.set_value();
      blocked.wait();
    });
  }
  ASSERT_EQ(entered.wait_for(milliseconds(2000)), std::future_status::ready);
  start = std::async(std::launch::async, [&] { task.start(); });
  const auto deadline = std::chrono::steady_clock::now() + milliseconds(2000);
  while (task.state() == ExecutorState::PENDING && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  ASSERT_EQ(task.state(), ExecutorState::RUNNING);
  // All real preparation threads are occupied. Submission alone is not readiness.
  EXPECT_EQ(start.wait_for(milliseconds(100)), std::future_status::timeout);
  if (GetParam()) {
    ASSERT_TRUE(waitForTerminal(task, milliseconds(5000)));
    EXPECT_EQ(task.state(), ExecutorState::FAILED);
    EXPECT_NE(task.errorMessage().find("Task deadline exceeded"), std::string::npos);
    ASSERT_EQ(start.wait_for(milliseconds(2000)), std::future_status::ready);
    EXPECT_THROW(start.get(), std::runtime_error);
  }
  unblock.set_value();
  releaseWorkers.dismiss();
  if (!GetParam()) {
    ASSERT_EQ(start.wait_for(milliseconds(2000)), std::future_status::ready);
    EXPECT_NO_THROW(start.get());
  }
  ASSERT_TRUE(waitForTerminal(task, milliseconds(2000)));
  EXPECT_EQ(task.state(), ExecutorState::FAILED);
  EXPECT_FALSE(task.errorMessage().empty());
  task.release();
  EXPECT_TRUE(task.isExecutionReleased());
}

INSTANTIATE_TEST_SUITE_P(QueuedPreparation, ResultStartupGateTest, ::testing::Values(false, true));

TEST(TaskExecutorStartTest, DeadlineExpiresWhileTaskIsPending) {
  auto config = makeConfig({});
  const auto now_ms = duration_cast<milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
  config.deadline_epoch_ms = now_ms + 50;

  TaskExecutor exec(config);
  ASSERT_TRUE(waitForTerminal(exec, milliseconds(2000)));
  EXPECT_EQ(exec.state(), ExecutorState::FAILED);
  EXPECT_NE(exec.errorMessage().find("Task deadline exceeded"), std::string::npos);
  EXPECT_NE(exec.errorMessage().find(config.task_id), std::string::npos);
}

TEST(TaskExecutorStartTest, AlreadyExpiredDeadlineFailsBeforeStart) {
  auto config = makeConfig({});
  const auto now_ms = duration_cast<milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
  config.deadline_epoch_ms = now_ms - 1;

  TaskExecutor exec(config);
  EXPECT_EQ(exec.state(), ExecutorState::FAILED);
  EXPECT_NE(exec.errorMessage().find("Task deadline exceeded"), std::string::npos);
  EXPECT_THROW(exec.start(), std::runtime_error);
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
  root->add_names("value");
  auto* read = root->mutable_input()->mutable_read();
  read->mutable_base_schema()->add_names("value");
  read->mutable_base_schema()->mutable_struct_()->add_types()->mutable_i64()
      ->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  auto* vt = read->mutable_virtual_table();
  // Add one row with a single BIGINT value so ValuesNode produces 1 row.
  auto* row = vt->add_expressions();
  row->add_fields()->mutable_literal()->set_i64(42);
  return serialize(plan);
}

std::vector<uint8_t> buildTableScanPlan() {
  substrait::Plan plan;
  auto* plan_rel = plan.add_relations();
  auto* root = plan_rel->mutable_root();
  root->add_names("value");
  auto* read = root->mutable_input()->mutable_read();
  read->mutable_named_table()->add_names("test_table");
  read->mutable_base_schema()->add_names("value");
  read->mutable_base_schema()->mutable_struct_()->add_types()->mutable_i64()
      ->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  return serialize(plan);
}

std::vector<uint8_t> buildProjectValuesPlan() {
  substrait::Plan plan;
  auto* plan_rel = plan.add_relations();
  auto* root = plan_rel->mutable_root();
  auto* project = root->mutable_input()->mutable_project();
  auto* read = project->mutable_input()->mutable_read();
  read->mutable_base_schema()->mutable_struct_();
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

TEST(TaskExecutorStartTest, ValuesPlanMissingNullabilityFailsWithoutPublishingRows) {
  substrait::Plan plan;
  const auto canonical = buildValuesPlan();
  ASSERT_TRUE(plan.ParseFromArray(canonical.data(), static_cast<int>(canonical.size())));
  plan.mutable_relations(0)->mutable_root()->mutable_input()->mutable_read()
      ->mutable_base_schema()->mutable_struct_()->mutable_types(0)->mutable_i64()
      ->clear_nullability();
  TaskExecutor exec(makeConfig(serialize(plan)));
  exec.start();
  ASSERT_TRUE(waitForTerminal(exec, milliseconds(2000)));
  ASSERT_EQ(exec.state(), ExecutorState::FAILED);
  EXPECT_NE(exec.errorMessage().find("Canonical value type requires explicit nullability"),
            std::string::npos);
  facebook::velox::RowVectorPtr rows;
  EXPECT_EQ(exec.getNextBatch(rows), kError);
  EXPECT_EQ(rows, nullptr);
  EXPECT_TRUE(exec.outputQueue()->isClosed());
  exec.release();
  EXPECT_TRUE(exec.isExecutionReleased());
}

TEST(TaskExecutorStartTest, StatsExposeSemanticCacheUsageBeforeRelease) {
  auto& cache = cache::SemanticResultCache::instance();
  cache.invalidateAll();
  auto config = makeConfig(buildValuesPlan());
  TaskExecutor exec(config);
  exec.start();
  ASSERT_TRUE(waitForTerminal(exec, milliseconds(2000)));
  ASSERT_EQ(exec.state(), ExecutorState::FINISHED);

  cache::ReuseIdentity identity{
      "tenant-a", "alice", "policy-v1", "model@1#digest",
      "text-generation@1#digest", "ai_complete", "VARCHAR", "temperature=0",
      "orders", "snapshot-1"};
  cache::ReusePolicy policy{60'000, 100, 1024 * 1024, 0.98, 256};
  const auto key = cache::SemanticResultCache::requestKey("hello", 256);
  EXPECT_FALSE(cache.lookup(identity, key, policy, config.task_id).has_value());
  ASSERT_TRUE(cache.put(
      identity, key, std::string("world"), policy, config.task_id));
  EXPECT_TRUE(cache.lookup(identity, key, policy, config.task_id).has_value());
  auto too_small = policy;
  too_small.max_bytes = 1;
  EXPECT_FALSE(cache.put(
      identity, key, std::string("not admitted"), too_small, config.task_id));

  const auto serialized = exec.collectTaskStats();
  protocol::v2::NativeTaskStats stats;
  ASSERT_TRUE(stats.ParseFromArray(
      serialized.data(), static_cast<int>(serialized.size())));
  EXPECT_EQ(stats.cache_hit_count(), 1);
  EXPECT_EQ(stats.cache_miss_count(), 1);
  EXPECT_GT(stats.cache_hit_bytes(), 0);
  EXPECT_GT(stats.cache_miss_bytes(), 0);
  EXPECT_EQ(stats.cache_admission_rejection_count(), 1);
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
  facebook::velox::DecodedVector value(*row_vector->childAt(0));
  ASSERT_FALSE(value.isNullAt(0));
  EXPECT_EQ(value.valueAt<int64_t>(0), 42);
}

// --- Project → VALUES: walks to leaf and produces batch ---

TEST(TaskExecutorStartTest, ProjectValuesWalksToLeaf) {
  auto config = makeConfig(buildProjectValuesPlan());
  TaskExecutor exec(config);
  exec.start();
  ASSERT_TRUE(waitForTerminal(exec, milliseconds(2000)));
  EXPECT_EQ(exec.state(), ExecutorState::FINISHED);
}

// --- Real TableScan without assigned splits finishes with no data ---

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
  facebook::velox::DecodedVector value(*row_vector->childAt(0));
  ASSERT_FALSE(value.isNullAt(0));
  EXPECT_EQ(value.valueAt<int64_t>(0), 42);
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
