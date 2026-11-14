#include <gtest/gtest.h>
#include "velox/expression/FunctionRegistrySnapshot.h"

#include <cmath>
#include <chrono>
#include <cstdint>
#include <memory>
#include <set>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "TestMemoryHelper.h"
#include "jni/RuntimeInitializer.h"
#include "indexing/retrieval/RetrievalFusion.h"
#include "engine/velox/plan/RetrievalMergeOperator.h"
#include "engine/velox/connector/PangrovaTableHandle.h"
#include "velox/vector/DecodedVector.h"

#include "velox/core/PlanFragment.h"
#include "velox/core/PlanNode.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/Task.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace pangrova::velox::test {
namespace {

using facebook::velox::RowVectorPtr;

struct OutputRow {
  std::string key;
  double score;
  std::string source;
  std::string payload;
  std::string snapshot;
  std::string object;
  int64_t position = -1;
};

class RetrievalMergeExecutionTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    pool_ = pangrova::test::testLeafPool("RetrievalMergeExecutionTest");
  }

  RowVectorPtr candidates() const {
    facebook::velox::test::VectorMaker maker(pool_.get());
    return maker.rowVector(
        {"stable_key", "score", "source_id", "payload"},
        {maker.flatVector<std::string>({"a", "b", "b", "c"}),
         maker.flatVector<double>({0.1, 0.2, 12.0, 11.0}),
         maker.flatVector<std::string>({"vector", "vector", "text", "text"}),
         maker.flatVector<std::string>(
             {"vector-a", "vector-b", "text-b", "text-c"})});
  }

  std::vector<OutputRow> execute(
      const std::shared_ptr<const RetrievalMergeNode>& merge) const {
    std::mutex mutex;
    std::vector<OutputRow> rows;
    auto consumer = [&mutex, &rows](
                        RowVectorPtr vector,
                        bool /*drained*/,
                        facebook::velox::ContinueFuture* /*future*/) {
      if (!vector) {
        return facebook::velox::exec::BlockingReason::kNotBlocked;
      }
      auto* keys = vector->childAt(0)->asFlatVector<facebook::velox::StringView>();
      auto* scores = vector->childAt(1)->asFlatVector<double>();
      auto* sources =
          vector->childAt(2)->asFlatVector<facebook::velox::StringView>();
      auto* payloads =
          vector->childAt(3)->asFlatVector<facebook::velox::StringView>();
      std::lock_guard<std::mutex> guard(mutex);
      for (facebook::velox::vector_size_t row = 0; row < vector->size(); ++row) {
        OutputRow output{
            keys->valueAt(row).str(),
            scores->valueAt(row),
            sources->valueAt(row).str(),
            payloads->valueAt(row).str(), "", "", -1};
        if (vector->childrenSize() == 8) {
          facebook::velox::DecodedVector snapshot(*vector->childAt(5));
          facebook::velox::DecodedVector object(*vector->childAt(6));
          facebook::velox::DecodedVector position(*vector->childAt(7));
          output.snapshot = snapshot.valueAt<facebook::velox::StringView>(row).str();
          output.object = object.valueAt<facebook::velox::StringView>(row).str();
          output.position = position.valueAt<int64_t>(row);
        }
        rows.push_back(std::move(output));
      }
      return facebook::velox::exec::BlockingReason::kNotBlocked;
    };

    auto query_context = facebook::velox::core::QueryCtx::create(
        pangrova::jni::globalExecutor().get(),
        facebook::velox::core::QueryConfig({{"require_function_registry_snapshot", "true"}},
            facebook::velox::exec::FunctionRegistrySnapshot::capture()));
    auto task = facebook::velox::exec::Task::create(
        "retrieval-merge-execution-test",
        facebook::velox::core::PlanFragment{merge},
        0,
        std::move(query_context),
        facebook::velox::exec::Task::ExecutionMode::kParallel,
        std::move(consumer));
    task->start(2);
    task->taskCompletionFuture().wait();
    EXPECT_FALSE(task->error()) << task->errorMessage();
    // Task completion state can become visible just before the last Driver
    // releases its operator.  Keep the plan's Values buffers and the fixture
    // pool alive until all Drivers have actually exited.
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (task->numRunningDrivers() != 0 &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::yield();
    }
    EXPECT_EQ(task->numRunningDrivers(), 0);
    return rows;
  }

  static std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
};

std::shared_ptr<facebook::velox::memory::MemoryPool>
    RetrievalMergeExecutionTest::pool_;

TEST_F(RetrievalMergeExecutionTest, RrfRunsAsARealVeloxOperator) {
  auto values = std::make_shared<facebook::velox::core::ValuesNode>(
      "candidate-values", std::vector<RowVectorPtr>{candidates()}, true);
  RetrievalMergeConfig config{
      RetrievalMergeKind::kRrf,
      1,
      2,
      {0},
      {{"vector", pangrova::retrieval::ScoreDirection::kLowerIsBetter, 1.0},
       {"text", pangrova::retrieval::ScoreDirection::kHigherIsBetter, 1.0}},
      2,
      100,
      1024 * 1024,
      2,
      60};
  auto merge = std::make_shared<RetrievalMergeNode>(
      "native-rrf", std::move(config), values);

  auto rows = execute(merge);

  ASSERT_EQ(rows.size(), 2);
  EXPECT_EQ(rows[0].key, "b");
  EXPECT_EQ(rows[0].source, "text");
  EXPECT_EQ(rows[0].payload, "text-b");
  EXPECT_NEAR(rows[0].score, 1.0 / 61.0 + 1.0 / 62.0, 1e-12);
  EXPECT_EQ(rows[1].key, "a");
  EXPECT_NEAR(rows[1].score, 1.0 / 61.0, 1e-12);
}

TEST_F(RetrievalMergeExecutionTest, LinearRunsAsARealVeloxOperator) {
  auto values = std::make_shared<facebook::velox::core::ValuesNode>(
      "candidate-values", std::vector<RowVectorPtr>{candidates()}, true);
  RetrievalMergeConfig config{
      RetrievalMergeKind::kLinear,
      1,
      2,
      {0},
      {{"vector", pangrova::retrieval::ScoreDirection::kLowerIsBetter, 0.7},
       {"text", pangrova::retrieval::ScoreDirection::kHigherIsBetter, 0.3}},
      3,
      100,
      1024 * 1024,
      2,
      0};
  auto merge = std::make_shared<RetrievalMergeNode>(
      "native-linear", std::move(config), values);

  auto rows = execute(merge);

  ASSERT_EQ(rows.size(), 3);
  EXPECT_EQ(rows[0].key, "a");
  EXPECT_NEAR(rows[0].score, 0.7, 1e-12);
  EXPECT_EQ(rows[1].key, "b");
  EXPECT_NEAR(rows[1].score, 0.3, 1e-12);
  EXPECT_EQ(rows[2].key, "c");
  EXPECT_NEAR(rows[2].score, 0.0, 1e-12);
}

TEST_F(RetrievalMergeExecutionTest, PhysicalIdentityPreservesDomainsAndFusesOnlyTheSameSourceRow) {
  facebook::velox::test::VectorMaker maker(pool_.get());
  auto batch = maker.rowVector(
      {"stable_key", "score", "source_id", "payload", std::string(kSourceIdColumnName),
       std::string(kSourceSnapshotColumnName), std::string(kSourceObjectIdColumnName),
       std::string(kPhysicalRowPositionColumnName)},
      {maker.flatVector<std::string>({"same", "same", "same", "same"}),
       maker.flatVector<double>({0.1, 0.2, 12.0, 11.0}),
       maker.flatVector<std::string>({"vector", "vector", "text", "text"}),
       maker.flatVector<std::string>({"same", "same", "same", "same"}),
       maker.flatVector<std::string>({"table", "table", "table", "table"}),
       maker.flatVector<std::string>({"snapshot-a", "snapshot-b", "snapshot-a", "snapshot-a"}),
       maker.flatVector<std::string>({"object-a", "object-a", "object-a", "object-b"}),
       maker.flatVector<int64_t>({7, 7, 7, 7})});
  auto values = std::make_shared<facebook::velox::core::ValuesNode>(
      "physical-candidates", std::vector<RowVectorPtr>{batch}, true);
  RetrievalMergeConfig config{RetrievalMergeKind::kRrf, 1, 2, {4, 5, 6, 7},
      {{"vector", pangrova::retrieval::ScoreDirection::kLowerIsBetter, 1.0},
       {"text", pangrova::retrieval::ScoreDirection::kHigherIsBetter, 1.0}},
      4, 100, 1024 * 1024, 2, 60, true};
  auto merge = std::make_shared<RetrievalMergeNode>("physical-merge", std::move(config), values);
  auto rows = execute(merge);
  ASSERT_EQ(rows.size(), 3);
  EXPECT_EQ(rows[0].snapshot, "snapshot-a");
  EXPECT_EQ(rows[0].object, "object-a");
  EXPECT_EQ(rows[0].position, 7);
  EXPECT_NEAR(rows[0].score, 2.0 / 61.0, 1e-12);
  std::set<std::pair<std::string, std::string>> identities;
  for (const auto& row : rows) {
    EXPECT_EQ(row.key, "same");
    EXPECT_EQ(row.payload, "same");
    EXPECT_EQ(row.position, 7);
    identities.emplace(row.snapshot, row.object);
  }
  EXPECT_EQ(identities, (std::set<std::pair<std::string, std::string>>{
      {"snapshot-a", "object-a"}, {"snapshot-a", "object-b"}, {"snapshot-b", "object-a"}}));
}

}  // namespace
}  // namespace pangrova::velox::test
