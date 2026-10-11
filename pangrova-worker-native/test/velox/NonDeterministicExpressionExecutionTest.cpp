// Actual production runner QueryCtx and published built-in function execution.
// Native component evidence, not SQL/JNI or distributed E2E.
#include "CanonicalPublishedTaskTest.h"

#include <atomic>
#include <chrono>
#include <cmath>
#include <memory>
#include <string>
#include <vector>

#include "executor/queue/OutputQueue.h"
#include "velox/runner/VeloxTaskRunner.h"
#include "velox/core/Expressions.h"
#include "velox/core/QueryConfig.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/Task.h"
#include "velox/expression/EvalCtx.h"
#include "velox/expression/Expr.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace pangrova::velox::test {
namespace {
using namespace facebook::velox;

class NonDeterministicExpressionExecutionTest : public CanonicalPublishedTaskTest {
 protected:
  static constexpr vector_size_t kRows = 64;

  void TearDown() override {
    if (runner_) {
      queue_.drain();
      runner_->cancel();
      runner_->releaseTaskAndWaitForDeletion();
      runner_.reset();
    }
    input_.reset();
    pool_.reset();
    CanonicalPublishedTaskTest::TearDown();
  }

  void startRunner(bool projectRandom = false) {
    static std::atomic<uint64_t> next{0};
    const auto id = std::to_string(next.fetch_add(1));
    pool_ = pangrova::test::testLeafPool("nondeterministic-expressions-" + id);
    facebook::velox::test::VectorMaker maker(pool_.get());
    input_ = maker.rowVector({maker.flatVector<double>(
        kRows, [](vector_size_t row) { return static_cast<double>(row); })});
    core::PlanNodePtr plan = std::make_shared<core::ValuesNode>(
        "input", std::vector<RowVectorPtr>{input_});
    if (projectRandom) {
      const auto random = randomExpression();
      plan = std::make_shared<core::ProjectNode>(
          "random-project", std::vector<std::string>{"first", "second"},
          std::vector<core::TypedExprPtr>{random, random}, plan);
    }
    VeloxTaskRunnerConfig config;
    config.function_registry = registry_;
    config.task_id = "nondeterministic-task-" + id;
    config.query_id = "nondeterministic-query-" + id;
    config.max_drivers = 2;
    // Neither the raw SDK key nor an apparent performance session key may
    // re-enable expression sharing for non-deterministic functions.
    config.session_properties[core::QueryConfig::kExprDedupNonDeterministic] = "true";
    config.session_properties["velox.expression.dedup_non_deterministic"] = "true";
    runner_ = std::make_shared<VeloxTaskRunner>(
        config, plan, std::vector<split::SplitConfig>{}, &queue_);
    runner_->start();
  }

  static core::TypedExprPtr randomExpression() {
    return std::make_shared<core::CallTypedExpr>(
        DOUBLE(), std::vector<core::TypedExprPtr>{}, "rand");
  }

  static void expectRandomValues(const VectorPtr& values, vector_size_t count) {
    ASSERT_EQ(values->size(), count);
    ASSERT_EQ(values->typeKind(), TypeKind::DOUBLE);
    DecodedVector decoded(*values);
    for (vector_size_t row = 0; row < count; ++row) {
      ASSERT_FALSE(decoded.isNullAt(row));
      const auto value = decoded.valueAt<double>(row);
      EXPECT_TRUE(std::isfinite(value));
      EXPECT_GE(value, 0.0);
      EXPECT_LT(value, 1.0);
    }
  }

  executor::OutputQueue queue_{4};
  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
  RowVectorPtr input_;
  std::shared_ptr<VeloxTaskRunner> runner_;
};

TEST_F(NonDeterministicExpressionExecutionTest,
       ProductionQueryContextCompilesAndEvaluatesEachRandomOccurrence) {
  startRunner();
  ASSERT_NE(runner_->veloxTask(), nullptr);
  const auto& query = runner_->veloxTask()->queryCtx();
  ASSERT_FALSE(query->queryConfig().exprDedupNonDeterministic());
  core::ExecCtx execution(pool_.get(), query.get());
  const auto random = randomExpression();
  exec::ExprSet expressions({random, random}, &execution);
  ASSERT_EQ(expressions.size(), 2);
  ASSERT_NE(expressions.expr(0).get(), expressions.expr(1).get());
  EXPECT_FALSE(expressions.expr(0)->isDeterministic());
  EXPECT_FALSE(expressions.expr(1)->isDeterministic());
  exec::EvalCtx evaluation(&execution, &expressions, input_.get());
  SelectivityVector rows(kRows);
  std::vector<VectorPtr> outputs;

  expressions.eval(rows, evaluation, outputs);

  ASSERT_EQ(outputs.size(), 2);
  for (const auto& output : outputs) expectRandomValues(output, kRows);
  for (const auto& expression : expressions.exprs()) {
    EXPECT_EQ(expression->stats().numProcessedRows, kRows);
    EXPECT_EQ(expression->stats().numProcessedVectors, 1);
  }
  const auto stats = expressions.stats();
  const auto randomStats = stats.find("rand");
  ASSERT_NE(randomStats, stats.end());
  EXPECT_EQ(randomStats->second.numProcessedRows, 2 * kRows);
  EXPECT_EQ(randomStats->second.numProcessedVectors, 2);
  // Random draws may coincide. Node identity and exact evaluation counts,
  // rather than a probabilistic inequality assertion, prove independent calls.
}

TEST_F(NonDeterministicExpressionExecutionTest,
       ProductionRunnerReturnsBothRandomColumnsAndFinishes) {
  startRunner(true);
  ASSERT_FALSE(runner_->veloxTask()->queryCtx()->queryConfig().exprDedupNonDeterministic());

  ASSERT_TRUE(runner_->waitForCompletion()) << runner_->errorMessage();

  EXPECT_TRUE(runner_->isFinished());
  EXPECT_EQ(runner_->veloxTask()->state(), exec::TaskState::kFinished);
  EXPECT_EQ(runner_->veloxTask()->numRunningDrivers(), 0);
  RowVectorPtr output;
  ASSERT_EQ(queue_.pop(output, std::chrono::seconds(5)), executor::PopResult::DATA);
  ASSERT_NE(output, nullptr);
  ASSERT_EQ(output->childrenSize(), 2);
  ASSERT_EQ(output->size(), kRows);
  expectRandomValues(output->childAt(0), kRows);
  expectRandomValues(output->childAt(1), kRows);
  EXPECT_EQ(queue_.size(), 0);
}

TEST_F(NonDeterministicExpressionExecutionTest,
       DeterministicExpressionsStillShareCompiledEvaluation) {
  startRunner();
  const auto& query = runner_->veloxTask()->queryCtx();
  ASSERT_FALSE(query->queryConfig().exprDedupNonDeterministic());
  core::ExecCtx execution(pool_.get(), query.get());
  const auto field = std::make_shared<core::FieldAccessTypedExpr>(DOUBLE(), "c0");
  const auto absolute = std::make_shared<core::CallTypedExpr>(
      DOUBLE(), std::vector<core::TypedExprPtr>{field}, "abs");
  exec::ExprSet expressions({absolute, absolute}, &execution);
  ASSERT_EQ(expressions.size(), 2);
  ASSERT_EQ(expressions.expr(0).get(), expressions.expr(1).get());
  EXPECT_TRUE(expressions.expr(0)->isDeterministic());
  exec::EvalCtx evaluation(&execution, &expressions, input_.get());
  SelectivityVector rows(kRows);
  std::vector<VectorPtr> outputs;

  expressions.eval(rows, evaluation, outputs);

  ASSERT_EQ(outputs.size(), 2);
  for (const auto& output : outputs) {
    ASSERT_EQ(output->size(), kRows);
    DecodedVector decoded(*output);
    for (vector_size_t row = 0; row < kRows; ++row) {
      ASSERT_FALSE(decoded.isNullAt(row));
      EXPECT_DOUBLE_EQ(decoded.valueAt<double>(row), static_cast<double>(row));
    }
  }
  const auto stats = expressions.stats();
  const auto absoluteStats = stats.find("abs");
  ASSERT_NE(absoluteStats, stats.end());
  EXPECT_EQ(absoluteStats->second.numProcessedRows, kRows);
  EXPECT_EQ(absoluteStats->second.numProcessedVectors, 1);
}
}  // namespace
}  // namespace pangrova::velox::test
