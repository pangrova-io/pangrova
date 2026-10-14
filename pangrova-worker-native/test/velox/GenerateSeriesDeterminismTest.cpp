// GenerateSeriesDeterminismTest: Parameterized tests for GENERATE_SERIES
// determinism verification.
//
// Property 10: GENERATE_SERIES Determinism
//
// TEST_P over (start, stop, step) including edge cases: start=stop, negative
// step, large ranges, step>range.
// Verifies:
//   - The exact sequence produced matches the specification:
//     [start, start+step, start+2*step, ...] containing all values v where
//     (step > 0 ∧ start ≤ v ≤ stop) ∨ (step < 0 ∧ stop ≤ v ≤ start).
//   - Empty result when step > 0 and start > stop, or step < 0 and start < stop.
//   - Sequence length matches the expected count.
//
// **Validates: Requirements 5.2**

#include <gtest/gtest.h>
#include "velox/expression/FunctionRegistrySnapshot.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <tuple>
#include <vector>

#include "../TestMemoryHelper.h"
#include "velox/plan/GenerateSeriesOperator.h"
#include "velox/plan/TableFunctionNode.h"
#include "velox/runtime/TableFunctionRegistry.h"

#include "velox/core/Expressions.h"
#include "velox/core/PlanFragment.h"
#include "velox/core/PlanNode.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/Task.h"

namespace pangrova::velox::test {
namespace {

using namespace facebook::velox;

// ===========================================================================
// Test parameters: (start, stop, step) combinations
// ===========================================================================

struct GenerateSeriesParam {
  std::string name;
  int64_t start;
  int64_t stop;
  int64_t step;
};

// ===========================================================================
// Helper: compute expected sequence per specification
// ===========================================================================

// Computes the expected sequence for GENERATE_SERIES(start, stop, step).
// Specification:
//   - step > 0: all values v where start <= v <= stop
//   - step < 0: all values v where stop <= v <= start
//   - Empty when step > 0 and start > stop, or step < 0 and start < stop
std::vector<int64_t> computeExpectedSequence(
    int64_t start,
    int64_t stop,
    int64_t step) {
  std::vector<int64_t> result;

  if (step > 0 && start > stop) {
    return result;  // Empty
  }
  if (step < 0 && start < stop) {
    return result;  // Empty
  }

  int64_t current = start;
  if (step > 0) {
    while (current <= stop) {
      result.push_back(current);
      current += step;
    }
  } else {
    // step < 0
    while (current >= stop) {
      result.push_back(current);
      current += step;
    }
  }

  return result;
}

// ===========================================================================
// Helper: build a standalone TableFunctionNode for GENERATE_SERIES
// ===========================================================================

std::shared_ptr<const TableFunctionNode> buildGenerateSeriesNode(
    int64_t start,
    int64_t stop,
    int64_t step) {
  std::vector<std::shared_ptr<const core::ITypedExpr>> arguments;
  arguments.push_back(std::make_shared<core::ConstantTypedExpr>(
      BIGINT(), variant(start)));
  arguments.push_back(std::make_shared<core::ConstantTypedExpr>(
      BIGINT(), variant(stop)));
  arguments.push_back(std::make_shared<core::ConstantTypedExpr>(
      BIGINT(), variant(step)));

  auto output_type = ROW({"series_value"}, {BIGINT()});

  return std::make_shared<TableFunctionNode>(
      "gen_series_test",
      "generate_series",
      std::move(arguments),
      std::move(output_type));
}

// ===========================================================================
// Helper: execute GenerateSeriesOperator via Velox Task and collect output
// ===========================================================================

std::vector<int64_t> executeGenerateSeries(
    const std::shared_ptr<const TableFunctionNode>& node) {
  auto plan = std::dynamic_pointer_cast<const core::PlanNode>(node);

  // Collect output vectors via consumer callback.
  std::mutex mu;
  std::vector<int64_t> values;

  auto consumer = [&mu, &values](
      RowVectorPtr vector,
      bool /*drained*/,
      facebook::velox::ContinueFuture* /*future*/) -> exec::BlockingReason {
    if (vector && vector->size() > 0) {
      std::lock_guard<std::mutex> lock(mu);
      auto* flat = vector->childAt(0)->as<FlatVector<int64_t>>();
      for (vector_size_t i = 0; i < vector->size(); ++i) {
        values.push_back(flat->valueAt(i));
      }
    }
    return exec::BlockingReason::kNotBlocked;
  };

  // QueryCtx requires the global executor for parallel task scheduling.
  auto executor = pangrova::jni::globalExecutor();
  auto queryCtx = core::QueryCtx::create(executor.get(),
      facebook::velox::core::QueryConfig({{"require_function_registry_snapshot", "true"}},
          facebook::velox::exec::FunctionRegistrySnapshot::capture()));
  auto task = exec::Task::create(
      "gen_series_determinism_test",
      core::PlanFragment{plan},
      0,
      std::move(queryCtx),
      exec::Task::ExecutionMode::kParallel,
      std::move(consumer));

  // Wait for task completion.
  task->start(1);
  auto future = task->taskCompletionFuture();
  future.wait();

  return values;
}

// ===========================================================================
// Test fixture
// ===========================================================================

class GenerateSeriesDeterminismTest
    : public ::testing::TestWithParam<GenerateSeriesParam> {
 protected:
  static void SetUpTestSuite() {
    pool_ = pangrova::test::testLeafPool("GenerateSeriesDeterminismTest");
  }

  static std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
};

std::shared_ptr<facebook::velox::memory::MemoryPool>
    GenerateSeriesDeterminismTest::pool_;

// ===========================================================================
// Property 10.1: Exact Sequence Matches Specification
//
// For any valid (start, stop, step) where step ≠ 0, GENERATE_SERIES SHALL
// produce exactly the sequence [start, start+step, start+2*step, ...]
// containing all values v where (step > 0 ∧ start ≤ v ≤ stop) ∨
// (step < 0 ∧ stop ≤ v ≤ start).
//
// **Validates: Requirements 5.2**
// ===========================================================================

TEST_P(GenerateSeriesDeterminismTest, ExactSequenceMatchesSpecification) {
  const auto& param = GetParam();

  // Compute expected sequence per specification.
  auto expected = computeExpectedSequence(param.start, param.stop, param.step);

  // Execute the operator via Velox Task.
  auto node = buildGenerateSeriesNode(param.start, param.stop, param.step);
  auto actual = executeGenerateSeries(node);

  // Verify exact sequence match.
  ASSERT_EQ(actual.size(), expected.size())
      << "Sequence length mismatch for " << param.name
      << ": start=" << param.start << ", stop=" << param.stop
      << ", step=" << param.step;

  for (size_t i = 0; i < expected.size(); ++i) {
    EXPECT_EQ(actual[i], expected[i])
        << "Value mismatch at index " << i << " for " << param.name
        << ": expected " << expected[i] << ", got " << actual[i];
  }
}

// ===========================================================================
// Property 10.2: Sequence Length Matches Expected Count
//
// Verifies the number of elements produced matches the formula:
//   step > 0: floor((stop - start) / step) + 1 (when start <= stop)
//   step < 0: floor((start - stop) / (-step)) + 1 (when start >= stop)
//   0 otherwise (empty sequence)
//
// **Validates: Requirements 5.2**
// ===========================================================================

TEST_P(GenerateSeriesDeterminismTest, SequenceLengthMatchesFormula) {
  const auto& param = GetParam();

  // Compute expected length.
  int64_t expected_length = 0;
  if (param.step > 0 && param.start <= param.stop) {
    expected_length = (param.stop - param.start) / param.step + 1;
  } else if (param.step < 0 && param.start >= param.stop) {
    expected_length = (param.start - param.stop) / (-param.step) + 1;
  }

  // Execute the operator.
  auto node = buildGenerateSeriesNode(param.start, param.stop, param.step);
  auto actual = executeGenerateSeries(node);

  EXPECT_EQ(static_cast<int64_t>(actual.size()), expected_length)
      << "Sequence length mismatch for " << param.name
      << ": start=" << param.start << ", stop=" << param.stop
      << ", step=" << param.step
      << ", expected_length=" << expected_length;
}

// ===========================================================================
// Property 10.3: Sequence Monotonicity
//
// When step > 0, the sequence must be strictly increasing.
// When step < 0, the sequence must be strictly decreasing.
//
// **Validates: Requirements 5.2**
// ===========================================================================

TEST_P(GenerateSeriesDeterminismTest, SequenceMonotonicity) {
  const auto& param = GetParam();

  auto node = buildGenerateSeriesNode(param.start, param.stop, param.step);
  auto actual = executeGenerateSeries(node);

  if (actual.size() <= 1) {
    // Trivially monotonic for 0 or 1 elements.
    return;
  }

  for (size_t i = 1; i < actual.size(); ++i) {
    if (param.step > 0) {
      EXPECT_GT(actual[i], actual[i - 1])
          << "Sequence must be strictly increasing for positive step at index "
          << i << " for " << param.name;
    } else {
      EXPECT_LT(actual[i], actual[i - 1])
          << "Sequence must be strictly decreasing for negative step at index "
          << i << " for " << param.name;
    }
  }
}

// ===========================================================================
// Property 10.4: Sequence Boundaries
//
// The first element must equal start.
// The last element must satisfy: last <= stop (step > 0) or last >= stop (step < 0).
// All elements must be within [min(start,stop), max(start,stop)].
//
// **Validates: Requirements 5.2**
// ===========================================================================

TEST_P(GenerateSeriesDeterminismTest, SequenceBoundaries) {
  const auto& param = GetParam();

  auto node = buildGenerateSeriesNode(param.start, param.stop, param.step);
  auto actual = executeGenerateSeries(node);

  if (actual.empty()) {
    // Empty sequence is valid for certain parameter combinations.
    return;
  }

  // First element must be start.
  EXPECT_EQ(actual.front(), param.start)
      << "First element must equal start for " << param.name;

  // Last element boundary check.
  if (param.step > 0) {
    EXPECT_LE(actual.back(), param.stop)
        << "Last element must be <= stop for positive step in " << param.name;
    // Next value would exceed stop.
    EXPECT_GT(actual.back() + param.step, param.stop)
        << "Next value after last must exceed stop for " << param.name;
  } else {
    EXPECT_GE(actual.back(), param.stop)
        << "Last element must be >= stop for negative step in " << param.name;
    // Next value would go below stop.
    EXPECT_LT(actual.back() + param.step, param.stop)
        << "Next value after last must be below stop for " << param.name;
  }

  // All elements within bounds.
  int64_t lower = std::min(param.start, param.stop);
  int64_t upper = std::max(param.start, param.stop);
  for (size_t i = 0; i < actual.size(); ++i) {
    EXPECT_GE(actual[i], lower)
        << "Element at index " << i << " below lower bound for " << param.name;
    EXPECT_LE(actual[i], upper)
        << "Element at index " << i << " above upper bound for " << param.name;
  }
}

// ===========================================================================
// Property 10.5: Determinism (Repeated Execution)
//
// Running GENERATE_SERIES with the same parameters must produce the exact
// same sequence every time.
//
// **Validates: Requirements 5.2**
// ===========================================================================

TEST_P(GenerateSeriesDeterminismTest, RepeatedExecutionProducesSameResult) {
  const auto& param = GetParam();

  auto node1 = buildGenerateSeriesNode(param.start, param.stop, param.step);
  auto result1 = executeGenerateSeries(node1);

  auto node2 = buildGenerateSeriesNode(param.start, param.stop, param.step);
  auto result2 = executeGenerateSeries(node2);

  ASSERT_EQ(result1.size(), result2.size())
      << "Repeated execution must produce same length for " << param.name;

  for (size_t i = 0; i < result1.size(); ++i) {
    EXPECT_EQ(result1[i], result2[i])
        << "Repeated execution must produce same value at index " << i
        << " for " << param.name;
  }
}

TEST_F(GenerateSeriesDeterminismTest, Int64BoundariesDoNotOverflow) {
  constexpr auto max = std::numeric_limits<int64_t>::max();
  constexpr auto min = std::numeric_limits<int64_t>::min();
  EXPECT_EQ(executeGenerateSeries(buildGenerateSeriesNode(max - 1, max, 1)),
            (std::vector<int64_t>{max - 1, max}));
  EXPECT_EQ(executeGenerateSeries(buildGenerateSeriesNode(min + 1, min, -1)),
            (std::vector<int64_t>{min + 1, min}));
  EXPECT_EQ(executeGenerateSeries(buildGenerateSeriesNode(min, max, max)),
            (std::vector<int64_t>{min, -1, max - 1}));
  EXPECT_EQ(executeGenerateSeries(buildGenerateSeriesNode(max, max, 1)),
            (std::vector<int64_t>{max}));
}

TEST_F(GenerateSeriesDeterminismTest, LateralGeneratorStopsAtInt64Boundaries) {
  constexpr auto max = std::numeric_limits<int64_t>::max();
  constexpr auto min = std::numeric_limits<int64_t>::min();
  TableFunctionRegistry registry;
  registry.registerBuiltins();
  auto generate = registry.resolve("generate_series");
  ASSERT_TRUE(generate);

  for (const auto& [start, stop, step, expected] :
       std::vector<std::tuple<int64_t, int64_t, int64_t, std::vector<int64_t>>>{
           {max - 1, max, 1, {max - 1, max}},
           {min + 1, min, -1, {min + 1, min}}}) {
    std::vector<int64_t> values;
    std::vector<vector_size_t> row_indices;
    auto node = buildGenerateSeriesNode(start, stop, step);
    const auto count = generate(nullptr, 0, node->arguments(), values,
                                row_indices, 100, pool_.get());
    EXPECT_EQ(values, expected);
    EXPECT_EQ(count, expected.size());
    EXPECT_EQ(row_indices.size(), expected.size());
  }
}

// ===========================================================================
// Parameterization: (start, stop, step) combinations including edge cases
// ===========================================================================

INSTANTIATE_TEST_SUITE_P(
    GenerateSeriesDeterminism,
    GenerateSeriesDeterminismTest,
    ::testing::Values(
        // Basic positive step
        GenerateSeriesParam{"Positive_1to10_step1", 1, 10, 1},
        GenerateSeriesParam{"Positive_0to100_step10", 0, 100, 10},
        GenerateSeriesParam{"Positive_1to1000_step7", 1, 1000, 7},

        // Edge case: start == stop
        GenerateSeriesParam{"StartEqualsStop_5_5_1", 5, 5, 1},
        GenerateSeriesParam{"StartEqualsStop_0_0_1", 0, 0, 1},
        GenerateSeriesParam{"StartEqualsStop_neg3_neg3_neg1", -3, -3, -1},

        // Negative step (descending)
        GenerateSeriesParam{"Negative_10to1_stepNeg1", 10, 1, -1},
        GenerateSeriesParam{"Negative_100to0_stepNeg10", 100, 0, -10},
        GenerateSeriesParam{"Negative_50toNeg50_stepNeg5", 50, -50, -5},

        // Large ranges
        GenerateSeriesParam{"LargeRange_1to10000_step1", 1, 10000, 1},
        GenerateSeriesParam{"LargeRange_0to1000000_step1000", 0, 1000000, 1000},

        // Step > range (produces single element)
        GenerateSeriesParam{"StepGtRange_1to5_step100", 1, 5, 100},
        GenerateSeriesParam{"StepGtRange_10to12_step50", 10, 12, 50},
        GenerateSeriesParam{"StepGtRange_neg5toNeg10_stepNeg100", -5, -10, -100},

        // Empty result: step > 0 and start > stop
        GenerateSeriesParam{"Empty_10to1_step1", 10, 1, 1},
        GenerateSeriesParam{"Empty_100to0_step5", 100, 0, 5},

        // Empty result: step < 0 and start < stop
        GenerateSeriesParam{"Empty_1to10_stepNeg1", 1, 10, -1},
        GenerateSeriesParam{"Empty_0to100_stepNeg5", 0, 100, -5},

        // Negative values
        GenerateSeriesParam{"NegativeRange_neg10toNeg1_step1", -10, -1, 1},
        GenerateSeriesParam{"NegativeRange_neg1toNeg10_stepNeg1", -1, -10, -1},

        // Crossing zero
        GenerateSeriesParam{"CrossZero_neg5to5_step1", -5, 5, 1},
        GenerateSeriesParam{"CrossZero_5toNeg5_stepNeg1", 5, -5, -1},
        GenerateSeriesParam{"CrossZero_neg10to10_step3", -10, 10, 3},

        // Non-unit step that doesn't evenly divide range
        GenerateSeriesParam{"UnevenStep_1to10_step3", 1, 10, 3},
        GenerateSeriesParam{"UnevenStep_0to100_step7", 0, 100, 7},
        GenerateSeriesParam{"UnevenStep_100to0_stepNeg7", 100, 0, -7}),
    [](const ::testing::TestParamInfo<GenerateSeriesParam>& info) {
      return info.param.name;
    });

}  // namespace
}  // namespace pangrova::velox::test
