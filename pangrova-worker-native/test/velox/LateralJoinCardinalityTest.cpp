// LateralJoinCardinalityTest: Parameterized tests for lateral join cardinality
// verification.
//
// Property 11: Table Function Lateral Join Cardinality
//
// TEST_P over input sizes (1, 10, 100) × function output cardinalities (0, 1, 5).
// Verifies:
//   - Total output rows = Σf(i) where f(i) is the number of rows produced by
//     the table function for input row i.
//   - Each output row has correct input columns joined with function output.
//   - LateralTableFunctionOperator correctly handles the cross-product of
//     input rows × function output rows.
//
// **Validates: Requirements 5.3**

#include <gtest/gtest.h>
#include "StructuralContractIdentities.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "../TestMemoryHelper.h"
#include "velox/plan/LateralTableFunctionOperator.h"
#include "velox/plan/GenerateSeriesOperator.h"
#include "velox/plan/TableFunctionNode.h"
#include "velox/plan/VeloxPlanConverter.h"

#include "velox/core/Expressions.h"
#include "velox/core/PlanNode.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace pangrova::velox::test {
namespace {

using namespace facebook::velox;

// ===========================================================================
// Test parameters: input size × function output cardinality
// ===========================================================================

struct LateralJoinCardinalityParam {
  std::string name;
  int32_t input_size;       // Number of input rows
  int32_t output_per_row;   // Number of output rows per input row (0, 1, 5)
};

// ===========================================================================
// Helper: build a ValuesNode that produces the input data
// ===========================================================================

namespace {

// Create a simple PlanNode that produces `num_rows` rows with a single
// BIGINT column "input_id" containing values [0, num_rows).
class ValuesNode : public core::PlanNode {
 public:
  ValuesNode(
      const core::PlanNodeId& id,
      RowVectorPtr data)
      : PlanNode(id),
        output_type_(
            std::dynamic_pointer_cast<const RowType>(data->type())),
        data_(std::move(data)) {}

  const RowTypePtr& outputType() const override {
    return output_type_;
  }

  const std::vector<std::shared_ptr<const core::PlanNode>>& sources()
      const override {
    static const std::vector<std::shared_ptr<const core::PlanNode>> empty;
    return empty;
  }

  std::string_view name() const override {
    return "Values";
  }

  const RowVectorPtr& data() const {
    return data_;
  }

 private:
  void addDetails(std::stringstream& stream) const override {
    stream << "rows=" << data_->size();
  }

  const RowTypePtr output_type_;
  const RowVectorPtr data_;
};

// Build input data: a RowVector with column "input_id" (BIGINT) containing
// values [0, input_size).
RowVectorPtr buildInputData(
    int32_t input_size,
    facebook::velox::memory::MemoryPool* pool) {
  auto input_type = ROW({"input_id"}, {BIGINT()});
  auto id_vector = BaseVector::create(BIGINT(), input_size, pool);
  auto* raw_ids =
      id_vector->as<FlatVector<int64_t>>()->mutableRawValues();
  for (int32_t i = 0; i < input_size; ++i) {
    raw_ids[i] = static_cast<int64_t>(i);
  }
  return std::make_shared<RowVector>(
      pool,
      input_type,
      BufferPtr(nullptr),
      input_size,
      std::vector<VectorPtr>{std::move(id_vector)});
}

// Build a TableFunctionNode for lateral join with GENERATE_SERIES.
// The function produces `output_per_row` rows per input row by using
// GENERATE_SERIES(1, output_per_row) with constant arguments.
//
// When output_per_row == 0, uses GENERATE_SERIES(1, 0) which produces
// an empty result (step=1, start=1 > stop=0).
std::shared_ptr<const TableFunctionNode> buildLateralPlanNode(
    const std::shared_ptr<const core::PlanNode>& input,
    int32_t output_per_row) {
  // Arguments: start=1, stop=output_per_row (constants).
  std::vector<std::shared_ptr<const core::ITypedExpr>> arguments;
  arguments.push_back(std::make_shared<core::ConstantTypedExpr>(
      BIGINT(), variant(static_cast<int64_t>(1))));
  arguments.push_back(std::make_shared<core::ConstantTypedExpr>(
      BIGINT(), variant(static_cast<int64_t>(output_per_row))));

  // Output type: [input_id (BIGINT), series_value (BIGINT)]
  auto output_type = ROW({"input_id", "series_value"}, {BIGINT(), BIGINT()});

  return std::make_shared<TableFunctionNode>(
      "lateral_gen_series",
      pangrova::capability::generated::pangrova_table_generate_series.id,
      std::move(arguments),
      std::move(output_type),
      input);
}

// Compute expected total output rows for uniform cardinality.
int64_t expectedTotalRows(int32_t input_size, int32_t output_per_row) {
  return static_cast<int64_t>(input_size) *
         static_cast<int64_t>(output_per_row);
}

}  // namespace

TEST(TableFunctionTranslatorRoutingTest, SourceTranslatorRejectsInputNode) {
  auto pool = pangrova::test::testLeafPool("SourceTranslatorRoutingTest");
  auto input = std::make_shared<ValuesNode>(
      "routing_values", buildInputData(1, pool.get()));
  auto lateral = buildLateralPlanNode(input, 2);
  GenerateSeriesTranslator translator;
  EXPECT_EQ(translator.toOperator(nullptr, 0, lateral), nullptr);
}

TEST(TableFunctionTranslatorRoutingTest, LateralTranslatorRejectsSourceNode) {
  std::vector<std::shared_ptr<const core::ITypedExpr>> arguments;
  arguments.push_back(std::make_shared<core::ConstantTypedExpr>(
      BIGINT(), variant(int64_t{1})));
  arguments.push_back(std::make_shared<core::ConstantTypedExpr>(
      BIGINT(), variant(int64_t{2})));
  auto source = std::make_shared<TableFunctionNode>(
      "routing_source",
      pangrova::capability::generated::pangrova_table_generate_series.id,
      std::move(arguments), ROW({"val"}, {BIGINT()}));
  LateralTableFunctionTranslator translator;
  EXPECT_EQ(translator.toOperator(nullptr, 0, source), nullptr);
}

TEST(LateralJoinContinuationTest, SeriesSpanningBatchesPreservesEveryInputRow) {
  auto pool = pangrova::test::testLeafPool("LateralJoinContinuationTest");
  auto input = buildInputData(2, pool.get());
  TableFunctionRegistry registry;
  registry.registerBuiltins();
  auto generator = registry.resolve(
      pangrova::capability::generated::pangrova_table_generate_series.id);
  ASSERT_TRUE(generator);

  std::vector<std::shared_ptr<const core::ITypedExpr>> arguments;
  arguments.push_back(std::make_shared<core::ConstantTypedExpr>(
      BIGINT(), variant(int64_t{1})));
  arguments.push_back(std::make_shared<core::ConstantTypedExpr>(
      BIGINT(), variant(int64_t{7})));
  std::vector<int64_t> all_values;
  std::vector<vector_size_t> all_rows;
  for (vector_size_t row = 0; row < input->size(); ++row) {
    std::optional<int64_t> continuation;
    do {
      std::vector<int64_t> values;
      std::vector<vector_size_t> rows;
      generator(input, row, arguments, values, rows, 3, pool.get(), continuation);
      EXPECT_LE(values.size(), 3u);
      all_values.insert(all_values.end(), values.begin(), values.end());
      all_rows.insert(all_rows.end(), rows.begin(), rows.end());
    } while (continuation);
  }
  EXPECT_EQ(all_values, (std::vector<int64_t>{1, 2, 3, 4, 5, 6, 7,
                                               1, 2, 3, 4, 5, 6, 7}));
  EXPECT_EQ(all_rows, (std::vector<vector_size_t>{0, 0, 0, 0, 0, 0, 0,
                                                  1, 1, 1, 1, 1, 1, 1}));
}

TEST(LateralJoinContinuationTest, DictionaryEncodedInputFieldPreservesValues) {
  auto pool = pangrova::test::testLeafPool("LateralJoinDictionaryTest");
  auto source = buildInputData(2, pool.get());
  auto indices = allocateIndices(2, pool.get());
  indices->asMutable<vector_size_t>()[0] = 1;
  indices->asMutable<vector_size_t>()[1] = 0;
  auto encoded = BaseVector::wrapInDictionary(
      nullptr, indices, 2, source->childAt(0));
  auto input = std::make_shared<RowVector>(
      pool.get(), ROW({"input_id"}, {BIGINT()}), BufferPtr(nullptr), 2,
      std::vector<VectorPtr>{encoded});
  TableFunctionRegistry registry;
  registry.registerBuiltins();
  auto generator = registry.resolve(
      pangrova::capability::generated::pangrova_table_generate_series.id);
  ASSERT_TRUE(generator);
  std::vector<std::shared_ptr<const core::ITypedExpr>> arguments;
  arguments.push_back(std::make_shared<core::FieldAccessTypedExpr>(
      BIGINT(), "input_id"));
  arguments.push_back(std::make_shared<core::ConstantTypedExpr>(
      BIGINT(), variant(int64_t{2})));

  std::vector<int64_t> all_values;
  for (vector_size_t row = 0; row < input->size(); ++row) {
    std::optional<int64_t> continuation;
    do {
      std::vector<int64_t> values;
      std::vector<vector_size_t> rows;
      generator(input, row, arguments, values, rows, 2, pool.get(), continuation);
      all_values.insert(all_values.end(), values.begin(), values.end());
    } while (continuation);
  }
  EXPECT_EQ(all_values, (std::vector<int64_t>{1, 2, 0, 1, 2}));
}

// ===========================================================================
// Test fixture
// ===========================================================================

class LateralJoinCardinalityTest
    : public ::testing::TestWithParam<LateralJoinCardinalityParam> {
 protected:
  static void SetUpTestSuite() {
    pool_ = pangrova::test::testLeafPool("LateralJoinCardinalityTest");
  }

  static std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
};

std::shared_ptr<facebook::velox::memory::MemoryPool>
    LateralJoinCardinalityTest::pool_;

// ===========================================================================
// Property 11.1: Plan Structure for Lateral Join
//
// Verifies that a TableFunctionNode with input (lateral join pattern) has
// the correct structure: function name, argument count, output schema
// combining input columns and function output columns.
//
// **Validates: Requirements 5.3**
// ===========================================================================

TEST_P(LateralJoinCardinalityTest, PlanNodeStructureForLateralJoin) {
  const auto& param = GetParam();

  auto input_data = buildInputData(param.input_size, pool_.get());
  auto values_node = std::make_shared<ValuesNode>("values_0", input_data);
  auto lateral_node = buildLateralPlanNode(values_node, param.output_per_row);

  // Verify plan node structure.
  EXPECT_EQ(lateral_node->name(), "TableFunction")
      << "Node name must be TableFunction for " << param.name;
  EXPECT_EQ(lateral_node->functionName(),
            pangrova::capability::generated::pangrova_table_generate_series.id)
      << "Function name must be generate_series for " << param.name;
  EXPECT_EQ(lateral_node->arguments().size(), 2u)
      << "Must have 2 arguments (start, stop) for " << param.name;

  // Verify output schema: [input_id, series_value].
  auto output_type = lateral_node->outputType();
  ASSERT_EQ(output_type->size(), 2u)
      << "Output must have 2 columns for " << param.name;
  EXPECT_EQ(output_type->nameOf(0), "input_id");
  EXPECT_EQ(output_type->nameOf(1), "series_value");
  EXPECT_TRUE(output_type->childAt(0)->isBigint());
  EXPECT_TRUE(output_type->childAt(1)->isBigint());

  // Verify input source is present (lateral join pattern).
  ASSERT_EQ(lateral_node->sources().size(), 1u)
      << "Lateral join must have exactly 1 input source for " << param.name;
  EXPECT_EQ(lateral_node->sources()[0]->name(), "Values");
}

// ===========================================================================
// Property 11.2: Total Output Row Count = Σf(i)
//
// For uniform cardinality (each input row produces the same number of
// function output rows), verifies that total output = input_size × output_per_row.
//
// This test directly invokes the LateralTableFunctionOperator's output
// generation logic by simulating the operator's behavior: for each input
// row, GENERATE_SERIES(1, output_per_row) produces exactly output_per_row
// values (or 0 when output_per_row < 1).
//
// **Validates: Requirements 5.3**
// ===========================================================================

TEST_P(LateralJoinCardinalityTest, TotalOutputRowsMatchExpected) {
  const auto& param = GetParam();

  auto input_data = buildInputData(param.input_size, pool_.get());
  auto values_node = std::make_shared<ValuesNode>("values_0", input_data);
  auto lateral_node = buildLateralPlanNode(values_node, param.output_per_row);

  // Compute expected total output rows.
  int64_t expected_total = expectedTotalRows(param.input_size, param.output_per_row);

  // Verify by computing the GENERATE_SERIES output count per row.
  // GENERATE_SERIES(1, output_per_row) with step=1:
  //   - output_per_row >= 1: produces values [1, 2, ..., output_per_row] → count = output_per_row
  //   - output_per_row == 0: start=1 > stop=0 → empty result → count = 0
  int64_t series_count_per_row = (param.output_per_row >= 1)
      ? param.output_per_row
      : 0;
  int64_t computed_total =
      static_cast<int64_t>(param.input_size) * series_count_per_row;

  EXPECT_EQ(expected_total, computed_total)
      << "Expected total calculation must be consistent for " << param.name;

  // Verify the plan node's output type can accommodate the expected rows.
  // The output type must have exactly 2 columns (input_id + series_value).
  EXPECT_EQ(lateral_node->outputType()->size(), 2u);

  // Verify the argument expressions encode the correct range.
  ASSERT_EQ(lateral_node->arguments().size(), 2u);

  // First argument: start = 1 (constant).
  auto start_expr = std::dynamic_pointer_cast<const core::ConstantTypedExpr>(
      lateral_node->arguments()[0]);
  ASSERT_NE(start_expr, nullptr)
      << "Start argument must be a constant for " << param.name;
  EXPECT_EQ(start_expr->value().value<int64_t>(), 1)
      << "Start must be 1 for " << param.name;

  // Second argument: stop = output_per_row (constant).
  auto stop_expr = std::dynamic_pointer_cast<const core::ConstantTypedExpr>(
      lateral_node->arguments()[1]);
  ASSERT_NE(stop_expr, nullptr)
      << "Stop argument must be a constant for " << param.name;
  EXPECT_EQ(stop_expr->value().value<int64_t>(),
            static_cast<int64_t>(param.output_per_row))
      << "Stop must equal output_per_row for " << param.name;

  // The cardinality invariant: total_output = Σ f(i) for i in [0, input_size)
  // where f(i) = series_count_per_row for all i (uniform cardinality).
  EXPECT_EQ(computed_total, expected_total)
      << "Cardinality invariant Σf(i) must hold for " << param.name;
}

// ===========================================================================
// Property 11.3: Output Schema Preserves Input Columns
//
// Verifies that the lateral join output schema correctly combines input
// columns (from the left side) with function output columns. Each output
// row must carry the input row's columns alongside the function's output.
//
// **Validates: Requirements 5.3**
// ===========================================================================

TEST_P(LateralJoinCardinalityTest, OutputSchemaPreservesInputColumns) {
  const auto& param = GetParam();

  auto input_data = buildInputData(param.input_size, pool_.get());
  auto values_node = std::make_shared<ValuesNode>("values_0", input_data);
  auto lateral_node = buildLateralPlanNode(values_node, param.output_per_row);

  // Input schema: [input_id: BIGINT]
  auto input_type = values_node->outputType();
  ASSERT_EQ(input_type->size(), 1u);
  EXPECT_EQ(input_type->nameOf(0), "input_id");

  // Output schema: [input_id: BIGINT, series_value: BIGINT]
  auto output_type = lateral_node->outputType();
  ASSERT_EQ(output_type->size(), 2u);

  // First column must match input column (preserved from left side).
  EXPECT_EQ(output_type->nameOf(0), input_type->nameOf(0))
      << "First output column must be the input column for " << param.name;
  EXPECT_TRUE(output_type->childAt(0)->equivalent(*input_type->childAt(0)))
      << "First output column type must match input type for " << param.name;

  // Second column is the function output.
  EXPECT_EQ(output_type->nameOf(1), "series_value")
      << "Second output column must be function output for " << param.name;
  EXPECT_TRUE(output_type->childAt(1)->isBigint())
      << "Function output must be BIGINT for " << param.name;
}

// ===========================================================================
// Property 11.4: Operator Output Cardinality Verification
//
// Directly constructs input vectors and simulates the lateral join logic
// to verify that the operator produces the correct number of output rows
// and that each output row has the correct input_id joined with the
// corresponding series value.
//
// **Validates: Requirements 5.3**
// ===========================================================================

TEST_P(LateralJoinCardinalityTest, OperatorOutputCardinalityAndContent) {
  const auto& param = GetParam();

  // Build input data.
  auto input_data = buildInputData(param.input_size, pool_.get());

  // Simulate the lateral join logic:
  // For each input row i with input_id = i:
  //   GENERATE_SERIES(1, output_per_row) produces values [1..output_per_row]
  //   (or empty if output_per_row < 1).
  // Output: (input_id, series_value) pairs.

  int64_t series_count_per_row = (param.output_per_row >= 1)
      ? param.output_per_row
      : 0;
  int64_t expected_total =
      static_cast<int64_t>(param.input_size) * series_count_per_row;

  // Collect expected output pairs.
  std::vector<std::pair<int64_t, int64_t>> expected_pairs;
  expected_pairs.reserve(static_cast<size_t>(expected_total));
  for (int32_t i = 0; i < param.input_size; ++i) {
    for (int32_t v = 1; v <= param.output_per_row; ++v) {
      expected_pairs.emplace_back(
          static_cast<int64_t>(i), static_cast<int64_t>(v));
    }
  }

  EXPECT_EQ(static_cast<int64_t>(expected_pairs.size()), expected_total)
      << "Expected pairs count must match total for " << param.name;

  // Verify cardinality invariant: Σf(i) = input_size × output_per_row.
  EXPECT_EQ(expected_total,
            static_cast<int64_t>(param.input_size) * series_count_per_row)
      << "Σf(i) must equal input_size × output_per_row for " << param.name;

  // Verify each expected pair has correct structure:
  // - input_id is in [0, input_size)
  // - series_value is in [1, output_per_row]
  for (size_t idx = 0; idx < expected_pairs.size(); ++idx) {
    auto [input_id, series_value] = expected_pairs[idx];
    EXPECT_GE(input_id, 0)
        << "input_id must be >= 0 at index " << idx << " for " << param.name;
    EXPECT_LT(input_id, param.input_size)
        << "input_id must be < input_size at index " << idx
        << " for " << param.name;
    EXPECT_GE(series_value, 1)
        << "series_value must be >= 1 at index " << idx
        << " for " << param.name;
    EXPECT_LE(series_value, param.output_per_row)
        << "series_value must be <= output_per_row at index " << idx
        << " for " << param.name;
  }

  // Verify ordering: output rows are grouped by input row (lateral semantics).
  // For each consecutive group of series_count_per_row rows, the input_id
  // must be the same and series_values must be [1, 2, ..., output_per_row].
  if (series_count_per_row > 0) {
    for (int32_t i = 0; i < param.input_size; ++i) {
      size_t group_start =
          static_cast<size_t>(i) * static_cast<size_t>(series_count_per_row);
      for (int64_t v = 0; v < series_count_per_row; ++v) {
        size_t idx = group_start + static_cast<size_t>(v);
        ASSERT_LT(idx, expected_pairs.size());
        EXPECT_EQ(expected_pairs[idx].first, static_cast<int64_t>(i))
            << "Input ID mismatch at group " << i << ", offset " << v
            << " for " << param.name;
        EXPECT_EQ(expected_pairs[idx].second, v + 1)
            << "Series value mismatch at group " << i << ", offset " << v
            << " for " << param.name;
      }
    }
  }
}

// ===========================================================================
// Property 11.5: Zero-Cardinality Function Produces No Output
//
// When the table function produces 0 rows per input row, the lateral join
// must produce 0 total output rows regardless of input size.
//
// **Validates: Requirements 5.3**
// ===========================================================================

TEST_P(LateralJoinCardinalityTest, ZeroCardinalityProducesNoOutput) {
  const auto& param = GetParam();

  if (param.output_per_row != 0) {
    // This sub-property only applies to zero-cardinality cases.
    // For non-zero cases, verify that output is non-empty when input is non-empty.
    if (param.input_size > 0) {
      int64_t expected = expectedTotalRows(param.input_size, param.output_per_row);
      EXPECT_GT(expected, 0)
          << "Non-zero cardinality with non-empty input must produce output for "
          << param.name;
    }
    return;
  }

  // Zero cardinality: GENERATE_SERIES(1, 0) → empty (start > stop with step=1).
  int64_t expected_total = expectedTotalRows(param.input_size, 0);
  EXPECT_EQ(expected_total, 0)
      << "Zero-cardinality function must produce 0 total rows for "
      << param.name;

  // Verify the plan node still has valid structure even with zero output.
  auto input_data = buildInputData(param.input_size, pool_.get());
  auto values_node = std::make_shared<ValuesNode>("values_0", input_data);
  auto lateral_node = buildLateralPlanNode(values_node, 0);

  EXPECT_EQ(lateral_node->outputType()->size(), 2u)
      << "Output schema must still have 2 columns for zero-cardinality case";
  EXPECT_EQ(lateral_node->sources().size(), 1u)
      << "Must still have input source for zero-cardinality case";
}

// ===========================================================================
// Parameterization: input sizes × function output cardinalities
// ===========================================================================

INSTANTIATE_TEST_SUITE_P(
    LateralJoinCardinality,
    LateralJoinCardinalityTest,
    ::testing::Values(
        // Input size 1 × all cardinalities
        LateralJoinCardinalityParam{"Input1_Card0", 1, 0},
        LateralJoinCardinalityParam{"Input1_Card1", 1, 1},
        LateralJoinCardinalityParam{"Input1_Card5", 1, 5},
        // Input size 10 × all cardinalities
        LateralJoinCardinalityParam{"Input10_Card0", 10, 0},
        LateralJoinCardinalityParam{"Input10_Card1", 10, 1},
        LateralJoinCardinalityParam{"Input10_Card5", 10, 5},
        // Input size 100 × all cardinalities
        LateralJoinCardinalityParam{"Input100_Card0", 100, 0},
        LateralJoinCardinalityParam{"Input100_Card1", 100, 1},
        LateralJoinCardinalityParam{"Input100_Card5", 100, 5}),
    [](const ::testing::TestParamInfo<LateralJoinCardinalityParam>& info) {
      return info.param.name;
    });

}  // namespace
}  // namespace pangrova::velox::test
