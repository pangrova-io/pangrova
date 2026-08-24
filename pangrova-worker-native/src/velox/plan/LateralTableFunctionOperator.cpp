// LateralTableFunctionOperator: Velox operator for lateral/correlated table
// function invocations (GENERATE_SERIES with correlated arguments).
//
// Executes the table function once per input row and produces the cross-product
// of input row columns and function output columns.
#include "velox/plan/LateralTableFunctionOperator.h"

#include <algorithm>
#include <stdexcept>

#include "velox/common/base/Exceptions.h"
#include "velox/core/Expressions.h"
#include "velox/vector/FlatVector.h"
#include "velox/runtime/TableFunctionRegistry.h"

// Forward declaration of globalTableFunctionRegistry (defined in RuntimeInitializer).
namespace pangrova::jni {
pangrova::velox::TableFunctionRegistry* globalTableFunctionRegistry();
}  // namespace pangrova::jni

namespace pangrova::velox {

LateralTableFunctionOperator::LateralTableFunctionOperator(
    int32_t operatorId,
    facebook::velox::exec::DriverCtx* driverCtx,
    const std::shared_ptr<const TableFunctionNode>& node)
    : Operator(
          driverCtx,
          node->outputType(),
          operatorId,
          node->id(),
          "LateralTableFunction"),
      function_name_(node->functionName()),
      arguments_(node->arguments()),
      input_column_count_(
          node->sources().empty()
              ? 0
              : node->sources()[0]->outputType()->size()) {
  VELOX_USER_CHECK(
      !node->sources().empty(),
      "LateralTableFunctionOperator requires an input source (lateral join)");
  auto* registry = pangrova::jni::globalTableFunctionRegistry();
  VELOX_USER_CHECK(
      registry != nullptr && registry->isRegistered(function_name_),
      "LateralTableFunctionOperator: function '{}' is not registered",
      function_name_);
  row_generator_ = registry->resolve(function_name_);
  VELOX_USER_CHECK(
      row_generator_ != nullptr,
      "LateralTableFunctionOperator: function '{}' has no RowGenerator",
      function_name_);
}

void LateralTableFunctionOperator::addInput(
    facebook::velox::RowVectorPtr input) {
  input_ = std::move(input);
  current_input_row_ = 0;
}

namespace {

// Build the output RowVector from collected values and replicated input columns.
facebook::velox::RowVectorPtr buildOutputBatch(
    const facebook::velox::RowVectorPtr& input,
    size_t input_column_count,
    const std::vector<int64_t>& output_values,
    const std::vector<facebook::velox::vector_size_t>& input_row_indices,
    const facebook::velox::RowTypePtr& output_type,
    facebook::velox::memory::MemoryPool* pool) {
  auto output_size =
      static_cast<facebook::velox::vector_size_t>(output_values.size());

  std::vector<facebook::velox::VectorPtr> output_columns;
  output_columns.reserve(output_type->size());

  // Replicate input columns based on input_row_indices.
  for (size_t col = 0; col < input_column_count; ++col) {
    auto source_vector = input->childAt(col);
    auto replicated = facebook::velox::BaseVector::create(
        source_vector->type(), output_size, pool);
    for (facebook::velox::vector_size_t i = 0; i < output_size; ++i) {
      replicated->copy(source_vector.get(), i, input_row_indices[i], 1);
    }
    output_columns.push_back(std::move(replicated));
  }

  // Function output column (the generated series values).
  auto series_vector =
      std::dynamic_pointer_cast<facebook::velox::FlatVector<int64_t>>(
          facebook::velox::BaseVector::create(
              facebook::velox::BIGINT(), output_size, pool));
  auto* raw_values = series_vector->mutableRawValues();
  for (facebook::velox::vector_size_t i = 0; i < output_size; ++i) {
    raw_values[i] = output_values[i];
  }
  output_columns.push_back(std::move(series_vector));

  return std::make_shared<facebook::velox::RowVector>(
      pool,
      output_type,
      facebook::velox::BufferPtr(nullptr),
      output_size,
      std::move(output_columns));
}

}  // namespace

facebook::velox::RowVectorPtr LateralTableFunctionOperator::getOutput() {
  if (!input_) {
    return nullptr;
  }

  auto input_size = input_->size();
  if (current_input_row_ >= input_size) {
    input_ = nullptr;
    return nullptr;
  }

  const auto max_output = static_cast<size_t>(outputBatchRows());
  std::vector<int64_t> output_values;
  std::vector<facebook::velox::vector_size_t> input_row_indices;
  output_values.reserve(max_output);
  input_row_indices.reserve(max_output);

  // Process input rows using the generic RowGenerator.
  while (output_values.size() < max_output &&
         current_input_row_ < input_size) {
    row_generator_(
        input_, current_input_row_, arguments_,
        output_values, input_row_indices, max_output,
        operatorCtx_->pool());
    ++current_input_row_;
  }

  if (output_values.empty()) {
    input_ = nullptr;
    return nullptr;
  }

  auto output_type = std::dynamic_pointer_cast<const facebook::velox::RowType>(
      outputType_);
  return buildOutputBatch(
      input_, input_column_count_, output_values, input_row_indices,
      output_type, operatorCtx_->pool());
}

bool LateralTableFunctionOperator::isFinished() {
  return noMoreInput_ && !input_;
}

facebook::velox::exec::BlockingReason LateralTableFunctionOperator::isBlocked(
    facebook::velox::ContinueFuture* /*future*/) {
  return facebook::velox::exec::BlockingReason::kNotBlocked;
}

// --- PlanNodeTranslator ---

std::unique_ptr<facebook::velox::exec::Operator>
LateralTableFunctionTranslator::toOperator(
    facebook::velox::exec::DriverCtx* ctx,
    int32_t id,
    const facebook::velox::core::PlanNodePtr& node) {
  auto table_func_node =
      std::dynamic_pointer_cast<const TableFunctionNode>(node);
  if (!table_func_node) {
    return nullptr;
  }
  // Only handle the lateral case (node has input sources).
  if (table_func_node->sources().empty()) {
    return nullptr;
  }
  auto* registry = pangrova::jni::globalTableFunctionRegistry();
  if (!registry || !registry->isRegistered(table_func_node->functionName())) {
    return nullptr;
  }
  return std::make_unique<LateralTableFunctionOperator>(
      id, ctx, table_func_node);
}

// --- Registration ---

void registerLateralTableFunctionOperator() {
  facebook::velox::exec::Operator::registerOperator(
      std::make_unique<LateralTableFunctionTranslator>());
}

}  // namespace pangrova::velox
