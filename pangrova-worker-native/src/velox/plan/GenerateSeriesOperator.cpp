// GenerateSeriesOperator: Velox source operator for GENERATE_SERIES.
// Produces integer sequences based on (start, stop, step) parameters.
// Registered with Velox's operator framework via PlanNodeTranslator.
#include "velox/plan/GenerateSeriesOperator.h"

#include <algorithm>
#include <stdexcept>

#include "velox/common/base/Exceptions.h"
#include "velox/vector/FlatVector.h"

namespace pangrova::velox {

namespace {

// Extract a constant int64 value from a ConstantTypedExpr argument.
// Table function arguments are resolved at plan conversion time as constants.
int64_t extractInt64Argument(
    const std::shared_ptr<const facebook::velox::core::ITypedExpr>& expr,
    const std::string& arg_name) {
  auto constant_expr =
      std::dynamic_pointer_cast<const facebook::velox::core::ConstantTypedExpr>(
          expr);
  if (!constant_expr) {
    VELOX_USER_FAIL(
        "GENERATE_SERIES: argument '{}' must be a constant integer, "
        "got non-constant expression",
        arg_name);
  }
  if (constant_expr->value().isNull()) {
    VELOX_USER_FAIL(
        "GENERATE_SERIES: argument '{}' must not be NULL", arg_name);
  }
  auto variant = constant_expr->value();
  // Support BIGINT and INTEGER argument types.
  if (constant_expr->type()->kind() == facebook::velox::TypeKind::BIGINT) {
    return variant.value<int64_t>();
  }
  if (constant_expr->type()->kind() == facebook::velox::TypeKind::INTEGER) {
    return static_cast<int64_t>(variant.value<int32_t>());
  }
  VELOX_USER_FAIL(
      "GENERATE_SERIES: argument '{}' must be an integer type, got {}",
      arg_name,
      constant_expr->type()->toString());
}

}  // namespace

GenerateSeriesOperator::GenerateSeriesOperator(
    int32_t operatorId,
    facebook::velox::exec::DriverCtx* driverCtx,
    const std::shared_ptr<const TableFunctionNode>& node)
    : Operator(
          driverCtx,
          node->outputType(),
          operatorId,
          node->id(),
          "GenerateSeries"),
      finished_(false) {
  const auto& args = node->arguments();
  VELOX_USER_CHECK(
      args.size() == 2 || args.size() == 3,
      "GENERATE_SERIES requires 2 or 3 arguments (start, stop[, step]), "
      "got {}",
      args.size());

  start_ = extractInt64Argument(args[0], "start");
  stop_ = extractInt64Argument(args[1], "stop");
  step_ = (args.size() == 3) ? extractInt64Argument(args[2], "step") : 1;

  VELOX_USER_CHECK_NE(
      step_, 0, "GENERATE_SERIES: step must not be zero");

  current_ = start_;

  // Determine if the sequence is immediately empty.
  if ((step_ > 0 && start_ > stop_) || (step_ < 0 && start_ < stop_)) {
    finished_ = true;
  }
}

facebook::velox::RowVectorPtr GenerateSeriesOperator::getOutput() {
  if (finished_) {
    return nullptr;
  }

  // Determine batch size: produce up to preferred output batch rows.
  const auto max_batch_size = outputBatchRows();

  // Calculate how many values remain in the sequence.
  __int128 remaining;
  if (step_ > 0) {
    remaining = (static_cast<__int128>(stop_) - current_) / step_ + 1;
  } else {
    remaining = (static_cast<__int128>(current_) - stop_)
        / -static_cast<__int128>(step_) + 1;
  }

  if (remaining <= 0) {
    finished_ = true;
    return nullptr;
  }

  auto batch_size = static_cast<facebook::velox::vector_size_t>(
      std::min(static_cast<__int128>(max_batch_size), remaining));

  // Allocate output vector.
  auto result = std::dynamic_pointer_cast<facebook::velox::FlatVector<int64_t>>(
      facebook::velox::BaseVector::create(
          facebook::velox::BIGINT(), batch_size, operatorCtx_->pool()));

  // Fill the sequence values.
  auto* raw_values = result->mutableRawValues();
  for (facebook::velox::vector_size_t i = 0; i < batch_size; ++i) {
    raw_values[i] = current_;
    if (i + 1 == batch_size && remaining == batch_size) {
      finished_ = true;
    } else {
      // A further in-range value exists, so the narrowed result is safe.
      current_ = static_cast<int64_t>(static_cast<__int128>(current_) + step_);
    }
  }

  // Wrap in a RowVector with the output type from the plan node.
  auto output_type = std::dynamic_pointer_cast<const facebook::velox::RowType>(
      outputType_);
  return std::make_shared<facebook::velox::RowVector>(
      operatorCtx_->pool(),
      output_type,
      facebook::velox::BufferPtr(nullptr),  // no nulls
      batch_size,
      std::vector<facebook::velox::VectorPtr>{result});
}

bool GenerateSeriesOperator::isFinished() {
  return finished_;
}

facebook::velox::exec::BlockingReason GenerateSeriesOperator::isBlocked(
    facebook::velox::ContinueFuture* /*future*/) {
  return facebook::velox::exec::BlockingReason::kNotBlocked;
}

// --- PlanNodeTranslator ---

std::unique_ptr<facebook::velox::exec::Operator>
GenerateSeriesTranslator::toOperator(
    facebook::velox::exec::DriverCtx* ctx,
    int32_t id,
    const facebook::velox::core::PlanNodePtr& node) {
  auto table_func_node =
      std::dynamic_pointer_cast<const TableFunctionNode>(node);
  if (!table_func_node) {
    return nullptr;
  }
  if (table_func_node->functionName() != "generate_series") {
    return nullptr;
  }
  return std::make_unique<GenerateSeriesOperator>(
      id, ctx, table_func_node);
}

// --- Registration ---

void registerGenerateSeriesOperator() {
  facebook::velox::exec::Operator::registerOperator(
      std::make_unique<GenerateSeriesTranslator>());
}

}  // namespace pangrova::velox
