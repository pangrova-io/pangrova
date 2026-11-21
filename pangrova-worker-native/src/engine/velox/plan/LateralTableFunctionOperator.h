// LateralTableFunctionOperator: Velox operator for lateral/correlated table
// function invocations.
//
// Accepts input rows from the left side of a lateral join and executes the
// table function once per input row. Produces the cross-product of each input
// row with the function's output rows for that invocation.
//
// Output schema: [input_col_0, ..., input_col_N, func_out_0, ..., func_out_M]
//
// Currently supports GENERATE_SERIES with correlated arguments (field
// references to input columns resolved per row).
//
// Placement: velox/plan/ — Velox plan node and operator extensions.
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

#include "engine/velox/plan/TableFunctionNode.h"
#include "engine/velox/runtime/TableFunctionRegistry.h"

#include "velox/exec/Operator.h"

namespace pangrova::velox {

// Operator that executes a table function once per input row (lateral join).
// Produces output rows combining input columns with function output columns.
class LateralTableFunctionOperator : public facebook::velox::exec::Operator {
 public:
  LateralTableFunctionOperator(
      int32_t operatorId,
      facebook::velox::exec::DriverCtx* driverCtx,
      const std::shared_ptr<const TableFunctionNode>& node);

  bool needsInput() const override {
    return !noMoreInput_ && !input_;
  }

  void addInput(facebook::velox::RowVectorPtr input) override;

  facebook::velox::RowVectorPtr getOutput() override;

  void noMoreInput() override {
    Operator::noMoreInput();
  }

  bool isFinished() override;

  facebook::velox::exec::BlockingReason isBlocked(
      facebook::velox::ContinueFuture* future) override;

 private:
  // The function name.
  const std::string function_name_;

  // Argument expressions (may be constants or field references to input).
  const std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>>
      arguments_;

  // Number of columns from the input (left side of lateral join).
  const size_t input_column_count_;

  // Resolved RowGenerator from the registry.
  TableFunctionRegistry::RowGenerator row_generator_;

  // Current input batch being processed.
  facebook::velox::RowVectorPtr input_;

  // Current row index within the input batch.
  facebook::velox::vector_size_t current_input_row_{0};

  // Next series value when one input row spans more than one output batch.
  std::optional<int64_t> continuation_;
};

// PlanNodeTranslator that maps TableFunctionNode with input sources
// (lateral join pattern) to LateralTableFunctionOperator.
class LateralTableFunctionTranslator
    : public facebook::velox::exec::Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<facebook::velox::exec::Operator> toOperator(
      facebook::velox::exec::DriverCtx* ctx,
      int32_t id,
      const facebook::velox::core::PlanNodePtr& node) override;
};

// Register the LateralTableFunctionTranslator with Velox's operator registry.
// Must be called once during initialization (NativeEngineLifecycle).
void registerLateralTableFunctionOperator();

}  // namespace pangrova::velox
