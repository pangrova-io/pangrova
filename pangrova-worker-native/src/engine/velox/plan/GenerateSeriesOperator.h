// GenerateSeriesOperator: Velox source operator for GENERATE_SERIES table function.
//
// Produces an integer sequence based on (start, stop, step) parameters:
//   - GENERATE_SERIES(start, stop): step defaults to 1
//   - GENERATE_SERIES(start, stop, step): explicit step
//
// Sequence semantics:
//   - step > 0: all values v where start <= v <= stop
//   - step < 0: all values v where stop <= v <= start
//   - step == 0: throws user error
//   - Empty result when step > 0 and start > stop, or step < 0 and start < stop
//
// Integrates with Velox execution framework via Operator::registerOperator()
// and PlanNodeTranslator. The operator is a source (no input) that produces
// batches of output rows containing the generated sequence.
//
// Placement: velox/plan/ — Velox plan node and operator extensions.
#pragma once

#include <cstdint>
#include <memory>

#include "engine/velox/plan/TableFunctionNode.h"

#include "velox/exec/Operator.h"

namespace pangrova::velox {

// Source operator that generates an integer sequence for GENERATE_SERIES.
// Produces output in batches (default batch size from Velox preferred output).
class GenerateSeriesOperator : public facebook::velox::exec::Operator {
 public:
  GenerateSeriesOperator(
      int32_t operatorId,
      facebook::velox::exec::DriverCtx* driverCtx,
      const std::shared_ptr<const TableFunctionNode>& node);

  bool needsInput() const override {
    return false;
  }

  void addInput(facebook::velox::RowVectorPtr /*input*/) override {}

  facebook::velox::RowVectorPtr getOutput() override;

  bool isFinished() override;

  facebook::velox::exec::BlockingReason isBlocked(
      facebook::velox::ContinueFuture* future) override;

 private:
  // Sequence parameters extracted from the plan node arguments.
  int64_t start_;
  int64_t stop_;
  int64_t step_;

  // Current position in the sequence.
  int64_t current_;

  // Whether the sequence is exhausted.
  bool finished_;
};

// PlanNodeTranslator that maps TableFunctionNode("generate_series")
// to GenerateSeriesOperator.
class GenerateSeriesTranslator
    : public facebook::velox::exec::Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<facebook::velox::exec::Operator> toOperator(
      facebook::velox::exec::DriverCtx* ctx,
      int32_t id,
      const facebook::velox::core::PlanNodePtr& node) override;
};

// Register the GenerateSeriesTranslator with Velox's operator registry.
// Must be called once during initialization (NativeEngineLifecycle).
void registerGenerateSeriesOperator();

}  // namespace pangrova::velox
