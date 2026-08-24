// TableFunctionNode: Custom Velox PlanNode for table-valued function execution.
// Velox does not provide a built-in TableFunctionNode, so this module defines
// one that integrates with the Velox execution framework via UnnestNode-style
// row generation semantics.
//
// The node takes an optional input (for lateral/correlated invocations),
// a registered function name, argument expressions, and an output schema.
// Execution produces zero or more output rows per input row (or per invocation
// if no input is provided).
//
// Placement: velox/plan/ — Velox plan node extensions.
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "velox/core/Expressions.h"
#include "velox/core/PlanNode.h"

namespace pangrova::velox {

// Plan node representing a table-valued function invocation.
// Produces zero or more rows per invocation based on the registered function.
class TableFunctionNode : public facebook::velox::core::PlanNode {
 public:
  // Construct a TableFunctionNode with an input source (lateral join pattern).
  // The function executes once per input row, producing a cross-product of
  // input row columns and function output columns.
  TableFunctionNode(
      const facebook::velox::core::PlanNodeId& id,
      std::string function_name,
      std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>>
          arguments,
      facebook::velox::RowTypePtr output_type,
      std::shared_ptr<const facebook::velox::core::PlanNode> input)
      : PlanNode(id),
        function_name_(std::move(function_name)),
        arguments_(std::move(arguments)),
        output_type_(std::move(output_type)),
        sources_{std::move(input)} {}

  // Construct a TableFunctionNode without input (standalone invocation).
  TableFunctionNode(
      const facebook::velox::core::PlanNodeId& id,
      std::string function_name,
      std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>>
          arguments,
      facebook::velox::RowTypePtr output_type)
      : PlanNode(id),
        function_name_(std::move(function_name)),
        arguments_(std::move(arguments)),
        output_type_(std::move(output_type)),
        sources_{} {}

  const facebook::velox::RowTypePtr& outputType() const override {
    return output_type_;
  }

  const std::vector<std::shared_ptr<const facebook::velox::core::PlanNode>>&
  sources() const override {
    return sources_;
  }

  const std::string& functionName() const {
    return function_name_;
  }

  const std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>>&
  arguments() const {
    return arguments_;
  }

  std::string_view name() const override {
    return "TableFunction";
  }

 private:
  void addDetails(std::stringstream& stream) const override {
    stream << "function=" << function_name_
           << ", args=" << arguments_.size();
  }

  const std::string function_name_;
  const std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>>
      arguments_;
  const facebook::velox::RowTypePtr output_type_;
  const std::vector<std::shared_ptr<const facebook::velox::core::PlanNode>>
      sources_;
};

}  // namespace pangrova::velox
