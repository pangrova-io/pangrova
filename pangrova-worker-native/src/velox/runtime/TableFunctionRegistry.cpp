// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include "velox/runtime/TableFunctionRegistry.h"

#include <iostream>
#include <stdexcept>

#include "velox/common/base/Exceptions.h"
#include "velox/core/Expressions.h"
#include "velox/vector/FlatVector.h"

namespace pangrova::velox {

namespace {

// Evaluate a constant or field-reference expression to int64.
int64_t evaluateIntArgument(
    const std::shared_ptr<const facebook::velox::core::ITypedExpr>& expr,
    const facebook::velox::RowVectorPtr& input,
    facebook::velox::vector_size_t row,
    const std::string& arg_name) {
  auto constant_expr =
      std::dynamic_pointer_cast<const facebook::velox::core::ConstantTypedExpr>(
          expr);
  if (constant_expr) {
    VELOX_USER_CHECK(
        !constant_expr->value().isNull(),
        "GENERATE_SERIES: argument '{}' must not be NULL", arg_name);
    if (constant_expr->type()->kind() == facebook::velox::TypeKind::BIGINT) {
      return constant_expr->value().template value<int64_t>();
    }
    if (constant_expr->type()->kind() == facebook::velox::TypeKind::INTEGER) {
      return static_cast<int64_t>(
          constant_expr->value().template value<int32_t>());
    }
    VELOX_USER_FAIL(
        "GENERATE_SERIES: argument '{}' must be an integer type, got {}",
        arg_name, constant_expr->type()->toString());
  }

  auto field_expr = std::dynamic_pointer_cast<
      const facebook::velox::core::FieldAccessTypedExpr>(expr);
  VELOX_USER_CHECK(
      field_expr != nullptr,
      "GENERATE_SERIES: argument '{}' must be a constant or field reference",
      arg_name);

  auto input_type = std::dynamic_pointer_cast<const facebook::velox::RowType>(
      input->type());
  auto field_index = input_type->getChildIdx(field_expr->name());
  auto child_vector = input->childAt(field_index);

  VELOX_USER_CHECK(
      !child_vector->isNullAt(row),
      "GENERATE_SERIES: argument '{}' must not be NULL at row {}",
      arg_name, row);

  if (child_vector->typeKind() == facebook::velox::TypeKind::BIGINT) {
    return child_vector->as<facebook::velox::FlatVector<int64_t>>()->valueAt(row);
  }
  if (child_vector->typeKind() == facebook::velox::TypeKind::INTEGER) {
    return static_cast<int64_t>(
        child_vector->as<facebook::velox::FlatVector<int32_t>>()->valueAt(row));
  }
  VELOX_USER_FAIL(
      "GENERATE_SERIES: argument '{}' must be an integer type, got {}",
      arg_name, child_vector->type()->toString());
}

// Built-in generate_series RowGenerator.
int64_t generateSeriesGenerator(
    const facebook::velox::RowVectorPtr& input,
    facebook::velox::vector_size_t row,
    const std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>>& arguments,
    std::vector<int64_t>& outputValues,
    std::vector<facebook::velox::vector_size_t>& inputRowIndices,
    size_t maxOutput,
    facebook::velox::memory::MemoryPool* /*pool*/) {
  VELOX_USER_CHECK(
      arguments.size() == 2 || arguments.size() == 3,
      "GENERATE_SERIES requires 2 or 3 arguments (start, stop[, step]), got {}",
      arguments.size());

  int64_t start = evaluateIntArgument(arguments[0], input, row, "start");
  int64_t stop = evaluateIntArgument(arguments[1], input, row, "stop");
  int64_t step = (arguments.size() == 3)
      ? evaluateIntArgument(arguments[2], input, row, "step")
      : 1;
  VELOX_USER_CHECK_NE(step, 0, "GENERATE_SERIES: step must not be zero");

  bool empty_series = (step > 0 && start > stop) || (step < 0 && start < stop);
  if (empty_series) {
    return 0;
  }

  int64_t count = 0;
  for (int64_t v = start;
       (step > 0 ? v <= stop : v >= stop) && outputValues.size() < maxOutput;) {
    outputValues.push_back(v);
    inputRowIndices.push_back(row);
    ++count;
    const auto next = static_cast<__int128>(v) + step;
    if (step > 0 ? next > stop : next < stop) {
      break;
    }
    v = static_cast<int64_t>(next);
  }
  return count;
}

}  // namespace

void TableFunctionRegistry::registerFunction(
    const std::string& name, RowGenerator generator) {
  if (name.empty()) {
    throw std::runtime_error(
        "Table function registration failed: function name must not be empty");
  }
  auto [it, inserted] = registry_.emplace(name, std::move(generator));
  if (!inserted) {
    throw std::runtime_error(
        "Table function registration failed: '" + name +
        "' is already registered");
  }
  std::cerr << "level=INFO event=table_function_registered"
            << " name=" << name << std::endl;
}

void TableFunctionRegistry::registerFunction(const std::string& name) {
  // Legacy name-only registration with no-op generator.
  registerFunction(name, nullptr);
}

TableFunctionRegistry::RowGenerator TableFunctionRegistry::resolve(
    const std::string& name) const {
  auto it = registry_.find(name);
  if (it == registry_.end()) {
    return nullptr;
  }
  return it->second;
}

bool TableFunctionRegistry::isRegistered(const std::string& name) const {
  return registry_.count(name) > 0;
}

void TableFunctionRegistry::registerBuiltins() {
  registerFunction("generate_series", generateSeriesGenerator);

  std::cerr << "level=INFO event=table_function_builtins_registered"
            << " count=" << registry_.size() << std::endl;
}

size_t TableFunctionRegistry::count() const {
  return registry_.size();
}

}  // namespace pangrova::velox
