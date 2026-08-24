// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

// TableFunctionRegistry: Registration point for table-valued functions.
// Wraps Velox's internal table function registry and provides a mechanism
// for registering custom table functions at startup.
//
// Built-in functions (GENERATE_SERIES) are registered during
// NativeEngineLifecycle initialization. Custom functions can be registered
// via registerFunction() before query execution begins.
//
// @NotThreadSafe — Registration must complete during single-threaded
// initialization. isRegistered() is safe for concurrent read access after
// initialization completes.
//
// Placement: velox/runtime/ — Velox runtime lifecycle management.
#pragma once

#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

#include "velox/core/Expressions.h"
#include "velox/vector/ComplexVector.h"
#include "velox/common/memory/Memory.h"

namespace pangrova::velox {

// Registry for table-valued functions available to the native engine.
// Holds RowGenerator function objects that can produce output rows
// given an input row. Provides registration for built-in and custom
// table functions.
//
// @NotThreadSafe — Registration must complete during single-threaded
// initialization. resolve()/isRegistered() are safe for concurrent
// read access after initialization completes.
class TableFunctionRegistry {
 public:
  // A RowGenerator receives an input batch + row index, and appends
  // output values to the provided output column vectors.
  // Returns the number of output rows produced for this input row.
  using RowGenerator = std::function<int64_t(
      const facebook::velox::RowVectorPtr& input,
      facebook::velox::vector_size_t row,
      const std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>>& arguments,
      std::vector<int64_t>& outputValues,
      std::vector<facebook::velox::vector_size_t>& inputRowIndices,
      size_t maxOutput,
      facebook::velox::memory::MemoryPool* pool)>;

  // Register a table function with its generator. Throws std::runtime_error
  // if name is empty or already registered.
  void registerFunction(const std::string& name, RowGenerator generator);

  // Legacy overload for name-only registration (no generator).
  void registerFunction(const std::string& name);

  // Resolve a function name to its RowGenerator. Returns nullptr if not found.
  RowGenerator resolve(const std::string& name) const;

  // Check whether a function name is registered.
  bool isRegistered(const std::string& name) const;

  // Register all built-in table functions (e.g., GENERATE_SERIES).
  // Called once during NativeEngineLifecycle initialization.
  void registerBuiltins();

  // Number of registered table functions.
  size_t count() const;

 private:
  std::unordered_map<std::string, RowGenerator> registry_;
};

}  // namespace pangrova::velox
