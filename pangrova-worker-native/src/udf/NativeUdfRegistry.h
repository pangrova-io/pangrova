// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

// Velox type system — guarded for graceful degradation.
#if __has_include("velox/type/Type.h")
#include "velox/type/Type.h"
#define PANGROVA_HAS_VELOX_TYPE 1
#else
#define PANGROVA_HAS_VELOX_TYPE 0
#endif

// Velox vector function registration API.
#if __has_include("velox/expression/VectorFunction.h")
#include "velox/expression/VectorFunction.h"
#define PANGROVA_HAS_VELOX_VECTOR_FUNC 1
#else
#define PANGROVA_HAS_VELOX_VECTOR_FUNC 0
#endif

// Velox aggregate function registration API.
#if __has_include("velox/exec/Aggregate.h")
#include "velox/exec/Aggregate.h"
#define PANGROVA_HAS_VELOX_AGGREGATE 1
#else
#define PANGROVA_HAS_VELOX_AGGREGATE 0
#endif

namespace pangrova::udf {

enum class UdfType { SCALAR, AGGREGATE };

#if PANGROVA_HAS_VELOX_TYPE
using TypePtr = facebook::velox::TypePtr;
#else
// Fallback: opaque placeholder when Velox headers are unavailable.
using TypePtr = std::shared_ptr<void>;
#endif

/// Scalar UDF specification.
/// Type representation uses velox::TypePtr for direct Velox integration.
struct ScalarUdfSpec {
  std::string name;
  std::vector<TypePtr> inputTypes;
  TypePtr outputType;

  /// If true, registers via registerStatefulVectorFunction (stateful).
  /// If false, registers via registerVectorFunction (stateless).
  bool stateful = false;

#if PANGROVA_HAS_VELOX_VECTOR_FUNC
  /// Factory for stateless vector functions.
  std::function<std::unique_ptr<facebook::velox::exec::VectorFunction>(
      const std::string& name,
      const std::vector<facebook::velox::exec::VectorFunctionArg>& inputArgs)>
      vectorFunctionFactory;
#endif
};

/// Aggregate UDF specification.
/// Type representation uses velox::TypePtr for direct Velox integration.
struct AggregateUdfSpec {
  std::string name;
  std::vector<TypePtr> inputTypes;
  TypePtr intermediateType;
  TypePtr outputType;

#if PANGROVA_HAS_VELOX_AGGREGATE
  /// Factory for Velox Aggregate instances.
  std::function<std::unique_ptr<facebook::velox::exec::Aggregate>(
      facebook::velox::core::AggregationNode::Step step,
      const std::vector<TypePtr>& argTypes,
      const TypePtr& resultType)>
      aggregateFactory;
#endif
};

/// Registry for native UDFs (scalar and aggregate).
/// Validates specs at registration time and registers directly with
/// Velox Function Registry for Substrait Plan resolution.
///
/// @NotThreadSafe — Registration must complete during single-threaded
/// initialization (e.g., in initializeVeloxRuntime) before any concurrent
/// lookup calls from execution threads.
class NativeUdfRegistry {
 public:
  /// Register a scalar UDF. Throws std::runtime_error on invalid spec.
  /// Error message includes UDF name and failure reason.
  void registerScalar(const ScalarUdfSpec& spec);

  /// Register an aggregate UDF. Throws std::runtime_error on invalid spec.
  /// Error message includes UDF name and failure reason.
  void registerAggregate(const AggregateUdfSpec& spec);

  /// Register all UDFs with Velox runtime Function Registry.
  /// Scalar UDFs are registered via registerVectorFunction() or
  /// registerStatefulVectorFunction(). Aggregate UDFs are registered
  /// via registerAggregateFunction().
  /// Throws std::runtime_error if any registration fails.
  void registerAllWithVelox();

  /// Lookup a scalar UDF by name.
  std::optional<ScalarUdfSpec> findScalar(const std::string& name) const;

  /// Lookup an aggregate UDF by name.
  std::optional<AggregateUdfSpec> findAggregate(const std::string& name) const;

  /// Number of registered scalar UDFs.
  size_t scalarCount() const;

  /// Number of registered aggregate UDFs.
  size_t aggregateCount() const;

 private:
  std::unordered_map<std::string, ScalarUdfSpec> scalarUdfs_;
  std::unordered_map<std::string, AggregateUdfSpec> aggregateUdfs_;
  // P2-UDF-2: Guard against registration after finalization.
  // Set to true after registerAllWithVelox() completes.
  bool finalized_ = false;
};

}  // namespace pangrova::udf
