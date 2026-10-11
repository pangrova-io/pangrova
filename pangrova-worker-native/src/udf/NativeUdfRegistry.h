// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "velox/exec/Aggregate.h"
#include "velox/expression/VectorFunction.h"
#include "velox/type/Type.h"

namespace pangrova::udf {

enum class UdfType { SCALAR, AGGREGATE };

using TypePtr = facebook::velox::TypePtr;

/// Scalar UDF specification.
/// Type representation uses velox::TypePtr for direct Velox integration.
struct ScalarUdfSpec {
  std::string name;
  std::vector<TypePtr> inputTypes;
  TypePtr outputType;

  /// If true, registers via registerStatefulVectorFunction (stateful).
  /// If false, registers via registerVectorFunction (stateless).
  bool stateful = false;

  /// Whether identical arguments always produce identical results.
  /// Non-deterministic callbacks must not participate in constant folding.
  bool deterministic = true;

  /// Pure functions use the strict NULL-input default of the runtime.
  /// Declared callbacks explicitly select whether NULL reaches their implementation.
  bool defaultNullBehavior = facebook::velox::exec::VectorFunctionMetadata{}.defaultNullBehavior;

  /// Factory for stateless vector functions.
  std::function<std::unique_ptr<facebook::velox::exec::VectorFunction>(
      const std::string& name,
      const std::vector<facebook::velox::exec::VectorFunctionArg>& inputArgs)>
      vectorFunctionFactory;
};

/// Aggregate UDF specification.
/// Type representation uses velox::TypePtr for direct Velox integration.
struct AggregateUdfSpec {
  std::string name;
  std::vector<TypePtr> inputTypes;
  TypePtr intermediateType;
  TypePtr outputType;

  /// Factory for Velox Aggregate instances.
  std::function<std::unique_ptr<facebook::velox::exec::Aggregate>(
      facebook::velox::core::AggregationNode::Step step,
      const std::vector<TypePtr>& argTypes,
      const TypePtr& resultType)>
      aggregateFactory;
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

  /// Validates an entire registration batch against the current lifecycle and
  /// overload set without changing either registry or invoking any factory.
  NativeUdfRegistry withScalars(const std::vector<ScalarUdfSpec>& specs) const;

  /// Register an aggregate UDF. Throws std::runtime_error on invalid spec.
  /// Error message includes UDF name and failure reason.
  void registerAggregate(const AggregateUdfSpec& spec);

  /// Register all UDFs with Velox runtime Function Registry.
  /// Scalar UDFs are registered via registerVectorFunction() or
  /// registerStatefulVectorFunction(). Aggregate UDFs are registered
  /// via registerAggregateFunction().
  /// Throws std::runtime_error if any registration fails.
  void registerAllWithVelox();

  /// After real callback/execution drain, retire only this runtime's actual
  /// SDK entries. A later unrelated registration of the same name survives.
  void retireOwnedRegistrations();

  /// Lookup a scalar UDF by name.
  std::optional<ScalarUdfSpec> findScalar(const std::string& name) const;

  /// Lookup an aggregate UDF by name.
  std::optional<AggregateUdfSpec> findAggregate(const std::string& name) const;

  /// Number of registered scalar UDFs.
  size_t scalarCount() const;

  /// Number of registered aggregate UDFs.
  size_t aggregateCount() const;

 private:
  // Keys follow the actual SDK sanitizeName identity; specs retain callback names.
  std::unordered_map<std::string, std::vector<ScalarUdfSpec>> scalarUdfs_;
  std::unordered_map<std::string, AggregateUdfSpec> aggregateUdfs_;
  // P2-UDF-2: Guard against registration after finalization.
  // Set to true after registerAllWithVelox() completes.
  bool finalized_ = false;
  std::shared_ptr<const facebook::velox::exec::FunctionRegistrationOwner> registration_owner_ =
      std::make_shared<const facebook::velox::exec::FunctionRegistrationOwner>();
};

}  // namespace pangrova::udf
