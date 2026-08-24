// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include "udf/NativeUdfRegistry.h"

#include <iostream>
#include <stdexcept>

// Velox function registration APIs — guarded for graceful degradation.
#if __has_include("velox/functions/FunctionRegistry.h")
#include "velox/functions/FunctionRegistry.h"
#define PANGROVA_HAS_VELOX_FUNC_REGISTRY 1
#else
#define PANGROVA_HAS_VELOX_FUNC_REGISTRY 0
#endif

namespace pangrova::udf {

namespace {

void validateName(const std::string& name, const std::string& udfKind) {
  if (name.empty()) {
    throw std::invalid_argument(
        "UDF registration failed: " + udfKind +
        " UDF name must not be empty");
  }
}

void validateTypes(
    const std::string& udfName,
    const std::vector<TypePtr>& inputTypes,
    const TypePtr& outputType) {
  if (inputTypes.empty()) {
    throw std::runtime_error(
        "UDF registration failed for '" + udfName +
        "': input types must not be empty");
  }
  for (size_t i = 0; i < inputTypes.size(); ++i) {
    if (!inputTypes[i]) {
      throw std::runtime_error(
          "UDF registration failed for '" + udfName +
          "': input type at index " + std::to_string(i) + " is null");
    }
  }
  if (!outputType) {
    throw std::runtime_error(
        "UDF registration failed for '" + udfName +
        "': output type is null");
  }
}

}  // namespace

void NativeUdfRegistry::registerScalar(const ScalarUdfSpec& spec) {
  validateName(spec.name, "scalar");
  validateTypes(spec.name, spec.inputTypes, spec.outputType);

  // P2-UDF-2: Reject registration after finalization.
  if (finalized_) {
    throw std::runtime_error(
        "UDF registration failed for '" + spec.name +
        "': registry is finalized, registerAllWithVelox() already called");
  }

#if PANGROVA_HAS_VELOX_VECTOR_FUNC
  if (!spec.vectorFunctionFactory) {
    throw std::runtime_error(
        "UDF registration failed for '" + spec.name +
        "': vectorFunctionFactory must not be null");
  }
#endif

  scalarUdfs_[spec.name] = spec;
}

void NativeUdfRegistry::registerAggregate(const AggregateUdfSpec& spec) {
  validateName(spec.name, "aggregate");
  validateTypes(spec.name, spec.inputTypes, spec.outputType);

  // P2-UDF-2: Reject registration after finalization.
  if (finalized_) {
    throw std::runtime_error(
        "UDF registration failed for '" + spec.name +
        "': registry is finalized, registerAllWithVelox() already called");
  }

  if (!spec.intermediateType) {
    throw std::runtime_error(
        "UDF registration failed for '" + spec.name +
        "': intermediate type is null");
  }

#if PANGROVA_HAS_VELOX_AGGREGATE
  if (!spec.aggregateFactory) {
    throw std::runtime_error(
        "UDF registration failed for '" + spec.name +
        "': aggregateFactory must not be null");
  }
#endif

  aggregateUdfs_[spec.name] = spec;
}

void NativeUdfRegistry::registerAllWithVelox() {
#if PANGROVA_HAS_VELOX_VECTOR_FUNC
  for (const auto& [name, spec] : scalarUdfs_) {
    try {
      // Build the function signature from spec types.
      std::vector<facebook::velox::exec::FunctionSignaturePtr> signatures;
      {
        auto builder = facebook::velox::exec::FunctionSignatureBuilder();
        for (size_t i = 0; i < spec.inputTypes.size(); ++i) {
          builder.argumentType(spec.inputTypes[i]->toString());
        }
        builder.returnType(spec.outputType->toString());
        signatures.push_back(builder.build());
      }

      if (spec.stateful) {
        // Stateful scalar UDF — registerStatefulVectorFunction.
        facebook::velox::exec::registerStatefulVectorFunction(
            name,
            signatures,
            [factory = spec.vectorFunctionFactory](
                const std::string& funcName,
                const std::vector<facebook::velox::exec::VectorFunctionArg>&
                    inputArgs,
                const facebook::velox::core::QueryConfig& /*config*/)
                -> std::unique_ptr<facebook::velox::exec::VectorFunction> {
              return factory(funcName, inputArgs);
            });
      } else {
        // P1-UDF-1: Stateless scalar UDF — use registerVectorFunction()
        // which avoids per-batch factory overhead for functions that don't
        // need QueryConfig or session state.
        facebook::velox::exec::registerVectorFunction(
            name,
            signatures,
            spec.vectorFunctionFactory("", {}));
      }

      std::cerr << "level=INFO event=udf_registered"
                << " type=scalar"
                << " name=" << name
                << " stateful=" << (spec.stateful ? "true" : "false")
                << std::endl;
    } catch (const std::exception& e) {
      throw std::runtime_error(
          "UDF registration failed for scalar '" + name + "': " + e.what());
    }
  }
#endif

#if PANGROVA_HAS_VELOX_AGGREGATE
  for (const auto& [name, spec] : aggregateUdfs_) {
    try {
      // Build the aggregate function signature.
      std::vector<facebook::velox::exec::AggregateFunctionSignaturePtr>
          signatures;
      {
        auto builder =
            facebook::velox::exec::AggregateFunctionSignatureBuilder();
        for (size_t i = 0; i < spec.inputTypes.size(); ++i) {
          builder.argumentType(spec.inputTypes[i]->toString());
        }
        builder.intermediateType(spec.intermediateType->toString());
        builder.returnType(spec.outputType->toString());
        signatures.push_back(builder.build());
      }

      facebook::velox::exec::registerAggregateFunction(
          name,
          std::move(signatures),
          [factory = spec.aggregateFactory](
              facebook::velox::core::AggregationNode::Step step,
              const std::vector<TypePtr>& argTypes,
              const TypePtr& resultType,
              const facebook::velox::core::QueryConfig& /*config*/)
              -> std::unique_ptr<facebook::velox::exec::Aggregate> {
            return factory(step, argTypes, resultType);
          },
          /*registerCompanionFunctions=*/false,
          /*overwrite=*/true);

      std::cerr << "level=INFO event=udf_registered"
                << " type=aggregate"
                << " name=" << name
                << std::endl;
    } catch (const std::exception& e) {
      throw std::runtime_error(
          "UDF registration failed for aggregate '" + name + "': " +
          e.what());
    }
  }
#endif

  std::cerr << "level=INFO event=udf_registration_complete"
            << " scalar_count=" << scalarUdfs_.size()
            << " aggregate_count=" << aggregateUdfs_.size()
            << std::endl;

  // P2-UDF-2: Mark registry as finalized — no further registrations allowed.
  finalized_ = true;
}

std::optional<ScalarUdfSpec> NativeUdfRegistry::findScalar(
    const std::string& name) const {
  auto it = scalarUdfs_.find(name);
  if (it == scalarUdfs_.end()) {
    return std::nullopt;
  }
  return it->second;
}

std::optional<AggregateUdfSpec> NativeUdfRegistry::findAggregate(
    const std::string& name) const {
  auto it = aggregateUdfs_.find(name);
  if (it == aggregateUdfs_.end()) {
    return std::nullopt;
  }
  return it->second;
}

size_t NativeUdfRegistry::scalarCount() const {
  return scalarUdfs_.size();
}

size_t NativeUdfRegistry::aggregateCount() const {
  return aggregateUdfs_.size();
}

}  // namespace pangrova::udf
