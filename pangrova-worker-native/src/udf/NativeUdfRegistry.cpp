// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include "udf/NativeUdfRegistry.h"

#include <iostream>
#include <stdexcept>

#include "velox/functions/FunctionRegistry.h"

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

// Velox Type::toString() is a display format (for example, ROW<a:VARCHAR>
// and ARRAY<REAL>), not the FunctionSignatureBuilder grammar. Build the
// positional signature from the type tree so FILE/ROW and VECTOR/ARRAY UDFs
// can be registered without dropping their concrete argument types.
std::string signatureType(const TypePtr& type) {
  switch (type->kind()) {
    case facebook::velox::TypeKind::ARRAY:
      return "array(" + signatureType(type->childAt(0)) + ")";
    case facebook::velox::TypeKind::MAP:
      return "map(" + signatureType(type->childAt(0)) + "," +
          signatureType(type->childAt(1)) + ")";
    case facebook::velox::TypeKind::ROW: {
      std::string result = "row(";
      for (uint32_t i = 0; i < type->size(); ++i) {
        if (i != 0) {
          result += ",";
        }
        result += signatureType(type->childAt(i));
      }
      return result + ")";
    }
    default:
      return type->toString();
  }
}

bool sameInputTypes(
    const std::vector<TypePtr>& expected,
    const std::vector<TypePtr>& actual) {
  if (expected.size() != actual.size()) {
    return false;
  }
  for (size_t i = 0; i < expected.size(); ++i) {
    if (!expected[i] || !actual[i] ||
        !expected[i]->equivalent(*actual[i])) {
      return false;
    }
  }
  return true;
}

}  // namespace

void NativeUdfRegistry::registerScalar(const ScalarUdfSpec& spec) {
  validateName(spec.name, "scalar");
  validateTypes(spec.name, spec.inputTypes, spec.outputType);

  if (finalized_) {
    throw std::runtime_error(
        "UDF registration failed for '" + spec.name +
        "': registry is finalized, registerAllWithVelox() already called");
  }

  if (!spec.vectorFunctionFactory) {
    throw std::runtime_error(
        "UDF registration failed for '" + spec.name +
        "': vectorFunctionFactory must not be null");
  }

  auto& overloads = scalarUdfs_[spec.name];
  for (const auto& existing : overloads) {
    if (sameInputTypes(existing.inputTypes, spec.inputTypes)) {
      throw std::runtime_error(
          "UDF registration failed for '" + spec.name +
          "': duplicate scalar signature");
    }
    if (!existing.outputType->equivalent(*spec.outputType)) {
      throw std::runtime_error(
          "UDF registration failed for '" + spec.name +
          "': overload return types must be identical");
    }
    if (existing.stateful != spec.stateful) {
      throw std::runtime_error(
          "UDF registration failed for '" + spec.name +
          "': overload statefulness must be identical");
    }
  }
  overloads.push_back(spec);
}

void NativeUdfRegistry::registerAggregate(const AggregateUdfSpec& spec) {
  validateName(spec.name, "aggregate");
  validateTypes(spec.name, spec.inputTypes, spec.outputType);

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

  if (!spec.aggregateFactory) {
    throw std::runtime_error(
        "UDF registration failed for '" + spec.name +
        "': aggregateFactory must not be null");
  }

  aggregateUdfs_[spec.name] = spec;
}

void NativeUdfRegistry::registerAllWithVelox() {
  for (const auto& [name, overloads] : scalarUdfs_) {
    try {
      if (overloads.empty()) {
        throw std::runtime_error("scalar overload set must not be empty");
      }
      const auto& spec = overloads.front();
      // Build the function signature from spec types.
      std::vector<facebook::velox::exec::FunctionSignaturePtr> signatures;
      for (const auto& overload : overloads) {
        auto builder = facebook::velox::exec::FunctionSignatureBuilder();
        for (const auto& inputType : overload.inputTypes) {
          builder.argumentType(signatureType(inputType));
        }
        builder.returnType(signatureType(overload.outputType));
        signatures.push_back(builder.build());
      }

      if (spec.stateful || overloads.size() > 1) {
        // Overloads may have distinct factories. The Velox registry accepts
        // one factory per name, so resolve the exact overload from the bound
        // argument types instead of silently using the first implementation.
        facebook::velox::exec::registerStatefulVectorFunction(
            name,
            signatures,
            [registered = overloads](
                const std::string& funcName,
                const std::vector<facebook::velox::exec::VectorFunctionArg>&
                    inputArgs,
                const facebook::velox::core::QueryConfig& /*config*/)
                -> std::unique_ptr<facebook::velox::exec::VectorFunction> {
              std::vector<TypePtr> actualTypes;
              actualTypes.reserve(inputArgs.size());
              for (const auto& arg : inputArgs) {
                actualTypes.push_back(arg.type);
              }
              const ScalarUdfSpec* match = nullptr;
              for (const auto& candidate : registered) {
                if (sameInputTypes(candidate.inputTypes, actualTypes)) {
                  if (match != nullptr) {
                    throw std::runtime_error(
                        "Ambiguous UDF overload: " + funcName);
                  }
                  match = &candidate;
                }
              }
              if (match == nullptr) {
                throw std::runtime_error(
                    "No UDF overload matches bound argument types: " +
                    funcName);
              }
              return match->vectorFunctionFactory(funcName, inputArgs);
            });
      } else {
        // Stateless scalar functions avoid per-batch factory overhead.
        facebook::velox::exec::registerVectorFunction(
            name,
            signatures,
            spec.vectorFunctionFactory("", {}));
      }

      std::cerr << "level=INFO event=udf_registered"
                << " type=scalar"
                << " name=" << name
                << " overloads=" << overloads.size()
                << " stateful=" << (spec.stateful ? "true" : "false")
                << std::endl;
    } catch (const std::exception& e) {
      throw std::runtime_error(
          "UDF registration failed for scalar '" + name + "': " + e.what());
    }
  }

  for (const auto& [name, spec] : aggregateUdfs_) {
    try {
      // Build the aggregate function signature.
      std::vector<facebook::velox::exec::AggregateFunctionSignaturePtr>
          signatures;
      {
        auto builder =
            facebook::velox::exec::AggregateFunctionSignatureBuilder();
        for (size_t i = 0; i < spec.inputTypes.size(); ++i) {
          builder.argumentType(signatureType(spec.inputTypes[i]));
        }
        builder.intermediateType(signatureType(spec.intermediateType));
        builder.returnType(signatureType(spec.outputType));
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

  std::cerr << "level=INFO event=udf_registration_complete"
            << " scalar_count=" << scalarUdfs_.size()
            << " aggregate_count=" << aggregateUdfs_.size()
            << std::endl;

  finalized_ = true;
}

std::optional<ScalarUdfSpec> NativeUdfRegistry::findScalar(
    const std::string& name) const {
  auto it = scalarUdfs_.find(name);
  if (it == scalarUdfs_.end() || it->second.empty()) {
    return std::nullopt;
  }
  return it->second.front();
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
