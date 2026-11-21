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

struct PreparedScalarRegistration {
  std::string name;
  std::vector<facebook::velox::exec::FunctionSignaturePtr> signatures;
  facebook::velox::exec::VectorFunctionMetadata metadata;
  facebook::velox::exec::VectorFunctionFactory factory;
};

PreparedScalarRegistration prepareScalar(
    const std::string& name, const std::vector<ScalarUdfSpec>& overloads) {
  if (overloads.empty()) {
    throw std::runtime_error("Scalar overload set must not be empty");
  }
  PreparedScalarRegistration prepared;
  prepared.name = name;
  prepared.metadata = facebook::velox::exec::VectorFunctionMetadataBuilder()
      .deterministic(overloads.front().deterministic)
      .defaultNullBehavior(overloads.front().defaultNullBehavior).build();
  for (const auto& overload : overloads) {
    auto builder = facebook::velox::exec::FunctionSignatureBuilder();
    for (const auto& type : overload.inputTypes) {
      builder.argumentType(signatureType(type));
    }
    prepared.signatures.push_back(
        builder.returnType(signatureType(overload.outputType)).build());
  }
  if (!overloads.front().stateful && overloads.size() == 1) {
    std::shared_ptr<facebook::velox::exec::VectorFunction> function =
        overloads.front().vectorFunctionFactory("", {});
    if (!function) {
      throw std::runtime_error("Stateless UDF factory returned null: " + name);
    }
    prepared.factory = [function](const auto&, const auto&, const auto&) {
      return function;
    };
  } else {
    prepared.factory = [registered = overloads](
        const std::string& functionName,
        const std::vector<facebook::velox::exec::VectorFunctionArg>& arguments,
        const facebook::velox::core::QueryConfig&) {
      std::vector<TypePtr> actualTypes;
      for (const auto& argument : arguments) {
        actualTypes.push_back(argument.type);
      }
      const ScalarUdfSpec* match = nullptr;
      for (const auto& candidate : registered) {
        if (sameInputTypes(candidate.inputTypes, actualTypes)) {
          if (match) {
            throw std::runtime_error("Ambiguous UDF overload: " + functionName);
          }
          match = &candidate;
        }
      }
      if (!match) {
        throw std::runtime_error("No UDF overload matches bound argument types: " + functionName);
      }
      return match->vectorFunctionFactory(functionName, arguments);
    };
  }
  return prepared;
}

std::vector<facebook::velox::exec::AggregateFunctionSignaturePtr>
prepareAggregateSignatures(const AggregateUdfSpec& spec) {
  auto builder = facebook::velox::exec::AggregateFunctionSignatureBuilder();
  for (const auto& type : spec.inputTypes) {
    builder.argumentType(signatureType(type));
  }
  return {builder.intermediateType(signatureType(spec.intermediateType))
              .returnType(signatureType(spec.outputType)).build()};
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

  const auto name = facebook::velox::exec::sanitizeName(spec.name);
  if (aggregateUdfs_.contains(name)) {
    throw std::runtime_error(
        "UDF registration failed for '" + spec.name +
        "': scalar/aggregate names must not collide in the runtime");
  }
  auto& overloads = scalarUdfs_[name];
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
    if (existing.defaultNullBehavior != spec.defaultNullBehavior) {
      throw std::runtime_error(
          "UDF registration failed for '" + spec.name +
          "': overload NULL-input policy must be identical");
    }
    if (existing.deterministic != spec.deterministic) {
      throw std::runtime_error(
          "UDF registration failed for '" + spec.name +
          "': overload determinism must be identical");
    }
  }
  overloads.push_back(spec);
}

NativeUdfRegistry NativeUdfRegistry::withScalars(
    const std::vector<ScalarUdfSpec>& specs) const {
  if (finalized_) {
    throw std::runtime_error("UDF registry is finalized, registerAllWithVelox() already called");
  }
  if (specs.empty()) {
    throw std::invalid_argument("Scalar registration batch must not be empty");
  }
  auto candidate = *this;
  for (const auto& spec : specs) {
    candidate.registerScalar(spec);
  }
  return candidate;
}

void NativeUdfRegistry::registerAggregate(const AggregateUdfSpec& spec) {
  validateName(spec.name, "aggregate");
  validateTypes(spec.name, spec.inputTypes, spec.outputType);

  if (finalized_) {
    throw std::runtime_error(
        "UDF registration failed for '" + spec.name +
        "': registry is finalized, registerAllWithVelox() already called");
  }

  const auto name = facebook::velox::exec::sanitizeName(spec.name);
  if (scalarUdfs_.contains(name)) {
    throw std::runtime_error(
        "UDF registration failed for '" + spec.name +
        "': scalar/aggregate names must not collide in the runtime");
  }
  if (aggregateUdfs_.contains(name)) {
    throw std::runtime_error(
        "UDF registration failed for '" + spec.name +
        "': duplicate aggregate runtime name");
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

  aggregateUdfs_.emplace(name, spec);
}

void NativeUdfRegistry::registerAllWithVelox() {
  if (finalized_) {
    throw std::runtime_error("UDF registry is finalized, registerAllWithVelox() already called");
  }
  // Prepare every signature and eager factory before the first SDK mutation.
  // Deferred/stateful initialization still belongs to execution, as in Velox.
  std::vector<PreparedScalarRegistration> scalars;
  for (const auto& [name, overloads] : scalarUdfs_) {
    scalars.push_back(prepareScalar(name, overloads));
  }
  std::unordered_map<std::string,
      std::vector<facebook::velox::exec::AggregateFunctionSignaturePtr>> aggregates;
  for (const auto& [name, spec] : aggregateUdfs_) {
    aggregates.emplace(name, prepareAggregateSignatures(spec));
  }
  for (auto& scalar : scalars) {
    facebook::velox::exec::registerStatefulVectorFunction(
        scalar.name, std::move(scalar.signatures), std::move(scalar.factory),
        scalar.metadata, /*overwrite=*/true, registration_owner_);
  }
  for (auto& [name, signatures] : aggregates) {
    const auto& spec = aggregateUdfs_.at(name);
    facebook::velox::exec::registerAggregateFunction(
        name, std::move(signatures),
        [factory = spec.aggregateFactory](
            facebook::velox::core::AggregationNode::Step step,
            const std::vector<TypePtr>& arguments, const TypePtr& result,
            const facebook::velox::core::QueryConfig&) {
          return factory(step, arguments, result);
        },
        {}, /*registerCompanionFunctions=*/false, /*overwrite=*/true,
        registration_owner_);
  }
  finalized_ = true;
  std::cerr << "level=INFO event=udf_registration_complete"
            << " scalar_count=" << scalarUdfs_.size()
            << " aggregate_count=" << aggregateUdfs_.size() << std::endl;
}

std::optional<ScalarUdfSpec> NativeUdfRegistry::findScalar(
    const std::string& name) const {
  auto it = scalarUdfs_.find(facebook::velox::exec::sanitizeName(name));
  if (it == scalarUdfs_.end() || it->second.empty()) {
    return std::nullopt;
  }
  return it->second.front();
}

std::optional<AggregateUdfSpec> NativeUdfRegistry::findAggregate(
    const std::string& name) const {
  auto it = aggregateUdfs_.find(facebook::velox::exec::sanitizeName(name));
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

void NativeUdfRegistry::retireOwnedRegistrations() {
  facebook::velox::exec::unregisterOwnedFunctionEntries(registration_owner_);
}

}  // namespace pangrova::udf
