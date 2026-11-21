#include "capabilities/capability/RegisteredFunctionSignature.h"

#include "capabilities/capability/FunctionTypeConstraintCompiler.h"

#include <algorithm>
#include <charconv>
#include <map>
#include <stdexcept>
#include <unordered_set>

namespace pangrova::capability {
namespace {
using namespace protocol::v2;
using Signature = facebook::velox::exec::FunctionSignature;
using TypeSignature = facebook::velox::exec::TypeSignature;

void variable(FunctionTypeVariable* target, const facebook::velox::exec::SignatureVariable& source,
              const std::unordered_set<std::string>& integers) {
  target->set_name(source.name());
  if (source.isTypeParameter()) {
    if (!source.constraint().empty()) throw std::invalid_argument("Registered type constraint is not a typed variable contract");
    target->set_kind(FUNCTION_VARIABLE_KIND_TYPE);
    target->set_known_types_only(source.knownTypesOnly());
    target->set_orderable_types_only(source.orderableTypesOnly());
    target->set_comparable_types_only(source.comparableTypesOnly());
  } else if (source.isIntegerParameter()) {
    target->set_kind(FUNCTION_VARIABLE_KIND_INTEGER);
    const auto& text = source.constraint();
    if (text.empty()) return;
    *target->mutable_formula() = compileRegisteredTypeConstraint(text, integers);
    if (std::all_of(text.begin(), text.end(), [](char value) { return value >= '0' && value <= '9'; })) {
      int32_t value;
      const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
      if (error != std::errc{} || end != text.data() + text.size()) {
        throw std::invalid_argument("Registered input integer constraint exceeds exact signed int32");
      }
      target->set_input_integer_constant(value);
    }
  } else {
    target->set_kind(FUNCTION_VARIABLE_KIND_ENUM);
    target->set_enum_constraint(source.constraint());
  }
}

void parameterOrder(const TypeSignature& type, const Signature& signature,
                    FunctionSignatureContract& target, int depth, int& remaining) {
  if (depth > 64 || --remaining < 0) throw std::invalid_argument("Registered parameter order exceeds its AST bound");
  for (const auto& parameter : type.parameters()) {
    const auto found = signature.variables().find(parameter.baseName());
    if (found != signature.variables().end() && found->second.isIntegerParameter()) {
      target.add_derived_parameter_order(parameter.baseName());
    } else {
      parameterOrder(parameter, signature, target, depth + 1, remaining);
    }
  }
}
}  // namespace

protocol::v2::FunctionSignatureContract projectRegisteredSignature(
    const facebook::velox::exec::FunctionSignature& signature,
    const RegisteredTypeTemplate& types) {
  if (signature.argumentTypes().size() > 256 || signature.variables().size() > 256
      || signature.constantArguments().size() != signature.argumentTypes().size()
      || (signature.variableArity() && signature.argumentTypes().empty())) {
    throw std::invalid_argument("Registered function signature exceeds its exact arity bound");
  }
  FunctionSignatureContract result;
  result.set_variable_arity(signature.variableArity());
  for (const auto& argument : signature.argumentTypes()) *result.add_arguments() = types.convert(argument, signature);
  for (const auto constant : signature.constantArguments()) result.add_constant_arguments(constant);
  *result.mutable_output() = types.convert(signature.returnType(), signature);
  const auto* aggregate = dynamic_cast<const facebook::velox::exec::AggregateFunctionSignature*>(&signature);
  if (aggregate) *result.mutable_intermediate() = types.convert(aggregate->intermediateType(), signature);
  std::unordered_set<std::string> integers;
  std::map<std::string, const facebook::velox::exec::SignatureVariable*> sorted;
  for (const auto& [name, source] : signature.variables()) {
    sorted.emplace(name, &source);
    if (source.isIntegerParameter()) integers.insert(name);
  }
  for (const auto& [name, source] : sorted) {
    (void)name;
    variable(result.add_variables(), *source, integers);
  }
  int remaining = 4096;
  parameterOrder(signature.returnType(), signature, result, 0, remaining);
  if (aggregate) parameterOrder(aggregate->intermediateType(), signature, result, 0, remaining);
  return result;
}
}  // namespace pangrova::capability
