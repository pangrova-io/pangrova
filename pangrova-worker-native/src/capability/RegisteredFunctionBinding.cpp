#include "capability/RegisteredFunctionBinding.h"

#include <algorithm>
#include <stdexcept>

#include "velox/exec/Aggregate.h"
#include "velox/exec/WindowFunction.h"
#include "velox/expression/FunctionRegistrySnapshot.h"

namespace pangrova::capability {
namespace {
using namespace facebook::velox;

std::vector<TypePtr> argumentTypes(const FunctionArguments& arguments) {
  std::vector<TypePtr> types;
  types.reserve(arguments.size());
  for (const auto& argument : arguments) {
    if (!argument || !argument->type()) throw std::invalid_argument("Function argument has no type");
    types.push_back(argument->type());
  }
  return types;
}

void requireOutput(const std::string& name, const TypePtr& declared, const TypePtr& resolved,
                   const std::string& kind) {
  if (!declared || !resolved) throw std::runtime_error("VeloxPlanConverter: no exact " + kind + " signature: " + name);
  if (!declared->equivalent(*resolved)) {
    throw std::runtime_error("VeloxPlanConverter: " + kind + " output contract mismatch: " + name +
        " declared=" + declared->toString() + " resolved=" + resolved->toString());
  }
}

void requireConstants(const std::string& name, const exec::FunctionSignature& signature,
                      const FunctionArguments& arguments) {
  const auto& constants = signature.constantArguments();
  if (constants.empty()) return;
  for (size_t ordinal = 0; ordinal < arguments.size(); ++ordinal) {
    // Actual variadic signatures repeat their final argument declaration.
    const auto declaration = std::min(ordinal, constants.size() - 1);
    if (constants[declaration] &&
        !std::dynamic_pointer_cast<const core::ConstantTypedExpr>(arguments[ordinal])) {
      throw std::runtime_error("Registered function requires a constant argument: " + name +
          " ordinal=" + std::to_string(ordinal));
    }
  }
}

core::CallTypedExprPtr call(const std::string& name, const TypePtr& output,
                          FunctionArguments arguments) {
  return std::make_shared<core::CallTypedExpr>(output, std::move(arguments), name);
}
}  // namespace

facebook::velox::core::CallTypedExprPtr bindRegisteredScalar(
    const facebook::velox::exec::FunctionRegistrySnapshot& registry,
    const std::string& name, const facebook::velox::TypePtr& output,
    FunctionArguments arguments) {
  // The target is consumed only by the selected special-form type programme
  // (e.g. CAST), never used to choose a different ordinary overload.
  const auto resolved = registry.bindScalar(name, argumentTypes(arguments), output);
  requireOutput(name, output, resolved ? resolved->resultType : nullptr, "scalar");
  if (resolved->implementation.signature) requireConstants(name, *resolved->implementation.signature, arguments);
  return call(name, output, std::move(arguments));
}

RegisteredAggregateBinding bindRegisteredRawAggregate(
    const facebook::velox::exec::FunctionRegistrySnapshot& registry,
    const std::string& name, const facebook::velox::TypePtr& output,
    FunctionArguments arguments, bool intermediateOutput) {
  const auto resolved = registry.bindAggregate(name, argumentTypes(arguments));
  if (!resolved) throw std::runtime_error("No exact registered aggregate signature: " + name);
  requireConstants(name, *resolved->signature, arguments);
  requireOutput(name, output, intermediateOutput ? resolved->intermediateType : resolved->resultType, "aggregate");
  return {call(name, output, std::move(arguments)), resolved->intermediateType};
}

facebook::velox::core::CallTypedExprPtr bindRegisteredWindow(
    const facebook::velox::exec::FunctionRegistrySnapshot& registry,
    const std::string& name, const facebook::velox::TypePtr& output,
    FunctionArguments arguments) {
  const auto resolved = registry.bindWindow(name, argumentTypes(arguments));
  if (!resolved) throw std::runtime_error("No exact registered window signature: " + name);
  requireConstants(name, *resolved->signature, arguments);
  requireOutput(name, output, resolved->resultType, "window");
  return call(name, output, std::move(arguments));
}

}  // namespace pangrova::capability
