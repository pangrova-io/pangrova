#include "capability/RegisteredFunctionBinding.h"

#include <algorithm>
#include <stdexcept>

#include "velox/exec/Aggregate.h"
#include "velox/exec/WindowFunction.h"
#include "velox/expression/SignatureBinder.h"
#include "velox/functions/FunctionRegistry.h"

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
    const std::string& name, const facebook::velox::TypePtr& output,
    FunctionArguments arguments) {
  const auto resolved = facebook::velox::resolveFunctionOrCallableSpecialForm(name, argumentTypes(arguments));
  requireOutput(name, output, resolved, "scalar");
  return call(name, output, std::move(arguments));
}

RegisteredAggregateBinding bindRegisteredRawAggregate(
    const std::string& name, const facebook::velox::TypePtr& output,
    FunctionArguments arguments, bool intermediateOutput) {
  const auto signatures = facebook::velox::exec::getAggregateFunctionSignatures(name);
  if (!signatures) throw std::runtime_error("Aggregate function is not registered: " + name);
  const auto types = argumentTypes(arguments);
  // Preserve the running registry's first-match resolution order. In
  // particular, do not search subsequent overloads for a desired output.
  for (const auto& signature : *signatures) {
    facebook::velox::exec::SignatureBinder binder(*signature, types);
    if (!binder.tryBind()) continue;
    requireConstants(name, *signature, arguments);
    auto intermediate = binder.tryResolveType(signature->intermediateType());
    const auto resolved = intermediateOutput ? intermediate : binder.tryResolveReturnType();
    requireOutput(name, output, resolved, "aggregate");
    if (!intermediate) throw std::runtime_error("Aggregate intermediate type cannot be resolved: " + name);
    return {call(name, output, std::move(arguments)), std::move(intermediate)};
  }
  throw std::runtime_error("No exact registered aggregate signature: " + name);
}

facebook::velox::core::CallTypedExprPtr bindRegisteredWindow(
    const std::string& name, const facebook::velox::TypePtr& output,
    FunctionArguments arguments) {
  const auto signatures = facebook::velox::exec::getWindowFunctionSignatures(name);
  if (!signatures) throw std::runtime_error("Window function is not registered: " + name);
  const auto types = argumentTypes(arguments);
  for (const auto& signature : *signatures) {
    facebook::velox::exec::SignatureBinder binder(*signature, types);
    if (!binder.tryBind()) continue;
    requireConstants(name, *signature, arguments);
    requireOutput(name, output, binder.tryResolveReturnType(), "window");
    return call(name, output, std::move(arguments));
  }
  throw std::runtime_error("No exact registered window signature: " + name);
}

}  // namespace pangrova::capability
