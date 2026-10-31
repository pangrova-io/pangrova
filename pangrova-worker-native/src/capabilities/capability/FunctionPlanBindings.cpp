#include "capabilities/capability/FunctionPlanBindings.h"

#include <stdexcept>
#include <utility>

#include "StructuralContractIdentities.h"

namespace pangrova::capability {
namespace {
using namespace protocol::v2;

}  // namespace

const FunctionCatalogManifest& declaredPlanFunctions() {
  static const auto manifest = [] {
    FunctionCatalogManifest result;
    generated::appendDeclaredFunctionDefinitions(&result);
    return result;
  }();
  return manifest;
}

namespace {

const SubstraitFunctionDefinition& exact(const std::string& urn, const std::string& name) {
  const SubstraitFunctionDefinition* result = nullptr;
  for (const auto& definition : declaredPlanFunctions().definitions()) {
    if (definition.ref().urn() != urn || definition.ref().name() != name) continue;
    if (result) throw std::invalid_argument("Ambiguous canonical function declaration");
    result = &definition;
  }
  if (!result) throw std::invalid_argument("Unbound canonical function declaration: " + urn + ":" + name);
  if (result->implementation_name().empty()) {
    throw std::invalid_argument("Structural or rewritten binding cannot be emitted as a direct function call");
  }
  return *result;
}
}  // namespace

const SubstraitFunctionDefinition& declaredPlanFunction(FunctionKind kind, const std::string& name) {
  for (const auto& definition : declaredPlanFunctions().definitions()) {
    if (definition.ref().kind() == kind && definition.ref().name() == name) return definition;
  }
  throw std::invalid_argument("Function is absent from the sole SQL binding declaration: " + name);
}

const SubstraitFunctionDefinition& declaredRewriteTarget(const FunctionRewriteNode& node) {
  if (!node.has_target() || !node.implementation_name().empty() ||
      node.target().kind() != FUNCTION_KIND_SCALAR) {
    throw std::invalid_argument("Rewrite requires its exact declared scalar target");
  }
  return exact(node.target().urn(), node.target().name());
}

std::unordered_map<uint32_t, SubstraitFunctionDefinition>
bindPlanFunctionDeclarations(const substrait::Plan& plan) {
  if (plan.extension_urns_size() > 4096 || plan.extensions_size() > 4096) {
    throw std::invalid_argument("Function declarations exceed their bound");
  }
  std::unordered_map<uint32_t, std::string> urns;
  for (const auto& urn : plan.extension_urns()) {
    if (urn.urn().empty() || urn.urn().size() > 512 ||
        !urns.emplace(urn.extension_urn_anchor(), urn.urn()).second) {
      throw std::invalid_argument("Function namespace has an invalid or duplicate anchor");
    }
  }
  std::unordered_map<uint32_t, SubstraitFunctionDefinition> result;
  for (const auto& extension : plan.extensions()) {
    if (!extension.has_extension_function()) continue;
    const auto& function = extension.extension_function();
    const auto urn = urns.find(function.extension_urn_reference());
    if (urn == urns.end()) throw std::invalid_argument("Function declaration has no exact canonical namespace");
    const auto name = function.name().substr(0, function.name().find(':'));
    if (function.name().size() > 4096 ||
        !result.emplace(function.function_anchor(), exact(urn->second, name)).second) {
      throw std::invalid_argument("Function declaration has a duplicate or unbounded anchor");
    }
  }
  return result;
}

}  // namespace pangrova::capability
