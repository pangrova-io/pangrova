#include "capabilities/capability/FunctionCatalogAssembler.h"

#include "capabilities/capability/FunctionContractEncoding.h"
#include "capabilities/capability/FunctionCanonicalDomain.h"
#include "capabilities/capability/RegisteredFunctionCatalog.h"
#include "capabilities/capability/FunctionPlanBindings.h"
#include "CanonicalPhysicalTypeProjection.h"
#include "ExecutionSelection.h"
#include "StructuralContractIdentities.h"
#include "velox/expression/FunctionRegistryIdentity.h"

#include <map>
#include <stdexcept>
#include <string_view>

namespace pangrova::capability {
namespace {
using namespace protocol::v2;
using Identity = std::pair<FunctionKind, std::string>;
using Registrations = std::map<Identity, const RegisteredFunctionContract*>;
constexpr size_t kMaximumEncodedInventoryBytes = 4 * 1024 * 1024;

std::string digestBytes(std::string_view text) {
  if (text.size() != 64) throw std::invalid_argument("Function identity must be an exact SHA-256");
  auto nibble = [](char value) -> unsigned char {
    if (value >= '0' && value <= '9') return value - '0';
    if (value >= 'a' && value <= 'f') return value - 'a' + 10;
    throw std::invalid_argument("Function identity must be canonical lowercase hexadecimal");
  };
  std::string result;
  result.reserve(32);
  for (size_t ordinal = 0; ordinal < text.size(); ordinal += 2) {
    result.push_back(static_cast<char>((nibble(text[ordinal]) << 4) | nibble(text[ordinal + 1])));
  }
  return result;
}

Registrations registrations(const FunctionCatalogManifest& manifest) {
  Registrations result;
  for (const auto& entry : manifest.registered_functions()) {
    if (!result.emplace(Identity{entry.kind(), entry.implementation_name()}, &entry).second) {
      throw std::invalid_argument("Actual registration projection contains duplicate identities");
    }
  }
  return result;
}

const RegisteredFunctionContract* registered(
    const Registrations& entries, FunctionKind kind, const std::string& name) {
  const auto found = entries.find({kind, name});
  return found == entries.end() ? nullptr : found->second;
}

void phases(SubstraitFunctionDefinition& definition) {
  if (definition.ref().kind() == FUNCTION_KIND_AGGREGATE) {
    for (const auto phase : {substrait::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE,
             substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_INTERMEDIATE,
             substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT,
             substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT}) {
      definition.add_aggregate_phases(phase);
    }
  } else if (definition.ref().kind() == FUNCTION_KIND_WINDOW) {
    definition.add_aggregate_phases(substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT);
  }
}

bool bindDefinition(SubstraitFunctionDefinition& definition, const Registrations& entries) {
  if (!definition.structural_lowering().empty()) return false;
  if (definition.has_rewrite()) {
    for (const auto& node : definition.rewrite().nodes()) {
      const auto& dependency = declaredRewriteTarget(node);
      const auto actual = registered(entries, dependency.ref().kind(), dependency.implementation_name());
      if (!actual || !hasCanonicalDomain(*actual)) return false;
    }
    definition.set_resolution_policy(FUNCTION_RESOLUTION_POLICY_SCALAR_RUNTIME_PRIORITY);
    return true;
  }
  const auto actual = registered(entries, definition.ref().kind(), definition.implementation_name());
  if (!actual || !hasCanonicalDomain(*actual)) return false;
  definition.set_resolution_policy(actual->resolution_policy());
  phases(definition);
  if (definition.ref().kind() == FUNCTION_KIND_WINDOW) {
    definition.set_window_process(actual->window_process());
    definition.set_aggregate_window(actual->aggregate_window());
  }
  if (definition.ref().kind() != FUNCTION_KIND_SCALAR) {
    definition.add_invocations(substrait::AggregateFunction::AGGREGATION_INVOCATION_ALL);
    if (definition.ref().kind() == FUNCTION_KIND_AGGREGATE || !actual->distinct_implementation().empty()) {
      definition.add_invocations(substrait::AggregateFunction::AGGREGATION_INVOCATION_DISTINCT);
    }
  }
  return true;
}

void expose(FunctionAvailability& availability, const SubstraitFunctionDefinition& definition,
            const std::string& build) {
  *availability.mutable_ref() = definition.ref();
  availability.set_implementation_build_id(build);
}

std::string definitionDigest(const SubstraitFunctionDefinition& definition,
                             const Registrations& entries) {
  FunctionCatalogManifest contract;
  *contract.add_definitions() = definition;
  std::map<Identity, const RegisteredFunctionContract*> dependencies;
  if (definition.has_rewrite()) {
    for (const auto& node : definition.rewrite().nodes()) {
      const auto& dependency = declaredRewriteTarget(node);
      if (const auto actual = registered(entries, dependency.ref().kind(), dependency.implementation_name())) {
        dependencies.emplace(Identity{actual->kind(), actual->implementation_name()}, actual);
      }
    }
  } else if (const auto actual = registered(entries, definition.ref().kind(), definition.implementation_name())) {
    dependencies.emplace(Identity{actual->kind(), actual->implementation_name()}, actual);
  }
  for (const auto& [identity, actual] : dependencies) {
    (void)identity;
    *contract.add_registered_functions() = *actual;
  }
  return functionContractDigest(contract);
}

void sealDefinitions(CapabilityInventory& inventory) {
  auto& manifest = *inventory.mutable_function_catalog();
  const auto entries = registrations(manifest);
  for (auto& definition : *manifest.mutable_definitions()) {
    const bool available = bindDefinition(definition, entries);
    definition.mutable_ref()->set_contract_sha256(definitionDigest(definition, entries));
    if (available) expose(*inventory.add_functions(), definition, manifest.implementation_build_id());
  }
}

void requireCanonicalEnvelope(const CapabilityInventory& inventory) {
  if (!inventory.has_protocol() || inventory.protocol().major() != 2 || inventory.protocol().minor() != 0 ||
      inventory.protocol().descriptor_set_sha256().size() != 32 || inventory.protocol().rollout_epoch().empty() ||
      inventory.native_engine_version().empty() || inventory.has_function_catalog() || inventory.functions_size()) {
    throw std::invalid_argument("Actual catalogue requires a new canonical Native capability envelope");
  }
  if (facebook::velox::exec::functionContractGenerationSha256() !=
      pangrova::contract::generated::executionSelectionInputSha256) {
    throw std::runtime_error("Actual SDK archive and Native function generation inputs do not match");
  }
}
}  // namespace

std::shared_ptr<const FunctionCatalogPublication> FunctionCatalogAssembler::assemble(
    std::shared_ptr<const FunctionCatalogPublication::Registry> registry, CapabilityInventory inventory) {
  if (!registry) throw std::invalid_argument("Actual catalogue requires its immutable SDK registry owner");
  requireCanonicalEnvelope(inventory);
  auto& manifest = *inventory.mutable_function_catalog();
  manifest.set_schema_version(1);
  manifest.set_canonical_descriptor_sha256(inventory.protocol().descriptor_set_sha256());
  manifest.set_implementation_build_id("pangrova-native-" + inventory.native_engine_version());
  manifest.set_execution_selection_sha256(digestBytes(registry->executionSelectionContractSha256()));
  generated::appendDeclaredFunctionDefinitions(&manifest);
  generated::appendCanonicalPhysicalProjections(&manifest);
  appendRegisteredFunctionCatalog(*registry, manifest);
  sealDefinitions(inventory);
  manifest.set_manifest_sha256(functionContractDigest(manifest));
  const size_t encoded = 4 * ((inventory.ByteSizeLong() + 2) / 3);
  if (encoded > kMaximumEncodedInventoryBytes) {
    throw std::runtime_error("Actual complete Native catalogue exceeds the canonical 4 MiB encoded inventory limit");
  }
  return std::make_shared<FunctionCatalogPublication>(std::move(registry), std::move(inventory),
      FunctionCatalogPublication::ConstructionAuthority{});
}

}  // namespace pangrova::capability
