#include "capability/RegisteredFunctionCatalog.h"

#include "capability/FunctionContractEncoding.h"
#include "capability/RegisteredFunctionSignature.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/WindowFunction.h"
#include "velox/expression/FunctionCallToSpecialForm.h"
#include "velox/window/DistinctAggregateWindow.h"

#include <map>
#include <stdexcept>

namespace pangrova::capability {
namespace {
using namespace protocol::v2;
using Registry = facebook::velox::exec::FunctionRegistrySnapshot;

FunctionPhysicalTypePattern physical(const facebook::velox::TypePtr& type,
                                    int depth = 0) {
  if (!type || depth > 64) throw std::invalid_argument("Registered physical type exceeds its bound");
  FunctionPhysicalTypePattern result;
  result.set_runtime_tag(facebook::velox::TypeKindName::toName(type->kind()));
  for (size_t index = 0; index < type->size(); ++index) {
    *result.add_children() = physical(type->childAt(index), depth + 1);
  }
  return result;
}

void vectorMetadata(FunctionImplementationMetadata& target,
                    const facebook::velox::exec::VectorFunctionMetadata& source) {
  target.set_deterministic(source.deterministic);
  target.set_default_null_behavior(source.defaultNullBehavior);
  target.set_supports_flattening(source.supportsFlattening);
  target.set_companion_function(source.companionFunction);
}

FunctionImplementationContract implementation(
    const Registry::ScalarEntry& source, uint32_t ordinal,
    const RegisteredTypeTemplate& types) {
  FunctionImplementationContract result;
  result.set_traversal_ordinal(ordinal);
  if (source.signature) *result.mutable_signature() = projectRegisteredSignature(*source.signature, types);
  if (source.special) {
    result.set_family(FUNCTION_IMPLEMENTATION_FAMILY_SPECIAL_FORM);
    result.set_special_type_program(std::string(source.special->typeProgram()));
    result.set_vector_companion(std::string(source.special->vectorCompanion()));
  } else if (source.vector) {
    result.set_family(FUNCTION_IMPLEMENTATION_FAMILY_VECTOR);
    vectorMetadata(*result.mutable_metadata(), source.vector->metadata);
  } else if (source.simple) {
    result.set_family(FUNCTION_IMPLEMENTATION_FAMILY_SCALAR);
    const auto& metadata = source.simple->getMetadata();
    result.set_priority(metadata.priority());
    result.mutable_metadata()->set_deterministic(metadata.isDeterministic());
    result.mutable_metadata()->set_default_null_behavior(metadata.defaultNullBehavior());
    *result.mutable_output_physical() = physical(metadata.resultPhysicalType());
    for (const auto& argument : metadata.argPhysicalTypes()) *result.add_argument_physical() = physical(argument);
  } else {
    throw std::invalid_argument("Owned registration has no actual implementation family");
  }
  result.set_contract_sha256(functionContractDigest(result));
  return result;
}

template <typename Signatures>
RegisteredFunctionContract ordered(const std::string& name, FunctionKind kind,
    const Signatures& signatures, const RegisteredTypeTemplate& types) {
  RegisteredFunctionContract result;
  result.set_implementation_name(name);
  result.set_kind(kind);
  result.set_resolution_policy(FUNCTION_RESOLUTION_POLICY_ORDERED_FIRST_MATCH);
  for (const auto& signature : signatures) *result.add_signatures() = projectRegisteredSignature(*signature, types);
  return result;
}

RegisteredFunctionContract scalar(const std::string& name,
    const std::vector<Registry::ScalarEntry>& entries, const RegisteredTypeTemplate& types) {
  if (entries.empty() || entries.size() > 4096) throw std::invalid_argument("Owned scalar entries exceed their bound");
  RegisteredFunctionContract result;
  result.set_implementation_name(name);
  result.set_kind(FUNCTION_KIND_SCALAR);
  result.set_resolution_policy(FUNCTION_RESOLUTION_POLICY_SCALAR_RUNTIME_PRIORITY);
  uint32_t ordinal = 0;
  for (const auto& entry : entries) {
    // Each entry owns its signature. Do not duplicate it into a second name-
    // level list or sort away the actual runtime selector's traversal order.
    *result.add_implementations() = implementation(entry, ordinal++, types);
  }
  return result;
}

RegisteredFunctionContract window(const std::string& name,
    const facebook::velox::exec::WindowFunctionEntry& entry, const Registry& registry,
    const RegisteredTypeTemplate& types) {
  auto result = ordered(name, FUNCTION_KIND_WINDOW, entry.signatures, types);
  result.set_aggregate_window(entry.metadata.isAggregate);
  result.set_window_process(entry.metadata.processMode == facebook::velox::exec::WindowFunction::ProcessMode::kRows
      ? FUNCTION_WINDOW_PROCESS_ROWS : FUNCTION_WINDOW_PROCESS_PARTITION);
  if (entry.metadata.isAggregate && registry.aggregate(name)) {
    const auto distinct = pangrova::velox::distinctAggregateWindowName(name);
    if (registry.window(distinct)) result.set_distinct_implementation(distinct);
  }
  return result;
}
}  // namespace

void appendRegisteredFunctionCatalog(const Registry& registry,
                                    FunctionCatalogManifest& manifest) {
  if (manifest.registered_functions_size() || manifest.vector_companions_size()) {
    throw std::invalid_argument("Actual registration catalogue can only be projected once");
  }
  FunctionCatalogManifest projected;
  RegisteredTypeTemplate types;
  std::map<std::pair<FunctionKind, std::string>, RegisteredFunctionContract> functions;
  for (const auto& [name, entries] : registry.scalars()) {
    functions.emplace(std::make_pair(FUNCTION_KIND_SCALAR, name), scalar(name, entries, types));
  }
  for (const auto& [name, entry] : registry.aggregates()) {
    functions.emplace(std::make_pair(FUNCTION_KIND_AGGREGATE, name), ordered(name, FUNCTION_KIND_AGGREGATE, entry->signatures, types));
  }
  for (const auto& [name, entry] : registry.windows()) {
    functions.emplace(std::make_pair(FUNCTION_KIND_WINDOW, name), window(name, *entry, registry, types));
  }
  for (auto& [identity, function] : functions) {
    (void)identity;
    function.set_contract_sha256(functionContractDigest(function));
    *projected.add_registered_functions() = std::move(function);
  }
  std::map<std::string, RegisteredVectorCompanion> companions;
  for (const auto& [name, entry] : registry.vectors()) {
    if (!entry->signatures.empty()) continue;
    RegisteredVectorCompanion companion;
    companion.set_implementation_name(name);
    vectorMetadata(*companion.mutable_metadata(), entry->metadata);
    companion.set_contract_sha256(functionContractDigest(companion));
    companions.emplace(name, std::move(companion));
  }
  for (auto& [name, companion] : companions) {
    (void)name;
    *projected.add_vector_companions() = std::move(companion);
  }
  // The registration digest covers only actual registrations, not SQL aliases
  // or an implementation-build label. Identical projections are reproducible.
  FunctionCatalogManifest registrations;
  *registrations.mutable_registered_functions() = projected.registered_functions();
  *registrations.mutable_vector_companions() = projected.vector_companions();
  projected.set_registration_sha256(functionContractDigest(registrations));
  manifest.mutable_registered_functions()->Swap(projected.mutable_registered_functions());
  manifest.mutable_vector_companions()->Swap(projected.mutable_vector_companions());
  manifest.mutable_registration_sha256()->swap(*projected.mutable_registration_sha256());
}
}  // namespace pangrova::capability
