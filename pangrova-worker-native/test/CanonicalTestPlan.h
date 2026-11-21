#pragma once

#include <cstdint>
#include <map>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include <google/protobuf/message.h>

#include "capabilities/capability/FunctionPlanBindings.h"
#include "CanonicalTestRewrite.h"
#include "substrait/plan.pb.h"

namespace pangrova::test {
namespace canonical_plan_detail {
using protocol::v2::FunctionKind;
using protocol::v2::SubstraitFunctionDefinition;
using CallKinds = std::unordered_map<uint32_t, FunctionKind>;

inline FunctionKind callKind(const google::protobuf::Descriptor& descriptor) {
  const auto& name = descriptor.full_name();
  if (name == "substrait.Expression.ScalarFunction") return protocol::v2::FUNCTION_KIND_SCALAR;
  if (name == "substrait.AggregateFunction") return protocol::v2::FUNCTION_KIND_AGGREGATE;
  if (name == "substrait.Expression.WindowFunction" ||
      name == "substrait.ConsistentPartitionWindowRel.WindowRelFunction") {
    return protocol::v2::FUNCTION_KIND_WINDOW;
  }
  return protocol::v2::FUNCTION_KIND_UNSPECIFIED;
}

inline void collectCallKinds(const google::protobuf::Message& message, CallKinds& kinds) {
  const auto* descriptor = message.GetDescriptor();
  const auto* reflection = message.GetReflection();
  const auto kind = callKind(*descriptor);
  if (kind != protocol::v2::FUNCTION_KIND_UNSPECIFIED) {
    const auto* reference = descriptor->FindFieldByName("function_reference");
    const auto anchor = reflection->GetUInt32(message, reference);
    const auto [found, inserted] = kinds.emplace(anchor, kind);
    if (!inserted && found->second != kind) {
      throw std::invalid_argument("Fixture reuses a function anchor for different invocation kinds");
    }
  }
  std::vector<const google::protobuf::FieldDescriptor*> fields;
  reflection->ListFields(message, &fields);
  for (const auto* field : fields) {
    if (field->cpp_type() != google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) continue;
    if (field->is_repeated()) {
      for (int index = 0; index < reflection->FieldSize(message, field); ++index) {
        collectCallKinds(reflection->GetRepeatedMessage(message, field, index), kinds);
      }
    } else {
      collectCallKinds(reflection->GetMessage(message, field), kinds);
    }
  }
}

inline const SubstraitFunctionDefinition& definition(FunctionKind kind, const std::string& name) {
  const SubstraitFunctionDefinition* exact = nullptr;
  const SubstraitFunctionDefinition* implementation = nullptr;
  int exactCount = 0;
  int implementationCount = 0;
  for (const auto& candidate : capability::declaredPlanFunctions().definitions()) {
    if (kind != protocol::v2::FUNCTION_KIND_UNSPECIFIED && candidate.ref().kind() != kind) continue;
    if (candidate.ref().name() == name) { exact = &candidate; ++exactCount; }
    if (candidate.implementation_name() == name) { implementation = &candidate; ++implementationCount; }
  }
  if (exactCount == 1) return *exact;
  if (exactCount == 0 && implementationCount == 1) return *implementation;
  throw std::invalid_argument("Fixture function lacks an unambiguous sole declaration: " + name);
}
}  // namespace canonical_plan_detail

// Explicit fixture construction adapter, never used by a production decoder.
// Uses actual IR invocation kinds and the sole generated SQL declarations;
// registered availability is still checked by the converter's actual owner.
// Already-qualified function declarations are untouched so malformed-URN oracles
// stay malformed. The fixture builder adds version metadata independently.
inline substrait::Plan canonicalTestPlan(substrait::Plan plan) {
  // This builder creates legitimate fixture plans. Version rejection tests
  // must mutate the completed plan and serialize it directly.
  if (!plan.has_version()) {
    auto* version = plan.mutable_version();
    version->set_major_number(0);
    version->set_minor_number(51);
    version->set_patch_number(0);
    version->set_producer("Pangrova/native-test");
  }
  if (plan.extension_urns_size() != 0) return plan;
  canonical_plan_detail::CallKinds kinds;
  canonical_plan_detail::collectCallKinds(plan, kinds);
  std::map<std::string, uint32_t> namespaces;
  canonical_plan_detail::FixtureDefinitions rewrites;
  for (auto& extension : *plan.mutable_extensions()) {
    if (!extension.has_extension_function()) continue;
    auto* function = extension.mutable_extension_function();
    const auto suffix = function->name().find(':');
    const auto shortName = function->name().substr(0, suffix);
    const auto call = kinds.find(function->function_anchor());
    const auto kind = call == kinds.end() ? protocol::v2::FUNCTION_KIND_UNSPECIFIED : call->second;
    const auto& definition = canonical_plan_detail::definition(kind, shortName);
    if (definition.implementation_name().empty()) {
      if (!definition.has_rewrite()) {
        throw std::invalid_argument("Fixture must lower its structural programme explicitly");
      }
      rewrites.emplace(function->function_anchor(), &definition);
      continue;
    }
    const auto [entry, inserted] = namespaces.emplace(
        definition.ref().urn(), static_cast<uint32_t>(namespaces.size()));
    if (inserted) {
      auto* urn = plan.add_extension_urns();
      urn->set_extension_urn_anchor(entry->second);
      urn->set_urn(entry->first);
    }
    function->set_extension_urn_reference(entry->second);
    function->set_name(definition.ref().name() +
        (suffix == std::string::npos ? "" : function->name().substr(suffix)));
  }
  canonical_plan_detail::lowerFixtureRewrites(plan, rewrites, plan);
  for (int index = plan.extensions_size() - 1; index >= 0; --index) {
    const auto& extension = plan.extensions(index);
    if (extension.has_extension_function() && rewrites.contains(extension.extension_function().function_anchor())) {
      plan.mutable_extensions()->DeleteSubrange(index, 1);
    }
  }
  return plan;
}
}  // namespace pangrova::test
