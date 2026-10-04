#pragma once

#include <algorithm>
#include <cstdint>
#include <limits>
#include <map>
#include <stdexcept>
#include <utility>
#include <vector>

#include <google/protobuf/message.h>

#include "capability/FunctionPlanBindings.h"
#include "substrait/plan.pb.h"

namespace pangrova::test::canonical_plan_detail {
using FixtureDefinitions =
    std::map<uint32_t, const protocol::v2::SubstraitFunctionDefinition*>;

inline uint32_t appendRewriteTarget(substrait::Plan& plan,
    const protocol::v2::SubstraitFunctionDefinition& target) {
  uint32_t anchor = 0;
  for (const auto& extension : plan.extensions()) {
    if (!extension.has_extension_function()) continue;
    const auto existing = extension.extension_function().function_anchor();
    if (existing == std::numeric_limits<uint32_t>::max()) {
      throw std::invalid_argument("Fixture has no remaining canonical function anchors");
    }
    anchor = std::max(anchor, existing + 1);
  }
  auto* urn = plan.add_extension_urns();
  urn->set_extension_urn_anchor(static_cast<uint32_t>(plan.extension_urns_size() - 1));
  urn->set_urn(target.ref().urn());
  auto* function = plan.add_extensions()->mutable_extension_function();
  function->set_extension_urn_reference(urn->extension_urn_anchor());
  function->set_function_anchor(anchor);
  function->set_name(target.ref().name());
  return anchor;
}

// Current declared predicate programmes are BOOLEAN throughout. Do not infer
// arbitrary intermediate types or invoke factories in this fixture adapter.
inline substrait::Expression lowerPredicateRewrite(substrait::Plan& plan,
    const substrait::Expression::ScalarFunction& original,
    const protocol::v2::FunctionRewrite& rewrite) {
  if (!original.output_type().has_bool_() || rewrite.nodes_size() == 0 || rewrite.nodes_size() > 32) {
    throw std::invalid_argument("Fixture predicate programme lacks bounded BOOLEAN semantics");
  }
  std::vector<substrait::Expression> nodes;
  for (const auto& node : rewrite.nodes()) {
    const auto& target = capability::declaredRewriteTarget(node);
    substrait::Expression expression;
    auto* call = expression.mutable_scalar_function();
    call->set_function_reference(appendRewriteTarget(plan, target));
    *call->mutable_output_type() = original.output_type();
    for (const auto& argument : node.arguments()) {
      if (argument.has_input_ordinal() && argument.input_ordinal() < static_cast<uint32_t>(original.arguments_size())) {
        *call->add_arguments() = original.arguments(argument.input_ordinal());
      } else if (argument.has_node_ordinal() && argument.node_ordinal() < nodes.size()) {
        *call->add_arguments()->mutable_value() = nodes[argument.node_ordinal()];
      } else {
        throw std::invalid_argument("Fixture programme references an absent input or a forward node");
      }
    }
    nodes.push_back(std::move(expression));
  }
  if (rewrite.result_node_ordinal() >= nodes.size()) {
    throw std::invalid_argument("Fixture predicate programme has an absent result");
  }
  return nodes[rewrite.result_node_ordinal()];
}

inline void lowerFixtureRewrites(google::protobuf::Message& message,
    const FixtureDefinitions& definitions, substrait::Plan& plan) {
  auto* reflection = message.GetReflection();
  std::vector<const google::protobuf::FieldDescriptor*> fields;
  reflection->ListFields(message, &fields);
  for (const auto* field : fields) {
    if (field->cpp_type() != google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) continue;
    if (field->is_repeated()) {
      const int count = reflection->FieldSize(message, field);
      for (int index = 0; index < count; ++index) {
        lowerFixtureRewrites(*reflection->MutableRepeatedMessage(&message, field, index), definitions, plan);
      }
    } else {
      lowerFixtureRewrites(*reflection->MutableMessage(&message, field), definitions, plan);
    }
  }
  if (message.GetDescriptor()->full_name() != "substrait.Expression") return;
  auto& expression = static_cast<substrait::Expression&>(message);
  if (!expression.has_scalar_function()) return;
  const auto definition = definitions.find(expression.scalar_function().function_reference());
  if (definition == definitions.end() || !definition->second->has_rewrite()) return;
  expression = lowerPredicateRewrite(plan, expression.scalar_function(), definition->second->rewrite());
}
}  // namespace pangrova::test::canonical_plan_detail
