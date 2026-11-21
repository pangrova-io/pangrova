// Structural UNNEST uses the shared typed envelope and actual Velox UnnestNode.
#include "engine/velox/plan/VeloxPlanConverter.h"

#include <stdexcept>

#include "StructuralContractIdentities.h"
#include "pangrova/protocol/v2/plan_extension.pb.h"
#include "engine/velox/plan/CanonicalOrdinalIdentity.h"

namespace pangrova::velox {
using namespace facebook::velox;

namespace {
bool requiredOption(const pangrova::protocol::v2::CapabilityInvocation& invocation,
                    const char* name) {
  const pangrova::protocol::v2::TypedValue* result = nullptr;
  for (const auto& option : invocation.options()) {
    if (option.name() != "outer" && option.name() != "with_ordinality") {
      throw std::runtime_error("Unknown structural UNNEST option");
    }
    if (option.name() != name) continue;
    if (result) throw std::runtime_error("Duplicate structural UNNEST option");
    result = &option.value();
  }
  if (!result || !result->has_bool_value() || !result->has_type() ||
      result->type().canonical_sql() != "BOOLEAN" ||
      !result->type().substrait_type().has_bool_() ||
      result->type().substrait_type().bool_().nullability() !=
          substrait::Type::NULLABILITY_REQUIRED ||
      result->type().constraints_size() != 0 ||
      result->type().substrait_type().bool_().type_variation_reference() != 0) {
    throw std::runtime_error("Structural UNNEST requires exact BOOLEAN options");
  }
  return result->bool_value();
}

void requireIdentity(const pangrova::protocol::v2::CapabilityRef& reference) {
  const auto& identity = pangrova::capability::generated::pangrova_table_unnest;
  const std::string signature(reinterpret_cast<const char*>(identity.signature.data()),
                              identity.signature.size());
  if (reference.capability_id() != identity.id ||
      reference.semantic_version() != identity.version ||
      reference.signature_sha256() != signature ||
      !reference.implementation_build_id().empty()) {
    throw std::runtime_error("Structural UNNEST capability identity mismatch");
  }
}

core::FieldAccessTypedExprPtr field(const RowTypePtr& type, size_t index) {
  return std::make_shared<core::FieldAccessTypedExpr>(
      type->childAt(index), type->nameOf(index));
}
}  // namespace

std::shared_ptr<const core::PlanNode> VeloxPlanConverter::convertUnnest(
    const pangrova::protocol::v2::TableFunctionDetail& detail,
    std::shared_ptr<const core::PlanNode> input) {
  const auto& invocation = detail.invocation();
  requireIdentity(invocation.capability());
  const bool ordinal = requiredOption(invocation, "with_ordinality");
  const bool outer = requiredOption(invocation, "outer");
  if (detail.plan_node_id().empty() || invocation.options_size() != 2 ||
      invocation.arguments_size() == 0 || !detail.has_output_schema()) {
    throw std::runtime_error("Incomplete structural UNNEST contract");
  }
  const auto logicalInput = logicalNodeType(input);
  const auto inputType = input->outputType();
  const auto declared = convertNamedStruct(detail.output_schema());
  std::vector<core::FieldAccessTypedExprPtr> replicate;
  std::vector<core::FieldAccessTypedExprPtr> collections;
  std::vector<TypePtr> expected = inputType->children();
  std::vector<std::string> outputNames;
  for (size_t i = 0; i < inputType->size(); ++i) replicate.push_back(field(inputType, i));
  for (int i = 0; i < invocation.arguments_size(); ++i) {
    const auto& argument = invocation.arguments(i);
    const auto& expression = argument.expression();
    if (argument.name() != "input_" + std::to_string(i) || !expression.has_selection() ||
        !expression.selection().has_root_reference() ||
        !expression.selection().has_direct_reference() ||
        !expression.selection().direct_reference().has_struct_field() ||
        expression.selection().direct_reference().struct_field().has_child()) {
      throw std::runtime_error("UNNEST arguments require ordered exact input field references");
    }
    auto collection = std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
        convertExpression(expression, inputType));
    const auto& type = collection->type();
    if (type->isArray()) {
      expected.push_back(type->asArray().elementType());
    } else if (type->isMap()) {
      expected.push_back(type->asMap().keyType());
      expected.push_back(type->asMap().valueType());
    } else {
      throw std::runtime_error("UNNEST collection must have ARRAY or MAP type");
    }
    collections.push_back(std::move(collection));
  }
  if (ordinal) expected.push_back(BIGINT());
  if (declared->size() != expected.size()) {
    throw std::runtime_error("UNNEST output schema arity does not match actual expansion");
  }
  for (size_t i = 0; i < expected.size(); ++i) {
    if (!declared->childAt(i)->equivalent(*expected[i]) ||
        (i < inputType->size() && declared->nameOf(i) != logicalInput->nameOf(i))) {
      throw std::runtime_error("UNNEST output schema does not match exact input/expansion type");
    }
    if (i >= inputType->size() && (!ordinal || i + 1 < expected.size())) {
      outputNames.push_back(physicalFieldAlias(i));
    }
  }
  const auto ordinalName = ordinal
      ? std::make_optional(physicalFieldAlias(expected.size() - 1)) : std::nullopt;
  const auto markerName = outer
      ? std::make_optional(physicalFieldAlias(expected.size())) : std::nullopt;
  auto unnest = std::make_shared<core::UnnestNode>(detail.plan_node_id(),
      std::move(replicate), std::move(collections), std::move(outputNames),
      ordinalName, markerName, input);
  if (!outer) return unnest;
  std::vector<std::string> names;
  std::vector<core::TypedExprPtr> projections;
  for (size_t i = 0; i < expected.size(); ++i) {
    names.push_back(physicalFieldAlias(i));
    projections.push_back(field(unnest->outputType(), i));
  }
  return std::make_shared<core::ProjectNode>(
      nextNodeId(), std::move(names), std::move(projections), unnest);
}

}  // namespace pangrova::velox
