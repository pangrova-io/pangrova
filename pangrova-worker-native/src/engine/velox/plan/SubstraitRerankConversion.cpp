#include "engine/velox/plan/VeloxPlanConverter.h"

#include <stdexcept>
#include <unordered_set>

#include "capabilities/capability/GovernedBindingArguments.h"
#include "capabilities/capability/NativeCapabilityCatalog.h"
#include "pangrova/protocol/v2/plan_extension.pb.h"
#include "velox/core/Expressions.h"
#include "engine/velox/plan/CanonicalOrdinalIdentity.h"
#include "engine/velox/plan/CandidateContractValidation.h"

namespace pangrova::velox {
namespace {

using TypedExpression = std::shared_ptr<const facebook::velox::core::ITypedExpr>;
using Binding = pangrova::protocol::v2::OperatorBinding;
using Detail = pangrova::protocol::v2::RerankDetail;

const substrait::Expression& requireRerankArgument(
    const pangrova::protocol::v2::CapabilityInvocation& invocation,
    const std::string& name) {
  const substrait::Expression* result = nullptr;
  for (const auto& argument : invocation.arguments()) {
    if (argument.name() != "score" && argument.name() != "query" && argument.name() != "document") {
      throw std::runtime_error("Unsupported rerank argument: " + argument.name());
    }
    if (argument.name() == name) {
      if (result != nullptr) throw std::runtime_error("Duplicate rerank argument: " + name);
      result = &argument.expression();
    }
  }
  if (!result) throw std::runtime_error("Missing rerank argument: " + name);
  return *result;
}

void validateRerankIdentity(const Detail& detail) {
  if (!pangrova::capability::isCrossEncoderRerank(detail.invocation().capability())) {
    throw std::runtime_error("Unsupported or mismatched rerank capability");
  }
  if (detail.plan_node_id().empty() || detail.input_limit() == 0 ||
      detail.input_limit() > pangrova::capability::kCrossEncoderRerankMaxRows ||
      detail.output_limit() == 0 || detail.output_limit() > detail.input_limit() ||
      detail.invocation().arguments_size() != 3 || !detail.has_model() ||
      detail.model().model_id().empty() || detail.model().version().empty() ||
      detail.model().signature_sha256().size() != 32) {
    throw std::runtime_error("Rerank identity, limits or arguments are invalid");
  }
}

void validateRerankBinding(const Detail& detail, const Binding& binding) {
  const auto& model = binding.model();
  const auto& policy = binding.execution_policy();
  const auto& resources = binding.resources();
  if (!binding.has_model() || model.model().id() != detail.model().model_id() ||
      model.model().version() != detail.model().version() ||
      model.model().signature_sha256() != detail.model().signature_sha256() ||
      binding.artifacts_size() != 0 || binding.policies_size() != 0 ||
      policy.coverage_policy() != pangrova::protocol::v2::COVERAGE_POLICY_REQUIRE_COMPLETE ||
      policy.fallback_policy() != pangrova::protocol::v2::FALLBACK_POLICY_FORBID ||
      policy.retry_class() != pangrova::protocol::v2::RETRY_CLASS_PROVIDER_IDEMPOTENCY_KEY ||
      policy.idempotency_key().empty()) {
    throw std::runtime_error("Rerank OperatorBinding policy/model mismatch");
  }
  if (!binding.has_resources() || resources.provider_concurrency() == 0 ||
      resources.provider_items() == 0 || resources.provider_attempts() == 0 ||
      resources.provider_request_bytes() == 0 || resources.provider_response_bytes() == 0 ||
      !model.has_request_limits() || model.request_limits().max_input_tokens() == 0 ||
      model.request_limits().max_request_bytes() == 0 || model.request_limits().max_response_bytes() == 0) {
    throw std::runtime_error("Rerank OperatorBinding resources/request constraints are incomplete");
  }
}

std::vector<TypedExpression> rerankArguments(const Detail& detail, const Binding& binding,
    const TypedExpression& query, const TypedExpression& document) {
  std::vector<TypedExpression> arguments;
  for (const auto& declared : pangrova::capability::generated::AI_RERANK) {
    if (std::string_view(declared.source) == "VALUE_EXPRESSION") {
      if (std::string_view(declared.name) == "query") arguments.push_back(query);
      else if (std::string_view(declared.name) == "document") arguments.push_back(document);
      else throw std::invalid_argument("Unknown declared rerank value expression");
    } else {
      const auto value = pangrova::capability::governedBindingArgument(
          declared, binding, detail.plan_node_id(), 0);
      arguments.push_back(std::make_shared<facebook::velox::core::ConstantTypedExpr>(
          facebook::velox::VARCHAR(), facebook::velox::variant(value)));
    }
  }
  return arguments;
}

struct RerankOrdering {
  std::vector<facebook::velox::core::FieldAccessTypedExprPtr> keys;
  std::vector<facebook::velox::core::SortOrder> orders;
};

RerankOrdering rerankOrdering(const Detail& detail, size_t score,
    const facebook::velox::RowTypePtr& logical, const facebook::velox::RowTypePtr& physical) {
  RerankOrdering ordering;
  ordering.keys.push_back(std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
      facebook::velox::DOUBLE(), physical->nameOf(score)));
  ordering.orders.emplace_back(false, false);
  std::unordered_set<std::string> stable_keys;
  for (const auto& name : detail.output_contract().stable_key_fields()) {
    const auto channel = logical->getChildIdxIfExists(name);
    if (!channel || !stable_keys.emplace(name).second || channel.value() == score) {
      throw std::runtime_error("Invalid rerank stable key: " + name);
    }
    ordering.keys.push_back(std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
        physical->childAt(channel.value()), physical->nameOf(channel.value())));
    ordering.orders.emplace_back(true, false);
  }
  if (stable_keys.empty()) throw std::runtime_error("Rerank requires a stable total-order key");
  return ordering;
}

}  // namespace

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convertRerank(const Detail& detail,
    std::shared_ptr<const facebook::velox::core::PlanNode> input,
    const facebook::velox::RowTypePtr& input_type) {
  validateRerankIdentity(detail);
  const auto& binding = requireOperatorBinding(detail.plan_node_id(), detail.invocation().capability());
  validateRerankBinding(detail, binding);
  const auto logical_input = logicalNodeType(input);
  const auto declared_type = convertNamedStruct(detail.output_contract().schema());
  validateContractSchema(detail.output_contract(), logical_input, declared_type, "rerank output");
  if (detail.output_contract().score_direction() != pangrova::protocol::v2::SCORE_DIRECTION_HIGHER_IS_BETTER) {
    throw std::runtime_error("Rerank score direction must be HIGHER_IS_BETTER");
  }
  auto require_field = [&](const std::string& name, facebook::velox::TypeKind kind) {
    const auto expression = convertExpression(requireRerankArgument(detail.invocation(), name), input_type);
    const auto field = std::dynamic_pointer_cast<const facebook::velox::core::FieldAccessTypedExpr>(expression);
    if (!field || field->type()->kind() != kind) throw std::runtime_error("Invalid typed rerank field: " + name);
    const auto channel = input_type->getChildIdxIfExists(field->name());
    if (!channel) throw std::runtime_error("Rerank field is absent: " + field->name());
    return channel.value();
  };
  const size_t score_channel = require_field("score", facebook::velox::TypeKind::DOUBLE);
  const size_t document_channel = require_field("document", facebook::velox::TypeKind::VARCHAR);
  const auto query = convertExpression(requireRerankArgument(detail.invocation(), "query"), input_type);
  const auto constant = std::dynamic_pointer_cast<const facebook::velox::core::ConstantTypedExpr>(query);
  if (!constant || constant->type()->kind() != facebook::velox::TypeKind::VARCHAR) {
    throw std::runtime_error("Rerank query must be a VARCHAR literal");
  }
  const auto document = std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
      input_type->childAt(document_channel), input_type->nameOf(document_channel));
  const auto score = checkedScalarCall(std::string(pangrova::capability::kGovernedRerankFunction),
      facebook::velox::DOUBLE(), rerankArguments(detail, binding, query, document));
  std::vector<TypedExpression> projections;
  for (size_t channel = 0; channel < input_type->size(); ++channel) {
    if (channel == score_channel) projections.push_back(score);
    else projections.push_back(std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
        input_type->childAt(channel), input_type->nameOf(channel)));
  }
  const auto project = std::make_shared<facebook::velox::core::ProjectNode>(
      nextNodeId(), physicalRowType(declared_type)->names(), std::move(projections), input);
  const auto ordering = rerankOrdering(detail, score_channel, declared_type, project->outputType());
  const auto partial = std::make_shared<facebook::velox::core::TopNNode>(nextNodeId(),
      ordering.keys, ordering.orders, static_cast<int32_t>(detail.output_limit()), true, project);
  const auto gathered = facebook::velox::core::LocalPartitionNode::gather(nextNodeId(), {partial});
  return std::make_shared<facebook::velox::core::TopNNode>(detail.plan_node_id(),
      ordering.keys, ordering.orders, static_cast<int32_t>(detail.output_limit()), false, gathered);
}

}  // namespace pangrova::velox
