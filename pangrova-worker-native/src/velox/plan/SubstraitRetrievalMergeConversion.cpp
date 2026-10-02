// Canonical retrieval fusion contract conversion.
#include "velox/plan/VeloxPlanConverter.h"
#include "velox/plan/CanonicalOrdinalIdentity.h"
#include "velox/plan/CandidateContractValidation.h"

#include <algorithm>
#include <charconv>
#include <cmath>
#include <functional>
#include <limits>
#include <unordered_set>

#include <openssl/evp.h>

#include "compat/NativeFeatureDetection.h"
#include "capability/NativeCapabilityCatalog.h"
#include "artifact/GeneratedValueSegment.h"
#include "artifact/ImmutableArtifactCache.h"
#include "artifact/RowLocatorSidecar.h"
#include "executor/NativeUnsupportedFeatureException.h"
#include "velox/connector/PangrovaTableHandle.h"
#include "velox/connector/PangrovaVeloxConnector.h"
#include "velox/core/Expressions.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/AggregateFunctionRegistry.h"
#include "velox/exec/HashPartitionFunction.h"
#include "velox/exec/RoundRobinPartitionFunction.h"
#include "velox/type/Type.h"

#include "substrait/algebra.pb.h"

#include <google/protobuf/wrappers.pb.h>
#include <sstream>

#include "pangrova/protocol/v2/plan_extension.pb.h"
#include "velox/plan/TableFunctionNode.h"
#include "velox/plan/RetrievalMergeOperator.h"
#include "velox/plan/ArtifactBuildOperator.h"
#include "velox/plan/GeneratedValuesOverlayOperator.h"
#include "velox/runtime/NativeEngineLifecycle.h"
#include "velox/runtime/TableFunctionRegistry.h"

namespace pangrova::velox {

namespace {

const substrait::Expression& requireMergeArgument(
    const pangrova::protocol::v2::CapabilityInvocation& invocation,
    const std::string& name) {
  const substrait::Expression* result = nullptr;
  for (const auto& argument : invocation.arguments()) {
    if (argument.name() != "score" && argument.name() != "source_id") {
      throw std::runtime_error(
          "VeloxPlanConverter: unsupported retrieval merge argument: " +
          argument.name());
    }
    if (argument.name() == name) {
      if (result != nullptr) {
        throw std::runtime_error(
            "VeloxPlanConverter: duplicate retrieval merge argument: " + name);
      }
      result = &argument.expression();
    }
  }
  if (result == nullptr) {
    throw std::runtime_error(
        "VeloxPlanConverter: missing retrieval merge argument: " + name);
  }
  return *result;
}


pangrova::retrieval::ScoreDirection toFusionDirection(
    pangrova::protocol::v2::ScoreDirection direction) {
  switch (direction) {
    case pangrova::protocol::v2::SCORE_DIRECTION_HIGHER_IS_BETTER:
      return pangrova::retrieval::ScoreDirection::kHigherIsBetter;
    case pangrova::protocol::v2::SCORE_DIRECTION_LOWER_IS_BETTER:
      return pangrova::retrieval::ScoreDirection::kLowerIsBetter;
    default:
      throw std::runtime_error(
          "VeloxPlanConverter: retrieval source score direction is unspecified");
  }
}

}  // namespace

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convertRetrievalMerge(
    const pangrova::protocol::v2::RetrievalMergeDetail& detail,
    std::shared_ptr<const facebook::velox::core::PlanNode> input,
    const facebook::velox::RowTypePtr& input_type) {
  const auto& capability = detail.invocation().capability();
  const auto& logical_input = logicalNodeType(input);
  bool is_rrf = pangrova::capability::isRrfRetrievalMerge(capability);
  bool is_linear = pangrova::capability::isLinearRetrievalMerge(capability);
  if (!is_rrf && !is_linear) {
    throw std::runtime_error(
        "VeloxPlanConverter: unsupported or mismatched retrieval merge capability: " +
        capability.capability_id());
  }
  if (detail.plan_node_id().empty() || detail.output_limit() == 0 ||
      detail.output_limit() >
          pangrova::capability::kExactTopKMaxCandidateRows ||
      detail.inputs_size() == 0 || detail.max_fan_in() == 0 ||
      detail.max_fan_in() > pangrova::capability::kRetrievalMergeMaxFanIn ||
      static_cast<uint32_t>(detail.inputs_size()) > detail.max_fan_in()) {
    throw std::runtime_error(
        "VeloxPlanConverter: invalid retrieval merge limits or source fan-in");
  }
  const auto& operator_binding = requireOperatorBinding(
      detail.plan_node_id(), detail.invocation().capability());
  const auto& execution_policy = operator_binding.execution_policy();
  if (operator_binding.artifacts_size() != 0 || operator_binding.has_model() ||
      operator_binding.policies_size() != 0 ||
      execution_policy.coverage_policy() !=
          pangrova::protocol::v2::COVERAGE_POLICY_REQUIRE_COMPLETE ||
      execution_policy.fallback_policy() !=
          pangrova::protocol::v2::FALLBACK_POLICY_FORBID ||
      execution_policy.retry_class() !=
          pangrova::protocol::v2::RETRY_CLASS_IDEMPOTENT ||
      !execution_policy.has_approximation() ||
      execution_policy.approximation().candidate_limit() <
          detail.output_limit()) {
    throw std::runtime_error(
        "VeloxPlanConverter: retrieval merge OperatorBinding policy mismatch");
  }

  const auto& output_contract = detail.output_contract();
  auto output_declared_type = convertNamedStruct(output_contract.schema());
  validateContractSchema(
      output_contract, logical_input, output_declared_type, "merge output");
  if (output_contract.score_direction() !=
      pangrova::protocol::v2::SCORE_DIRECTION_HIGHER_IS_BETTER) {
    throw std::runtime_error(
        "VeloxPlanConverter: fused score direction must be HIGHER_IS_BETTER");
  }

  const auto& invocation = detail.invocation();
  if (invocation.arguments_size() != 2) {
    throw std::runtime_error(
        "VeloxPlanConverter: retrieval merge requires score and source_id "
        "arguments");
  }
  auto require_field = [&](const std::string& name,
                           facebook::velox::TypeKind kind) {
    auto expression = convertExpression(
        requireMergeArgument(invocation, name), input_type);
    auto field = std::dynamic_pointer_cast<
        const facebook::velox::core::FieldAccessTypedExpr>(expression);
    if (!field || field->type()->kind() != kind) {
      throw std::runtime_error(
          "VeloxPlanConverter: retrieval merge " + name +
          " must be a field with the canonical type");
    }
    auto channel = input_type->getChildIdxIfExists(field->name());
    if (!channel.has_value()) {
      throw std::runtime_error(
          "VeloxPlanConverter: retrieval merge field is absent: " +
          field->name());
    }
    return channel.value();
  };

  size_t score_channel = require_field("score", facebook::velox::TypeKind::DOUBLE);
  size_t source_id_channel =
      require_field("source_id", facebook::velox::TypeKind::VARCHAR);

  std::vector<size_t> stable_key_channels;
  std::unordered_set<std::string> stable_key_names;
  for (const auto& name : output_contract.stable_key_fields()) {
    auto channel = logical_input->getChildIdxIfExists(name);
    if (!channel.has_value() || !stable_key_names.emplace(name).second ||
        channel.value() == score_channel ||
        channel.value() == source_id_channel) {
      throw std::runtime_error(
          "VeloxPlanConverter: invalid retrieval merge stable key: " + name);
    }
    stable_key_channels.push_back(channel.value());
  }

  std::unordered_map<std::string, double> weights;
  uint64_t rrf_k = 0;
  if (is_rrf) {
    if (invocation.options_size() != 1 ||
        invocation.options(0).name() != "rrf_k" ||
        !invocation.options(0).value().has_int64_value() ||
        invocation.options(0).value().int64_value() <= 0) {
      throw std::runtime_error(
          "VeloxPlanConverter: RRF requires one positive int64 rrf_k option");
    }
    rrf_k = static_cast<uint64_t>(
        invocation.options(0).value().int64_value());
  } else {
    if (invocation.options_size() != detail.inputs_size()) {
      throw std::runtime_error(
          "VeloxPlanConverter: linear merge requires one weight per source");
    }
    for (const auto& option : invocation.options()) {
      constexpr std::string_view prefix = "weight.";
      if (!option.name().starts_with(prefix) ||
          !option.value().has_double_value() ||
          !std::isfinite(option.value().double_value()) ||
          option.value().double_value() <= 0.0) {
        throw std::runtime_error(
            "VeloxPlanConverter: invalid linear retrieval weight option");
      }
      std::string source_id = option.name().substr(prefix.size());
      if (source_id.empty() ||
          !weights.emplace(source_id, option.value().double_value()).second) {
        throw std::runtime_error(
            "VeloxPlanConverter: duplicate or empty linear weight source");
      }
    }
  }

  std::vector<pangrova::retrieval::SourceConfig> sources;
  sources.reserve(detail.inputs_size());
  std::unordered_set<std::string> source_ids;
  for (const auto& source : detail.inputs()) {
    if (source.source_id().empty() ||
        !source_ids.emplace(source.source_id()).second) {
      throw std::runtime_error(
          "VeloxPlanConverter: duplicate or empty retrieval merge source");
    }
    auto source_type = convertNamedStruct(source.contract().schema());
    validateContractSchema(
        source.contract(), logical_input, source_type, "merge input");
    if (source.contract().stable_key_fields_size() !=
            output_contract.stable_key_fields_size() ||
        !std::equal(
            source.contract().stable_key_fields().begin(),
            source.contract().stable_key_fields().end(),
            output_contract.stable_key_fields().begin())) {
      throw std::runtime_error(
          "VeloxPlanConverter: retrieval source stable-key contract drift");
    }
    double weight = 1.0;
    if (is_linear) {
      auto found = weights.find(source.source_id());
      if (found == weights.end()) {
        throw std::runtime_error(
            "VeloxPlanConverter: linear weight missing for source " +
            source.source_id());
      }
      weight = found->second;
    }
    sources.push_back(pangrova::retrieval::SourceConfig{
        source.source_id(),
        toFusionDirection(source.contract().score_direction()),
        weight});
  }
  if (is_linear && weights.size() != source_ids.size()) {
    throw std::runtime_error(
        "VeloxPlanConverter: linear weights contain an unknown source");
  }

  RetrievalMergeConfig config{
      is_rrf ? RetrievalMergeKind::kRrf : RetrievalMergeKind::kLinear,
      score_channel,
      source_id_channel,
      std::move(stable_key_channels),
      std::move(sources),
      detail.output_limit(),
      pangrova::capability::kRetrievalMergeMaxCandidateRows,
      pangrova::capability::kRetrievalMergeMaxCandidateBytes,
      detail.max_fan_in(),
      rrf_k};
  return std::make_shared<RetrievalMergeNode>(
      detail.plan_node_id(), std::move(config), std::move(input));
}


}  // namespace pangrova::velox
