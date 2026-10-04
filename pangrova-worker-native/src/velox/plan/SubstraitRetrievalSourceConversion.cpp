// Canonical typed source retrieval contract conversion.
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

const substrait::Expression& requireExactTopKArgument(
    const pangrova::protocol::v2::CapabilityInvocation& invocation,
    const std::string& name) {
  const substrait::Expression* result = nullptr;
  for (const auto& argument : invocation.arguments()) {
    if (argument.name() != "score") {
      throw std::runtime_error(
          "VeloxPlanConverter: unsupported exact Top-K argument: " +
          argument.name());
    }
    if (argument.name() == name) {
      if (result != nullptr) {
        throw std::runtime_error(
            "VeloxPlanConverter: duplicate exact Top-K argument: " + name);
      }
      result = &argument.expression();
    }
  }
  if (result == nullptr) {
    throw std::runtime_error(
        "VeloxPlanConverter: missing exact Top-K argument: " + name);
  }
  return *result;
}

}  // namespace

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convertTopKRetrieval(
    const pangrova::protocol::v2::RetrievalSourceDetail& detail,
    std::shared_ptr<const facebook::velox::core::PlanNode> input,
    const facebook::velox::RowTypePtr& input_type) {
  const auto& logical_input = logicalNodeType(input);
  const bool exact = pangrova::capability::isExactTopKRetrieval(
      detail.invocation().capability());
  const bool diskann = pangrova::capability::isDiskAnnRetrieval(
      detail.invocation().capability());
  const bool hnsw = pangrova::capability::isHnswRetrieval(
      detail.invocation().capability());
  const bool ivf_pq = pangrova::capability::isIvfPqRetrieval(
      detail.invocation().capability());
  const bool bm25 = pangrova::capability::isBm25Retrieval(
      detail.invocation().capability());
  const bool ann = diskann || hnsw || ivf_pq;
  if (!exact && !ann && !bm25) {
    throw std::runtime_error(
        "VeloxPlanConverter: unsupported or mismatched retrieval capability: " +
        detail.invocation().capability().capability_id());
  }
  if (detail.plan_node_id().empty() || detail.source_id().empty()) {
    throw std::runtime_error(
        "VeloxPlanConverter: retrieval plan_node_id and source_id are required");
  }
  const auto& operator_binding = requireOperatorBinding(
      detail.plan_node_id(), detail.invocation().capability());
  const auto& execution_policy = operator_binding.execution_policy();
  if (operator_binding.has_model() || operator_binding.policies_size() != 0 ||
      execution_policy.coverage_policy() !=
          pangrova::protocol::v2::COVERAGE_POLICY_REQUIRE_COMPLETE ||
      execution_policy.fallback_policy() !=
          pangrova::protocol::v2::FALLBACK_POLICY_FORBID ||
      execution_policy.retry_class() !=
          pangrova::protocol::v2::RETRY_CLASS_IDEMPOTENT ||
      !execution_policy.has_approximation() ||
      execution_policy.approximation().candidate_limit() !=
          detail.candidate_limit() ||
      execution_policy.approximation().exact_rescore_limit() !=
          (exact ? detail.candidate_limit() : detail.exact_rescore_limit()) ||
      (exact &&
       (execution_policy.approximation().required_recall_at_k() != 1.0 ||
        execution_policy.approximation().recall_k() != detail.candidate_limit())) ||
      (ann &&
       (execution_policy.approximation().required_recall_at_k() <= 0.0 ||
        execution_policy.approximation().required_recall_at_k() > 1.0 ||
        execution_policy.approximation().recall_k() == 0 ||
        execution_policy.approximation().recall_k() > detail.candidate_limit())) ||
      (bm25 &&
       (execution_policy.approximation().required_recall_at_k() != 1.0 ||
        execution_policy.approximation().recall_k() != detail.candidate_limit()))) {
    throw std::runtime_error(
        "VeloxPlanConverter: retrieval OperatorBinding policy mismatch");
  }
  if (detail.candidate_limit() == 0 ||
      detail.candidate_limit() >
          pangrova::capability::kExactTopKMaxCandidateRows ||
      (exact && detail.exact_rescore_limit() != 0) ||
      ((ann || bm25) &&
       (detail.exact_rescore_limit() == 0 ||
        detail.exact_rescore_limit() > detail.candidate_limit()))) {
    throw std::runtime_error(
        "VeloxPlanConverter: invalid retrieval candidate or rescore limit");
  }
  if (exact && operator_binding.artifacts_size() != 0) {
    throw std::runtime_error(
        "VeloxPlanConverter: exact Top-K must not bind artifacts");
  }
  if (ann || bm25) {
    const auto expected_format = diskann
        ? pangrova::capability::diskAnnArtifactFormat()
        : hnsw
        ? pangrova::capability::hnswArtifactFormat()
        : ivf_pq
        ? pangrova::capability::ivfPqArtifactFormat()
        : pangrova::capability::bm25ArtifactFormat();
    if (operator_binding.artifacts_size() != 1 ||
        operator_binding.artifacts(0).artifact_type() !=
            (ann ? "VECTOR_INDEX" : "TEXT_INDEX") ||
        operator_binding.artifacts(0).source_id() != detail.source_id() ||
        operator_binding.artifacts(0).format().SerializeAsString() !=
            expected_format.SerializeAsString() ||
        operator_binding.artifacts(0).row_locator().SerializeAsString() !=
            pangrova::capability::physicalRowLocatorContract().SerializeAsString()) {
      throw std::runtime_error(
          "VeloxPlanConverter: retrieval artifact binding is incomplete or mismatched");
    }
  }
  const auto& contract = detail.output_contract();
  if (contract.schema_version() !=
          pangrova::capability::kRetrievalCandidateSchemaVersion ||
      contract.score_direction() ==
          pangrova::protocol::v2::SCORE_DIRECTION_UNSPECIFIED ||
      contract.stable_key_fields_size() == 0) {
    throw std::runtime_error(
        "VeloxPlanConverter: invalid exact Top-K candidate contract");
  }

  const auto& invocation = detail.invocation();
  if (invocation.arguments_size() != 1 ||
      (exact && invocation.options_size() != 0) ||
      (ann && invocation.options_size() != 3) ||
      (bm25 && invocation.options_size() != 4)) {
    throw std::runtime_error(
        "VeloxPlanConverter: retrieval arguments or options are invalid");
  }
  if (ann) {
    std::unordered_set<std::string> option_names;
    for (const auto& option : invocation.options()) {
      if (!option_names.emplace(option.name()).second) {
        throw std::runtime_error(
            "VeloxPlanConverter: duplicate ANN retrieval option");
      }
    }
    const std::string search_parameter = diskann
        ? "search_list_size"
        : hnsw ? "ef_search" : "nprobes";
    if (!option_names.contains("query_vector") ||
        !option_names.contains("metric") ||
        !option_names.contains(search_parameter)) {
      throw std::runtime_error(
          "VeloxPlanConverter: ANN retrieval options are incomplete");
    }
  }
  if (bm25) {
    std::unordered_set<std::string> option_names;
    for (const auto& option : invocation.options()) {
      if (!option_names.emplace(option.name()).second) {
        throw std::runtime_error(
            "VeloxPlanConverter: duplicate BM25 retrieval option");
      }
    }
    if (!option_names.contains("query_text") ||
        !option_names.contains("analyzer") ||
        !option_names.contains("k1") || !option_names.contains("b")) {
      throw std::runtime_error(
          "VeloxPlanConverter: BM25 retrieval options are incomplete");
    }
  }
  auto score_expression = convertExpression(
      requireExactTopKArgument(invocation, "score"), input_type);
  auto score_field = std::dynamic_pointer_cast<
      const facebook::velox::core::FieldAccessTypedExpr>(score_expression);
  if (!score_field || score_field->type()->kind() !=
          facebook::velox::TypeKind::DOUBLE) {
    throw std::runtime_error(
        "VeloxPlanConverter: exact Top-K score must be a DOUBLE field");
  }

  auto declared_type = convertNamedStruct(contract.schema());
  validateContractSchema(contract, logical_input, declared_type, "source");

  std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>>
      sorting_keys = {score_field};
  std::vector<facebook::velox::core::SortOrder> sorting_orders = {
      facebook::velox::core::SortOrder(
          contract.score_direction() ==
              pangrova::protocol::v2::SCORE_DIRECTION_LOWER_IS_BETTER,
          true)};
  for (const auto& stable_key : contract.stable_key_fields()) {
    auto index = logical_input->getChildIdxIfExists(stable_key);
    if (!index.has_value() || input_type->nameOf(index.value()) == score_field->name()) {
      throw std::runtime_error(
          "VeloxPlanConverter: invalid candidate stable key: " + stable_key);
    }
    sorting_keys.push_back(
        std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
            input_type->childAt(index.value()), input_type->nameOf(index.value())));
    sorting_orders.emplace_back(true, true);
  }

  int32_t k = static_cast<int32_t>(detail.candidate_limit());

  auto partial_topn = std::make_shared<facebook::velox::core::TopNNode>(
      nextNodeId(),
      sorting_keys,
      sorting_orders,
      k,
      /*isPartial=*/true,
      input);
  const auto gathered = facebook::velox::core::LocalPartitionNode::gather(
      nextNodeId(), {partial_topn});

  return std::make_shared<facebook::velox::core::TopNNode>(
      nextNodeId(),
      sorting_keys,
      sorting_orders,
      k,
      /*isPartial=*/false,
      gathered);
}


}  // namespace pangrova::velox
