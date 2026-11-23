// SubstraitRelConversion: Operator conversion methods for VeloxPlanConverter.
// Split from VeloxPlanConverter.cpp for maintainability.
//
// Contains: convertReadRel, convertFilterRel, convertProjectRel,
// convertAggregateRel, convertHashJoinRel, convertJoinRel,
// convertExchangeRel, convertSortRel, convertFetchRel, convertWindowRel,
// convertSetRel, convertCrossRel, convertSortOrder.
#include "engine/velox/plan/VeloxPlanConverter.h"
#include "StructuralContractIdentities.h"
#include "engine/velox/plan/CanonicalOrdinalIdentity.h"
#include "engine/velox/plan/CandidateContractValidation.h"

#include <algorithm>
#include <charconv>
#include <cmath>
#include <functional>
#include <limits>
#include <unordered_set>

#include <openssl/evp.h>

#include "engine/compat/NativeFeatureDetection.h"
#include "capabilities/capability/NativeCapabilityCatalog.h"
#include "capabilities/artifact/GeneratedValueSegment.h"
#include "capabilities/artifact/ImmutableArtifactCache.h"
#include "capabilities/artifact/RowLocatorSidecar.h"
#include "engine/executor/NativeUnsupportedFeatureException.h"
#include "engine/velox/connector/PangrovaTableHandle.h"
#include "engine/velox/connector/PangrovaVeloxConnector.h"
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
#include "engine/velox/plan/TableFunctionNode.h"
#include "engine/velox/plan/RetrievalMergeOperator.h"
#include "engine/velox/plan/ArtifactBuildOperator.h"
#include "engine/velox/plan/GeneratedValuesOverlayOperator.h"
#include "engine/velox/runtime/NativeEngineLifecycle.h"
#include "engine/velox/runtime/TableFunctionRegistry.h"

namespace pangrova::velox {

namespace {

std::string resolveExchangeSerde(
    pangrova::protocol::v2::BatchSerde batch_serde) {
  if (batch_serde != pangrova::protocol::v2::BATCH_SERDE_ARROW_BATCH) {
    throw std::runtime_error("VeloxPlanConverter: exchange requires canonical Arrow batch serde");
  }
  return "ArrowBatch";
}

void validateExchangeInputDetail(
    const pangrova::protocol::v2::ExchangeInputDetail& detail) {
  if (detail.plan_node_id().empty() || detail.exchange_id().empty()) {
    throw std::runtime_error(
        "VeloxPlanConverter: exchange IDs must not be empty");
  }
  if (detail.upstream_stage_id() < 0 || detail.downstream_stage_id() < 0 ||
      detail.upstream_stage_id() == detail.downstream_stage_id()) {
    throw std::runtime_error(
        "VeloxPlanConverter: exchange stage IDs are invalid");
  }
  if (detail.partition_count() == 0) {
    throw std::runtime_error(
        "VeloxPlanConverter: exchange partition count is unbound");
  }
  if (detail.distribution() ==
      pangrova::protocol::v2::EXCHANGE_DISTRIBUTION_UNSPECIFIED) {
    throw std::runtime_error(
        "VeloxPlanConverter: exchange distribution must be explicitly bound");
  }
  if (detail.distribution() ==
          pangrova::protocol::v2::EXCHANGE_DISTRIBUTION_SINGLETON &&
      detail.partition_count() != 1) {
    throw std::runtime_error(
        "VeloxPlanConverter: singleton exchange must have one partition");
  }
  if (!detail.require_complete()) {
    throw std::runtime_error(
        "VeloxPlanConverter: implicit partial exchange input is forbidden");
  }
  const auto& schema = detail.output_schema();
  if (!detail.has_output_schema() || !schema.has_struct_()) {
    throw std::runtime_error(
        "VeloxPlanConverter: exchange output schema must be explicitly bound");
  }
}

bool isCompleteSingletonExchangeInput(const substrait::Rel& rel) {
  if (!rel.has_extension_leaf() || !rel.extension_leaf().has_detail()) {
    return false;
  }
  pangrova::protocol::v2::RelationDetailEnvelope envelope;
  if (!rel.extension_leaf().detail().UnpackTo(&envelope) ||
      envelope.schema_version() != 1 || !envelope.has_exchange_input()) {
    return false;
  }
  const auto& input = envelope.exchange_input();
  return input.distribution() ==
          pangrova::protocol::v2::EXCHANGE_DISTRIBUTION_SINGLETON &&
      input.partition_count() == 1 && input.require_complete();
}


}  // namespace

// ---------------------------------------------------------------------------
// ReadRel → TableScanNode / ValuesNode
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// ExtensionLeafRel → typed native source.
// ---------------------------------------------------------------------------

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convertExtensionLeafRel(
    const substrait::ExtensionLeafRel& ext_rel) {
  pangrova::protocol::v2::RelationDetailEnvelope envelope;
  if (!ext_rel.has_detail() || !ext_rel.detail().UnpackTo(&envelope)) {
    throw std::runtime_error(
        "VeloxPlanConverter: extension leaf requires a typed detail envelope");
  }
  if (envelope.schema_version() != 1) {
    throw std::runtime_error(
        "VeloxPlanConverter: unsupported relation detail schema version: " +
        std::to_string(envelope.schema_version()));
  }
  if (envelope.has_table_function()) {
    return convertTableFunctionDetail(envelope.table_function());
  }
  if (!envelope.has_exchange_input()) {
    throw std::runtime_error(
        "VeloxPlanConverter: unsupported typed extension leaf detail: " +
        std::to_string(envelope.detail_case()));
  }

  const auto& detail = envelope.exchange_input();
  validateExchangeInputDetail(detail);
  auto output_type = physicalRowType(convertNamedStruct(detail.output_schema()));
  auto serde = resolveExchangeSerde(detail.batch_serde());
  return std::make_shared<facebook::velox::core::ExchangeNode>(
      detail.plan_node_id(), std::move(output_type), std::move(serde));
}

// ---------------------------------------------------------------------------
// ExtensionSingleRel → typed Pangrova relation or UnnestNode.
// ---------------------------------------------------------------------------

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convertExtensionSingleRel(
    const substrait::ExtensionSingleRel& ext_rel,
    const std::vector<split::SplitConfig>& splits) {
  pangrova::protocol::v2::RelationDetailEnvelope envelope;
  const bool has_typed_detail =
      ext_rel.has_detail() && ext_rel.detail().UnpackTo(&envelope);
  if (has_typed_detail && envelope.has_table_function() &&
      envelope.table_function().invocation().capability().capability_id() ==
          pangrova::capability::generated::pangrova_table_generate_series.id) {
    throw std::runtime_error(
        "GENERATE_SERIES lateral input is not a declared SQL capability");
  }
  const std::vector<split::SplitConfig>* effective_splits = &splits;
  bool has_retrieval_override = false;
  if (has_typed_detail && envelope.has_retrieval_source()) {
    auto found = retrieval_split_overrides_.find(
        envelope.retrieval_source().plan_node_id());
    if (found != retrieval_split_overrides_.end()) {
      effective_splits = &found->second;
      has_retrieval_override = true;
    }
  }
  std::unordered_set<std::string> existing_scan_ids;
  if (has_retrieval_override) {
    for (const auto& [scan_id, output_type] : scan_node_output_types_) {
      (void)output_type;
      existing_scan_ids.emplace(scan_id);
    }
  }
  auto input = convertRel(ext_rel.input(), *effective_splits);
  if (has_retrieval_override) {
    for (const auto& [scan_id, output_type] : scan_node_output_types_) {
      (void)output_type;
      if (!existing_scan_ids.contains(scan_id)) {
        scan_node_split_overrides_.emplace(scan_id, *effective_splits);
      }
    }
  }
  auto input_type = std::dynamic_pointer_cast<const facebook::velox::RowType>(
      input->outputType());

  if (has_typed_detail) {
    if (envelope.schema_version() != 1) {
      throw std::runtime_error(
          "VeloxPlanConverter: unsupported relation detail schema version: " +
          std::to_string(envelope.schema_version()));
    }
    if (envelope.has_retrieval_source()) {
      return convertTopKRetrieval(
          envelope.retrieval_source(), input, input_type, *effective_splits);
    }
    if (envelope.has_retrieval_merge()) {
      if (!isCompleteSingletonExchangeInput(ext_rel.input())) {
        throw std::runtime_error(
            "VeloxPlanConverter: retrieval merge must consume a complete "
            "singleton exchange input");
      }
      return convertRetrievalMerge(
          envelope.retrieval_merge(), input, input_type);
    }
    if (envelope.has_rerank()) {
      return convertRerank(envelope.rerank(), input, input_type);
    }
    if (envelope.has_enforce_single_row()) {
      const auto ordinal = envelope.enforce_single_row().input_ordinal();
      if (!input_type || ordinal >= input_type->size()) {
        throw std::runtime_error("Single-row value is outside the input schema");
      }
      if (input_type->size() != 1) {
        auto field = std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
            input_type->childAt(ordinal), input_type->nameOf(ordinal));
        input = std::make_shared<facebook::velox::core::ProjectNode>(nextNodeId(),
            std::vector<std::string>{input_type->nameOf(ordinal)},
            std::vector<facebook::velox::core::TypedExprPtr>{std::move(field)}, std::move(input));
      }
      return std::make_shared<facebook::velox::core::EnforceSingleRowNode>(
          nextNodeId(), insertLocalPartitionExchange(std::move(input), {}));
    }
    if (envelope.has_artifact_build()) {
      return convertArtifactBuild(
          envelope.artifact_build(), input, input_type);
    }
    if (envelope.has_table_function() &&
        pangrova::capability::isGeneratedValuesOverlay(
            envelope.table_function().invocation().capability())) {
      return convertGeneratedValuesOverlay(
          envelope.table_function(), input, input_type);
    }
    if (envelope.has_table_function()) {
      const auto& capability_id =
          envelope.table_function().invocation().capability().capability_id();
      if (capability_id ==
          pangrova::capability::generated::pangrova_table_unnest.id) {
        return convertUnnest(envelope.table_function(), input);
      }
      throw std::runtime_error(
          "VeloxPlanConverter: unsupported table function capability: " + capability_id);
    }
    throw std::runtime_error(
        "VeloxPlanConverter: unsupported typed relation detail: " +
        std::to_string(envelope.detail_case()));
  }

  throw std::runtime_error("ExtensionSingleRel requires a canonical typed detail envelope");
}

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convertExchangeRel(
    const substrait::ExchangeRel& exchange_rel,
    const std::vector<split::SplitConfig>& splits) {
  auto input = convertRel(exchange_rel.input(), splits);
  auto output_type = std::dynamic_pointer_cast<const facebook::velox::RowType>(
      input->outputType());

  if (exchange_rel.partition_count() <= 0) {
    throw std::runtime_error(
        "VeloxPlanConverter: ExchangeRel partition_count is unbound; "
        "the stage scheduler must bind it before native execution");
  }
  int num_partitions = exchange_rel.partition_count();
  if (shuffle_partition_count_ != num_partitions || shuffle_output_spec_) {
    throw std::runtime_error(
        "Producer ExchangeRel requires one matching shuffle task binding");
  }

  ShuffleOutputSpec spec;
  spec.partition_count = num_partitions;

  if (exchange_rel.has_scatter_by_fields()) {
    spec.distribution = ShuffleOutputSpec::Distribution::HASH;
    for (const auto& field_ref :
         exchange_rel.scatter_by_fields().fields()) {
      if (field_ref.has_direct_reference() &&
          field_ref.direct_reference().has_struct_field()) {
        int idx = field_ref.direct_reference().struct_field().field();
        if (idx >= 0 &&
            idx < static_cast<int>(output_type->size())) {
          spec.key_indices.push_back(idx);
          continue;
        }
      }
      throw std::runtime_error(
          "Producer ExchangeRel has an invalid hash partition key");
    }
    if (spec.key_indices.empty()) {
      throw std::runtime_error(
          "VeloxPlanConverter: scatter ExchangeRel requires partition keys");
    }
  } else if (exchange_rel.has_broadcast()) {
    spec.distribution = ShuffleOutputSpec::Distribution::BROADCAST;
  } else if (exchange_rel.has_round_robin()) {
    spec.distribution = ShuffleOutputSpec::Distribution::ROUND_ROBIN;
  } else {
    throw std::runtime_error(
        "VeloxPlanConverter: unsupported ExchangeRel distribution kind");
  }
  shuffle_output_spec_ = std::move(spec);
  // Velox PartitionedOutput would bypass the Native callback and strand the
  // canonical Flight shuffle queue. The callback is the sole producer sink.
  return input;
}

// ---------------------------------------------------------------------------
// SortOrder conversion helper
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------

}  // namespace pangrova::velox
