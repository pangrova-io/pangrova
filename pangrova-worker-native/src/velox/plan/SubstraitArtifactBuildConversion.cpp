// Typed convertArtifactBuild contract conversion.
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

#include "capability/LogicalTypeContract.h"

namespace pangrova::velox {

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convertArtifactBuild(
    const pangrova::protocol::v2::ArtifactBuildDetail& detail,
    std::shared_ptr<const facebook::velox::core::PlanNode> input,
    const facebook::velox::RowTypePtr& input_type) {
  using namespace pangrova::protocol::v2;
  const auto& logical_input = logicalNodeType(input);
  if (!artifact_build_context_ || !artifact_build_context_->report_state ||
      artifact_build_context_->job_id.empty() ||
      artifact_build_context_->page_id.empty() ||
      artifact_build_context_->fencing_token == 0 ||
      artifact_build_context_->scratch_directory.empty() ||
      artifact_build_context_->max_input_bytes == 0 ||
      artifact_build_context_->max_spill_bytes == 0 ||
      artifact_build_context_->max_object_store_read_bytes == 0 ||
      artifact_build_context_->max_object_store_write_bytes == 0) {
    throw std::runtime_error(
        "VeloxPlanConverter: artifact build runtime context is incomplete");
  }
  const bool is_diskann = pangrova::capability::isDiskAnnArtifactBuild(
      detail.invocation().capability());
  const bool is_hnsw = pangrova::capability::isHnswArtifactBuild(
      detail.invocation().capability());
  const bool is_ivf_pq = pangrova::capability::isIvfPqArtifactBuild(
      detail.invocation().capability());
  const bool is_bm25 = pangrova::capability::isBm25ArtifactBuild(
      detail.invocation().capability());
  const bool is_generated_values =
      pangrova::capability::isGeneratedValuesArtifactBuild(
          detail.invocation().capability());
  const bool is_tabular_export = pangrova::capability::isTabularExport(
      detail.invocation().capability());
  const bool is_vector = is_diskann || is_hnsw || is_ivf_pq;
  if (detail.plan_node_id().empty() || detail.report_schema_version() != 2 ||
      detail.invocation().options_size() != 0 ||
      (!is_vector && !is_bm25 && !is_generated_values &&
       !is_tabular_export) ||
      (is_tabular_export &&
       (static_cast<size_t>(detail.input_fields_size()) != input_type->size() ||
        detail.input_fields_size() == 0 ||
        detail.invocation().arguments_size() != 0)) ||
      (!is_tabular_export &&
       (detail.input_fields_size() != 2 ||
        detail.invocation().arguments_size() != 2))) {
    throw std::runtime_error(
        "VeloxPlanConverter: invalid artifact build relation");
  }
  const auto& binding = requireOperatorBinding(
      detail.plan_node_id(), detail.invocation().capability());
  if (!binding.has_output_artifact() || binding.artifacts_size() > 1 ||
      binding.has_model() ||
      !binding.has_execution_policy() ||
      binding.execution_policy().coverage_policy() !=
          COVERAGE_POLICY_REQUIRE_COMPLETE ||
      binding.execution_policy().fallback_policy() != FALLBACK_POLICY_FORBID ||
      binding.execution_policy().retry_class() != RETRY_CLASS_IDEMPOTENT ||
      binding.execution_policy().has_approximation() ||
      binding.execution_policy().idempotency_key().empty()) {
    throw std::runtime_error(
        "VeloxPlanConverter: artifact output binding policy mismatch");
  }
  const auto& output = binding.output_artifact();
  const bool source_map = output.work_kind() ==
      ARTIFACT_WORK_KIND_SOURCE_MAP;
  const bool dependency_reduce = output.work_kind() ==
      ARTIFACT_WORK_KIND_DEPENDENCY_REDUCE;
  const bool generation_finalize = output.work_kind() ==
      ARTIFACT_WORK_KIND_GENERATION_FINALIZE;
  const bool relational_partition = output.work_kind() ==
      ARTIFACT_WORK_KIND_RELATIONAL_PARTITION;
  const auto expected_format = is_diskann
      ? pangrova::capability::diskAnnArtifactFormat()
      : is_hnsw
      ? pangrova::capability::hnswArtifactFormat()
      : is_ivf_pq
      ? pangrova::capability::ivfPqArtifactFormat()
      : is_generated_values
      ? pangrova::capability::generatedValuesArtifactFormat()
      : is_tabular_export
      ? pangrova::capability::tabularExportFormat()
      : pangrova::capability::bm25ArtifactFormat();
  const auto expected_locator =
      pangrova::capability::physicalRowLocatorContract();
  if (output.publication_fencing_token() !=
          artifact_build_context_->fencing_token ||
      output.work_unit_id().empty() ||
      output.idempotency_key() !=
          binding.execution_policy().idempotency_key() ||
      (!source_map && !dependency_reduce && !generation_finalize &&
       !relational_partition) ||
      (source_map && (output.expected_source_object_ids_size() != 1 ||
                      !output.output_scope_id().empty() ||
                      binding.artifacts_size() != 0)) ||
      ((dependency_reduce || generation_finalize) &&
                      (output.expected_source_object_ids_size() != 0 ||
                       output.output_scope_id().empty() ||
                       binding.artifacts_size() != 1 || !is_bm25)) ||
      (relational_partition &&
       (!is_tabular_export || output.expected_source_object_ids_size() != 0 ||
        output.output_scope_id() != output.work_unit_id() ||
        binding.artifacts_size() != 0 || output.has_row_locator())) ||
      (is_generated_values &&
       output.artifact_type() != "GENERATED_COLUMN_VALUES") ||
      (is_tabular_export && (output.artifact_type() != "TABULAR_EXPORT" ||
                            !relational_partition)) ||
      (dependency_reduce &&
       output.output_scope_id() != output.work_unit_id()) ||
      (generation_finalize &&
       output.output_scope_id() != output.generation()) ||
      !output.encryption_key_ref().empty() ||
      output.format().format_id() != expected_format.format_id() ||
      output.format().major_version() != expected_format.major_version() ||
      output.format().minor_version() != expected_format.minor_version() ||
      output.format().schema_sha256() != expected_format.schema_sha256() ||
      output.format().writer_build_id() != expected_format.writer_build_id() ||
      (!is_tabular_export && (!output.has_row_locator() ||
      output.row_locator().schema_version() !=
          expected_locator.schema_version() ||
      output.row_locator().kind() != expected_locator.kind() ||
      output.row_locator().key_types_size() != 0 ||
      output.row_locator().schema_sha256() !=
          expected_locator.schema_sha256() ||
      output.row_locator().source_object_scoped() !=
          expected_locator.source_object_scoped()))) {
    throw std::runtime_error(
        "VeloxPlanConverter: artifact format, locator, fencing or encryption mismatch");
  }

  std::unordered_map<std::string, size_t> channels;
  if (is_tabular_export) {
    std::vector<std::string> names;
    std::unordered_set<std::string> unique_names;
    std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>> projections;
    bool renamed = false;
    for (size_t field = 0; field < input_type->size(); ++field) {
      const auto& name = detail.input_fields(static_cast<int>(field));
      if (name.empty() || !unique_names.emplace(name).second) {
        throw std::runtime_error(
            "VeloxPlanConverter: tabular export requires unique non-empty output names");
      }
      names.push_back(name);
      renamed |= name != input_type->nameOf(field);
      projections.push_back(std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
          input_type->childAt(field), input_type->nameOf(field)));
    }
    // RelRoot aliases belong to the exported relation, not the internal
    // expression names (e.g. proj_2 for a mask). Rename by ordinal, preserving
    // both types and values, before the sink captures its Arrow schema.
    if (renamed) {
      input = std::make_shared<facebook::velox::core::ProjectNode>(
          nextNodeId(), std::move(names), std::move(projections), std::move(input));
    }
  }
  for (const auto& argument : detail.invocation().arguments()) {
    const bool supported_argument = argument.name() == "row_position" ||
        (is_vector && argument.name() == "vector") ||
        (is_bm25 && argument.name() == "text") ||
        (is_generated_values && argument.name() == "generated_value");
    if (!supported_argument ||
        !argument.expression().has_selection() ||
        !argument.expression().selection().has_direct_reference() ||
        !argument.expression().selection().direct_reference().has_struct_field() ||
        argument.expression().selection().direct_reference().struct_field().has_child()) {
      throw std::runtime_error(
          "VeloxPlanConverter: artifact arguments must be direct input fields");
    }
    convertExpression(argument.expression(), input_type);
    const int32_t field = argument.expression()
                              .selection()
                              .direct_reference()
                              .struct_field()
                              .field();
    if (field < 0 || static_cast<size_t>(field) >= input_type->size() ||
        !channels.emplace(argument.name(), static_cast<size_t>(field)).second) {
      throw std::runtime_error(
          "VeloxPlanConverter: artifact argument field is invalid or duplicated");
    }
  }
  const std::string value_argument = is_vector
      ? "vector"
      : is_generated_values ? "generated_value" : "text";
  if (!is_tabular_export &&
      (!channels.contains("row_position") || !channels.contains(value_argument) ||
       detail.input_fields(0) !=
           logical_input->nameOf(channels.at("row_position")) ||
       detail.input_fields(1) != logical_input->nameOf(channels.at(value_argument)) ||
       logical_input->nameOf(channels.at("row_position")) !=
           kPhysicalRowPositionColumnName ||
       input_type->childAt(channels.at("row_position"))->kind() !=
           facebook::velox::TypeKind::BIGINT)) {
    throw std::runtime_error(
        "VeloxPlanConverter: artifact build requires the Native physical row-position BIGINT");
  }
  if (is_vector &&
      (input_type->childAt(channels.at("vector"))->kind() !=
           facebook::velox::TypeKind::ARRAY ||
       input_type->childAt(channels.at("vector"))->childAt(0)->kind() !=
           facebook::velox::TypeKind::REAL)) {
    throw std::runtime_error(
        "VeloxPlanConverter: ANN build requires an ARRAY(REAL) vector");
  }
  if (is_generated_values &&
      (input_type->childAt(channels.at("generated_value"))->kind() !=
           facebook::velox::TypeKind::ARRAY ||
       input_type->childAt(channels.at("generated_value"))->childAt(0)->kind() !=
           facebook::velox::TypeKind::REAL)) {
    throw std::runtime_error(
        "VeloxPlanConverter: generated-value build requires an ARRAY(REAL) value");
  }
  if (is_bm25 && input_type->childAt(channels.at("text"))->kind() !=
          facebook::velox::TypeKind::VARCHAR) {
    throw std::runtime_error(
        "VeloxPlanConverter: BM25 build requires a VARCHAR text input");
  }

  vector::diskann::VamanaConfig diskann;
  vector::hnsw::HnswConfig hnsw;
  vector::ivfpq::IvfPqConfig ivf_pq;
  bool has_dimension = false;
  bool has_ann_metric = false;
  uint32_t generated_value_dimension = 0;
  bool has_target_column = false;
  bool has_output_type = false;
  uint32_t generated_output_dimension = 0;
  bool has_source_encoding = false;
  bool has_execution_mode = false;
  std::unordered_set<size_t> source_columns;
  std::unordered_set<std::string> property_names;
  for (const auto& property : output.properties()) {
    const auto& value = property.value();
    if (!property_names.emplace(property.name()).second) {
      throw std::runtime_error(
          "VeloxPlanConverter: artifact property is duplicated: " +
          property.name());
    }
    if (is_vector && property.name() == "dimension") {
      if (!value.has_int64_value()) {
        throw std::runtime_error("ANN dimension must be INT64");
      }
      diskann.dimension = value.int64_value();
      hnsw.dimension = value.int64_value();
      ivf_pq.dimension = value.int64_value();
      has_dimension = true;
    } else if (is_vector && property.name() == "metric") {
      if (!value.has_string_value()) {
        throw std::runtime_error("ANN metric must be VARCHAR");
      }
      if (is_diskann) {
        if (value.string_value() != "L2") {
          throw std::runtime_error("DiskANN metric must be L2");
        }
      } else {
        hnsw.metric = vector::distanceMetricFromName(value.string_value());
        ivf_pq.metric = hnsw.metric;
      }
      has_ann_metric = true;
    } else if (is_diskann && property.name() == "max_degree") {
      if (!value.has_int64_value()) {
        throw std::runtime_error("DiskANN max_degree must be INT64");
      }
      diskann.maxDegree = value.int64_value();
    } else if (is_diskann && property.name() == "search_list_size") {
      if (!value.has_int64_value()) {
        throw std::runtime_error("DiskANN search_list_size must be INT64");
      }
      diskann.searchListSize = value.int64_value();
    } else if (is_diskann && property.name() == "alpha") {
      if (!value.has_double_value()) {
        throw std::runtime_error("DiskANN alpha must be DOUBLE");
      }
      diskann.alpha = value.double_value();
    } else if ((is_diskann || is_ivf_pq) &&
               property.name() == "pq_subvectors") {
      if (!value.has_int64_value()) {
        throw std::runtime_error("DiskANN pq_subvectors must be INT64");
      }
      diskann.pqSubvectors = value.int64_value();
      ivf_pq.pq_subvectors = value.int64_value();
    } else if ((is_diskann || is_ivf_pq) &&
               property.name() == "pq_bits_per_code") {
      if (!value.has_int64_value()) {
        throw std::runtime_error("DiskANN pq_bits_per_code must be INT64");
      }
      diskann.pqBitsPerCode = value.int64_value();
      ivf_pq.bits_per_code = value.int64_value();
    } else if (is_diskann && property.name() == "beam_width") {
      if (!value.has_int64_value()) {
        throw std::runtime_error("DiskANN beam_width must be INT64");
      }
      diskann.beamWidth = value.int64_value();
    } else if (is_hnsw && property.name() == "max_connections") {
      if (!value.has_int64_value()) {
        throw std::runtime_error("HNSW max_connections must be INT64");
      }
      hnsw.max_connections = value.int64_value();
    } else if (is_hnsw && property.name() == "ef_construction") {
      if (!value.has_int64_value()) {
        throw std::runtime_error("HNSW ef_construction must be INT64");
      }
      hnsw.ef_construction = value.int64_value();
    } else if (is_ivf_pq && property.name() == "coarse_clusters") {
      if (!value.has_int64_value()) {
        throw std::runtime_error("IVF-PQ coarse_clusters must be INT64");
      }
      ivf_pq.coarse_clusters = value.int64_value();
    } else if (is_ivf_pq && property.name() == "training_iterations") {
      if (!value.has_int64_value()) {
        throw std::runtime_error("IVF-PQ training_iterations must be INT64");
      }
      ivf_pq.training_iterations = value.int64_value();
    } else if (is_ivf_pq && property.name() == "training_samples") {
      if (!value.has_int64_value()) {
        throw std::runtime_error("IVF-PQ training_samples must be INT64");
      }
      ivf_pq.training_samples = value.int64_value();
    } else if (is_bm25 && property.name() == "analyzer" &&
               value.has_string_value() &&
               value.string_value() == "standard-v1") {
      continue;
    } else if (is_generated_values && property.name() == "target_column" &&
               value.has_string_value() && !value.string_value().empty()) {
      has_target_column = true;
    } else if (is_generated_values && property.name() == "output_type" &&
               value.has_string_value()) {
      generated_output_dimension =
          capability::requireCanonicalFloatVectorDimension(value.string_value());
      has_output_type = generated_output_dimension != 0;
    } else if (is_generated_values && property.name() == "vector_dimension" &&
               value.has_int64_value() && value.int64_value() > 0 &&
               value.int64_value() <=
                   artifact::GeneratedValueSegment::kMaxDimension) {
      generated_value_dimension = static_cast<uint32_t>(value.int64_value());
      has_dimension = true;
    } else if (is_generated_values && property.name() == "source_encoding" &&
               value.has_string_value() &&
               value.string_value() == "ordered-unit-separator-v1") {
      has_source_encoding = true;
    } else if (is_generated_values && property.name() == "execution_mode" &&
               value.has_string_value() &&
               (value.string_value() == "EXECUTION_MODE_SYNC" ||
                value.string_value() == "EXECUTION_MODE_ASYNC")) {
      has_execution_mode = true;
    } else if (is_generated_values &&
               property.name().starts_with("source_column.") &&
               value.has_string_value() && !value.string_value().empty()) {
      const auto suffix = property.name().substr(
          std::string("source_column.").size());
      size_t consumed = 0;
      size_t ordinal = 0;
      try {
        ordinal = std::stoull(suffix, &consumed);
      } catch (const std::exception&) {
        throw std::runtime_error(
            "VeloxPlanConverter: invalid generated-value source column ordinal");
      }
      if (consumed != suffix.size() ||
          !source_columns.emplace(ordinal).second) {
        throw std::runtime_error(
            "VeloxPlanConverter: invalid generated-value source column ordinal");
      }
    } else if (is_generated_values &&
               property.name().starts_with("definition_option.") &&
               property.name().size() >
                   std::string("definition_option.").size()) {
      continue;
    } else {
      throw std::runtime_error(
          "VeloxPlanConverter: unsupported artifact property: " + property.name());
    }
  }
  if (is_diskann && (!has_dimension || !has_ann_metric ||
      diskann.dimension <= 0 || diskann.dimension > 65'536 ||
      diskann.maxDegree <= 0 || diskann.maxDegree > 512 ||
      diskann.searchListSize < diskann.maxDegree ||
      diskann.searchListSize > 100'000 || diskann.alpha < 1.0 ||
      diskann.alpha > 2.0 || diskann.pqSubvectors <= 0 ||
      diskann.pqSubvectors > diskann.dimension ||
      diskann.dimension % diskann.pqSubvectors != 0 ||
      diskann.pqBitsPerCode <= 0 || diskann.pqBitsPerCode > 8 ||
      diskann.beamWidth <= 0 || diskann.beamWidth > 1'024)) {
    throw std::runtime_error(
        "VeloxPlanConverter: DiskANN build parameters are outside safe bounds");
  }
  if (is_hnsw && (!has_dimension || !has_ann_metric || hnsw.dimension <= 0 ||
      hnsw.dimension > 65'536 || hnsw.max_connections < 2 ||
      hnsw.max_connections > 512 ||
      hnsw.ef_construction < hnsw.max_connections ||
      hnsw.ef_construction > 100'000 || output.properties_size() != 4)) {
    throw std::runtime_error(
        "VeloxPlanConverter: HNSW build parameters are outside safe bounds");
  }
  if (is_ivf_pq && (!has_dimension || !has_ann_metric || ivf_pq.dimension <= 0 ||
      ivf_pq.dimension > 65'536 || ivf_pq.coarse_clusters <= 0 ||
      ivf_pq.coarse_clusters > 4'096 || ivf_pq.pq_subvectors <= 0 ||
      ivf_pq.pq_subvectors > 1'024 ||
      ivf_pq.dimension % ivf_pq.pq_subvectors != 0 ||
      ivf_pq.bits_per_code <= 0 || ivf_pq.bits_per_code > 8 ||
      ivf_pq.training_iterations <= 0 || ivf_pq.training_iterations > 100 ||
      ivf_pq.training_samples < 256 || ivf_pq.training_samples > 1'000'000 ||
      output.properties_size() != 7)) {
    throw std::runtime_error(
        "VeloxPlanConverter: IVF-PQ build parameters are outside safe bounds");
  }
  if (is_bm25 && (output.properties_size() != 1 ||
               output.properties(0).name() != "analyzer" ||
               !output.properties(0).value().has_string_value() ||
               output.properties(0).value().string_value() != "standard-v1")) {
    throw std::runtime_error(
        "VeloxPlanConverter: BM25 build requires analyzer=standard-v1");
  }
  if (is_generated_values &&
      (!has_target_column || !has_output_type || !has_dimension ||
       generated_output_dimension != generated_value_dimension ||
       !has_source_encoding || !has_execution_mode || source_columns.empty())) {
    throw std::runtime_error(
        "VeloxPlanConverter: generated-value build properties are incomplete");
  }
  if (is_tabular_export && output.properties_size() != 0) {
    throw std::runtime_error(
        "VeloxPlanConverter: tabular export does not accept artifact properties");
  }
  if (is_generated_values) {
    for (size_t ordinal = 0; ordinal < source_columns.size(); ++ordinal) {
      if (!source_columns.contains(ordinal)) {
        throw std::runtime_error(
            "VeloxPlanConverter: generated-value source columns are not contiguous");
      }
    }
  }

  ArtifactBuildConfig config;
  if (is_diskann) {
    config.kind = ArtifactBuildKind::DISKANN;
  } else if (is_hnsw) {
    config.kind = ArtifactBuildKind::HNSW;
  } else if (is_ivf_pq) {
    config.kind = ArtifactBuildKind::IVF_PQ;
  } else if (is_generated_values) {
    config.kind = ArtifactBuildKind::GENERATED_VALUES;
  } else if (is_tabular_export) {
    config.kind = ArtifactBuildKind::TABULAR_EXPORT;
  } else if (dependency_reduce) {
    config.kind = ArtifactBuildKind::BM25_REDUCE;
  } else if (generation_finalize) {
    config.kind = ArtifactBuildKind::BM25_FINALIZE;
  } else {
    config.kind = ArtifactBuildKind::BM25;
  }
  config.output = output;
  if (dependency_reduce || generation_finalize) {
    const auto& dependency = binding.artifacts(0);
    if (dependency.artifact_id() != output.artifact_id() ||
        dependency.artifact_type() != output.artifact_type() ||
        dependency.generation() != output.generation() ||
        dependency.source_id() != output.source_id() ||
        dependency.source_snapshot() != output.source_snapshot() ||
        dependency.format().SerializeAsString() !=
            output.format().SerializeAsString() ||
        dependency.split_objects_size() == 0 ||
        dependency.split_objects_size() > 64) {
      throw std::runtime_error(
          "VeloxPlanConverter: artifact reduction input identity is invalid");
    }
    config.input_objects.assign(
        dependency.split_objects().begin(), dependency.split_objects().end());
  }
  config.task = *artifact_build_context_;
  config.task.page_id = output.work_unit_id();
  if (!is_tabular_export) {
    config.row_position_channel = channels.at("row_position");
  }
  if (is_diskann) {
    config.vector_channel = channels.at("vector");
    config.max_rows = pangrova::capability::kDiskAnnArtifactMaxRows;
    config.diskann = diskann;
  } else if (is_hnsw) {
    config.vector_channel = channels.at("vector");
    config.max_rows = pangrova::capability::kHnswArtifactMaxRows;
    config.hnsw = hnsw;
  } else if (is_ivf_pq) {
    config.vector_channel = channels.at("vector");
    config.max_rows = pangrova::capability::kIvfPqArtifactMaxRows;
    config.ivf_pq = ivf_pq;
  } else if (is_generated_values) {
    config.generated_value_channel = channels.at("generated_value");
    config.generated_value_dimension = generated_value_dimension;
    config.max_rows = pangrova::capability::kGeneratedValuesArtifactMaxRows;
  } else if (is_tabular_export) {
    config.max_rows = pangrova::capability::kTabularExportMaxRows;
  } else {
    config.text_channel = channels.at("text");
    config.max_rows = pangrova::capability::kBm25ArtifactMaxRows;
  }
  return std::make_shared<ArtifactBuildNode>(
      detail.plan_node_id(), std::move(config), std::move(input));
}


}  // namespace pangrova::velox
