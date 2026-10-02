// Typed convertGeneratedValuesOverlay contract conversion.
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

#include "velox/plan/GeneratedVectorShape.h"

namespace pangrova::velox {

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convertGeneratedValuesOverlay(
    const pangrova::protocol::v2::TableFunctionDetail& detail,
    std::shared_ptr<const facebook::velox::core::PlanNode> input,
    const facebook::velox::RowTypePtr& input_type) {
  using namespace pangrova::protocol::v2;
  constexpr size_t kMaxSourceObjectsPerTask = 4'096;
  if (!input || !input_type || detail.plan_node_id().empty() ||
      detail.invocation().arguments_size() != 2 ||
      detail.invocation().options_size() != 0 || !detail.has_output_schema() ||
      !pangrova::capability::isGeneratedValuesOverlay(
          detail.invocation().capability())) {
    throw std::runtime_error(
        "VeloxPlanConverter: invalid generated-values overlay relation");
  }
  const auto& logical_input = logicalNodeType(input);
  const auto& binding = requireOperatorBinding(
      detail.plan_node_id(), detail.invocation().capability());
  if (binding.artifacts_size() != 1 || binding.has_model() ||
      binding.policies_size() != 0 || binding.has_output_artifact() ||
      !binding.has_execution_policy() || !binding.has_resources() ||
      binding.execution_policy().coverage_policy() !=
          COVERAGE_POLICY_REQUIRE_COMPLETE ||
      binding.execution_policy().fallback_policy() != FALLBACK_POLICY_FORBID ||
      binding.execution_policy().retry_class() != RETRY_CLASS_IDEMPOTENT ||
      binding.execution_policy().has_approximation() ||
      binding.resources().fill_keys() == 0 ||
      binding.resources().object_store_read_bytes() == 0 ||
      binding.resources().candidate_rows() != 0 ||
      binding.resources().candidate_bytes() != 0 ||
      binding.resources().provider_concurrency() != 0 ||
      binding.resources().provider_items() != 0 ||
      binding.resources().provider_attempts() != 0 ||
      binding.resources().provider_request_bytes() != 0 ||
      binding.resources().provider_response_bytes() != 0 ||
      binding.resources().media_bytes() != 0 ||
      binding.resources().object_store_write_bytes() != 0) {
    throw std::runtime_error(
        "VeloxPlanConverter: generated-values overlay binding is unsafe");
  }

  std::unordered_map<std::string, size_t> channels;
  for (const auto& argument : detail.invocation().arguments()) {
    if ((argument.name() != "source_object_id" &&
         argument.name() != "row_position") ||
        !argument.expression().has_selection() ||
        !argument.expression().selection().has_direct_reference() ||
        !argument.expression().selection().direct_reference().has_struct_field() ||
        argument.expression().selection().direct_reference().struct_field().has_child()) {
      throw std::runtime_error(
          "VeloxPlanConverter: generated-values overlay arguments must be exact direct fields");
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
          "VeloxPlanConverter: generated-values overlay argument is duplicated or invalid");
    }
  }
  if (!channels.contains("source_object_id") ||
      !channels.contains("row_position") ||
      logical_input->nameOf(channels.at("source_object_id")) !=
          kSourceObjectIdColumnName ||
      input_type->childAt(channels.at("source_object_id"))->kind() !=
          facebook::velox::TypeKind::VARCHAR ||
      logical_input->nameOf(channels.at("row_position")) !=
          kPhysicalRowPositionColumnName ||
      input_type->childAt(channels.at("row_position"))->kind() !=
          facebook::velox::TypeKind::BIGINT) {
    throw std::runtime_error(
        "VeloxPlanConverter: generated-values overlay requires Native source-object and physical-position keys");
  }

  const auto& artifact_binding = binding.artifacts(0);
  const auto expected_format =
      pangrova::capability::generatedValuesArtifactFormat();
  const auto expected_locator =
      pangrova::capability::physicalRowLocatorContract();
  if (artifact_binding.artifact_type() != "GENERATED_COLUMN_VALUES" ||
      artifact_binding.format().SerializeAsString() !=
          expected_format.SerializeAsString() ||
      artifact_binding.row_locator().SerializeAsString() !=
          expected_locator.SerializeAsString() ||
      artifact_binding.generation().empty() ||
      artifact_binding.source_snapshot().empty() ||
      artifact_binding.split_objects_size() < 2 ||
      artifact_binding.split_objects_size() % 2 != 0 ||
      static_cast<size_t>(artifact_binding.split_objects_size() / 2) >
          kMaxSourceObjectsPerTask) {
    throw std::runtime_error(
        "VeloxPlanConverter: generated-values artifact identity or format mismatch");
  }

  std::string target_column;
  uint32_t dimension = 0;
  uint32_t output_type_dimension = 0;
  bool source_encoding_valid = false;
  bool execution_mode_valid = false;
  std::unordered_set<std::string> property_names;
  std::unordered_set<size_t> source_columns;
  for (const auto& property : artifact_binding.properties()) {
    if (!property_names.emplace(property.name()).second) {
      throw std::runtime_error(
          "VeloxPlanConverter: duplicate generated-values artifact property");
    }
    const auto& value = property.value();
    if (property.name() == "target_column" && value.has_string_value() &&
        !value.string_value().empty()) {
      target_column = value.string_value();
    } else if (property.name() == "output_type" && value.has_string_value()) {
      output_type_dimension =
          parseGeneratedVectorDimension(value.string_value());
    } else if (property.name() == "vector_dimension" &&
               value.has_int64_value() && value.int64_value() > 0 &&
               value.int64_value() <=
                   artifact::GeneratedValueSegment::kMaxDimension) {
      dimension = static_cast<uint32_t>(value.int64_value());
    } else if (property.name() == "source_encoding" &&
               value.has_string_value() &&
               value.string_value() == "ordered-unit-separator-v1") {
      source_encoding_valid = true;
    } else if (property.name() == "execution_mode" &&
               value.has_string_value() &&
               (value.string_value() == "EXECUTION_MODE_SYNC" ||
                value.string_value() == "EXECUTION_MODE_ASYNC")) {
      execution_mode_valid = true;
    } else if (property.name().starts_with("source_column.") &&
               value.has_string_value() && !value.string_value().empty()) {
      const auto suffix = property.name().substr(
          std::string("source_column.").size());
      size_t consumed = 0;
      size_t ordinal = 0;
      try {
        ordinal = std::stoull(suffix, &consumed);
      } catch (const std::exception&) {
        throw std::runtime_error(
            "VeloxPlanConverter: invalid generated-values source column ordinal");
      }
      if (consumed != suffix.size() ||
          !source_columns.emplace(ordinal).second) {
        throw std::runtime_error(
            "VeloxPlanConverter: invalid generated-values source column ordinal");
      }
    } else if (property.name().starts_with("definition_option.") &&
               property.name().size() >
                   std::string("definition_option.").size()) {
      continue;
    } else {
      throw std::runtime_error(
          "VeloxPlanConverter: unsupported generated-values artifact property: " +
          property.name());
    }
  }
  if (target_column.empty() || dimension == 0 ||
      output_type_dimension != dimension ||
      !source_encoding_valid || !execution_mode_valid || source_columns.empty()) {
    throw std::runtime_error(
        "VeloxPlanConverter: generated-values artifact properties are incomplete");
  }
  for (size_t ordinal = 0; ordinal < source_columns.size(); ++ordinal) {
    if (!source_columns.contains(ordinal)) {
      throw std::runtime_error(
          "VeloxPlanConverter: generated-values source columns are not contiguous");
    }
  }

  auto output_type = convertNamedStruct(detail.output_schema());
  if (!output_type || output_type->size() != input_type->size() + 1 ||
      output_type->nameOf(output_type->size() - 1) != target_column ||
      output_type->childAt(output_type->size() - 1)->kind() !=
          facebook::velox::TypeKind::ARRAY ||
      output_type->childAt(output_type->size() - 1)->childAt(0)->kind() !=
          facebook::velox::TypeKind::REAL) {
    throw std::runtime_error(
        "VeloxPlanConverter: generated-values overlay output schema is invalid");
  }
  for (size_t index = 0; index < input_type->size(); ++index) {
    if (output_type->nameOf(index) != logical_input->nameOf(index) ||
        !output_type->childAt(index)->equivalent(*input_type->childAt(index))) {
      throw std::runtime_error(
          "VeloxPlanConverter: generated-values overlay must preserve every input column");
    }
  }

  struct BundleObjects final {
    const ArtifactObject* values{nullptr};
    const ArtifactObject* locators{nullptr};
  };
  std::unordered_map<std::string, BundleObjects> bundles;
  bundles.reserve(
      static_cast<size_t>(artifact_binding.split_objects_size() / 2));
  uint64_t object_bytes = 0;
  for (const auto& object : artifact_binding.split_objects()) {
    if (object.scope() != ARTIFACT_OBJECT_SCOPE_SOURCE_OBJECT ||
        object.source_object_id().empty() || object.row_count() == 0 ||
        !object.encryption_key_ref().empty() ||
        object.size_bytes() >
            std::numeric_limits<uint64_t>::max() - object_bytes) {
      throw std::runtime_error(
          "VeloxPlanConverter: invalid generated-values artifact object");
    }
    object_bytes += object.size_bytes();
    auto& bundle = bundles[object.source_object_id()];
    if (object.object_role() == "GENERATED_VALUES" && object.ordinal() == 0) {
      if (bundle.values != nullptr) {
        throw std::runtime_error(
            "VeloxPlanConverter: duplicate GENERATED_VALUES object");
      }
      bundle.values = &object;
    } else if (object.object_role() == "ROW_LOCATORS" &&
               object.ordinal() == 1) {
      if (bundle.locators != nullptr) {
        throw std::runtime_error(
            "VeloxPlanConverter: duplicate ROW_LOCATORS object");
      }
      bundle.locators = &object;
    } else {
      throw std::runtime_error(
          "VeloxPlanConverter: unknown generated-values artifact role or ordinal");
    }
  }
  if (bundles.empty() || bundles.size() > kMaxSourceObjectsPerTask ||
      object_bytes > binding.resources().object_store_read_bytes()) {
    throw std::runtime_error(
        "VeloxPlanConverter: generated-values artifacts exceed task bounds");
  }

  uint64_t total_rows = 0;
  for (const auto& [source_id, bundle] : bundles) {
    (void)source_id;
    if (bundle.values == nullptr || bundle.locators == nullptr ||
        bundle.values->row_count() != bundle.locators->row_count() ||
        bundle.values->row_count() >
            std::numeric_limits<uint64_t>::max() - total_rows) {
      throw std::runtime_error(
          "VeloxPlanConverter: generated-values bundle is incomplete or has mismatched rows");
    }
    total_rows += bundle.values->row_count();
  }
  if (total_rows != binding.resources().fill_keys()) {
    throw std::runtime_error(
        "VeloxPlanConverter: generated-values row count differs from fill-key grant");
  }

  GeneratedValuesOverlayConfig config;
  config.source_object_channel = channels.at("source_object_id");
  config.row_position_channel = channels.at("row_position");
  config.max_fill_keys = binding.resources().fill_keys();
  config.sources.reserve(bundles.size());
  uint64_t remaining_read_bytes = binding.resources().object_store_read_bytes();
  std::vector<std::string> ordered_sources;
  ordered_sources.reserve(bundles.size());
  for (const auto& [source_id, bundle] : bundles) {
    (void)bundle;
    ordered_sources.push_back(source_id);
  }
  std::sort(ordered_sources.begin(), ordered_sources.end());
  for (const auto& source_id : ordered_sources) {
    const auto& bundle = bundles.at(source_id);
    auto values_object = artifact::ImmutableArtifactCache::instance().materialize(
        *bundle.values, remaining_read_bytes);
    remaining_read_bytes -= bundle.values->size_bytes();
    auto locator_object = artifact::ImmutableArtifactCache::instance().materialize(
        *bundle.locators, remaining_read_bytes);
    remaining_read_bytes -= bundle.locators->size_bytes();
    auto segment = artifact::GeneratedValueSegment::read(
        values_object.local_path, bundle.values->row_count(), dimension,
        bundle.values->size_bytes());
    auto locators = artifact::RowLocatorSidecar::readPhysicalRowPositions(
        locator_object.local_path, bundle.locators->row_count());
    if (locators.size() != segment.row_count) {
      throw std::runtime_error(
          "VeloxPlanConverter: generated-values segment and locators differ");
    }
    GeneratedValuesForSource source;
    source.dimension = segment.dimension;
    source.values = std::move(segment.values);
    source.validity = std::move(segment.validity);
    source.row_ordinals.reserve(locators.size());
    for (size_t ordinal = 0; ordinal < locators.size(); ++ordinal) {
      if (!source.row_ordinals.emplace(locators[ordinal], ordinal).second) {
        throw std::runtime_error(
            "VeloxPlanConverter: generated-values locators contain a duplicate");
      }
    }
    source.leases.push_back(std::move(values_object));
    source.leases.push_back(std::move(locator_object));
    if (!config.sources.emplace(source_id, std::move(source)).second) {
      throw std::runtime_error(
          "VeloxPlanConverter: duplicate generated-values source object");
    }
  }
  return std::make_shared<GeneratedValuesOverlayNode>(
      detail.plan_node_id(), std::move(config), physicalRowType(output_type),
      std::move(input));
}

// ---------------------------------------------------------------------------
// ExchangeRel → callback-backed SerializedShuffleQueue producer
// ---------------------------------------------------------------------------


}  // namespace pangrova::velox
