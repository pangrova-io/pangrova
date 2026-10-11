#include <gtest/gtest.h>
#include "../CanonicalTestPlan.h"
#include "velox/expression/FunctionRegistrySnapshot.h"

#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "artifact/ArtifactWorkReportState.h"
#include "capability/NativeCapabilityCatalog.h"
#include "pangrova/protocol/v2/plan_extension.pb.h"
#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"
#include "velox/connector/PangrovaTableHandle.h"
#include "velox/plan/ArtifactBuildOperator.h"
#include "velox/plan/CanonicalOrdinalIdentity.h"
#include "velox/plan/GeneratedVectorShape.h"
#include "velox/core/Expressions.h"
#include "velox/plan/VeloxPlanConverter.h"

namespace pangrova::velox::test {
namespace {

std::vector<uint8_t> serializePlan(const substrait::Rel& rel) {
  substrait::Plan plan;
  *plan.add_relations()->mutable_root()->mutable_input() = rel;
  const auto bytes = pangrova::test::canonicalTestPlan(plan).SerializeAsString();
  return {bytes.begin(), bytes.end()};
}

substrait::Rel artifactRelation() {
  substrait::Rel rel;
  auto* extension = rel.mutable_extension_single();
  auto* read = extension->mutable_input()->mutable_read();
  read->mutable_named_table()->add_names("items");
  read->mutable_base_schema()->add_names(
      std::string(kPhysicalRowPositionColumnName));
  read->mutable_base_schema()->mutable_struct_()->add_types()->mutable_i64()
      ->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  read->mutable_base_schema()->add_names("embedding");
  auto* embedding = read->mutable_base_schema()->mutable_struct_()->add_types()->mutable_list();
  embedding->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  embedding->mutable_type()->mutable_fp32()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);

  protocol::v2::RelationDetailEnvelope envelope;
  envelope.set_schema_version(1);
  auto* detail = envelope.mutable_artifact_build();
  detail->set_plan_node_id("artifact-build-1");
  detail->set_report_schema_version(2);
  detail->add_input_fields(std::string(kPhysicalRowPositionColumnName));
  detail->add_input_fields("embedding");
  *detail->mutable_invocation()->mutable_capability() =
      capability::diskAnnArtifactBuildDescriptor().ref();
  for (const auto& [name, field] :
       std::vector<std::pair<std::string, int>>{
           {"row_position", 0}, {"vector", 1}}) {
    auto* argument = detail->mutable_invocation()->add_arguments();
    argument->set_name(name);
    argument->mutable_expression()->mutable_selection()->mutable_root_reference();
    argument->mutable_expression()
        ->mutable_selection()
        ->mutable_direct_reference()
        ->mutable_struct_field()
        ->set_field(field);
  }
  extension->mutable_detail()->PackFrom(envelope);
  return rel;
}

protocol::v2::OperatorBinding artifactBinding() {
  protocol::v2::OperatorBinding binding;
  binding.set_plan_node_id("artifact-build-1");
  *binding.mutable_capability() =
      capability::diskAnnArtifactBuildDescriptor().ref();
  auto* policy = binding.mutable_execution_policy();
  policy->set_coverage_policy(protocol::v2::COVERAGE_POLICY_REQUIRE_COMPLETE);
  policy->set_fallback_policy(protocol::v2::FALLBACK_POLICY_FORBID);
  policy->set_retry_class(protocol::v2::RETRY_CLASS_IDEMPOTENT);
  policy->set_idempotency_key("job-1:page-1:generation-7");
  auto* output = binding.mutable_output_artifact();
  output->set_artifact_id("index-1");
  output->set_artifact_type("VECTOR_INDEX");
  output->set_generation("7");
  output->set_source_id("catalog.schema.items");
  output->set_source_snapshot("snapshot-7");
  output->set_staging_uri("file:/tmp/pangrova-artifact-plan-test");
  *output->mutable_format() = capability::diskAnnArtifactFormat();
  *output->mutable_row_locator() = capability::physicalRowLocatorContract();
  output->add_expected_source_object_ids("source-1");
  output->set_publication_fencing_token(9);
  output->set_work_unit_id("page-1");
  output->set_idempotency_key(policy->idempotency_key());
  output->set_work_kind(protocol::v2::ARTIFACT_WORK_KIND_SOURCE_MAP);
  auto* dimension = output->add_properties();
  dimension->set_name("dimension");
  dimension->mutable_value()->set_int64_value(4);
  auto* pq = output->add_properties();
  pq->set_name("pq_subvectors");
  pq->mutable_value()->set_int64_value(1);
  return binding;
}

substrait::Rel bm25ArtifactRelation() {
  substrait::Rel rel;
  auto* extension = rel.mutable_extension_single();
  auto* read = extension->mutable_input()->mutable_read();
  read->mutable_named_table()->add_names("items");
  read->mutable_base_schema()->add_names(
      std::string(kPhysicalRowPositionColumnName));
  read->mutable_base_schema()->mutable_struct_()->add_types()->mutable_i64()
      ->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  read->mutable_base_schema()->add_names("description");
  read->mutable_base_schema()->mutable_struct_()->add_types()->mutable_string()
      ->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  protocol::v2::RelationDetailEnvelope envelope;
  envelope.set_schema_version(1);
  auto* detail = envelope.mutable_artifact_build();
  detail->set_plan_node_id("artifact-build-bm25");
  detail->set_report_schema_version(2);
  detail->add_input_fields(std::string(kPhysicalRowPositionColumnName));
  detail->add_input_fields("description");
  *detail->mutable_invocation()->mutable_capability() =
      capability::bm25ArtifactBuildDescriptor().ref();
  for (const auto& [name, field] :
       std::vector<std::pair<std::string, int>>{
           {"row_position", 0}, {"text", 1}}) {
    auto* argument = detail->mutable_invocation()->add_arguments();
    argument->set_name(name);
    argument->mutable_expression()->mutable_selection()->mutable_root_reference();
    argument->mutable_expression()
        ->mutable_selection()
        ->mutable_direct_reference()
        ->mutable_struct_field()
        ->set_field(field);
  }
  extension->mutable_detail()->PackFrom(envelope);
  return rel;
}

protocol::v2::OperatorBinding bm25ArtifactBinding() {
  auto binding = artifactBinding();
  binding.set_plan_node_id("artifact-build-bm25");
  *binding.mutable_capability() = capability::bm25ArtifactBuildDescriptor().ref();
  auto* output = binding.mutable_output_artifact();
  output->set_artifact_type("TEXT_INDEX");
  *output->mutable_format() = capability::bm25ArtifactFormat();
  output->clear_properties();
  auto* analyzer = output->add_properties();
  analyzer->set_name("analyzer");
  analyzer->mutable_value()->set_string_value("standard-v1");
  return binding;
}

substrait::Rel generatedValuesArtifactRelation() {
  auto rel = artifactRelation();
  auto* extension = rel.mutable_extension_single();
  auto* read = extension->mutable_input()->mutable_read();
  read->mutable_base_schema()->set_names(1, "generated_value");
  read->mutable_base_schema()
      ->mutable_struct_()
      ->mutable_types(1)
      ->mutable_list()
      ->mutable_type()
      ->mutable_fp32()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  protocol::v2::RelationDetailEnvelope envelope;
  envelope.set_schema_version(1);
  auto* detail = envelope.mutable_artifact_build();
  detail->set_plan_node_id("artifact-build-generated-values");
  detail->set_report_schema_version(2);
  detail->add_input_fields(std::string(kPhysicalRowPositionColumnName));
  detail->add_input_fields("generated_value");
  *detail->mutable_invocation()->mutable_capability() =
      capability::generatedValuesArtifactBuildDescriptor().ref();
  for (const auto& [name, field] :
       std::vector<std::pair<std::string, int>>{
           {"row_position", 0}, {"generated_value", 1}}) {
    auto* argument = detail->mutable_invocation()->add_arguments();
    argument->set_name(name);
    argument->mutable_expression()->mutable_selection()->mutable_root_reference();
    argument->mutable_expression()
        ->mutable_selection()
        ->mutable_direct_reference()
        ->mutable_struct_field()
        ->set_field(field);
  }
  extension->mutable_detail()->PackFrom(envelope);
  return rel;
}

protocol::v2::OperatorBinding generatedValuesArtifactBinding() {
  auto binding = artifactBinding();
  binding.set_plan_node_id("artifact-build-generated-values");
  *binding.mutable_capability() =
      capability::generatedValuesArtifactBuildDescriptor().ref();
  auto* output = binding.mutable_output_artifact();
  output->set_artifact_id("generated-column-1");
  output->set_artifact_type("GENERATED_COLUMN_VALUES");
  *output->mutable_format() = capability::generatedValuesArtifactFormat();
  output->clear_properties();
  const auto add_string = [output](
                              const std::string& name,
                              const std::string& value) {
    auto* property = output->add_properties();
    property->set_name(name);
    property->mutable_value()->set_string_value(value);
  };
  add_string("target_column", "embedding");
  add_string("output_type", "VECTOR(2)");
  auto* dimension = output->add_properties();
  dimension->set_name("vector_dimension");
  dimension->mutable_value()->set_int64_value(2);
  add_string("source_encoding", "ordered-unit-separator-v1");
  add_string("execution_mode", "EXECUTION_MODE_ASYNC");
  add_string("source_column.0", "description");
  return binding;
}

substrait::Rel tabularExportRelation() {
  substrait::Rel rel;
  auto* extension = rel.mutable_extension_single();
  auto* read = extension->mutable_input()->mutable_read();
  read->mutable_named_table()->add_names("items");
  read->mutable_base_schema()->add_names("item_id");
  read->mutable_base_schema()->mutable_struct_()->add_types()->mutable_i64()
      ->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  read->mutable_base_schema()->add_names("feature_value");
  read->mutable_base_schema()->mutable_struct_()->add_types()->mutable_string()
      ->set_nullability(substrait::Type::NULLABILITY_NULLABLE);

  protocol::v2::RelationDetailEnvelope envelope;
  envelope.set_schema_version(1);
  auto* detail = envelope.mutable_artifact_build();
  detail->set_plan_node_id("artifact-build-tabular-export");
  detail->set_report_schema_version(2);
  detail->add_input_fields("item_id");
  detail->add_input_fields("feature_value");
  *detail->mutable_invocation()->mutable_capability() =
      capability::tabularExportDescriptor().ref();
  extension->mutable_detail()->PackFrom(envelope);
  return rel;
}

protocol::v2::OperatorBinding tabularExportBinding() {
  auto binding = artifactBinding();
  binding.set_plan_node_id("artifact-build-tabular-export");
  *binding.mutable_capability() = capability::tabularExportDescriptor().ref();
  auto* output = binding.mutable_output_artifact();
  output->set_artifact_id("feature-view-export-1");
  output->set_artifact_type("TABULAR_EXPORT");
  *output->mutable_format() = capability::tabularExportFormat();
  output->set_work_kind(protocol::v2::ARTIFACT_WORK_KIND_RELATIONAL_PARTITION);
  output->set_output_scope_id(output->work_unit_id());
  output->clear_expected_source_object_ids();
  output->clear_row_locator();
  output->clear_properties();
  return binding;
}

protocol::v2::OperatorBinding bm25ReductionBinding(bool finalizer) {
  auto binding = bm25ArtifactBinding();
  auto* output = binding.mutable_output_artifact();
  output->clear_expected_source_object_ids();
  output->set_work_unit_id(finalizer ? "page-final" : "page-reduce");
  output->set_work_kind(finalizer
      ? protocol::v2::ARTIFACT_WORK_KIND_GENERATION_FINALIZE
      : protocol::v2::ARTIFACT_WORK_KIND_DEPENDENCY_REDUCE);
  output->set_output_scope_id(finalizer
      ? output->generation() : output->work_unit_id());
  output->set_idempotency_key(
      "job-1:" + output->work_unit_id() + ":generation-7");
  binding.mutable_execution_policy()->set_idempotency_key(
      output->idempotency_key());
  auto* dependency = binding.add_artifacts();
  dependency->set_artifact_id(output->artifact_id());
  dependency->set_artifact_type(output->artifact_type());
  dependency->set_generation(output->generation());
  dependency->set_source_id(output->source_id());
  dependency->set_source_snapshot(output->source_snapshot());
  *dependency->mutable_format() = output->format();
  *dependency->mutable_row_locator() = output->row_locator();
  auto* object = dependency->add_split_objects();
  object->set_object_id("stats-page-a");
  object->set_opaque_uri("file:/tmp/stats-page-a");
  object->set_size_bytes(128);
  object->mutable_checksum()->set_algorithm(
      protocol::v2::Checksum::ALGORITHM_SHA256);
  object->mutable_checksum()->set_digest(std::string(32, '\x11'));
  object->set_object_role("BM25_STATS_SHARD");
  object->set_source_object_id("page-a");
  object->set_row_count(10);
  object->set_ordinal(0);
  object->set_scope(protocol::v2::ARTIFACT_OBJECT_SCOPE_WORK_UNIT);
  return binding;
}

std::shared_ptr<ArtifactBuildTaskContext> runtimeContext() {
  auto context = std::make_shared<ArtifactBuildTaskContext>();
  context->job_id = "job-1";
  context->page_id = "page-1";
  context->fencing_token = 9;
  context->scratch_directory = "/tmp";
  context->max_input_bytes = 1024 * 1024;
  context->max_spill_bytes = 1024 * 1024;
  context->max_object_store_read_bytes = 1024 * 1024;
  context->max_object_store_write_bytes = 1024 * 1024;
  context->report_state = std::make_shared<artifact::ArtifactWorkReportState>();
  return context;
}

}  // namespace

TEST(ArtifactBuildPlanConversionTest, GeneratedVectorShapeUsesCanonicalFloat32Type) {
  EXPECT_EQ(parseGeneratedVectorDimension("VECTOR(2)"), 2);
  EXPECT_EQ(parseGeneratedVectorDimension("VECTOR(65535)"), 65535);
  for (const auto type : {"VECTOR(0)", "VECTOR(65536)", "VECTOR(2, COSINE)",
                          "VECTOR(2,DOUBLE)", "VECTOR(2) trailing", "VECTOR()"}) {
    EXPECT_EQ(parseGeneratedVectorDimension(type), 0) << type;
  }
}

TEST(ArtifactBuildPlanConversionTest, BuildsTypedNativeSink) {
  auto rel = artifactRelation();
  auto binding = artifactBinding();
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto result = converter.convert(
      serializePlan(rel), {}, false, "ArrowBatch",
      {{binding.plan_node_id(), binding}}, runtimeContext());
  auto artifact = std::dynamic_pointer_cast<const ArtifactBuildNode>(result);
  ASSERT_NE(artifact, nullptr);
  EXPECT_TRUE(artifact->requiresSingleThread());
  EXPECT_EQ(artifact->config().row_position_channel, 0);
  EXPECT_EQ(artifact->config().vector_channel, 1);
  EXPECT_EQ(artifact->config().diskann.dimension, 4);
}

TEST(ArtifactBuildPlanConversionTest, RejectsNonRootAndNestedArtifactArguments) {
  const std::vector<std::pair<substrait::Rel, protocol::v2::OperatorBinding>> cases{
      {artifactRelation(), artifactBinding()},
      {bm25ArtifactRelation(), bm25ArtifactBinding()},
      {generatedValuesArtifactRelation(), generatedValuesArtifactBinding()}};
  for (const auto& [valid, binding] : cases) {
    const auto accepted_registry =
        facebook::velox::exec::FunctionRegistrySnapshot::capture();
    VeloxPlanConverter accepted(accepted_registry);
    ASSERT_NE(accepted.convert(serializePlan(valid), {}, false, "ArrowBatch",
                              {{binding.plan_node_id(), binding}}, runtimeContext()),
              nullptr);
    for (int origin = 0; origin < 3; ++origin) {
      auto invalid = valid;
      protocol::v2::RelationDetailEnvelope envelope;
      ASSERT_TRUE(invalid.extension_single().detail().UnpackTo(&envelope));
      auto* field = envelope.mutable_artifact_build()->mutable_invocation()
                        ->mutable_arguments(0)->mutable_expression()->mutable_selection();
      if (origin == 0) {
        field->clear_root_reference();
      } else if (origin == 1) {
        field->mutable_outer_reference()->set_steps_out(1);
      } else {
        field->mutable_direct_reference()->mutable_struct_field()
            ->mutable_child()->mutable_struct_field()->set_field(0);
      }
      invalid.mutable_extension_single()->mutable_detail()->PackFrom(envelope);
      const auto rejected_registry =
          facebook::velox::exec::FunctionRegistrySnapshot::capture();
      VeloxPlanConverter rejected(rejected_registry);
      EXPECT_THROW(rejected.convert(serializePlan(invalid), {}, false, "ArrowBatch",
                                    {{binding.plan_node_id(), binding}}, runtimeContext()),
                   std::runtime_error);
    }
  }
}

TEST(ArtifactBuildPlanConversionTest, PhysicalAliasCannotReplaceLogicalRowIdentity) {
  auto rel = artifactRelation();
  rel.mutable_extension_single()->mutable_input()->mutable_read()
      ->mutable_base_schema()->set_names(0, physicalFieldAlias(0));
  protocol::v2::RelationDetailEnvelope envelope;
  ASSERT_TRUE(rel.extension_single().detail().UnpackTo(&envelope));
  envelope.mutable_artifact_build()->set_input_fields(0, physicalFieldAlias(0));
  rel.mutable_extension_single()->mutable_detail()->PackFrom(envelope);
  auto binding = artifactBinding();
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  EXPECT_THROW(converter.convert(serializePlan(rel), {}, false, "ArrowBatch",
                                 {{binding.plan_node_id(), binding}}, runtimeContext()),
               std::runtime_error);
}

TEST(ArtifactBuildPlanConversionTest, BuildsTypedHnswAndIvfPqNativeSinks) {
  for (const bool hnsw : {true, false}) {
    for (const std::string metric : {"L2", "COSINE", "INNER_PRODUCT"}) {
      auto rel = artifactRelation();
      protocol::v2::RelationDetailEnvelope envelope;
      ASSERT_TRUE(rel.extension_single().detail().UnpackTo(&envelope));
      const auto capability = hnsw
          ? capability::hnswArtifactBuildDescriptor().ref()
          : capability::ivfPqArtifactBuildDescriptor().ref();
      *envelope.mutable_artifact_build()
           ->mutable_invocation()
           ->mutable_capability() = capability;
      rel.mutable_extension_single()->mutable_detail()->PackFrom(envelope);

      auto binding = artifactBinding();
      *binding.mutable_capability() = capability;
      auto* output = binding.mutable_output_artifact();
      *output->mutable_format() = hnsw
          ? capability::hnswArtifactFormat()
          : capability::ivfPqArtifactFormat();
      output->clear_properties();
      auto add_integer = [&](const std::string& name, int64_t value) {
        auto* property = output->add_properties();
        property->set_name(name);
        property->mutable_value()->set_int64_value(value);
      };
      add_integer("dimension", 4);
      auto* metric_property = output->add_properties();
      metric_property->set_name("metric");
      metric_property->mutable_value()->set_string_value(metric);
      if (hnsw) {
        add_integer("max_connections", 8);
        add_integer("ef_construction", 32);
      } else {
        add_integer("coarse_clusters", 4);
        add_integer("pq_subvectors", 2);
        add_integer("pq_bits_per_code", 4);
        add_integer("training_iterations", 8);
        add_integer("training_samples", 256);
      }

      const auto converter_registry =
          facebook::velox::exec::FunctionRegistrySnapshot::capture();
      VeloxPlanConverter converter(converter_registry);
      auto result = converter.convert(
          serializePlan(rel), {}, false, "ArrowBatch",
          {{binding.plan_node_id(), binding}}, runtimeContext());
      auto artifact = std::dynamic_pointer_cast<const ArtifactBuildNode>(result);
      ASSERT_NE(artifact, nullptr);
      EXPECT_EQ(
          artifact->config().kind,
          hnsw ? ArtifactBuildKind::HNSW : ArtifactBuildKind::IVF_PQ);
      EXPECT_EQ(
          hnsw ? artifact->config().hnsw.dimension
               : artifact->config().ivf_pq.dimension,
          4);
      EXPECT_EQ(hnsw ? artifact->config().hnsw.metric
                     : artifact->config().ivf_pq.metric,
                vector::distanceMetricFromName(metric));

      // The metric is a required typed field in the v2 build contract.
      metric_property->mutable_value()->set_string_value("MANHATTAN");
      EXPECT_THROW(converter.convert(
          serializePlan(rel), {}, false, "ArrowBatch",
          {{binding.plan_node_id(), binding}}, runtimeContext()), std::invalid_argument);
      metric_property->mutable_value()->set_int64_value(1);
      EXPECT_THROW(converter.convert(
          serializePlan(rel), {}, false, "ArrowBatch",
          {{binding.plan_node_id(), binding}}, runtimeContext()), std::runtime_error);
      output->mutable_properties()->DeleteSubrange(1, 1);
      EXPECT_THROW(converter.convert(
          serializePlan(rel), {}, false, "ArrowBatch",
          {{binding.plan_node_id(), binding}}, runtimeContext()), std::runtime_error);
    }
  }
}

TEST(ArtifactBuildPlanConversionTest, RejectsFormatDrift) {
  auto rel = artifactRelation();
  auto binding = artifactBinding();
  binding.mutable_output_artifact()->mutable_format()->set_major_version(3);
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  EXPECT_THROW(
      converter.convert(
          serializePlan(rel), {}, false, "ArrowBatch",
          {{binding.plan_node_id(), binding}}, runtimeContext()),
      std::runtime_error);
}

TEST(ArtifactBuildPlanConversionTest, BuildsTypedNativeBm25Sink) {
  auto rel = bm25ArtifactRelation();
  auto binding = bm25ArtifactBinding();
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto result = converter.convert(
      serializePlan(rel), {}, false, "ArrowBatch",
      {{binding.plan_node_id(), binding}}, runtimeContext());
  auto artifact = std::dynamic_pointer_cast<const ArtifactBuildNode>(result);
  ASSERT_NE(artifact, nullptr);
  EXPECT_EQ(artifact->config().kind, ArtifactBuildKind::BM25);
  EXPECT_EQ(artifact->config().row_position_channel, 0);
  EXPECT_EQ(artifact->config().text_channel, 1);
}

TEST(ArtifactBuildPlanConversionTest, BuildsTypedGeneratedValuesNativeSink) {
  auto rel = generatedValuesArtifactRelation();
  auto binding = generatedValuesArtifactBinding();
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto result = converter.convert(
      serializePlan(rel), {}, false, "ArrowBatch",
      {{binding.plan_node_id(), binding}}, runtimeContext());
  auto artifact = std::dynamic_pointer_cast<const ArtifactBuildNode>(result);
  ASSERT_NE(artifact, nullptr);
  EXPECT_EQ(artifact->config().kind, ArtifactBuildKind::GENERATED_VALUES);
  EXPECT_EQ(artifact->config().row_position_channel, 0);
  EXPECT_EQ(artifact->config().generated_value_channel, 1);
  EXPECT_EQ(artifact->config().generated_value_dimension, 2);

  protocol::v2::RelationDetailEnvelope envelope;
  ASSERT_TRUE(rel.extension_single().detail().UnpackTo(&envelope));
  auto* option = envelope.mutable_artifact_build()
                     ->mutable_invocation()
                     ->add_options();
  option->set_name("fallback");
  option->mutable_value()->set_string_value("empty");
  rel.mutable_extension_single()->mutable_detail()->PackFrom(envelope);
  const auto rejected_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter rejected(rejected_registry);
  EXPECT_THROW(
      rejected.convert(
          serializePlan(rel), {}, false, "ArrowBatch",
          {{binding.plan_node_id(), binding}}, runtimeContext()),
      std::runtime_error);
}

TEST(ArtifactBuildPlanConversionTest, BuildsTypedTabularExportNativeSink) {
  auto rel = tabularExportRelation();
  auto binding = tabularExportBinding();
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto result = converter.convert(
      serializePlan(rel), {}, false, "ArrowBatch",
      {{binding.plan_node_id(), binding}}, runtimeContext());
  auto artifact = std::dynamic_pointer_cast<const ArtifactBuildNode>(result);
  ASSERT_NE(artifact, nullptr);
  EXPECT_EQ(artifact->config().kind, ArtifactBuildKind::TABULAR_EXPORT);
  EXPECT_EQ(
      artifact->config().max_rows,
      capability::kTabularExportMaxRows);

  auto bad_binding = binding;
  *bad_binding.mutable_output_artifact()->mutable_row_locator() = capability::physicalRowLocatorContract();
  const auto physical_locator_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter physical_locator(physical_locator_registry);
  EXPECT_THROW(physical_locator.convert(
      serializePlan(rel), {}, false, "ArrowBatch", {{bad_binding.plan_node_id(), bad_binding}}, runtimeContext()),
      std::runtime_error);
  bad_binding = binding;
  bad_binding.mutable_output_artifact()->set_output_scope_id("another-partition");
  const auto wrong_scope_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter wrong_scope(wrong_scope_registry);
  EXPECT_THROW(wrong_scope.convert(
      serializePlan(rel), {}, false, "ArrowBatch", {{bad_binding.plan_node_id(), bad_binding}}, runtimeContext()),
      std::runtime_error);

  protocol::v2::RelationDetailEnvelope envelope;
  ASSERT_TRUE(rel.extension_single().detail().UnpackTo(&envelope));
  envelope.mutable_artifact_build()->set_input_fields(1, "masked_feature");
  rel.mutable_extension_single()->mutable_detail()->PackFrom(envelope);
  const auto renamed_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter renamed(renamed_registry);
  auto renamed_result = renamed.convert(
      serializePlan(rel), {}, false, "ArrowBatch",
      {{binding.plan_node_id(), binding}}, runtimeContext());
  auto renamed_artifact = std::dynamic_pointer_cast<const ArtifactBuildNode>(renamed_result);
  ASSERT_NE(renamed_artifact, nullptr);
  const auto& exported_input = renamed_artifact->sources().at(0);
  EXPECT_EQ(exported_input->outputType()->names(),
            (std::vector<std::string>{"item_id", "masked_feature"}));
  EXPECT_EQ(exported_input->outputType()->childAt(0)->kind(), facebook::velox::TypeKind::BIGINT);
  EXPECT_EQ(exported_input->outputType()->childAt(1)->kind(), facebook::velox::TypeKind::VARCHAR);
  ASSERT_EQ(exported_input->sources().size(), 1);
  EXPECT_EQ(exported_input->sources().at(0)->outputType()->nameOf(1), physicalFieldAlias(1));
  const auto renamed_projection = std::dynamic_pointer_cast<
      const facebook::velox::core::ProjectNode>(exported_input);
  ASSERT_NE(renamed_projection, nullptr);
  const auto projected_feature = std::dynamic_pointer_cast<
      const facebook::velox::core::FieldAccessTypedExpr>(renamed_projection->projections().at(1));
  ASSERT_NE(projected_feature, nullptr);
  EXPECT_EQ(projected_feature->name(), physicalFieldAlias(1));

  envelope.mutable_artifact_build()->set_input_fields(1, "item_id");
  rel.mutable_extension_single()->mutable_detail()->PackFrom(envelope);
  const auto rejected_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter rejected(rejected_registry);
  EXPECT_THROW(
      rejected.convert(
          serializePlan(rel), {}, false, "ArrowBatch",
          {{binding.plan_node_id(), binding}}, runtimeContext()),
      std::runtime_error);
}

TEST(ArtifactBuildPlanConversionTest, BuildsTypedBm25ReductionAndFinalizer) {
  auto rel = bm25ArtifactRelation();
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  for (const bool finalizer : {false, true}) {
    auto binding = bm25ReductionBinding(finalizer);
    auto result = converter.convert(
        serializePlan(rel), {}, false, "ArrowBatch",
        {{binding.plan_node_id(), binding}}, runtimeContext());
    auto artifact = std::dynamic_pointer_cast<const ArtifactBuildNode>(result);
    ASSERT_NE(artifact, nullptr);
    EXPECT_EQ(
        artifact->config().kind,
        finalizer ? ArtifactBuildKind::BM25_FINALIZE
                  : ArtifactBuildKind::BM25_REDUCE);
    ASSERT_EQ(artifact->config().input_objects.size(), 1);
    EXPECT_EQ(
        artifact->config().input_objects[0].object_role(),
        "BM25_STATS_SHARD");
  }
}

TEST(ArtifactBuildPlanConversionTest, RejectsLocatorDrift) {
  auto rel = artifactRelation();
  auto binding = artifactBinding();
  binding.mutable_output_artifact()
      ->mutable_row_locator()
      ->set_source_object_scoped(false);
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  EXPECT_THROW(
      converter.convert(
          serializePlan(rel), {}, false, "ArrowBatch",
          {{binding.plan_node_id(), binding}}, runtimeContext()),
      std::runtime_error);
}

TEST(ArtifactBuildPlanConversionTest, RejectsMissingWorkerLocalScratchPolicy) {
  auto rel = artifactRelation();
  auto binding = artifactBinding();
  auto context = runtimeContext();
  context->scratch_directory.clear();
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  EXPECT_THROW(
      converter.convert(
          serializePlan(rel), {}, false, "ArrowBatch",
          {{binding.plan_node_id(), binding}}, context),
      std::runtime_error);
}

}  // namespace pangrova::velox::test
