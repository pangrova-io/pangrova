#include <gtest/gtest.h>
#include "../CanonicalTestPlan.h"
#include "velox/expression/FunctionRegistrySnapshot.h"

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <google/protobuf/wrappers.pb.h>

#include "capability/NativeCapabilityCatalog.h"
#include "pangrova/protocol/v2/plan_extension.pb.h"
#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"
#include "velox/core/PlanNode.h"
#include "velox/plan/VeloxPlanConverter.h"
#include "velox/plan/RetrievalMergeOperator.h"
#include "velox/plan/CanonicalOrdinalIdentity.h"
#include "StructuralContractIdentities.h"
#include "udf/JvmScalarFunction.h"

namespace pangrova::velox::test {
namespace {

void addI64Column(substrait::NamedStruct* schema, const std::string& name) {
  schema->add_names(name);
  schema->mutable_struct_()->add_types()->mutable_i64()
      ->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
}

void addFp64Column(substrait::NamedStruct* schema, const std::string& name) {
  schema->add_names(name);
  schema->mutable_struct_()->add_types()->mutable_fp64()
      ->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
}

void addStringColumn(substrait::NamedStruct* schema, const std::string& name) {
  schema->add_names(name);
  schema->mutable_struct_()->add_types()->mutable_string()
      ->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
}

void addFieldArgument(
    pangrova::protocol::v2::CapabilityInvocation* invocation,
    const std::string& name,
    int field) {
  auto* argument = invocation->add_arguments();
  argument->set_name(name);
  argument->mutable_expression()->mutable_selection()->mutable_root_reference();
  argument->mutable_expression()
      ->mutable_selection()
      ->mutable_direct_reference()
      ->mutable_struct_field()
      ->set_field(field);
}

std::vector<uint8_t> serializePlan(const substrait::Rel& rel) {
  substrait::Plan plan;
  auto* root = plan.add_relations()->mutable_root();
  *root->mutable_input() = rel;
  const auto& output = rel.has_set() ? rel.set().inputs(0) : rel;
  pangrova::protocol::v2::RelationDetailEnvelope envelope;
  if (output.has_extension_single() &&
      output.extension_single().detail().UnpackTo(&envelope)) {
    if (envelope.has_retrieval_source()) {
      *root->mutable_names() = envelope.retrieval_source().output_contract().schema().names();
    } else if (envelope.has_retrieval_merge()) {
      *root->mutable_names() = envelope.retrieval_merge().output_contract().schema().names();
    } else if (envelope.has_rerank()) {
      *root->mutable_names() = envelope.rerank().output_contract().schema().names();
    }
  }
  std::string bytes;
  pangrova::test::canonicalTestPlan(plan).SerializeToString(&bytes);
  return {bytes.begin(), bytes.end()};
}

substrait::Rel exactTopKRelation() {
  substrait::Rel rel;
  auto* extension = rel.mutable_extension_single();
  auto* read = extension->mutable_input()->mutable_read();
  read->mutable_named_table()->add_names("items");
  addI64Column(read->mutable_base_schema(), "id");
  addFp64Column(read->mutable_base_schema(), "score");

  pangrova::protocol::v2::RelationDetailEnvelope envelope;
  envelope.set_schema_version(1);
  auto* detail = envelope.mutable_retrieval_source();
  detail->set_plan_node_id("retrieval-source-1");
  detail->set_source_id("vector-exact-1");
  detail->set_candidate_limit(7);
  *detail->mutable_invocation()->mutable_capability() =
      pangrova::capability::exactTopKRetrievalDescriptor().ref();

  auto* score_argument = detail->mutable_invocation()->add_arguments();
  score_argument->set_name("score");
  score_argument->mutable_expression()->mutable_selection()->mutable_root_reference();
  score_argument->mutable_expression()
      ->mutable_selection()
      ->mutable_direct_reference()
      ->mutable_struct_field()
      ->set_field(1);

  auto* contract = detail->mutable_output_contract();
  contract->set_schema_version(
      pangrova::capability::kRetrievalCandidateSchemaVersion);
  contract->set_score_direction(
      pangrova::protocol::v2::SCORE_DIRECTION_LOWER_IS_BETTER);
  contract->add_stable_key_fields("id");
  addI64Column(contract->mutable_schema(), "id");
  addFp64Column(contract->mutable_schema(), "score");

  extension->mutable_detail()->PackFrom(envelope);
  return rel;
}

substrait::Rel retrievalMergeRelation(bool linear = false) {
  substrait::Rel rel;
  auto* extension = rel.mutable_extension_single();
  auto* leaf = extension->mutable_input()->mutable_extension_leaf();
  pangrova::protocol::v2::RelationDetailEnvelope exchange_envelope;
  exchange_envelope.set_schema_version(1);
  auto* exchange = exchange_envelope.mutable_exchange_input();
  exchange->set_plan_node_id("retrieval-exchange-input");
  exchange->set_exchange_id("exchange-candidates-to-fusion");
  exchange->set_upstream_stage_id(1);
  exchange->set_downstream_stage_id(2);
  exchange->set_distribution(
      pangrova::protocol::v2::EXCHANGE_DISTRIBUTION_SINGLETON);
  exchange->set_partition_count(1);
  exchange->set_batch_serde(
      pangrova::protocol::v2::BATCH_SERDE_ARROW_BATCH);
  exchange->set_require_complete(true);
  addI64Column(exchange->mutable_output_schema(), "id");
  addFp64Column(exchange->mutable_output_schema(), "score");
  addStringColumn(exchange->mutable_output_schema(), "source_id");
  leaf->mutable_detail()->PackFrom(exchange_envelope);

  pangrova::protocol::v2::RelationDetailEnvelope envelope;
  envelope.set_schema_version(1);
  auto* detail = envelope.mutable_retrieval_merge();
  detail->set_plan_node_id("retrieval-merge-1");
  detail->set_output_limit(10);
  detail->set_max_fan_in(2);
  auto* invocation = detail->mutable_invocation();
  *invocation->mutable_capability() = linear
      ? pangrova::capability::linearRetrievalMergeDescriptor().ref()
      : pangrova::capability::rrfRetrievalMergeDescriptor().ref();
  addFieldArgument(invocation, "score", 1);
  addFieldArgument(invocation, "source_id", 2);
  if (linear) {
    auto* vector_weight = invocation->add_options();
    vector_weight->set_name("weight.vector");
    vector_weight->mutable_value()->set_double_value(0.7);
    auto* text_weight = invocation->add_options();
    text_weight->set_name("weight.text");
    text_weight->mutable_value()->set_double_value(0.3);
  } else {
    auto* option = invocation->add_options();
    option->set_name("rrf_k");
    option->mutable_value()->set_int64_value(60);
  }

  auto populate_contract = [](pangrova::protocol::v2::CandidateContract* contract,
                              pangrova::protocol::v2::ScoreDirection direction) {
    contract->set_schema_version(
        pangrova::capability::kRetrievalCandidateSchemaVersion);
    contract->set_score_direction(direction);
    contract->add_stable_key_fields("id");
    addI64Column(contract->mutable_schema(), "id");
    addFp64Column(contract->mutable_schema(), "score");
    addStringColumn(contract->mutable_schema(), "source_id");
  };
  auto* vector_input = detail->add_inputs();
  vector_input->set_source_id("vector");
  populate_contract(
      vector_input->mutable_contract(),
      pangrova::protocol::v2::SCORE_DIRECTION_LOWER_IS_BETTER);
  auto* text_input = detail->add_inputs();
  text_input->set_source_id("text");
  populate_contract(
      text_input->mutable_contract(),
      pangrova::protocol::v2::SCORE_DIRECTION_HIGHER_IS_BETTER);
  populate_contract(
      detail->mutable_output_contract(),
      pangrova::protocol::v2::SCORE_DIRECTION_HIGHER_IS_BETTER);
  extension->mutable_detail()->PackFrom(envelope);
  return rel;
}

substrait::Rel rerankRelation() {
  substrait::Rel rel;
  auto* extension = rel.mutable_extension_single();
  auto* read = extension->mutable_input()->mutable_read();
  read->mutable_named_table()->add_names("candidates");
  addI64Column(read->mutable_base_schema(), "id");
  addFp64Column(read->mutable_base_schema(), "score");
  addStringColumn(read->mutable_base_schema(), "document");

  pangrova::protocol::v2::RelationDetailEnvelope envelope;
  envelope.set_schema_version(1);
  auto* detail = envelope.mutable_rerank();
  detail->set_plan_node_id("rerank-1");
  detail->set_input_limit(100);
  detail->set_output_limit(10);
  *detail->mutable_invocation()->mutable_capability() =
      pangrova::capability::crossEncoderRerankDescriptor().ref();
  addFieldArgument(detail->mutable_invocation(), "score", 1);
  auto* query = detail->mutable_invocation()->add_arguments();
  query->set_name("query");
  query->mutable_expression()->mutable_literal()->set_string(
      "distributed systems");
  addFieldArgument(detail->mutable_invocation(), "document", 2);
  detail->mutable_model()->set_model_id("catalog.schema.reranker");
  detail->mutable_model()->set_version("2026-08-27");
  detail->mutable_model()->set_signature_sha256(std::string(32, 'm'));

  auto* contract = detail->mutable_output_contract();
  contract->set_schema_version(
      pangrova::capability::kRetrievalCandidateSchemaVersion);
  contract->set_score_direction(
      pangrova::protocol::v2::SCORE_DIRECTION_HIGHER_IS_BETTER);
  contract->add_stable_key_fields("id");
  addI64Column(contract->mutable_schema(), "id");
  addFp64Column(contract->mutable_schema(), "score");
  addStringColumn(contract->mutable_schema(), "document");
  extension->mutable_detail()->PackFrom(envelope);
  return rel;
}

std::unordered_map<std::string, pangrova::protocol::v2::OperatorBinding>
operatorBindings(const substrait::Rel& rel) {
  pangrova::protocol::v2::RelationDetailEnvelope envelope;
  if (!rel.extension_single().detail().UnpackTo(&envelope)) {
    throw std::runtime_error("test relation has no typed relation envelope");
  }
  pangrova::protocol::v2::OperatorBinding binding;
  uint64_t candidate_limit = 0;
  if (envelope.has_retrieval_source()) {
    const auto& detail = envelope.retrieval_source();
    binding.set_plan_node_id(detail.plan_node_id());
    *binding.mutable_capability() = detail.invocation().capability();
    candidate_limit = detail.candidate_limit();
    auto* approximation =
        binding.mutable_execution_policy()->mutable_approximation();
    approximation->set_profile_id("exact");
    approximation->set_exact_rescore_limit(candidate_limit);
    approximation->set_required_recall_at_k(1.0);
    approximation->set_recall_k(candidate_limit);
  } else if (envelope.has_retrieval_merge()) {
    const auto& detail = envelope.retrieval_merge();
    binding.set_plan_node_id(detail.plan_node_id());
    *binding.mutable_capability() = detail.invocation().capability();
    candidate_limit = detail.output_limit();
  } else if (envelope.has_rerank()) {
    const auto& detail = envelope.rerank();
    binding.set_plan_node_id(detail.plan_node_id());
    *binding.mutable_capability() = detail.invocation().capability();
    auto* model = binding.mutable_model()->mutable_model();
    model->set_id(detail.model().model_id());
    model->set_version(detail.model().version());
    model->set_signature_sha256(detail.model().signature_sha256());
    binding.mutable_model()->set_service_id("worker-local-governed-provider");
    binding.mutable_model()->set_route_policy_version("exact-worker-inventory");
    auto* request_limits = binding.mutable_model()->mutable_request_limits();
    request_limits->set_max_input_tokens(4096);
    request_limits->set_max_request_bytes(4096);
    request_limits->set_max_response_bytes(4096);
    auto* resources = binding.mutable_resources();
    resources->set_provider_concurrency(2);
    resources->set_provider_items(detail.input_limit());
    resources->set_provider_attempts(detail.input_limit());
    resources->set_provider_request_bytes(4'096);
    resources->set_provider_response_bytes(4096);
    resources->set_media_bytes(0);
  } else {
    throw std::runtime_error("unsupported test relation envelope");
  }
  auto* policy = binding.mutable_execution_policy();
  policy->set_coverage_policy(
      pangrova::protocol::v2::COVERAGE_POLICY_REQUIRE_COMPLETE);
  policy->set_fallback_policy(
      pangrova::protocol::v2::FALLBACK_POLICY_FORBID);
  policy->set_retry_class(envelope.has_rerank()
          ? pangrova::protocol::v2::RETRY_CLASS_PROVIDER_IDEMPOTENCY_KEY
          : pangrova::protocol::v2::RETRY_CLASS_IDEMPOTENT);
  if (envelope.has_rerank()) {
    policy->set_idempotency_key("query:stage:partition:rerank-1");
  } else {
    policy->mutable_approximation()->set_candidate_limit(candidate_limit);
  }
  return {{binding.plan_node_id(), std::move(binding)}};
}

std::shared_ptr<const facebook::velox::core::PlanNode> convertRelation(
    VeloxPlanConverter& converter,
    const substrait::Rel& rel) {
  return converter.convert(
      serializePlan(rel), {}, false, "ArrowBatch", operatorBindings(rel));
}

}  // namespace

TEST(TypedRetrievalPlanConversionTest, ExactTopKBuildsDeterministicPlan) {
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto rel = exactTopKRelation();
  auto result = convertRelation(converter, rel);

  auto final_topn = std::dynamic_pointer_cast<
      const facebook::velox::core::TopNNode>(result);
  ASSERT_NE(final_topn, nullptr);
  EXPECT_FALSE(final_topn->isPartial());
  EXPECT_EQ(final_topn->count(), 7);
  ASSERT_EQ(final_topn->sortingKeys().size(), 2);
  EXPECT_EQ(final_topn->sortingKeys()[0]->name(), physicalFieldAlias(1));
  EXPECT_EQ(final_topn->sortingKeys()[1]->name(), physicalFieldAlias(0));
  EXPECT_EQ(converter.resultColumnLabels(), (std::vector<std::string>{"id", "score"}));

  auto gather = std::dynamic_pointer_cast<const facebook::velox::core::LocalPartitionNode>(final_topn->sources()[0]);
  ASSERT_NE(gather, nullptr);
  EXPECT_EQ(gather->type(), facebook::velox::core::LocalPartitionNode::Type::kGather);
  ASSERT_EQ(gather->sources().size(), 1);
  auto partial_topn = std::dynamic_pointer_cast<
      const facebook::velox::core::TopNNode>(gather->sources()[0]);
  ASSERT_NE(partial_topn, nullptr);
  EXPECT_TRUE(partial_topn->isPartial());
  EXPECT_TRUE(final_topn->requiresSingleThread());
  EXPECT_FALSE(partial_topn->requiresSingleThread());
  ASSERT_EQ(partial_topn->sources().size(), 1);
  EXPECT_EQ(partial_topn->sources()[0]->name(), "TableScan");
}

TEST(TypedRetrievalPlanConversionTest, EveryAnnBuildsExactLocalTopNOverCandidates) {
  struct AnnCase final {
    pangrova::protocol::v2::CapabilityDescriptor capability;
    pangrova::protocol::v2::ArtifactFormatRef format;
    std::string search_parameter;
  };
  const std::vector<AnnCase> cases{
      {pangrova::capability::diskAnnRetrievalDescriptor(),
       pangrova::capability::diskAnnArtifactFormat(), "search_list_size"},
      {pangrova::capability::hnswRetrievalDescriptor(),
       pangrova::capability::hnswArtifactFormat(), "ef_search"},
      {pangrova::capability::ivfPqRetrievalDescriptor(),
       pangrova::capability::ivfPqArtifactFormat(), "nprobes"}};
  for (const auto& test_case : cases) {
    auto rel = exactTopKRelation();
    pangrova::protocol::v2::RelationDetailEnvelope envelope;
    ASSERT_TRUE(rel.extension_single().detail().UnpackTo(&envelope));
    auto* detail = envelope.mutable_retrieval_source();
    detail->set_source_id("catalog.schema.items");
    detail->set_exact_rescore_limit(detail->candidate_limit());
    *detail->mutable_invocation()->mutable_capability() =
        test_case.capability.ref();
    auto* vector = detail->mutable_invocation()->add_options();
    vector->set_name("query_vector");
    vector->mutable_value()->mutable_tensor_value()->set_element_type(
        pangrova::protocol::v2::TensorValue::ELEMENT_TYPE_FLOAT32);
    auto* metric = detail->mutable_invocation()->add_options();
    metric->set_name("metric");
    metric->mutable_value()->set_string_value("L2");
    auto* search = detail->mutable_invocation()->add_options();
    search->set_name(test_case.search_parameter);
    search->mutable_value()->set_int64_value(64);
    rel.mutable_extension_single()->mutable_detail()->PackFrom(envelope);

    auto bindings = operatorBindings(rel);
    auto& binding = bindings.at("retrieval-source-1");
    auto* artifact = binding.add_artifacts();
    artifact->set_artifact_type("VECTOR_INDEX");
    artifact->set_source_id("catalog.schema.items");
    *artifact->mutable_format() = test_case.format;
    *artifact->mutable_row_locator() =
        pangrova::capability::physicalRowLocatorContract();

    const auto converter_registry =
        facebook::velox::exec::FunctionRegistrySnapshot::capture();
    VeloxPlanConverter converter(converter_registry);
    auto result = converter.convert(
        serializePlan(rel), {}, false, "ArrowBatch", bindings);
    auto final_topn = std::dynamic_pointer_cast<
        const facebook::velox::core::TopNNode>(result);
    ASSERT_NE(final_topn, nullptr);
    EXPECT_FALSE(final_topn->isPartial());
    EXPECT_EQ(final_topn->count(), 7);
    auto gather = std::dynamic_pointer_cast<const facebook::velox::core::LocalPartitionNode>(final_topn->sources()[0]);
    ASSERT_NE(gather, nullptr);
    EXPECT_EQ(gather->type(), facebook::velox::core::LocalPartitionNode::Type::kGather);
    ASSERT_EQ(gather->sources().size(), 1);
    auto partial_topn = std::dynamic_pointer_cast<
        const facebook::velox::core::TopNNode>(gather->sources()[0]);
    ASSERT_NE(partial_topn, nullptr);
    EXPECT_TRUE(partial_topn->isPartial());
    EXPECT_TRUE(final_topn->requiresSingleThread());
    EXPECT_FALSE(partial_topn->requiresSingleThread());
  }
}

TEST(TypedRetrievalPlanConversionTest, FanOutRoutesArtifactSplitsToIndependentScans) {
  auto vector_rel = exactTopKRelation();
  auto text_rel = exactTopKRelation();
  pangrova::protocol::v2::RelationDetailEnvelope text_envelope;
  ASSERT_TRUE(text_rel.extension_single().detail().UnpackTo(&text_envelope));
  text_envelope.mutable_retrieval_source()->set_plan_node_id(
      "retrieval-source-2");
  text_envelope.mutable_retrieval_source()->set_source_id("text-source");
  text_rel.mutable_extension_single()->mutable_detail()->PackFrom(text_envelope);

  substrait::Rel fan_out;
  auto* set = fan_out.mutable_set();
  set->set_op(substrait::SetRel_SetOp_SET_OP_UNION_ALL);
  *set->add_inputs() = vector_rel;
  *set->add_inputs() = text_rel;

  auto bindings = operatorBindings(vector_rel);
  auto text_bindings = operatorBindings(text_rel);
  bindings.insert(text_bindings.begin(), text_bindings.end());

  split::SplitConfig vector_split;
  vector_split.split_id = "vector-artifact-split";
  vector_split.file_path = "file:///vector.parquet";
  split::SplitConfig text_split;
  text_split.split_id = "text-artifact-split";
  text_split.file_path = "file:///text.parquet";
  std::unordered_map<std::string, std::vector<split::SplitConfig>> overrides{
      {"retrieval-source-1", {vector_split}},
      {"retrieval-source-2", {text_split}}};

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto result = converter.convert(
      serializePlan(fan_out), {}, false, "ArrowBatch", bindings, nullptr,
      overrides);

  ASSERT_NE(result, nullptr);
  ASSERT_EQ(converter.scanNodeSplitOverrides().size(), 2);
  std::unordered_set<std::string> routed_split_ids;
  for (const auto& [scan_id, splits] : converter.scanNodeSplitOverrides()) {
    EXPECT_FALSE(scan_id.empty());
    ASSERT_EQ(splits.size(), 1);
    routed_split_ids.emplace(splits[0].split_id);
  }
  EXPECT_EQ(
      routed_split_ids,
      (std::unordered_set<std::string>{
          "vector-artifact-split", "text-artifact-split"}));
}

TEST(TypedRetrievalPlanConversionTest, Bm25BuildsExactLocalTopNOverMaterializedScores) {
  auto rel = exactTopKRelation();
  pangrova::protocol::v2::RelationDetailEnvelope envelope;
  ASSERT_TRUE(rel.extension_single().detail().UnpackTo(&envelope));
  auto* detail = envelope.mutable_retrieval_source();
  detail->set_source_id("catalog.schema.items");
  detail->set_exact_rescore_limit(detail->candidate_limit());
  *detail->mutable_invocation()->mutable_capability() =
      pangrova::capability::bm25RetrievalDescriptor().ref();
  auto* query = detail->mutable_invocation()->add_options();
  query->set_name("query_text");
  query->mutable_value()->set_string_value("distributed search");
  auto* analyzer = detail->mutable_invocation()->add_options();
  analyzer->set_name("analyzer");
  analyzer->mutable_value()->set_string_value("standard-v1");
  auto* k1 = detail->mutable_invocation()->add_options();
  k1->set_name("k1");
  k1->mutable_value()->set_double_value(1.2);
  auto* b = detail->mutable_invocation()->add_options();
  b->set_name("b");
  b->mutable_value()->set_double_value(0.75);
  rel.mutable_extension_single()->mutable_detail()->PackFrom(envelope);

  auto bindings = operatorBindings(rel);
  auto& binding = bindings.at("retrieval-source-1");
  auto* artifact = binding.add_artifacts();
  artifact->set_artifact_type("TEXT_INDEX");
  artifact->set_source_id("catalog.schema.items");
  *artifact->mutable_format() = pangrova::capability::bm25ArtifactFormat();
  *artifact->mutable_row_locator() =
      pangrova::capability::physicalRowLocatorContract();

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto result = converter.convert(
      serializePlan(rel), {}, false, "ArrowBatch", bindings);
  auto final_topn = std::dynamic_pointer_cast<
      const facebook::velox::core::TopNNode>(result);
  ASSERT_NE(final_topn, nullptr);
  EXPECT_FALSE(final_topn->isPartial());
  EXPECT_EQ(final_topn->count(), 7);
  auto gather = std::dynamic_pointer_cast<const facebook::velox::core::LocalPartitionNode>(final_topn->sources()[0]);
  ASSERT_NE(gather, nullptr);
  EXPECT_EQ(gather->type(), facebook::velox::core::LocalPartitionNode::Type::kGather);
  ASSERT_EQ(gather->sources().size(), 1);
  auto partial_topn = std::dynamic_pointer_cast<
      const facebook::velox::core::TopNNode>(gather->sources()[0]);
  ASSERT_NE(partial_topn, nullptr);
  EXPECT_TRUE(partial_topn->isPartial());
  EXPECT_TRUE(final_topn->requiresSingleThread());
  EXPECT_FALSE(partial_topn->requiresSingleThread());
}

TEST(TypedRetrievalPlanConversionTest, UnknownCapabilityFailsClosed) {
  auto rel = exactTopKRelation();
  pangrova::protocol::v2::RelationDetailEnvelope envelope;
  ASSERT_TRUE(rel.extension_single().detail().UnpackTo(&envelope));
  envelope.mutable_retrieval_source()
      ->mutable_invocation()
      ->mutable_capability()
      ->set_capability_id("pangrova.retrieval.vector.unknown");
  rel.mutable_extension_single()->mutable_detail()->PackFrom(envelope);

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  EXPECT_THROW(convertRelation(converter, rel), std::runtime_error);
}

TEST(TypedRetrievalPlanConversionTest, UnknownOptionFailsClosed) {
  auto rel = exactTopKRelation();
  pangrova::protocol::v2::RelationDetailEnvelope envelope;
  ASSERT_TRUE(rel.extension_single().detail().UnpackTo(&envelope));
  envelope.mutable_retrieval_source()
      ->mutable_invocation()
      ->add_options()
      ->set_name("metric");
  rel.mutable_extension_single()->mutable_detail()->PackFrom(envelope);

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  EXPECT_THROW(convertRelation(converter, rel), std::runtime_error);
}

TEST(TypedRetrievalPlanConversionTest, CandidateSchemaDriftFailsClosed) {
  auto rel = exactTopKRelation();
  pangrova::protocol::v2::RelationDetailEnvelope envelope;
  ASSERT_TRUE(rel.extension_single().detail().UnpackTo(&envelope));
  envelope.mutable_retrieval_source()
      ->mutable_output_contract()
      ->mutable_schema()
      ->mutable_names(1)
      ->assign("distance");
  rel.mutable_extension_single()->mutable_detail()->PackFrom(envelope);

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  EXPECT_THROW(convertRelation(converter, rel), std::runtime_error);
}

TEST(TypedRetrievalPlanConversionTest, JsonVectorSideChannelIsRejected) {
  substrait::Rel rel;
  auto* extension = rel.mutable_extension_single();
  auto* read = extension->mutable_input()->mutable_read();
  read->mutable_named_table()->add_names("items");
  addI64Column(read->mutable_base_schema(), "id");
  google::protobuf::StringValue detail;
  detail.set_value(
      R"({"op":"vector_search","vectorColumn":"embedding","topK":10})");
  extension->mutable_detail()->PackFrom(detail);

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  EXPECT_THROW(converter.convert(serializePlan(rel), {}), std::runtime_error);
}

TEST(TypedRetrievalPlanConversionTest, RrfMergeBuildsNativeBoundedPlanNode) {
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto rel = retrievalMergeRelation();
  auto result = convertRelation(converter, rel);

  auto merge = std::dynamic_pointer_cast<const RetrievalMergeNode>(result);
  ASSERT_NE(merge, nullptr);
  EXPECT_EQ(merge->config().kind, RetrievalMergeKind::kRrf);
  EXPECT_EQ(merge->config().rrf_k, 60);
  EXPECT_EQ(merge->config().output_limit, 10);
  EXPECT_EQ(merge->config().sources.size(), 2);
  ASSERT_EQ(merge->sources().size(), 1);
  const auto exchange = std::dynamic_pointer_cast<
      const facebook::velox::core::ExchangeNode>(merge->sources()[0]);
  ASSERT_NE(exchange, nullptr);
  EXPECT_EQ(exchange->id(), "retrieval-exchange-input");
  EXPECT_EQ(exchange->serdeKind(), "ArrowBatch");
  EXPECT_TRUE(exchange->sources().empty());
  ASSERT_EQ(exchange->outputType()->size(), 3);
  EXPECT_EQ(exchange->outputType()->names(),
      (std::vector<std::string>{physicalFieldAlias(0), physicalFieldAlias(1), physicalFieldAlias(2)}));
  EXPECT_EQ(exchange->outputType()->childAt(0)->kind(), facebook::velox::TypeKind::BIGINT);
  EXPECT_EQ(exchange->outputType()->childAt(1)->kind(), facebook::velox::TypeKind::DOUBLE);
  EXPECT_EQ(exchange->outputType()->childAt(2)->kind(), facebook::velox::TypeKind::VARCHAR);
  EXPECT_EQ(merge->config().score_channel, 1);
  EXPECT_EQ(merge->config().source_id_channel, 2);
  EXPECT_EQ(merge->config().stable_key_channels, (std::vector<size_t>{0}));
  EXPECT_EQ(merge->outputType()->names(),
      (std::vector<std::string>{physicalFieldAlias(0), physicalFieldAlias(1), physicalFieldAlias(2)}));
  EXPECT_EQ(converter.resultColumnLabels(),
      (std::vector<std::string>{"id", "score", "source_id"}));
}

TEST(TypedRetrievalPlanConversionTest, LinearMergeBindsPerSourceWeights) {
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto rel = retrievalMergeRelation(true);
  auto result = convertRelation(converter, rel);

  auto merge = std::dynamic_pointer_cast<const RetrievalMergeNode>(result);
  ASSERT_NE(merge, nullptr);
  EXPECT_EQ(merge->config().kind, RetrievalMergeKind::kLinear);
  ASSERT_EQ(merge->config().sources.size(), 2);
  EXPECT_DOUBLE_EQ(merge->config().sources[0].weight, 0.7);
  EXPECT_DOUBLE_EQ(merge->config().sources[1].weight, 0.3);
}

TEST(TypedRetrievalPlanConversionTest, MergeMissingOptionFailsClosed) {
  auto rel = retrievalMergeRelation();
  pangrova::protocol::v2::RelationDetailEnvelope envelope;
  ASSERT_TRUE(rel.extension_single().detail().UnpackTo(&envelope));
  envelope.mutable_retrieval_merge()->mutable_invocation()->clear_options();
  rel.mutable_extension_single()->mutable_detail()->PackFrom(envelope);

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  EXPECT_THROW(convertRelation(converter, rel), std::runtime_error);
}

TEST(TypedRetrievalPlanConversionTest, MergeSchemaDriftFailsClosed) {
  auto rel = retrievalMergeRelation();
  pangrova::protocol::v2::RelationDetailEnvelope envelope;
  ASSERT_TRUE(rel.extension_single().detail().UnpackTo(&envelope));
  envelope.mutable_retrieval_merge()
      ->mutable_output_contract()
      ->mutable_schema()
      ->set_names(1, "fused_score");
  rel.mutable_extension_single()->mutable_detail()->PackFrom(envelope);

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  EXPECT_THROW(convertRelation(converter, rel), std::runtime_error);
}

TEST(TypedRetrievalPlanConversionTest, MergeOutsideSingletonStageFailsClosed) {
  auto rel = retrievalMergeRelation();
  auto* input = rel.mutable_extension_single()->mutable_input();
  input->clear_extension_leaf();
  auto* read = input->mutable_read();
  read->mutable_named_table()->add_names("candidates");
  addI64Column(read->mutable_base_schema(), "id");
  addFp64Column(read->mutable_base_schema(), "score");
  addStringColumn(read->mutable_base_schema(), "source_id");

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  EXPECT_THROW(convertRelation(converter, rel), std::runtime_error);
}

TEST(TypedRetrievalPlanConversionTest, CrossEncoderRerankBuildsGovernedBoundedPlan) {
  // Actual JVM callback implementation/signature registration; this component
  // conversion check does not claim a Java/provider invocation or an E2E pass.
  pangrova::udf::NativeUdfRegistry registry;
  for (const auto& declared : pangrova::capability::generated::runtimeCallbackSignatures(
           pangrova::capability::kGovernedRerankFunction)) {
    registry.registerScalar(pangrova::udf::makeJvmScalarFunctionSpec(declared.name,
        std::vector<std::string>(declared.arguments.begin(), declared.arguments.end()),
        declared.returnType, declared.deterministic));
  }
  registry.registerAllWithVelox();
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto rel = rerankRelation();
  auto result = convertRelation(converter, rel);

  auto final_topn = std::dynamic_pointer_cast<
      const facebook::velox::core::TopNNode>(result);
  ASSERT_NE(final_topn, nullptr);
  EXPECT_FALSE(final_topn->isPartial());
  EXPECT_EQ(final_topn->count(), 10);
  ASSERT_EQ(final_topn->sortingKeys().size(), 2);
  EXPECT_EQ(final_topn->sortingKeys()[0]->name(), physicalFieldAlias(1));
  EXPECT_EQ(final_topn->sortingKeys()[1]->name(), physicalFieldAlias(0));

  auto gather = std::dynamic_pointer_cast<const facebook::velox::core::LocalPartitionNode>(final_topn->sources()[0]);
  ASSERT_NE(gather, nullptr);
  EXPECT_EQ(gather->type(), facebook::velox::core::LocalPartitionNode::Type::kGather);
  ASSERT_EQ(gather->sources().size(), 1);
  auto partial_topn = std::dynamic_pointer_cast<
      const facebook::velox::core::TopNNode>(gather->sources()[0]);
  ASSERT_NE(partial_topn, nullptr);
  EXPECT_TRUE(partial_topn->isPartial());
  EXPECT_TRUE(final_topn->requiresSingleThread());
  EXPECT_FALSE(partial_topn->requiresSingleThread());
  auto project = std::dynamic_pointer_cast<
      const facebook::velox::core::ProjectNode>(partial_topn->sources()[0]);
  ASSERT_NE(project, nullptr);
  ASSERT_EQ(project->projections().size(), 3);
  auto score = std::dynamic_pointer_cast<
      const facebook::velox::core::CallTypedExpr>(project->projections()[1]);
  ASSERT_NE(score, nullptr);
  EXPECT_EQ(score->name(), pangrova::capability::kGovernedRerankFunction);
  EXPECT_EQ(score->inputs().size(), pangrova::capability::generated::AI_RERANK.size());
}

TEST(TypedRetrievalPlanConversionTest, CrossEncoderModelDriftFailsClosed) {
  auto rel = rerankRelation();
  auto bindings = operatorBindings(rel);
  bindings.at("rerank-1")
      .mutable_model()
      ->mutable_model()
      ->set_version("wrong-version");

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  EXPECT_THROW(
      converter.convert(serializePlan(rel), {}, false, "ArrowBatch", bindings),
      std::runtime_error);
}

}  // namespace pangrova::velox::test
