#include <gtest/gtest.h>
#include "../CanonicalTestPlan.h"
#include "velox/expression/FunctionRegistrySnapshot.h"

#include <openssl/evp.h>

#include <array>
#include <bit>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <limits>
#include <string>
#include <unordered_map>
#include <vector>

#include "artifact/ArtifactRetrievalPlanner.h"
#include "artifact/ImmutableArtifactCache.h"
#include "artifact/RowLocatorSidecar.h"
#include "capability/NativeCapabilityCatalog.h"
#include "pangrova/protocol/v2/plan_extension.pb.h"
#include "substrait/plan.pb.h"
#include "text/bm25/Bm25Segment.h"
#include "vector/ann/BinaryIndexIo.h"
#include "vector/diskann/DiskAnnBuilder.h"
#include "vector/diskann/DiskAnnIndexCache.h"
#include "vector/hnsw/HnswIndex.h"
#include "vector/ivfpq/IvfPqIndex.h"
#include "velox/plan/VeloxPlanConverter.h"

namespace pangrova::artifact::test {
namespace {

namespace proto = pangrova::protocol::v2;

enum class AnnAlgorithm : uint8_t { DISKANN, HNSW, IVF_PQ };

std::string annName(AnnAlgorithm algorithm) {
  if (algorithm == AnnAlgorithm::HNSW) {
    return "hnsw";
  }
  return algorithm == AnnAlgorithm::IVF_PQ ? "ivfpq" : "diskann";
}

proto::CapabilityDescriptor annRetrieval(AnnAlgorithm algorithm) {
  if (algorithm == AnnAlgorithm::HNSW) {
    return pangrova::capability::hnswRetrievalDescriptor();
  }
  return algorithm == AnnAlgorithm::IVF_PQ
      ? pangrova::capability::ivfPqRetrievalDescriptor()
      : pangrova::capability::diskAnnRetrievalDescriptor();
}

proto::ArtifactFormatRef annFormat(AnnAlgorithm algorithm) {
  if (algorithm == AnnAlgorithm::HNSW) {
    return pangrova::capability::hnswArtifactFormat();
  }
  return algorithm == AnnAlgorithm::IVF_PQ
      ? pangrova::capability::ivfPqArtifactFormat()
      : pangrova::capability::diskAnnArtifactFormat();
}

class TestDirectory final {
 public:
  TestDirectory() {
    path_ = std::filesystem::temp_directory_path() /
        ("pangrova-artifact-retrieval-" + std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(path_);
  }
  ~TestDirectory() {
    ImmutableArtifactCache::instance().shutdown();
    if (vector::diskann::DiskAnnIndexCache::instance().enabled()) {
      vector::diskann::DiskAnnIndexCache::instance().shutdown();
    }
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  const std::filesystem::path& path() const { return path_; }

 private:
  std::filesystem::path path_;
};

std::string sha256File(const std::filesystem::path& path) {
  std::ifstream input(path, std::ios::binary);
  EVP_MD_CTX* context = EVP_MD_CTX_new();
  EXPECT_NE(context, nullptr);
  EXPECT_EQ(EVP_DigestInit_ex(context, EVP_sha256(), nullptr), 1);
  std::string buffer(4096, '\0');
  while (input) {
    input.read(buffer.data(), buffer.size());
    if (input.gcount() > 0) {
      EXPECT_EQ(EVP_DigestUpdate(
          context, buffer.data(), static_cast<size_t>(input.gcount())), 1);
    }
  }
  std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
  unsigned int size = 0;
  EXPECT_EQ(EVP_DigestFinal_ex(context, digest.data(), &size), 1);
  EVP_MD_CTX_free(context);
  return {reinterpret_cast<const char*>(digest.data()), size};
}

void addObject(
    proto::ArtifactBinding* artifact,
    const std::filesystem::path& path,
    const std::string& role,
    uint32_t ordinal) {
  auto* object = artifact->add_split_objects();
  object->set_object_id("object-1:" + role);
  object->set_opaque_uri(path.string());
  object->set_size_bytes(std::filesystem::file_size(path));
  object->mutable_checksum()->set_algorithm(proto::Checksum::ALGORITHM_SHA256);
  object->mutable_checksum()->set_digest(sha256File(path));
  object->set_object_role(role);
  object->set_source_object_id("object-1");
  object->set_row_count(4);
  object->set_ordinal(ordinal);
  object->set_scope(proto::ARTIFACT_OBJECT_SCOPE_SOURCE_OBJECT);
}

void addGenerationObject(
    proto::ArtifactBinding* artifact,
    const std::filesystem::path& path,
    const std::string& role,
    uint32_t ordinal) {
  auto* object = artifact->add_split_objects();
  object->set_object_id("generation-1:" + role);
  object->set_opaque_uri(path.string());
  object->set_size_bytes(std::filesystem::file_size(path));
  object->mutable_checksum()->set_algorithm(proto::Checksum::ALGORITHM_SHA256);
  object->mutable_checksum()->set_digest(sha256File(path));
  object->set_object_role(role);
  object->set_source_object_id(artifact->generation());
  object->set_row_count(4);
  object->set_ordinal(ordinal);
  object->set_scope(proto::ARTIFACT_OBJECT_SCOPE_GENERATION);
}

void addOption(
    proto::CapabilityInvocation* invocation,
    const std::string& name,
    const proto::TypedValue& value) {
  auto* option = invocation->add_options();
  option->set_name(name);
  *option->mutable_value() = value;
}

std::vector<uint8_t> buildPlan(
    AnnAlgorithm algorithm = AnnAlgorithm::DISKANN,
    const std::string& metric_name = "L2", uint32_t candidate_limit = 2) {
  substrait::Plan plan;
  auto* root = plan.add_relations()->mutable_root();
  root->add_names("id");
  root->add_names("score");
  auto* extension = root->mutable_input()->mutable_extension_single();
  auto* read = extension->mutable_input()->mutable_read();
  read->mutable_named_table()->add_names("items");
  read->mutable_base_schema()->add_names("id");
  read->mutable_base_schema()->mutable_struct_()->add_types()->mutable_i64()
      ->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  read->mutable_base_schema()->add_names("score");
  read->mutable_base_schema()->mutable_struct_()->add_types()->mutable_fp64()
      ->set_nullability(substrait::Type::NULLABILITY_NULLABLE);

  proto::RelationDetailEnvelope envelope;
  envelope.set_schema_version(1);
  auto* detail = envelope.mutable_retrieval_source();
  detail->set_plan_node_id(annName(algorithm) + "-1");
  detail->set_source_id("catalog.schema.items");
  detail->set_candidate_limit(candidate_limit);
  detail->set_output_limit(2);
  detail->set_exact_rescore_limit(2);
  auto* invocation = detail->mutable_invocation();
  *invocation->mutable_capability() = annRetrieval(algorithm).ref();
  auto* score = invocation->add_arguments();
  score->set_name("score");
  score->mutable_expression()->mutable_selection()->mutable_root_reference();
  score->mutable_expression()
      ->mutable_selection()
      ->mutable_direct_reference()
      ->mutable_struct_field()
      ->set_field(1);
  auto* contract = detail->mutable_output_contract();
  contract->set_schema_version(
      pangrova::capability::kRetrievalCandidateSchemaVersion);
  contract->set_identity_kind(pangrova::protocol::v2::CANDIDATE_IDENTITY_KIND_VALUE_FIELDS);
  contract->set_score_direction(proto::SCORE_DIRECTION_LOWER_IS_BETTER);
  contract->add_stable_key_fields("id");
  contract->mutable_schema()->add_names("id");
  contract->mutable_schema()->mutable_struct_()->add_types()->mutable_i64()
      ->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  contract->mutable_schema()->add_names("score");
  contract->mutable_schema()->mutable_struct_()->add_types()->mutable_fp64()
      ->set_nullability(substrait::Type::NULLABILITY_NULLABLE);

  proto::TypedValue vector;
  vector.mutable_type()->set_canonical_sql("VECTOR(4, FLOAT)");
  auto* tensor = vector.mutable_tensor_value();
  tensor->set_element_type(proto::TensorValue::ELEMENT_TYPE_FLOAT32);
  tensor->add_shape(4);
  std::string packed;
  for (float value : std::vector<float>{1, 0, 0, 0}) {
    const uint32_t bits = std::bit_cast<uint32_t>(value);
    for (size_t byte = 0; byte < sizeof(uint32_t); ++byte) {
      packed.push_back(static_cast<char>((bits >> (byte * 8)) & 0xffU));
    }
  }
  tensor->set_packed_little_endian_values(packed);
  addOption(invocation, "query_vector", vector);
  proto::TypedValue metric;
  metric.set_string_value(metric_name);
  addOption(invocation, "metric", metric);
  proto::TypedValue search_list;
  search_list.set_int64_value(4);
  const std::string search_parameter = algorithm == AnnAlgorithm::HNSW
      ? "ef_search"
      : algorithm == AnnAlgorithm::IVF_PQ ? "nprobes" : "search_list_size";
  addOption(invocation, search_parameter, search_list);
  extension->mutable_detail()->PackFrom(envelope);

  const auto bytes = pangrova::test::canonicalTestPlan(plan).SerializeAsString();
  return {bytes.begin(), bytes.end()};
}

std::vector<uint8_t> buildBm25Plan() {
  substrait::Plan plan;
  auto* root = plan.add_relations()->mutable_root();
  root->add_names("id");
  root->add_names("score");
  auto* extension = root->mutable_input()->mutable_extension_single();
  auto* read = extension->mutable_input()->mutable_read();
  read->mutable_named_table()->add_names("items");
  read->mutable_base_schema()->add_names("id");
  read->mutable_base_schema()->mutable_struct_()->add_types()->mutable_i64()
      ->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  read->mutable_base_schema()->add_names("score");
  read->mutable_base_schema()->mutable_struct_()->add_types()->mutable_fp64()
      ->set_nullability(substrait::Type::NULLABILITY_NULLABLE);

  proto::RelationDetailEnvelope envelope;
  envelope.set_schema_version(1);
  auto* detail = envelope.mutable_retrieval_source();
  detail->set_plan_node_id("bm25-1");
  detail->set_source_id("catalog.schema.items");
  detail->set_candidate_limit(2);
  detail->set_output_limit(2);
  detail->set_exact_rescore_limit(2);
  auto* invocation = detail->mutable_invocation();
  *invocation->mutable_capability() =
      pangrova::capability::bm25RetrievalDescriptor().ref();
  auto* score = invocation->add_arguments();
  score->set_name("score");
  score->mutable_expression()->mutable_selection()->mutable_root_reference();
  score->mutable_expression()
      ->mutable_selection()
      ->mutable_direct_reference()
      ->mutable_struct_field()
      ->set_field(1);
  proto::TypedValue query;
  query.mutable_type()->set_canonical_sql("VARCHAR");
  query.set_string_value("distributed search");
  addOption(invocation, "query_text", query);
  proto::TypedValue analyzer;
  analyzer.mutable_type()->set_canonical_sql("VARCHAR");
  analyzer.set_string_value("standard-v1");
  addOption(invocation, "analyzer", analyzer);
  proto::TypedValue k1;
  k1.mutable_type()->set_canonical_sql("DOUBLE");
  k1.set_double_value(1.2);
  addOption(invocation, "k1", k1);
  proto::TypedValue b;
  b.mutable_type()->set_canonical_sql("DOUBLE");
  b.set_double_value(0.75);
  addOption(invocation, "b", b);
  extension->mutable_detail()->PackFrom(envelope);
  const auto bytes = pangrova::test::canonicalTestPlan(plan).SerializeAsString();
  return {bytes.begin(), bytes.end()};
}

proto::OperatorBinding buildBinding(
    const std::filesystem::path& root,
    AnnAlgorithm algorithm = AnnAlgorithm::DISKANN,
    const std::string& metric_name = "L2", uint32_t candidate_limit = 2) {
  proto::OperatorBinding binding;
  binding.set_plan_node_id(annName(algorithm) + "-1");
  *binding.mutable_capability() = annRetrieval(algorithm).ref();
  auto* policy = binding.mutable_execution_policy();
  policy->set_coverage_policy(proto::COVERAGE_POLICY_REQUIRE_COMPLETE);
  policy->set_fallback_policy(proto::FALLBACK_POLICY_FORBID);
  policy->set_retry_class(proto::RETRY_CLASS_IDEMPOTENT);
  auto* approximation = policy->mutable_approximation();
  approximation->set_profile_id(annName(algorithm) +
      (algorithm == AnnAlgorithm::DISKANN ? "-l2-exact-rescore" : "-metric-exact-rescore"));
  approximation->set_candidate_limit(candidate_limit);
  approximation->set_exact_rescore_limit(2);
  approximation->set_required_recall_at_k(0.95);
  approximation->set_recall_k(2);

  auto* artifact = binding.add_artifacts();
  artifact->set_artifact_id("index-1");
  artifact->set_artifact_type("VECTOR_INDEX");
  artifact->set_generation("1");
  artifact->set_source_id("catalog.schema.items");
  artifact->set_source_snapshot("snapshot-1");
  *artifact->mutable_format() = annFormat(algorithm);
  *artifact->mutable_row_locator() =
      pangrova::capability::physicalRowLocatorContract();
  auto* dimension = artifact->add_properties();
  dimension->set_name("dimension");
  dimension->mutable_value()->set_int64_value(4);
  if (algorithm != AnnAlgorithm::DISKANN) {
    auto* metric = artifact->add_properties();
    metric->set_name("metric");
    metric->mutable_value()->set_string_value(metric_name);
  }
  const std::string role = algorithm == AnnAlgorithm::HNSW
      ? "HNSW_GRAPH"
      : algorithm == AnnAlgorithm::IVF_PQ ? "IVF_PQ_INDEX" : "DISKANN_GRAPH";
  addObject(
      artifact, root / ("segment." + annName(algorithm)), role, 0);
  addObject(
      artifact, root / ("segment." + annName(algorithm) + ".vecs"),
      "RAW_VECTORS", 1);
  addObject(artifact, root / "segment.locators", "ROW_LOCATORS", 2);
  return binding;
}

proto::OperatorBinding buildBm25Binding(const std::filesystem::path& root) {
  proto::OperatorBinding binding;
  binding.set_plan_node_id("bm25-1");
  *binding.mutable_capability() =
      pangrova::capability::bm25RetrievalDescriptor().ref();
  auto* policy = binding.mutable_execution_policy();
  policy->set_coverage_policy(proto::COVERAGE_POLICY_REQUIRE_COMPLETE);
  policy->set_fallback_policy(proto::FALLBACK_POLICY_FORBID);
  policy->set_retry_class(proto::RETRY_CLASS_IDEMPOTENT);
  auto* approximation = policy->mutable_approximation();
  approximation->set_profile_id("bm25-shard-exact");
  approximation->set_candidate_limit(2);
  approximation->set_exact_rescore_limit(2);
  approximation->set_required_recall_at_k(1.0);
  approximation->set_recall_k(2);

  auto* artifact = binding.add_artifacts();
  artifact->set_artifact_id("text-index-1");
  artifact->set_artifact_type("TEXT_INDEX");
  artifact->set_generation("1");
  artifact->set_source_id("catalog.schema.items");
  artifact->set_source_snapshot("snapshot-1");
  *artifact->mutable_format() = pangrova::capability::bm25ArtifactFormat();
  *artifact->mutable_row_locator() =
      pangrova::capability::physicalRowLocatorContract();
  auto* analyzer = artifact->add_properties();
  analyzer->set_name("analyzer");
  analyzer->mutable_value()->set_string_value("standard-v1");
  addObject(artifact, root / "segment.bm25", "BM25_POSTINGS", 0);
  addObject(artifact, root / "segment.locators", "ROW_LOCATORS", 1);
  addGenerationObject(
      artifact, root / "corpus.stats", "BM25_CORPUS_STATS", 0);
  return binding;
}

void buildArtifacts(
    const std::filesystem::path& root,
    AnnAlgorithm algorithm = AnnAlgorithm::DISKANN,
    vector::DistanceMetric metric = vector::DistanceMetric::L2,
    const std::vector<float>& vectors = {
        1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1}) {
  vector::diskann::VamanaConfig config;
  config.dimension = 4;
  config.maxDegree = 2;
  config.searchListSize = 4;
  config.pqSubvectors = 1;
  config.pqBitsPerCode = 2;
  config.beamWidth = 2;
  const auto index_path = root / ("segment." + annName(algorithm));
  if (algorithm == AnnAlgorithm::DISKANN) {
    vector::diskann::DiskAnnBuilder(config).buildFromVectors(
        vectors.data(), 4, 4, index_path.string());
  } else if (algorithm == AnnAlgorithm::HNSW) {
    vector::hnsw::HnswIndex index({4, 3, 4, metric});
    index.build(vectors.data(), 4, 4);
    ASSERT_TRUE(index.save(index_path.string()));
    std::ofstream raw(index_path.string() + ".vecs", std::ios::binary);
    for (float value : vectors) {
      vector::ann::writeFloat(raw, value);
    }
  } else {
    vector::ivfpq::IvfPqIndex index({4, 4, 1, 2, 4, 4, metric});
    index.build(vectors.data(), 4, 4);
    ASSERT_TRUE(index.save(index_path.string()));
    std::ofstream raw(index_path.string() + ".vecs", std::ios::binary);
    for (float value : vectors) {
      vector::ann::writeFloat(raw, value);
    }
  }
  const std::vector<uint64_t> positions{10, 20, 30, 40};
  RowLocatorSidecar::writePhysicalRowPositions(
      (root / "segment.locators").string(), positions);
}

void buildBm25Artifacts(const std::filesystem::path& root) {
  text::bm25::Bm25SegmentBuilder builder;
  builder.addDocument("distributed query engine search");
  builder.addDocument("unrelated document");
  builder.addDocument("distributed distributed search infrastructure");
  builder.addDocument("search engine");
  builder.write((root / "segment.bm25").string());
  builder.writeStatistics((root / "corpus.stats").string());
  const std::vector<uint64_t> positions{10, 20, 30, 40};
  RowLocatorSidecar::writePhysicalRowPositions(
      (root / "segment.locators").string(), positions);
}

}  // namespace

TEST(ArtifactRetrievalPlannerTest, MaterializesSearchesAndReusesImmutableCache) {
  TestDirectory directory;
  buildArtifacts(directory.path());
  ImmutableArtifactCache::instance().configure(
      (directory.path() / "cache").string(), 16 * 1024 * 1024);
  vector::diskann::DiskAnnIndexCache::instance().configure(16 * 1024 * 1024);
  auto binding = buildBinding(directory.path());
  split::SplitConfig split;
  split.split_id = "split-1";
  split.table_name = "catalog.schema.items";
  split.source_snapshot = "snapshot-1";
  split.data_object_id = "object-1";
  split.file_path = (directory.path() / "data.parquet").string();

  auto prepared = ArtifactRetrievalPlanner::prepare(
      buildPlan(), {{"diskann-1", binding}}, {split}, 16 * 1024 * 1024);
  ASSERT_EQ(prepared->splits.size(), 1);
  EXPECT_FALSE(prepared->splits[0].row_position_filter_enabled);
  ASSERT_TRUE(prepared->split_overrides.contains("diskann-1"));
  const auto& prepared_split = prepared->split_overrides.at("diskann-1")[0];
  EXPECT_TRUE(prepared_split.row_position_filter_enabled);
  ASSERT_EQ(prepared_split.row_position_allowlist.size(), 2);
  EXPECT_TRUE(std::binary_search(
      prepared_split.row_position_allowlist.begin(),
      prepared_split.row_position_allowlist.end(),
      10));
  EXPECT_GT(prepared->downloaded_bytes, 0);
  EXPECT_EQ(prepared->leases.size(), 3);

  auto cached = ArtifactRetrievalPlanner::prepare(
      buildPlan(), {{"diskann-1", binding}}, {split}, 0);
  EXPECT_EQ(cached->downloaded_bytes, 0);
  EXPECT_EQ(cached->split_overrides.at("diskann-1")[0].row_position_allowlist,
            prepared_split.row_position_allowlist);
}

TEST(ArtifactRetrievalPlannerTest, ExactTopKRequiresZeroDetailRescoreAndNoArtifact) {
  TestDirectory directory;
  buildArtifacts(directory.path());
  auto bytes = buildPlan();
  substrait::Plan plan;
  ASSERT_TRUE(plan.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));
  auto* extension = plan.mutable_relations(0)
      ->mutable_root()
      ->mutable_input()
      ->mutable_extension_single();
  proto::RelationDetailEnvelope envelope;
  ASSERT_TRUE(extension->detail().UnpackTo(&envelope));
  auto* detail = envelope.mutable_retrieval_source();
  *detail->mutable_invocation()->mutable_capability() =
      pangrova::capability::exactTopKRetrievalDescriptor().ref();
  detail->mutable_invocation()->clear_options();
  detail->set_exact_rescore_limit(0);
  extension->mutable_detail()->PackFrom(envelope);
  const auto serialized = pangrova::test::canonicalTestPlan(plan).SerializeAsString();

  auto binding = buildBinding(directory.path());
  *binding.mutable_capability() =
      pangrova::capability::exactTopKRetrievalDescriptor().ref();
  binding.clear_artifacts();
  binding.mutable_execution_policy()
      ->mutable_approximation()
      ->set_exact_rescore_limit(detail->candidate_limit());
  split::SplitConfig split;
  split.split_id = "split-1";
  auto prepared = ArtifactRetrievalPlanner::prepare(
      {serialized.begin(), serialized.end()}, {{"diskann-1", binding}},
      {split}, 0);
  EXPECT_EQ(prepared->splits.size(), 1);
  EXPECT_TRUE(prepared->split_overrides.empty());
  EXPECT_TRUE(prepared->leases.empty());
}

TEST(ArtifactRetrievalPlannerTest, HnswAndIvfPqExecuteInNativeWithExactRescore) {
  for (AnnAlgorithm algorithm : {AnnAlgorithm::HNSW, AnnAlgorithm::IVF_PQ}) {
    TestDirectory directory;
    buildArtifacts(directory.path(), algorithm);
    ImmutableArtifactCache::instance().configure(
        (directory.path() / "cache").string(), 16 * 1024 * 1024);
    vector::diskann::DiskAnnIndexCache::instance().configure(16 * 1024 * 1024);
    auto binding = buildBinding(directory.path(), algorithm);
    split::SplitConfig split;
    split.split_id = "split-1";
    split.table_name = "catalog.schema.items";
    split.source_snapshot = "snapshot-1";
    split.data_object_id = "object-1";

    const std::string plan_node = annName(algorithm) + "-1";
    auto prepared = ArtifactRetrievalPlanner::prepare(
        buildPlan(algorithm), {{plan_node, binding}}, {split},
        16 * 1024 * 1024);
    const auto& positions = prepared->split_overrides.at(plan_node)[0]
        .row_position_allowlist;
    ASSERT_EQ(positions.size(), 2);
    EXPECT_TRUE(std::binary_search(positions.begin(), positions.end(), 10));
    EXPECT_EQ(prepared->leases.size(), 3);

    const auto converter_registry =
        facebook::velox::exec::FunctionRegistrySnapshot::capture();
    pangrova::velox::VeloxPlanConverter converter(converter_registry);
    auto converted = converter.convert(
        buildPlan(algorithm), prepared->splits, false, "ArrowBatch",
        {{plan_node, binding}}, nullptr, prepared->split_overrides);
    auto final_topn = std::dynamic_pointer_cast<
        const facebook::velox::core::TopNNode>(converted);
    ASSERT_NE(final_topn, nullptr);
    EXPECT_FALSE(final_topn->isPartial());
    EXPECT_EQ(final_topn->count(), 2);
    EXPECT_EQ(converter.scanNodeSplitOverrides().size(), 1);
  }
}

TEST(ArtifactRetrievalPlannerTest, EveryAnnMetricUsesMatchingExactRescore) {
  const std::vector<float> vectors{
      1, 0.25F, 0, 0,
      2, 0, 0, 0,
      100, 1, 0, 0,
      -1, 0, 0, 0};
  for (auto algorithm : {AnnAlgorithm::HNSW, AnnAlgorithm::IVF_PQ}) {
    for (const std::string metric_name : {"L2", "COSINE", "INNER_PRODUCT"}) {
      TestDirectory directory;
      buildArtifacts(directory.path(), algorithm,
          vector::distanceMetricFromName(metric_name), vectors);
      ImmutableArtifactCache::instance().configure(
          (directory.path() / "cache").string(), 16 * 1024 * 1024);
      vector::diskann::DiskAnnIndexCache::instance().configure(16 * 1024 * 1024);
      auto binding = buildBinding(directory.path(), algorithm, metric_name, 4);
      split::SplitConfig split;
      split.split_id = "split-1";
      split.table_name = "catalog.schema.items";
      split.source_snapshot = "snapshot-1";
      split.data_object_id = "object-1";
      const auto plan_node = annName(algorithm) + "-1";
      auto prepared = ArtifactRetrievalPlanner::prepare(
          buildPlan(algorithm, metric_name, 4), {{plan_node, binding}}, {split},
          16 * 1024 * 1024);
      // All four candidates are scored from RAW_VECTORS; only two survive.
      // Hard-coded L2 rescore would return {10,20} for every metric.
      EXPECT_EQ(prepared->split_overrides.at(plan_node)[0].row_position_allowlist,
          metric_name == "L2" ? (std::vector<uint64_t>{10, 20})
                              : (std::vector<uint64_t>{20, 30}));
      EXPECT_EQ(prepared->leases.size(), 3);
    }
  }
}

TEST(ArtifactRetrievalPlannerTest, AnnMetricFencesQueryBindingAndPersistedBytes) {
  for (auto algorithm : {AnnAlgorithm::HNSW, AnnAlgorithm::IVF_PQ}) {
    TestDirectory directory;
    buildArtifacts(directory.path(), algorithm); // Persisted L2 bytes.
    ImmutableArtifactCache::instance().configure(
        (directory.path() / "cache").string(), 16 * 1024 * 1024);
    vector::diskann::DiskAnnIndexCache::instance().configure(16 * 1024 * 1024);
    const auto plan_node = annName(algorithm) + "-1";
    split::SplitConfig split;
    split.split_id = "split-1";
    split.table_name = "catalog.schema.items";
    split.source_snapshot = "snapshot-1";
    split.data_object_id = "object-1";
    auto binding = buildBinding(directory.path(), algorithm, "COSINE");
    EXPECT_THROW(ArtifactRetrievalPlanner::prepare(
        buildPlan(algorithm, "INNER_PRODUCT"), {{plan_node, binding}}, {split},
        16 * 1024 * 1024), std::runtime_error);
    EXPECT_THROW(ArtifactRetrievalPlanner::prepare(
        buildPlan(algorithm, "COSINE"), {{plan_node, binding}}, {split},
        16 * 1024 * 1024), std::runtime_error);
    // A metric is mandatory in v2 metadata, not inferred from a query.
    binding.mutable_artifacts(0)->mutable_properties()->DeleteSubrange(1, 1);
    EXPECT_THROW(ArtifactRetrievalPlanner::prepare(
        buildPlan(algorithm), {{plan_node, binding}}, {split},
        16 * 1024 * 1024), std::runtime_error);
  }
}

TEST(ArtifactRetrievalPlannerTest, RejectsNonfiniteRecallBeforeReadingActualArtifacts) {
  TestDirectory directory;
  buildArtifacts(directory.path());
  split::SplitConfig split;
  split.table_name = "catalog.schema.items";
  split.source_snapshot = "snapshot-1";
  split.data_object_id = "object-1";
  for (double invalid : {std::numeric_limits<double>::quiet_NaN(),
      std::numeric_limits<double>::infinity()}) {
    auto binding = buildBinding(directory.path());
    binding.mutable_execution_policy()->mutable_approximation()->set_required_recall_at_k(invalid);
    EXPECT_THROW(ArtifactRetrievalPlanner::prepare(buildPlan(), {{"diskann-1", binding}},
        {split}, 16 * 1024 * 1024), std::runtime_error);
  }
}

TEST(ArtifactRetrievalPlannerTest, SharedSourceRangesRetainOneActualMaterializationAndLocatorDomain) {
  TestDirectory directory;
  buildArtifacts(directory.path());
  ImmutableArtifactCache::instance().configure((directory.path() / "cache").string(), 16 * 1024 * 1024);
  vector::diskann::DiskAnnIndexCache::instance().configure(16 * 1024 * 1024);
  split::SplitConfig first;
  first.split_id = "range-a";
  first.table_name = "catalog.schema.items";
  first.source_snapshot = "snapshot-1";
  first.data_object_id = "object-1";
  first.start_offset = 0;
  first.length = 100;
  auto second = first;
  second.split_id = "range-b";
  second.start_offset = 100;
  auto prepared = ArtifactRetrievalPlanner::prepare(buildPlan(),
      {{"diskann-1", buildBinding(directory.path())}}, {first, second}, 16 * 1024 * 1024);
  // Split byte ranges are protocol input here. Actual Parquet range reading is
  // covered separately, not inferred from this graph preparation component.
  const auto& selected = prepared->split_overrides.at("diskann-1");
  ASSERT_EQ(selected.size(), 2);
  EXPECT_EQ(selected[0].row_position_allowlist, selected[1].row_position_allowlist);
  EXPECT_EQ(selected[0].row_position_allowlist.size(), 2);
  EXPECT_EQ(prepared->leases.size(), 3);
  EXPECT_EQ(prepared->vector_search_owners.size(), 1);
  EXPECT_GT(prepared->downloaded_bytes, 0);
}

TEST(ArtifactRetrievalPlannerTest, FailsClosedOnDimensionDrift) {
  TestDirectory directory;
  buildArtifacts(directory.path());
  ImmutableArtifactCache::instance().configure(
      (directory.path() / "cache").string(), 16 * 1024 * 1024);
  vector::diskann::DiskAnnIndexCache::instance().configure(16 * 1024 * 1024);
  auto binding = buildBinding(directory.path());
  binding.mutable_artifacts(0)
      ->mutable_properties(0)
      ->mutable_value()
      ->set_int64_value(3);
  split::SplitConfig split;
  split.table_name = "catalog.schema.items";
  split.source_snapshot = "snapshot-1";
  split.data_object_id = "object-1";
  EXPECT_THROW(
      ArtifactRetrievalPlanner::prepare(
          buildPlan(), {{"diskann-1", binding}}, {split}, 16 * 1024 * 1024),
      std::runtime_error);
}

TEST(ArtifactRetrievalPlannerTest, MaterializesBm25CandidatesAndExactScores) {
  TestDirectory directory;
  buildBm25Artifacts(directory.path());
  ImmutableArtifactCache::instance().configure(
      (directory.path() / "cache").string(), 16 * 1024 * 1024);
  vector::diskann::DiskAnnIndexCache::instance().configure(16 * 1024 * 1024);
  auto binding = buildBm25Binding(directory.path());
  split::SplitConfig split;
  split.split_id = "split-1";
  split.table_name = "catalog.schema.items";
  split.source_snapshot = "snapshot-1";
  split.data_object_id = "object-1";
  split.file_path = (directory.path() / "data.parquet").string();

  auto prepared = ArtifactRetrievalPlanner::prepare(
      buildBm25Plan(), {{"bm25-1", binding}}, {split}, 16 * 1024 * 1024);

  ASSERT_EQ(prepared->splits.size(), 1);
  EXPECT_FALSE(prepared->splits[0].row_position_filter_enabled);
  ASSERT_TRUE(prepared->split_overrides.contains("bm25-1"));
  const auto& prepared_split = prepared->split_overrides.at("bm25-1")[0];
  EXPECT_TRUE(prepared_split.row_position_filter_enabled);
  EXPECT_TRUE(prepared_split.materialized_retrieval_score_enabled);
  ASSERT_EQ(prepared_split.row_position_allowlist.size(), 2);
  EXPECT_EQ(prepared_split.materialized_retrieval_scores.size(), 2);
  EXPECT_TRUE(std::binary_search(
      prepared_split.row_position_allowlist.begin(),
      prepared_split.row_position_allowlist.end(), 30));
  for (uint64_t position : prepared_split.row_position_allowlist) {
    auto score = prepared_split.materialized_retrieval_scores.find(position);
    ASSERT_NE(score, prepared_split.materialized_retrieval_scores.end());
    EXPECT_GT(score->second, 0.0);
  }
  EXPECT_EQ(prepared->leases.size(), 3);
}

TEST(ArtifactRetrievalPlannerTest, Bm25AnalyzerDriftFailsClosed) {
  TestDirectory directory;
  buildBm25Artifacts(directory.path());
  ImmutableArtifactCache::instance().configure(
      (directory.path() / "cache").string(), 16 * 1024 * 1024);
  vector::diskann::DiskAnnIndexCache::instance().configure(16 * 1024 * 1024);
  auto binding = buildBm25Binding(directory.path());
  binding.mutable_artifacts(0)
      ->mutable_properties(0)
      ->mutable_value()
      ->set_string_value("unknown");
  split::SplitConfig split;
  split.table_name = "catalog.schema.items";
  split.source_snapshot = "snapshot-1";
  split.data_object_id = "object-1";

  EXPECT_THROW(
      ArtifactRetrievalPlanner::prepare(
          buildBm25Plan(), {{"bm25-1", binding}}, {split}, 16 * 1024 * 1024),
      std::runtime_error);
}

}  // namespace pangrova::artifact::test
