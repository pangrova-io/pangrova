#include <gtest/gtest.h>

#include <openssl/evp.h>
#include <boost/multiprecision/cpp_bin_float.hpp>

#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <set>
#include <folly/CancellationToken.h>

#include "TestMemoryHelper.h"
#include "capabilities/artifact/ArtifactVectorSearch.h"
#include "capabilities/artifact/RowLocatorSidecar.h"
#include "engine/resource/TaskObjectReadBudget.h"
#include "vector/diskann/DiskAnnBuilder.h"
#include "vector/diskann/DiskAnnIndexCache.h"
#include "engine/velox/plan/OperatorMemoryResource.h"

namespace pangrova::artifact::test {
namespace {
namespace proto = pangrova::protocol::v2;

std::string digest(const std::filesystem::path& path) {
  std::ifstream input(path, std::ios::binary);
  std::string bytes((std::istreambuf_iterator<char>(input)), {});
  std::array<unsigned char, EVP_MAX_MD_SIZE> output{};
  unsigned int length = 0;
  if (EVP_Digest(bytes.data(), bytes.size(), output.data(), &length, EVP_sha256(), nullptr) != 1
      || length != 32) throw std::runtime_error("SHA-256 failed for real artifact bytes");
  return {reinterpret_cast<const char*>(output.data()), length};
}

class ArtifactVectorSearchTest : public ::testing::Test {
 protected:
  void SetUp() override {
    directory_ = std::filesystem::temp_directory_path() / ("pangrova-filtered-artifact-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(directory_);
    if (vector::diskann::DiskAnnIndexCache::instance().enabled()) {
      vector::diskann::DiskAnnIndexCache::instance().shutdown();
    }
    ImmutableArtifactCache::instance().shutdown();
    ImmutableArtifactCache::instance().configure((directory_ / "cache").string(), 4 * 1024 * 1024);
    vector::diskann::DiskAnnIndexCache::instance().configure(4 * 1024 * 1024);
    root_ = pangrova::test::testRootPool("FilteredArtifactSearchRoot");
    pool_ = root_->addLeafChild("FilteredArtifactSearch");
    resource_ = std::make_unique<velox::OperatorMemoryResource>(pool_.get());
    build();
  }
  void TearDown() override {
    resource_.reset();
    pool_.reset();
    root_.reset();
    vector::diskann::DiskAnnIndexCache::instance().shutdown();
    ImmutableArtifactCache::instance().shutdown();
    std::filesystem::remove_all(directory_);
  }
  void build() {
    const std::array<float, 16> values{0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0};
    vector::diskann::VamanaConfig config;
    config.dimension = 4;
    config.maxDegree = 3;
    config.searchListSize = 4;
    config.pqSubvectors = 2;
    config.pqBitsPerCode = 2;
    config.beamWidth = 2;
    graph_ = directory_ / "source.graph";
    vector::diskann::DiskAnnBuilder(config).buildFromVectors(values.data(), 4, 4, graph_.string());
    auto locators = directory_ / "source.locators";
    RowLocatorSidecar::writePhysicalRowPositions(locators.string(), positions_);
    add(graph_, "DISKANN_GRAPH", 0);
    add(graph_.string() + ".vecs", "RAW_VECTORS", 1);
    add(locators, "ROW_LOCATORS", 2);
  }
  void add(const std::filesystem::path& path, std::string role, uint32_t ordinal) {
    proto::ArtifactObject object;
    object.set_object_id("source-object:" + role);
    object.set_source_object_id("source-object");
    object.set_opaque_uri(path.string());
    object.set_scope(proto::ARTIFACT_OBJECT_SCOPE_SOURCE_OBJECT);
    object.set_object_role(std::move(role));
    object.set_ordinal(ordinal);
    object.set_row_count(4);
    object.set_size_bytes(std::filesystem::file_size(path));
    object.mutable_checksum()->set_algorithm(proto::Checksum::ALGORITHM_SHA256);
    object.mutable_checksum()->set_digest(digest(path));
    bytes_ += object.size_bytes();
    objects_.push_back(std::move(object));
  }
  std::unique_ptr<ArtifactVectorSearch> source() {
    return std::make_unique<ArtifactVectorSearch>(objects_, 4, bytes_, *resource_, [] {});
  }
  std::vector<uint64_t> exact(const std::set<uint64_t>& eligible, size_t k) {
    using Number = boost::multiprecision::cpp_bin_float_100;
    std::vector<std::pair<Number, uint64_t>> scored;
    for (size_t node = 0; node < positions_.size(); ++node) {
      if (!eligible.contains(positions_[node])) continue;
      Number delta = Number(node) - Number(1) / 4;
      scored.emplace_back(delta * delta, positions_[node]);
    }
    std::sort(scored.begin(), scored.end());
    std::vector<uint64_t> result;
    for (size_t row = 0; row < std::min(k, scored.size()); ++row) result.push_back(scored[row].second);
    return result;
  }
  const std::array<float, 4> query_{0.25f, 0, 0, 0};
  const std::array<uint64_t, 4> positions_{3, 9, 23, 17};
  std::filesystem::path directory_;
  std::filesystem::path graph_;
  std::vector<proto::ArtifactObject> objects_;
  uint64_t bytes_ = 0;
  std::shared_ptr<facebook::velox::memory::MemoryPool> root_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
  std::unique_ptr<velox::OperatorMemoryResource> resource_;
};

TEST_F(ArtifactVectorSearchTest, TraversesNonmatchingBridgesThenReturnsExactEligibleOrder) {
  auto search = source();
  search->addEligible(23);
  search->addEligible(17);
  VectorSearchUsage usage;
  {
    auto result = search->search(query_, {2, 4, 4}, usage, [] {});
    EXPECT_EQ(std::vector<uint64_t>(result.begin(), result.end()), exact({23, 17}, 2));
  }
  EXPECT_EQ(usage.expanded, 4);
  EXPECT_EQ(usage.rescored, 2);
  EXPECT_EQ(search->eligibleCount(), 2);
  EXPECT_GT(pool_->usedBytes(), 0);
  search.reset();
  EXPECT_EQ(pool_->usedBytes(), 0);
}

TEST_F(ArtifactVectorSearchTest, EmptyEligibilityIsSuccessfulAndUnderfilledExplorationReportsShortfall) {
  auto search = source();
  VectorSearchUsage empty;
  EXPECT_TRUE(search->search(query_, {3, 4, 4}, empty, [] {}).empty());
  EXPECT_EQ(empty.expanded, 0);
  EXPECT_EQ(empty.rescored, 0);
  auto graph = vector::diskann::DiskAnnIndexCache::instance().getOrLoad(graph_.string(), 4);
  ASSERT_NE(graph, nullptr);
  const auto nonmedoid = (graph->medoid() + 1) % 4;
  search->addEligible(positions_[nonmedoid]);
  VectorSearchUsage bounded;
  auto underfilled = search->search(query_, {1, 1, 4}, bounded, [] {});
  EXPECT_TRUE(underfilled.empty());
  EXPECT_EQ(bounded.rescored, 0);
  EXPECT_EQ(bounded.recall_shortfall, 1u);
}

TEST_F(ArtifactVectorSearchTest, FewerEligibleRowsThanKAreACompletePopulationAndQuotaCannotExpand) {
  auto search = source();
  search->addEligible(17);
  VectorSearchUsage usage;
  auto output = search->search(query_, {3, 4, 4}, usage, [] {});
  EXPECT_EQ(std::vector<uint64_t>(output.begin(), output.end()), exact({17}, 3));
  EXPECT_EQ(usage.rescored, 1);
  EXPECT_THROW(search->search(query_, {3, 4, 4}, usage, [] {}), std::runtime_error);
  EXPECT_THROW(search->addEligible(17), std::runtime_error);
  EXPECT_THROW(search->addEligible(999), std::runtime_error);
}

TEST_F(ArtifactVectorSearchTest, ActualColdMaterializationAndWarmHitConsumeOneSharedGrant) {
  auto owner = std::make_shared<resource::TaskObjectReadBudget>(bytes_);
  auto cold = std::make_unique<ArtifactVectorSearch>(objects_, 4, bytes_, *resource_, [] {}, owner);
  EXPECT_EQ(cold->downloadedBytes(), bytes_);
  EXPECT_EQ(owner->requestedBytes(), bytes_);
  auto warm = std::make_unique<ArtifactVectorSearch>(objects_, 4, 0, *resource_, [] {}, owner);
  EXPECT_EQ(warm->downloadedBytes(), 0);
  EXPECT_EQ(owner->requestedBytes(), bytes_);
  EXPECT_EQ(owner->activeReads(), 0);
  EXPECT_THROW(owner->begin(1), std::runtime_error);
}

TEST_F(ArtifactVectorSearchTest, CancellationDuringRealGraphWorkUnwindsOperatorChargedBuffers) {
  auto search = source();
  for (auto position : positions_) search->addEligible(position);
  const auto retained = pool_->usedBytes();
  int checkpoints = 0;
  folly::CancellationSource cancellation;
  const auto token = cancellation.getToken();
  VectorSearchUsage usage;
  EXPECT_THROW(search->search(query_, {3, 4, 4}, usage, [&] {
    if (++checkpoints == 4) cancellation.requestCancellation();
    if (token.isCancellationRequested()) throw std::runtime_error("Owned task cancellation");
  }), std::runtime_error);
  EXPECT_EQ(pool_->usedBytes(), retained);
  search.reset();
  EXPECT_EQ(pool_->usedBytes(), 0);
}

TEST_F(ArtifactVectorSearchTest, CorruptRealRawBytesAndInvalidLocatorBundleFailBeforeSearch) {
  auto corrupt = objects_;
  corrupt[1].mutable_checksum()->set_digest(std::string(32, 'x'));
  EXPECT_THROW(ArtifactVectorSearch(corrupt, 4, bytes_, *resource_, [] {}), std::runtime_error);
  EXPECT_EQ(pool_->usedBytes(), 0);
  auto locators = directory_ / "duplicated.locators";
  RowLocatorSidecar::writePhysicalRowPositions(locators.string(), std::array<uint64_t, 4>{3, 3, 23, 17});
  corrupt = objects_;
  corrupt[2].set_opaque_uri(locators.string());
  corrupt[2].mutable_checksum()->set_digest(digest(locators));
  EXPECT_THROW(ArtifactVectorSearch(corrupt, 4, bytes_, *resource_, [] {}), std::runtime_error);
  EXPECT_EQ(pool_->usedBytes(), 0);
}

}  // namespace
}  // namespace pangrova::artifact::test
