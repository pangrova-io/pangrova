#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "vector/hnsw/HnswIndex.h"
#include "vector/ivfpq/IvfPqIndex.h"
#include "vector/ann/BinaryIndexIo.h"

namespace pangrova::vector::test {
namespace {

class TestDirectory final {
 public:
  TestDirectory() {
    path_ = std::filesystem::temp_directory_path() /
        ("pangrova-native-ann-" + std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(path_);
  }

  ~TestDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }

  const std::filesystem::path& path() const { return path_; }

 private:
  std::filesystem::path path_;
};

std::vector<float> lineVectors(int64_t count, int64_t dimension) {
  std::vector<float> values(static_cast<size_t>(count * dimension));
  for (int64_t row = 0; row < count; ++row) {
    values[static_cast<size_t>(row * dimension)] = static_cast<float>(row);
    values[static_cast<size_t>(row * dimension + 1)] =
        static_cast<float>(row % 5) / 10.0F;
  }
  return values;
}

TEST(NativeAnnIndexTest, HnswBuildsPersistsAndEnforcesResidentQuota) {
  TestDirectory directory;
  constexpr int64_t kCount = 64;
  constexpr int64_t kDimension = 4;
  auto vectors = lineVectors(kCount, kDimension);
  hnsw::HnswIndex index({kDimension, 8, 32});
  index.build(vectors.data(), kCount, kDimension);
  const auto path = directory.path() / "segment.hnsw";
  ASSERT_TRUE(index.save(path.string()));

  const auto resident = hnsw::HnswIndex::residentBytesFromDisk(
      path.string(), kDimension);
  ASSERT_TRUE(resident.has_value());
  hnsw::HnswIndex rejected({kDimension, 2, 2});
  EXPECT_FALSE(rejected.load(path.string(), *resident - 1));
  hnsw::HnswIndex loaded({kDimension, 2, 2});
  ASSERT_TRUE(loaded.load(path.string(), *resident));
  EXPECT_EQ(loaded.size(), kCount);
  EXPECT_EQ(loaded.dimension(), kDimension);

  const float query[] = {11.1F, 0.1F, 0.0F, 0.0F};
  auto result = loaded.search(query, 5, 32);
  ASSERT_EQ(result.size(), 5);
  EXPECT_EQ(result.front().node_id, 11);
}

TEST(NativeAnnIndexTest, HnswRejectsTruncatedArtifactBeforeAllocation) {
  TestDirectory directory;
  const auto path = directory.path() / "truncated.hnsw";
  std::ofstream output(path, std::ios::binary);
  output.write("HNSW", 4);
  output.close();

  EXPECT_FALSE(hnsw::HnswIndex::residentBytesFromDisk(path.string(), 4));
  hnsw::HnswIndex loaded({4, 2, 2});
  EXPECT_FALSE(loaded.load(path.string(), 1024));
}

// ef_construction bounds searchLayer's two priority queues and visited set, so a
// larger value needs more scratch. The estimate used to omit it entirely, which
// let a build pass admission and then fail mid-way against the real pool.
TEST(NativeAnnIndexTest, HnswEstimateGrowsWithEfConstruction) {
  constexpr int64_t kCount = 10'000;
  constexpr int64_t kDimension = 8;
  constexpr int64_t kConnections = 16;
  const uint64_t small = hnsw::HnswIndex::estimatePeakBuildBytes(
      kCount, kDimension, kConnections, kConnections);
  const uint64_t large = hnsw::HnswIndex::estimatePeakBuildBytes(
      kCount, kDimension, kConnections, 4096);
  EXPECT_GT(large, small);
  EXPECT_LT(small, large);
}

TEST(NativeAnnIndexTest, HnswEstimateRejectsEfBelowConnections) {
  EXPECT_THROW(
      hnsw::HnswIndex::estimatePeakBuildBytes(100, 8, 16, 8),
      std::invalid_argument);
}

TEST(NativeAnnIndexTest, HnswBuildRejectsInsufficientMemoryGrant) {
  constexpr int64_t kCount = 64;
  constexpr int64_t kDimension = 4;
  constexpr int64_t kConnections = 8;
  auto vectors = lineVectors(kCount, kDimension);
  const uint64_t estimated = hnsw::HnswIndex::estimatePeakBuildBytes(
      kCount, kDimension, kConnections, 32);

  hnsw::HnswIndex rejected({kDimension, kConnections, 32});
  try {
    rejected.build(vectors.data(), kCount, kDimension, estimated - 1);
    FAIL() << "HNSW must reject an undersized grant before graph allocation";
  } catch (const std::runtime_error& error) {
    EXPECT_NE(std::string(error.what()).find("RESOURCE_EXHAUSTED"),
              std::string::npos);
  }

  hnsw::HnswIndex admitted({kDimension, kConnections, 32});
  EXPECT_NO_THROW(admitted.build(
      vectors.data(), kCount, kDimension, estimated));
  EXPECT_EQ(admitted.size(), kCount);

  hnsw::HnswIndex runtime_checked({kDimension, kConnections, 32});
  EXPECT_THROW(runtime_checked.build(
      vectors.data(), kCount, kDimension, estimated, estimated),
      std::runtime_error);
}

TEST(NativeAnnIndexTest, IvfPqBuildsPersistsAndSearchesEveryProbeList) {
  TestDirectory directory;
  constexpr int64_t kCount = 128;
  constexpr int64_t kDimension = 4;
  constexpr int64_t kClusters = 8;
  auto vectors = lineVectors(kCount, kDimension);
  ivfpq::IvfPqIndex index({kDimension, kClusters, 2, 4, 12, 128});
  index.build(vectors.data(), kCount, kDimension);
  const auto path = directory.path() / "segment.ivfpq";
  ASSERT_TRUE(index.save(path.string()));

  const auto resident = ivfpq::IvfPqIndex::residentBytesFromDisk(
      path.string(), kDimension);
  ASSERT_TRUE(resident.has_value());
  ivfpq::IvfPqIndex rejected({kDimension, 1, 1, 1, 1, 1});
  EXPECT_FALSE(rejected.load(path.string(), *resident - 1));
  ivfpq::IvfPqIndex loaded({kDimension, 1, 1, 1, 1, 1});
  ASSERT_TRUE(loaded.load(path.string(), *resident));
  EXPECT_EQ(loaded.size(), kCount);
  EXPECT_EQ(loaded.coarseClusters(), kClusters);

  const float query[] = {37.05F, 0.2F, 0.0F, 0.0F};
  auto result = loaded.search(query, 16, kClusters);
  ASSERT_EQ(result.size(), 16);
  EXPECT_NE(
      std::find_if(result.begin(), result.end(), [](const auto& candidate) {
        return candidate.node_id == 37;
      }),
      result.end());
}

TEST(NativeAnnIndexTest, IvfPqPreservesRequestedClusterContractForSmallPages) {
  TestDirectory directory;
  constexpr int64_t kDimension = 4;
  auto vectors = lineVectors(4, kDimension);
  ivfpq::IvfPqIndex index({kDimension, 16, 2, 2, 4, 4});
  index.build(vectors.data(), 4, kDimension);
  const auto path = directory.path() / "small.ivfpq";
  ASSERT_TRUE(index.save(path.string()));

  ivfpq::IvfPqIndex loaded({kDimension, 1, 1, 1, 1, 1});
  const auto resident = ivfpq::IvfPqIndex::residentBytesFromDisk(
      path.string(), kDimension);
  ASSERT_TRUE(resident.has_value());
  ASSERT_TRUE(loaded.load(path.string(), *resident));
  EXPECT_EQ(loaded.coarseClusters(), 16);
  const float query[] = {0, 0, 0, 0};
  EXPECT_FALSE(loaded.search(query, 1, 8).empty());
}

TEST(NativeAnnIndexTest, AllMetricsSurvivePersistenceAndProduceDistinctRankings) {
  // The closest vector is different for each metric; L2-only implementations
  // cannot accidentally satisfy this regression.
  const std::vector<float> vectors{1, 0.25F, 2, 0, 100, 1, -1, 0};
  const float query[]{1, 0};
  for (const auto metric : {DistanceMetric::L2, DistanceMetric::COSINE,
                            DistanceMetric::INNER_PRODUCT}) {
    TestDirectory directory;
    const int64_t expected = metric == DistanceMetric::L2 ? 0
        : metric == DistanceMetric::COSINE ? 1 : 2;
    const double expected_distance = metricDistance(
        metric, query, vectors.data() + expected * 2, 2);
    hnsw::HnswIndex graph({2, 3, 8, metric});
    graph.build(vectors.data(), 4, 2);
    const auto graph_path = directory.path() / "metric.hnsw";
    ASSERT_TRUE(graph.save(graph_path.string()));
    hnsw::HnswIndex loaded_graph({2, 2, 2});
    ASSERT_TRUE(loaded_graph.load(graph_path.string(), 1024 * 1024));
    EXPECT_EQ(loaded_graph.metric(), metric);
    auto graph_candidates = loaded_graph.search(query, 4, 8);
    ASSERT_EQ(graph_candidates.size(), 4);
    EXPECT_EQ(graph_candidates.front().node_id, expected);
    EXPECT_NEAR(graph_candidates.front().distance, expected_distance, 1e-6);

    ivfpq::IvfPqIndex pq({2, 4, 2, 2, 4, 4, metric});
    pq.build(vectors.data(), 4, 2);
    const auto pq_path = directory.path() / "metric.ivfpq";
    ASSERT_TRUE(pq.save(pq_path.string()));
    ivfpq::IvfPqIndex loaded_pq({2, 1, 1, 1, 1, 1});
    ASSERT_TRUE(loaded_pq.load(pq_path.string(), 1024 * 1024));
    EXPECT_EQ(loaded_pq.metric(), metric);
    auto pq_candidates = loaded_pq.search(query, 4, 4);
    ASSERT_EQ(pq_candidates.size(), 4);
    EXPECT_EQ(pq_candidates.front().node_id, expected);
    EXPECT_NEAR(pq_candidates.front().distance, expected_distance, 1e-6);
  }
}

TEST(NativeAnnIndexTest, RejectsLegacyFormatAndUnknownPersistedMetric) {
  const std::vector<float> vectors{1, 0, 0, 1};
  for (bool hnsw : {true, false}) {
    for (bool legacy : {true, false}) {
      TestDirectory directory;
      const auto path = directory.path() / "invalid.index";
      if (hnsw) {
        hnsw::HnswIndex index({2, 2, 2});
        index.build(vectors.data(), 2, 2);
        ASSERT_TRUE(index.save(path.string()));
      } else {
        ivfpq::IvfPqIndex index({2, 2, 1, 1, 2, 2});
        index.build(vectors.data(), 2, 2);
        ASSERT_TRUE(index.save(path.string()));
      }
      std::fstream bytes(path, std::ios::binary | std::ios::in | std::ios::out);
      bytes.seekp(legacy ? 0 : (hnsw ? 32 : 56));
      ann::writeUint64(bytes, legacy
          ? (hnsw ? 0x484E535700000001ULL : 0x4956465051000001ULL)
          : 3);
      bytes.close();
      if (hnsw) {
        EXPECT_FALSE(hnsw::HnswIndex::residentBytesFromDisk(path.string(), 2));
        hnsw::HnswIndex loaded({2, 2, 2});
        EXPECT_FALSE(loaded.load(path.string(), 1024 * 1024));
      } else {
        EXPECT_FALSE(ivfpq::IvfPqIndex::residentBytesFromDisk(path.string(), 2));
        ivfpq::IvfPqIndex loaded({2, 1, 1, 1, 1, 1});
        EXPECT_FALSE(loaded.load(path.string(), 1024 * 1024));
      }
    }
  }
}

TEST(NativeAnnIndexTest, InnerProductCandidatesRetainDoubleRange) {
  TestDirectory directory;
  const std::vector<float> vectors{1e30F, 0, 2e30F, 0, 3e30F, 0, -1e30F, 0};
  const float query[]{1e30F, 0};
  hnsw::HnswIndex graph({2, 3, 8, DistanceMetric::INNER_PRODUCT});
  graph.build(vectors.data(), 4, 2);
  auto graph_candidates = graph.search(query, 4, 8);
  ASSERT_EQ(graph_candidates.size(), 4);
  EXPECT_EQ(graph_candidates.front().node_id, 2);
  EXPECT_TRUE(std::isfinite(graph_candidates.front().distance));
  EXPECT_LT(graph_candidates.front().distance, -1e60);
  ivfpq::IvfPqIndex pq({2, 4, 2, 2, 4, 4, DistanceMetric::INNER_PRODUCT});
  pq.build(vectors.data(), 4, 2);
  auto pq_candidates = pq.search(query, 4, 4);
  ASSERT_EQ(pq_candidates.size(), 4);
  EXPECT_EQ(pq_candidates.front().node_id, 2);
  EXPECT_TRUE(std::isfinite(pq_candidates.front().distance));
  EXPECT_LT(pq_candidates.front().distance, -1e60);
}

}  // namespace
}  // namespace pangrova::vector::test
