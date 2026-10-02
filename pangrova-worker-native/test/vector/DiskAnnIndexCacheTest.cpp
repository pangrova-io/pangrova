#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <barrier>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "vector/diskann/DiskAnnBuilder.h"
#include "vector/diskann/DiskAnnIndexCache.h"
#include "vector/diskann/VamanaGraph.h"

namespace pangrova::vector::diskann::test {
namespace {

class DiskAnnIndexCacheTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (DiskAnnIndexCache::instance().enabled()) {
      DiskAnnIndexCache::instance().shutdown();
    }
    directory_ = std::filesystem::temp_directory_path() /
        ("pangrova-diskann-cache-" + std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(directory_);
  }

  void TearDown() override {
    if (DiskAnnIndexCache::instance().enabled()) {
      DiskAnnIndexCache::instance().shutdown();
    }
    std::error_code ignored;
    std::filesystem::remove_all(directory_, ignored);
  }

  std::filesystem::path buildGraph(const std::string& name, float offset = 0) {
    const std::array<float, 16> vectors{
        offset + 0, 0, 0, 0,
        offset + 1, 0, 0, 0,
        offset + 2, 0, 0, 0,
        offset + 3, 0, 0, 0};
    VamanaConfig config;
    config.maxDegree = 2;
    config.searchListSize = 4;
    config.pqSubvectors = 2;
    config.pqBitsPerCode = 2;
    config.beamWidth = 2;
    auto path = directory_ / name;
    DiskAnnBuilder(config).buildFromVectors(
        vectors.data(), 4, 4, path.string());
    return path;
  }

  static uint64_t declaredBytes(const std::filesystem::path& path) {
    auto bytes = VamanaIndex::residentBytesFromDisk(path.string(), 4);
    EXPECT_TRUE(bytes.has_value());
    return bytes.value_or(0);
  }

  std::filesystem::path directory_;
};

TEST_F(DiskAnnIndexCacheTest, RejectsGraphBeforeLoadWhenResidentQuotaIsTooSmall) {
  auto graph = buildGraph("quota.graph");
  const uint64_t required = declaredBytes(graph);
  ASSERT_GT(required, 1);
  DiskAnnIndexCache::instance().configure(required - 1);

  EXPECT_EQ(
      DiskAnnIndexCache::instance().getOrLoad(graph.string(), 4), nullptr);
  EXPECT_EQ(DiskAnnIndexCache::instance().size(), 0);
  EXPECT_EQ(DiskAnnIndexCache::instance().usedBytes(), 0);
  EXPECT_EQ(DiskAnnIndexCache::instance().reservedBytes(), 0);
}

TEST_F(DiskAnnIndexCacheTest, NeverEvictsPinnedGraphToAdmitAnotherGraph) {
  auto first_graph = buildGraph("first.graph");
  auto second_graph = buildGraph("second.graph", 10);
  const uint64_t capacity = std::max(
      declaredBytes(first_graph), declaredBytes(second_graph));
  DiskAnnIndexCache::instance().configure(capacity);

  auto pinned = DiskAnnIndexCache::instance().getOrLoad(first_graph.string(), 4);
  ASSERT_NE(pinned, nullptr);
  EXPECT_EQ(
      DiskAnnIndexCache::instance().getOrLoad(second_graph.string(), 4), nullptr);
  EXPECT_EQ(DiskAnnIndexCache::instance().size(), 1);

  pinned.reset();
  auto replacement =
      DiskAnnIndexCache::instance().getOrLoad(second_graph.string(), 4);
  ASSERT_NE(replacement, nullptr);
  EXPECT_EQ(DiskAnnIndexCache::instance().size(), 1);
  EXPECT_LE(
      DiskAnnIndexCache::instance().usedBytes(),
      DiskAnnIndexCache::instance().capacityBytes());
}

TEST_F(DiskAnnIndexCacheTest, CoalescesConcurrentLoadsForTheSameGraph) {
  auto graph = buildGraph("coalesced.graph");
  DiskAnnIndexCache::instance().configure(declaredBytes(graph));
  constexpr size_t kThreads = 12;
  std::barrier start(static_cast<std::ptrdiff_t>(kThreads));
  std::array<std::shared_ptr<VamanaIndex>, kThreads> results;
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (size_t index = 0; index < kThreads; ++index) {
    threads.emplace_back([&, index] {
      start.arrive_and_wait();
      results[index] =
          DiskAnnIndexCache::instance().getOrLoad(graph.string(), 4);
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }

  ASSERT_NE(results.front(), nullptr);
  for (const auto& result : results) {
    EXPECT_EQ(result.get(), results.front().get());
  }
  EXPECT_EQ(DiskAnnIndexCache::instance().size(), 1);
  EXPECT_EQ(DiskAnnIndexCache::instance().reservedBytes(), 0);
}

TEST_F(DiskAnnIndexCacheTest, UnderstatedHeaderCannotEscapeReservation) {
  auto graph = buildGraph("understated.graph");
  {
    std::fstream output(graph, std::ios::binary | std::ios::in | std::ios::out);
    ASSERT_TRUE(output.good());
    output.seekp(7 * sizeof(uint64_t));
    const std::array<char, 8> one{1, 0, 0, 0, 0, 0, 0, 0};
    output.write(one.data(), static_cast<std::streamsize>(one.size()));
    ASSERT_TRUE(output.good());
  }
  DiskAnnIndexCache::instance().configure(1024 * 1024);

  EXPECT_EQ(
      DiskAnnIndexCache::instance().getOrLoad(graph.string(), 4), nullptr);
  EXPECT_EQ(DiskAnnIndexCache::instance().usedBytes(), 0);
  EXPECT_EQ(DiskAnnIndexCache::instance().reservedBytes(), 0);
}

}  // namespace
}  // namespace pangrova::vector::diskann::test
