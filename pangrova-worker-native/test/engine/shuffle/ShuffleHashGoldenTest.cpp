// ShuffleHashGoldenTest: Validates that Velox HashPartitionFunction produces
// consistent and well-distributed partition assignments.
//
// The shuffle hash is now delegated to Velox's HashPartitionFunction which
// uses VectorHasher (SIMD-optimized). This test verifies:
//   1. Partition assignments are deterministic (same input → same partition)
//   2. Partition numbers are within bounds [0, numPartitions)
//   3. Distribution is reasonably uniform (no partition starvation)
//
// Cross-language consistency is validated by E2E tests
// (ShuffleHashCrossLanguageE2ETest) which verify that GROUP BY / JOIN
// queries produce correct results across the native shuffle boundary.

#include <gtest/gtest.h>
#include <cstdint>
#include <climits>
#include <memory>
#include <vector>

#include "velox/exec/HashPartitionFunction.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"
#include "velox/common/memory/Memory.h"

namespace pangrova::shuffle {
namespace {

class ShuffleHashGoldenTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = facebook::velox::memory::memoryManager()->addLeafPool(
        "ShuffleHashGoldenTest");
  }

  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
};

TEST_F(ShuffleHashGoldenTest, SingleColumnPartitionDeterminism) {
  // Same input must always produce the same partition assignment.
  facebook::velox::test::VectorMaker maker(pool_.get());
  auto vector = maker.rowVector({
      maker.flatVector<int64_t>({0, 1, -1, INT64_MAX, INT64_MIN, 42}),
  });

  auto inputType = std::dynamic_pointer_cast<const facebook::velox::RowType>(
      vector->type());
  std::vector<facebook::velox::column_index_t> keyChannels = {0};

  facebook::velox::exec::HashPartitionFunction pf1(
      false, 16, inputType, keyChannels);
  facebook::velox::exec::HashPartitionFunction pf2(
      false, 16, inputType, keyChannels);

  std::vector<uint32_t> partitions1(vector->size());
  std::vector<uint32_t> partitions2(vector->size());
  pf1.partition(*vector, partitions1);
  pf2.partition(*vector, partitions2);

  EXPECT_EQ(partitions1, partitions2);
}

TEST_F(ShuffleHashGoldenTest, PartitionBounds) {
  facebook::velox::test::VectorMaker maker(pool_.get());
  auto vector = maker.rowVector({
      maker.flatVector<int64_t>(
          {0, 1, -1, INT64_MAX, INT64_MIN, 42, 123456789, -999999999999LL}),
  });

  auto inputType = std::dynamic_pointer_cast<const facebook::velox::RowType>(
      vector->type());
  std::vector<facebook::velox::column_index_t> keyChannels = {0};

  constexpr int kNumPartitions = 8;
  facebook::velox::exec::HashPartitionFunction pf(
      false, kNumPartitions, inputType, keyChannels);

  std::vector<uint32_t> partitions(vector->size());
  pf.partition(*vector, partitions);

  for (size_t i = 0; i < partitions.size(); ++i) {
    EXPECT_LT(partitions[i], static_cast<uint32_t>(kNumPartitions))
        << "partition out of range for row " << i;
  }
}

TEST_F(ShuffleHashGoldenTest, HashDistribution) {
  // Verify no partition starvation over 1000 sequential values.
  constexpr int kNumPartitions = 16;
  constexpr int kNumValues = 1000;

  std::vector<int64_t> values(kNumValues);
  for (int i = 0; i < kNumValues; ++i) {
    values[i] = static_cast<int64_t>(i);
  }

  facebook::velox::test::VectorMaker maker(pool_.get());
  auto vector = maker.rowVector({maker.flatVector<int64_t>(values)});

  auto inputType = std::dynamic_pointer_cast<const facebook::velox::RowType>(
      vector->type());
  std::vector<facebook::velox::column_index_t> keyChannels = {0};

  facebook::velox::exec::HashPartitionFunction pf(
      false, kNumPartitions, inputType, keyChannels);

  std::vector<uint32_t> partitions(kNumValues);
  pf.partition(*vector, partitions);

  int counts[kNumPartitions] = {};
  for (auto p : partitions) {
    counts[p]++;
  }
  for (int i = 0; i < kNumPartitions; ++i) {
    EXPECT_GT(counts[i], 10)
        << "partition " << i << " got only " << counts[i] << " rows";
  }
}

TEST_F(ShuffleHashGoldenTest, MultiColumnPartition) {
  facebook::velox::test::VectorMaker maker(pool_.get());
  auto vector = maker.rowVector({
      maker.flatVector<int64_t>({100, 200, 300}),
      maker.flatVector<int64_t>({400, 500, 600}),
  });

  auto inputType = std::dynamic_pointer_cast<const facebook::velox::RowType>(
      vector->type());
  std::vector<facebook::velox::column_index_t> keyChannels = {0, 1};

  constexpr int kNumPartitions = 8;
  facebook::velox::exec::HashPartitionFunction pf(
      false, kNumPartitions, inputType, keyChannels);

  std::vector<uint32_t> partitions(vector->size());
  pf.partition(*vector, partitions);

  for (size_t i = 0; i < partitions.size(); ++i) {
    EXPECT_LT(partitions[i], static_cast<uint32_t>(kNumPartitions))
        << "partition out of range for row " << i;
  }

  // Determinism: same input → same result.
  facebook::velox::exec::HashPartitionFunction pf2(
      false, kNumPartitions, inputType, keyChannels);
  std::vector<uint32_t> partitions2(vector->size());
  pf2.partition(*vector, partitions2);
  EXPECT_EQ(partitions, partitions2);
}

TEST_F(ShuffleHashGoldenTest, StringColumnPartition) {
  facebook::velox::test::VectorMaker maker(pool_.get());
  auto vector = maker.rowVector({
      maker.flatVector<facebook::velox::StringView>(
          {"hello", "world", "", "pangrova", "velox"}),
  });

  auto inputType = std::dynamic_pointer_cast<const facebook::velox::RowType>(
      vector->type());
  std::vector<facebook::velox::column_index_t> keyChannels = {0};

  constexpr int kNumPartitions = 4;
  facebook::velox::exec::HashPartitionFunction pf(
      false, kNumPartitions, inputType, keyChannels);

  std::vector<uint32_t> partitions(vector->size());
  pf.partition(*vector, partitions);

  for (size_t i = 0; i < partitions.size(); ++i) {
    EXPECT_LT(partitions[i], static_cast<uint32_t>(kNumPartitions))
        << "partition out of range for row " << i;
  }
}

}  // namespace
}  // namespace pangrova::shuffle
