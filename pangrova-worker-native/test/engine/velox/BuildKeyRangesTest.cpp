#include "engine/velox/runner/BuildKeyRanges.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>

namespace pangrova::velox {
namespace {

TEST(BuildKeyRangesTest, MergesBatchesInAnyOrderAndKeepsGaps) {
  BuildKeyRanges first;
  BuildKeyRanges second;
  for (int64_t value : {36, 2, 2, 35}) first.add(value);
  for (int64_t value : {37, 4, 3}) second.add(value);
  first.merge(second);
  const auto ranges = first.ranges();
  ASSERT_EQ(ranges.size(), 2);
  EXPECT_EQ(ranges[0].minimum, 2);
  EXPECT_EQ(ranges[0].maximum, 4);
  EXPECT_EQ(ranges[1].minimum, 35);
  EXPECT_EQ(ranges[1].maximum, 37);
}

TEST(BuildKeyRangesTest, BoundsStorageAndRetainsExtremeKeysAfterCollapsing) {
  BuildKeyRanges summary;
  for (std::size_t index = 0; index < BuildKeyRanges::kMaximumRanges; ++index) {
    summary.add(static_cast<int64_t>(2 * index));
  }
  EXPECT_EQ(summary.ranges().size(), BuildKeyRanges::kMaximumRanges);
  summary.add(std::numeric_limits<int64_t>::max());
  summary.add(std::numeric_limits<int64_t>::min());
  ASSERT_EQ(summary.ranges().size(), 1);
  EXPECT_EQ(summary.ranges()[0].minimum, std::numeric_limits<int64_t>::min());
  EXPECT_EQ(summary.ranges()[0].maximum, std::numeric_limits<int64_t>::max());
}

TEST(BuildKeyRangesTest, CoalescesAdjacencyAtBothSignedLimits) {
  BuildKeyRanges summary;
  EXPECT_TRUE(summary.ranges().empty());
  const auto minimum = std::numeric_limits<int64_t>::min();
  const auto maximum = std::numeric_limits<int64_t>::max();
  for (auto value : {maximum, minimum, minimum + 1, maximum - 1}) summary.add(value);
  ASSERT_EQ(summary.ranges().size(), 2);
  EXPECT_EQ(summary.ranges()[0].minimum, minimum);
  EXPECT_EQ(summary.ranges()[0].maximum, minimum + 1);
  EXPECT_EQ(summary.ranges()[1].minimum, maximum - 1);
  EXPECT_EQ(summary.ranges()[1].maximum, maximum);
}

}  // namespace
}  // namespace pangrova::velox
