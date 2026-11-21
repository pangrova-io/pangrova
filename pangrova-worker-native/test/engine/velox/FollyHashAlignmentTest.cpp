#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>

#include <folly/hash/SpookyHashV2.h>

namespace pangrova::velox {
namespace {

TEST(FollyHashAlignmentTest, ShortHashIsIndependentOfInputAlignment) {
  alignas(8) std::array<uint8_t, 32> aligned{};
  alignas(8) std::array<uint8_t, 33> unaligned{};
  for (std::size_t index = 0; index < aligned.size(); ++index) {
    aligned[index] = static_cast<uint8_t>(index * 17 + 3);
  }
  std::copy(aligned.begin(), aligned.end(), unaligned.begin() + 1);

  using folly::hash::SpookyHashV2;
  for (std::size_t length = 4; length < aligned.size(); ++length) {
    EXPECT_EQ(SpookyHashV2::Hash64(aligned.data(), length, 42),
              SpookyHashV2::Hash64(unaligned.data() + 1, length, 42))
        << "input length " << length;
  }
}

}  // namespace
}  // namespace pangrova::velox
