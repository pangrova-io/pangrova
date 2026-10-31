#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <thread>

#include "folly/container/detail/F14Table.h"

namespace pangrova::velox::test {
namespace {

TEST(FollyPendingInsertIntentTest, ActualCounterMatchesBoundedArithmeticAtZeroAndPositiveDepths) {
  bool matches = true;
  std::uint64_t checks = 0;
  std::thread([&] {
    std::size_t expected = 0;
    auto verify = [&](std::ptrdiff_t delta) {
      // The independent reference subtracts explicitly, without signed-to-unsigned conversion.
      if (delta > 0) expected += static_cast<std::size_t>(delta);
      else if (delta == -1 && expected > 0) --expected;
      ++checks;
      return folly::f14::detail::tlsPendingSafeInserts(delta) == (expected != 0);
    };
    if (!verify(0) || !verify(-1)) { matches = false; return; }
    for (std::size_t cycle = 0; cycle < 256; ++cycle) {
      const auto depth = 1 + (cycle * 17) % 4096;
      if (!verify(static_cast<std::ptrdiff_t>(depth)) || !verify(0)) { matches = false; return; }
      for (std::size_t i = 0; i < depth; ++i) {
        if (!verify(-1)) { matches = false; return; }
      }
      if (!verify(-1) || !verify(0)) { matches = false; return; }
    }
  }).join();
  EXPECT_TRUE(matches);
  EXPECT_GT(checks, 490000);
  RecordProperty("counter_operations", std::to_string(checks));
}
}  // namespace
}  // namespace pangrova::velox::test
