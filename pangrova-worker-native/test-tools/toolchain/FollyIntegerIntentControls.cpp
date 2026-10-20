#include <folly/hash/Hash.h>
#include <folly/synchronization/DistributedMutex.h>

#include <cstdint>
#include <cstdio>
#include <limits>

namespace {
using Wide = unsigned __int128;

std::uint64_t product(std::uint64_t value, std::uint64_t factor) {
  return static_cast<std::uint64_t>(Wide(value) * factor);
}

std::uint64_t first(std::uint64_t value, unsigned bits) {
  return static_cast<std::uint64_t>(Wide(value) * ((std::uint64_t{1} << bits) - 1)
      + std::numeric_limits<std::uint64_t>::max());
}

std::uint64_t reference64(std::uint64_t value) {
  value = first(value, 21);
  value ^= value >> 24;
  value = product(value, 265);
  value ^= value >> 14;
  value = product(value, 21);
  value ^= value >> 28;
  return product(value, 2147483649ULL);
}

std::uint32_t reference32(std::uint64_t value) {
  value = first(value, 18);
  value ^= value >> 31;
  value = product(value, 21);
  value ^= value >> 11;
  value = product(value, 65);
  value ^= value >> 22;
  return static_cast<std::uint32_t>(value);
}

bool check(std::uint64_t value) {
  constexpr std::uint64_t low56 = (std::uint64_t{1} << 56) - 1;
  const auto packed = folly::detail::distributed_mutex::strip(value);
  const auto expected = static_cast<std::uint64_t>(Wide(value) * 256);
  return folly::hash::twang_mix64(value) == reference64(value)
      && folly::hash::twang_32from64(value) == reference32(value)
      && folly::hash::twang_unmix64(folly::hash::twang_mix64(value)) == value
      && packed == expected && (packed & 255) == 0
      && folly::detail::distributed_mutex::recover(packed) == (value & low56);
}
} // namespace

int main() {
  std::uint64_t checks = 0;
  auto verify = [&](std::uint64_t value) {
    ++checks;
    return check(value);
  };
  if (!verify(0) || !verify(UINT64_MAX)) return 1;
  for (unsigned bit = 0; bit < 64; ++bit) {
    const auto value = std::uint64_t{1} << bit;
    if (!verify(value) || !verify(value - 1) || !verify(~value)) return 1;
  }
  constexpr std::uint64_t low56 = (std::uint64_t{1} << 56) - 1;
  for (std::uint64_t high = 0; high < 256; ++high) {
    for (auto low : {std::uint64_t{0}, low56, low56 - 1}) {
      if (!verify(high * (std::uint64_t{1} << 56) | low)) return 1;
    }
  }
  std::uint64_t value = 0x123456789abcdef0ULL;
  for (unsigned index = 0; index < 100000; ++index) {
    value = static_cast<std::uint64_t>(Wide(value) * 6364136223846793005ULL + 1442695040888963407ULL);
    if (!verify(value)) return 1;
  }
  const auto mask = static_cast<std::uint32_t>(-1);
  if (mask != std::numeric_limits<std::uint32_t>::max()) return 1;
  if (folly::detail::distributed_mutex::recover(
          folly::detail::distributed_mutex::strip(UINT64_MAX)) != low56) return 1;
  std::printf("checks=%llu masks=%u timestamp_sentinel=%llu\n",
      static_cast<unsigned long long>(checks), mask, static_cast<unsigned long long>(low56));
}
