#include <gtest/gtest.h>

#include <bit>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <limits>
#include <string>

#include "velox/connector/PaimonFloatingText.h"

namespace pangrova::velox::test {
namespace {

TEST(PaimonFloatingTextTest, NativeTextMatchesActualJdk17BoundaryAndRandomOracle) {
  auto path = std::filesystem::path(__FILE__).parent_path().parent_path() /
      "resources" / "paimon-floating" / "jdk17.tsv";
  std::ifstream input(path);
  ASSERT_TRUE(input.is_open()) << path;
  std::string type;
  std::string hex;
  std::string expected;
  size_t count = 0;
  while (input >> type >> hex >> expected) {
    const auto bits = std::stoull(hex, nullptr, 16);
    ASSERT_TRUE(type == "F" || type == "D");
    const auto actual = type == "F" ? paimonFloatingText(std::bit_cast<float>(static_cast<uint32_t>(bits)))
                                    : paimonFloatingText(std::bit_cast<double>(bits));
    ASSERT_EQ(actual, expected) << "type=" << type << " raw_bits=" << hex << " row=" << count;
    ++count;
  }
  EXPECT_TRUE(input.eof());
  EXPECT_GT(count, 40'000);
}

TEST(PaimonFloatingTextTest, FloatAndDoubleRetainZeroSignsAndSpecialValues) {
  EXPECT_EQ(paimonFloatingText(0.0f), "0.0");
  EXPECT_EQ(paimonFloatingText(-0.0f), "-0.0");
  EXPECT_EQ(paimonFloatingText(0.0), "0.0");
  EXPECT_EQ(paimonFloatingText(-0.0), "-0.0");
  EXPECT_EQ(paimonFloatingText(std::numeric_limits<float>::infinity()), "Infinity");
  EXPECT_EQ(paimonFloatingText(-std::numeric_limits<float>::infinity()), "-Infinity");
  EXPECT_EQ(paimonFloatingText(std::numeric_limits<float>::quiet_NaN()), "NaN");
  EXPECT_EQ(paimonFloatingText(std::numeric_limits<double>::infinity()), "Infinity");
  EXPECT_EQ(paimonFloatingText(-std::numeric_limits<double>::infinity()), "-Infinity");
  EXPECT_EQ(paimonFloatingText(std::numeric_limits<double>::quiet_NaN()), "NaN");
}

}  // namespace
}  // namespace pangrova::velox::test
