#include <gtest/gtest.h>

#include <bit>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>

#include "velox/connector/PaimonBinaryRow.h"

namespace pangrova::velox::test {
namespace {

std::string row(int arity) {
  std::string value(4 + ((arity + 71) / 64) * 8 + arity * 8, '\0');
  for (int index = 0; index < 4; ++index) value[index] = static_cast<char>(arity >> (24 - index * 8));
  return value;
}
void little(std::string& bytes, size_t offset, uint64_t value, size_t width = 8) {
  for (size_t index = 0; index < width; ++index) bytes.at(offset + index) = static_cast<char>(value >> (index * 8));
}
std::optional<std::string> scalar(const std::string& bytes, const char* declaration,
    const char* target = nullptr, bool partition = false) {
  auto sourceType = PaimonScalarType::parse(declaration);
  return PaimonBinaryRowView(bytes).text(0, sourceType,
      target ? PaimonScalarType::parse(target) : sourceType, partition);
}

TEST(PaimonBinaryRowTest, SignedPrimitivesAndNullBitsPreserveSdkValues) {
  auto bytes = row(2);
  little(bytes, 12, std::bit_cast<uint64_t>(int64_t{-129}));
  EXPECT_EQ(scalar(bytes, "SMALLINT"), "-129");
  little(bytes, 12, std::bit_cast<uint64_t>(std::numeric_limits<int64_t>::min()));
  EXPECT_EQ(scalar(bytes, "BIGINT"), "-9223372036854775808");
  bytes[5] = 1;
  EXPECT_EQ(scalar(bytes, "BIGINT"), std::nullopt);
}

TEST(PaimonBinaryRowTest, InlineUtf8AndVariableStringsRetainExactBytes) {
  auto bytes = row(1);
  const std::string inlineText = "caf\xc3\xa9";
  bytes.replace(12, inlineText.size(), inlineText);
  bytes[19] = static_cast<char>(0x80 | inlineText.size());
  EXPECT_EQ(scalar(bytes, "STRING"), inlineText);
  const std::string longText = "region=long-label";
  bytes.resize(20);
  little(bytes, 12, (uint64_t{16} << 32) | longText.size());
  bytes += longText;
  EXPECT_EQ(scalar(bytes, "VARCHAR(100)"), longText);
}

TEST(PaimonBinaryRowTest, RejectsTruncationInvalidArityAndOverlappingOffsets) {
  EXPECT_THROW(PaimonBinaryRowView("abc"), std::runtime_error);
  std::string excessive("\x00\x10\x00\x00", 4);
  EXPECT_THROW(PaimonBinaryRowView{excessive}, std::runtime_error);
  auto bytes = row(1);
  bytes.resize(19);
  EXPECT_THROW(PaimonBinaryRowView{bytes}, std::runtime_error);
  bytes = row(1);
  little(bytes, 12, (uint64_t{8} << 32) | 8);
  EXPECT_THROW(scalar(bytes, "STRING"), std::runtime_error);
  little(bytes, 12, (uint64_t{16} << 32) | 8);
  EXPECT_THROW(scalar(bytes, "STRING"), std::runtime_error);
}

TEST(PaimonBinaryRowTest, RejectsInvalidInlineLengthsAndFieldIndices) {
  auto bytes = row(1);
  bytes[19] = static_cast<char>(0x88);
  EXPECT_THROW(scalar(bytes, "STRING"), std::runtime_error);
  auto type = PaimonScalarType::parse("BIGINT");
  EXPECT_THROW(PaimonBinaryRowView(bytes).text(1, type, type), std::runtime_error);
}

TEST(PaimonBinaryRowTest, CompactDecimalAndIntegerEvolutionAreExactAndChecked) {
  auto bytes = row(1);
  little(bytes, 12, std::bit_cast<uint64_t>(int64_t{-12345}));
  EXPECT_EQ(scalar(bytes, "DECIMAL(18, 2)"), "-123.45");
  EXPECT_EQ(scalar(bytes, "DECIMAL(18, 2)", "DECIMAL(20, 4)"), "-123.4500");
  EXPECT_THROW(scalar(bytes, "DECIMAL(18, 2)", "DECIMAL(18, 1)"), std::runtime_error);
  little(bytes, 12, 42);
  EXPECT_EQ(scalar(bytes, "BIGINT", "DECIMAL(20, 2)"), "42.00");
  little(bytes, 12, std::numeric_limits<int64_t>::max());
  EXPECT_THROW(scalar(bytes, "BIGINT", "DECIMAL(38, 38)"), std::runtime_error);
}

TEST(PaimonBinaryRowTest, LargeDecimalSignExtensionAndSmallValuesRetainSdkPlainScale) {
  auto bytes = row(1);
  little(bytes, 12, (uint64_t{16} << 32) | 2);
  bytes += std::string("\xcf\xc7", 2);
  EXPECT_EQ(scalar(bytes, "DECIMAL(38, 2)"), "-123.45");
  bytes = row(1);
  little(bytes, 12, 1);
  EXPECT_EQ(scalar(bytes, "DECIMAL(18, 9)"), "0.000000001");
  EXPECT_EQ(scalar(bytes, "DECIMAL(18, 18)"), "0.000000000000000001");
  little(bytes, 12, 0);
  EXPECT_EQ(scalar(bytes, "DECIMAL(18, 18)"), "0.000000000000000000");
}

TEST(PaimonBinaryRowTest, RejectsDecimalPayloadOutsideItsDeclaredPrecisionBeforeFormatting) {
  auto bytes = row(1);
  little(bytes, 12, (uint64_t{16} << 32) | 16);
  bytes += std::string(16, '\0');
  bytes[20] = static_cast<char>(0x80);
  EXPECT_THROW(scalar(bytes, "DECIMAL(38, 0)"), std::runtime_error);
  bytes = row(1);
  little(bytes, 12, 1000);
  EXPECT_THROW(scalar(bytes, "DECIMAL(3, 0)"), std::runtime_error);
}

TEST(PaimonBinaryRowTest, DateStatsRemainIntegersWhilePartitionCastIsCalendarText) {
  auto bytes = row(1);
  little(bytes, 12, 1);
  EXPECT_EQ(scalar(bytes, "DATE"), "1");
  EXPECT_EQ(scalar(bytes, "DATE", nullptr, true), "1970-01-02");
}

TEST(PaimonBinaryRowTest, FloatingSpecialValuesAndNegativeZeroMatchJavaText) {
  auto bytes = row(1);
  little(bytes, 12, std::bit_cast<uint64_t>(-0.0));
  EXPECT_EQ(scalar(bytes, "DOUBLE"), "-0.0");
  little(bytes, 12, std::bit_cast<uint64_t>(std::numeric_limits<double>::infinity()));
  EXPECT_EQ(scalar(bytes, "DOUBLE"), "Infinity");
  little(bytes, 12, std::bit_cast<uint64_t>(std::numeric_limits<double>::quiet_NaN()));
  EXPECT_EQ(scalar(bytes, "DOUBLE"), "NaN");
  little(bytes, 12, std::bit_cast<uint32_t>(1.5f));
  EXPECT_EQ(scalar(bytes, "FLOAT"), "1.5");
}

TEST(PaimonBinaryRowTest, TimestampAndTimeDistinguishSdkStatsFromPartitionCasts) {
  auto bytes = row(1);
  EXPECT_EQ(scalar(bytes, "TIMESTAMP(3)"), "1970-01-01T00:00");
  EXPECT_EQ(scalar(bytes, "TIMESTAMP(3)", nullptr, true), "1970-01-01 00:00:00.000");
  little(bytes, 12, 120);
  EXPECT_EQ(scalar(bytes, "TIMESTAMP(3)"), "1970-01-01T00:00:00.120");
  EXPECT_EQ(scalar(bytes, "TIME(3)"), "120");
  EXPECT_EQ(scalar(bytes, "TIME(3)", nullptr, true), "00:00:00.12");
}

TEST(PaimonBinaryRowTest, RejectsInvalidScalarPrecisionAndTimestampOffsets) {
  EXPECT_THROW(PaimonScalarType::parse("DECIMAL(39,0)"), std::runtime_error);
  EXPECT_THROW(PaimonScalarType::parse("DECIMAL(3,4)"), std::runtime_error);
  EXPECT_THROW(PaimonScalarType::parse("TIMESTAMP(10)"), std::runtime_error);
  auto bytes = row(1);
  little(bytes, 12, uint64_t{64} << 32);
  EXPECT_THROW(scalar(bytes, "TIMESTAMP(9)"), std::runtime_error);
  little(bytes, 12, 2);
  EXPECT_THROW(scalar(bytes, "BOOLEAN"), std::runtime_error);
}

}  // namespace
}  // namespace pangrova::velox::test
