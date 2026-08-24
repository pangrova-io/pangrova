// Tests for VariantColumnReader — Variant Shredded encoding decoding.
// GoogleTest only (no rapidcheck per pangrova-rules.md §12).

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "variant/VariantColumnReader.h"

namespace pangrova::variant::test {
namespace {

// --- decodeVariantValue tests ---

TEST(VariantColumnReaderTest, EmptyMetadataPassthroughJson) {
  std::string json = R"({"name":"alice","age":30})";
  auto result = decodeVariantValue(nullptr, 0, json.data(), json.size());
  EXPECT_EQ(result, json);
}

TEST(VariantColumnReaderTest, EmptyMetadataEmptyValueReturnsNull) {
  auto result = decodeVariantValue(nullptr, 0, nullptr, 0);
  EXPECT_EQ(result, "null");
}

TEST(VariantColumnReaderTest, PrimitiveNull) {
  // basic_type=0 (Primitive), physical_type=0 (null) → header = 0x00
  uint8_t header = 0x00;
  auto result = decodeVariantValue(
      reinterpret_cast<const char*>(&header), 1, nullptr, 0);
  EXPECT_EQ(result, "null");
}

TEST(VariantColumnReaderTest, PrimitiveTrue) {
  // basic_type=0, physical_type=1 → header = (1 << 2) | 0 = 0x04
  uint8_t header = 0x04;
  auto result = decodeVariantValue(
      reinterpret_cast<const char*>(&header), 1, nullptr, 0);
  EXPECT_EQ(result, "true");
}

TEST(VariantColumnReaderTest, PrimitiveFalse) {
  // basic_type=0, physical_type=2 → header = (2 << 2) | 0 = 0x08
  uint8_t header = 0x08;
  auto result = decodeVariantValue(
      reinterpret_cast<const char*>(&header), 1, nullptr, 0);
  EXPECT_EQ(result, "false");
}

TEST(VariantColumnReaderTest, PrimitiveInt32) {
  // basic_type=0, physical_type=5 (int32) → header = (5 << 2) | 0 = 0x14
  uint8_t header = 0x14;
  int32_t value = 42;
  auto result = decodeVariantValue(
      reinterpret_cast<const char*>(&header), 1,
      reinterpret_cast<const char*>(&value), sizeof(value));
  EXPECT_EQ(result, "42");
}

TEST(VariantColumnReaderTest, PrimitiveInt64) {
  // basic_type=0, physical_type=6 (int64) → header = (6 << 2) | 0 = 0x18
  uint8_t header = 0x18;
  int64_t value = 1234567890123LL;
  auto result = decodeVariantValue(
      reinterpret_cast<const char*>(&header), 1,
      reinterpret_cast<const char*>(&value), sizeof(value));
  EXPECT_EQ(result, "1234567890123");
}

TEST(VariantColumnReaderTest, PrimitiveString) {
  // basic_type=0, physical_type=16 (string) → header = (16 << 2) | 0 = 0x40
  uint8_t header = 0x40;
  std::string value = "hello world";
  auto result = decodeVariantValue(
      reinterpret_cast<const char*>(&header), 1,
      value.data(), value.size());
  EXPECT_EQ(result, "\"hello world\"");
}

TEST(VariantColumnReaderTest, ShortString) {
  // basic_type=1 (ShortString), length=5 → header = (5 << 2) | 1 = 0x15
  uint8_t header = 0x15;
  std::string value = "hello";
  auto result = decodeVariantValue(
      reinterpret_cast<const char*>(&header), 1,
      value.data(), value.size());
  EXPECT_EQ(result, "\"hello\"");
}

TEST(VariantColumnReaderTest, ObjectPassthrough) {
  // basic_type=2 (Object) → header = 0x02
  uint8_t header = 0x02;
  std::string json = R"({"key":"value"})";
  auto result = decodeVariantValue(
      reinterpret_cast<const char*>(&header), 1,
      json.data(), json.size());
  EXPECT_EQ(result, json);
}

TEST(VariantColumnReaderTest, ArrayPassthrough) {
  // basic_type=3 (Array) → header = 0x03
  uint8_t header = 0x03;
  std::string json = R"([1,2,3])";
  auto result = decodeVariantValue(
      reinterpret_cast<const char*>(&header), 1,
      json.data(), json.size());
  EXPECT_EQ(result, json);
}

TEST(VariantColumnReaderTest, EmptyObjectDefault) {
  uint8_t header = 0x02;
  auto result = decodeVariantValue(
      reinterpret_cast<const char*>(&header), 1, nullptr, 0);
  EXPECT_EQ(result, "{}");
}

TEST(VariantColumnReaderTest, EmptyArrayDefault) {
  uint8_t header = 0x03;
  auto result = decodeVariantValue(
      reinterpret_cast<const char*>(&header), 1, nullptr, 0);
  EXPECT_EQ(result, "[]");
}

// --- decodeVariantBatch tests ---

TEST(VariantColumnReaderTest, BatchDecodeMultipleRows) {
  // Row 0: passthrough JSON
  std::string json0 = R"({"a":1})";
  // Row 1: null (both null)
  // Row 2: primitive int32
  uint8_t header2 = 0x14;
  int32_t val2 = 99;

  std::vector<std::pair<const char*, size_t>> metaBufs = {
      {nullptr, 0},
      {nullptr, 0},
      {reinterpret_cast<const char*>(&header2), 1},
  };
  std::vector<std::pair<const char*, size_t>> valBufs = {
      {json0.data(), json0.size()},
      {nullptr, 0},
      {reinterpret_cast<const char*>(&val2), sizeof(val2)},
  };

  std::vector<std::string> output;
  decodeVariantBatch(metaBufs, valBufs, 3, output);

  ASSERT_EQ(output.size(), 3u);
  EXPECT_EQ(output[0], json0);
  EXPECT_TRUE(output[1].empty());  // Both null → empty string.
  EXPECT_EQ(output[2], "99");
}

TEST(VariantColumnReaderTest, InsufficientValueLenReturnsNull) {
  // int32 requires 4 bytes, provide only 2.
  uint8_t header = 0x14;
  char shortBuf[2] = {0, 0};
  auto result = decodeVariantValue(
      reinterpret_cast<const char*>(&header), 1, shortBuf, 2);
  EXPECT_EQ(result, "null");
}

}  // namespace
}  // namespace pangrova::variant::test
