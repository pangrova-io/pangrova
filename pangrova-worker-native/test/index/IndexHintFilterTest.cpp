// IndexHintFilterTest: Unit tests for predicate JSON parsing.
//
// F-NATIVE-WRITE-10: Validates the simdjson-based predicate parser
// correctly handles whitespace, escaped strings, additional fields,
// nested objects, and malformed input — all cases where the legacy
// hand-rolled regex parser produced incorrect results.

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <unistd.h>

#include "index/IndexHintFilter.h"
#include "pangrova/protocol/v2/connector.pb.h"
#include "velox/common/file/FileSystems.h"

namespace pangrova::index {
namespace {

void appendU32(std::string& bytes, uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) {
    bytes.push_back(static_cast<char>(value >> shift));
  }
}

uint32_t artifactCrc32(const std::string& bytes) {
  uint32_t crc = 0xffffffffU;
  for (unsigned char byte : bytes) {
    crc ^= byte;
    for (int bit = 0; bit < 8; ++bit) {
      crc = (crc >> 1) ^ (0xedb88320U & (0U - (crc & 1U)));
    }
  }
  return ~crc;
}

std::string bloomArtifact(const std::string& bloom) {
  std::string payload;
  appendU32(payload, 8);  // BloomFilterIndexData.numHashFunctions
  appendU32(payload, 0);
  appendU32(payload, 256);  // numBits
  appendU32(payload, bloom.size());
  payload += bloom;

  std::string artifact;
  appendU32(artifact, 0x41455448);  // IndexSerializer AETH
  artifact.push_back(0);
  artifact.push_back(1);  // version 1
  artifact.push_back(0);
  artifact.push_back(3);  // BLOOM_FILTER stable ID
  appendU32(artifact, 0);
  appendU32(artifact, payload.size());
  artifact += payload;
  appendU32(artifact, artifactCrc32(artifact));
  return artifact;
}

std::string emptyBloomArtifact() {
  std::string bloom;
  appendU32(bloom, 0x53424246);  // SBBF
  appendU32(bloom, 1);
  appendU32(bloom, 1);
  appendU32(bloom, 0);
  bloom.append(32, '\0');
  return bloomArtifact(bloom);
}

std::string fromHex(const std::string& hex) {
  std::string bytes;
  bytes.reserve(hex.size() / 2);
  for (size_t i = 0; i < hex.size(); i += 2) {
    bytes.push_back(static_cast<char>(std::stoi(hex.substr(i, 2), nullptr, 16)));
  }
  return bytes;
}

TEST(IndexHintFilterTest, JavaStringBloomFixtureKeepsInsertedValues) {
  facebook::velox::filesystems::registerLocalFileSystem();
  // Produced by Java SplitBlockBloomFilter.builder(1, 0.01)
  //     .put("ACTIVE").put("X").build().toByteArray().
  const auto bloom = fromHex(
      "53424246000000010000000100000002"
      "40000100008000400080001000009000"
      "0000c000020000402800000010004000");
  auto path = std::filesystem::temp_directory_path() /
      ("pangrova-index-string-hint-" + std::to_string(::getpid()) + ".idx");
  const auto artifact = bloomArtifact(bloom);
  { std::ofstream out(path, std::ios::binary); out.write(artifact.data(), artifact.size()); }

  pangrova::protocol::v2::FileSplitDetail detail;
  auto* hint = detail.add_index_eval_hints();
  hint->set_index_type("BLOOM_FILTER");
  hint->set_index_file_path(path.string());
  hint->set_data_file_path("/data/file.parquet");
  hint->mutable_predicate()->set_column("status");
  auto* value = hint->mutable_predicate()->mutable_equal_to()->mutable_value();
  value->set_string_value("ACTIVE");
  EXPECT_TRUE(IndexHintFilter::shouldKeepTypedSplit(
      detail.SerializeAsString(), "/data/file.parquet"));
  value->set_string_value("X");
  EXPECT_TRUE(IndexHintFilter::shouldKeepTypedSplit(
      detail.SerializeAsString(), "/data/file.parquet"));
  value->set_string_value("INACTIVE");
  EXPECT_FALSE(IndexHintFilter::shouldKeepTypedSplit(
      detail.SerializeAsString(), "/data/file.parquet"));

  // An unknown SBBF version must never become a negative proof.
  auto future_bloom = bloom;
  future_bloom[7] = 2;
  const auto future_artifact = bloomArtifact(future_bloom);
  { std::ofstream out(path, std::ios::binary);
    out.write(future_artifact.data(), future_artifact.size()); }
  EXPECT_TRUE(IndexHintFilter::shouldKeepTypedSplit(
      detail.SerializeAsString(), "/data/file.parquet"));
  std::filesystem::remove(path);
}

TEST(IndexHintFilterTest, CanonicalTypedBloomHintPrunesOnlyValidatedNegative) {
  facebook::velox::filesystems::registerLocalFileSystem();
  auto path = std::filesystem::temp_directory_path() /
      ("pangrova-index-hint-" + std::to_string(::getpid()) + ".idx");
  const auto artifact = emptyBloomArtifact();
  { std::ofstream out(path, std::ios::binary); out.write(artifact.data(), artifact.size()); }

  pangrova::protocol::v2::FileSplitDetail detail;
  auto* hint = detail.add_index_eval_hints();
  hint->set_index_type("BLOOM_FILTER");
  hint->set_index_file_path(path.string());
  hint->set_data_file_path("/data/file.parquet");
  hint->mutable_predicate()->set_column("user_id");
  hint->mutable_predicate()->mutable_equal_to()->mutable_value()->set_int64_value(1);

  bool evaluated = false;
  EXPECT_FALSE(IndexHintFilter::shouldKeepTypedSplit(
      detail.SerializeAsString(), "/data/file.parquet", &evaluated));
  EXPECT_TRUE(evaluated);
  evaluated = false;
  EXPECT_TRUE(IndexHintFilter::shouldKeepTypedSplit(
      detail.SerializeAsString(), "/other/file.parquet", &evaluated));
  EXPECT_FALSE(evaluated);

  std::string corrupt = artifact;
  corrupt.back() ^= 1;
  { std::ofstream out(path, std::ios::binary); out.write(corrupt.data(), corrupt.size()); }
  EXPECT_TRUE(IndexHintFilter::shouldKeepTypedSplit(
      detail.SerializeAsString(), "/data/file.parquet"));
  std::filesystem::remove(path);
}

// --- Happy path: documented predicate shapes -------------------------------

TEST(IndexHintFilterTest, ParseEqPredicate) {
  auto p = IndexHintFilter::parsePredicate(
      R"({"op":"eq","column":"user_id","value":"abc123"})");
  EXPECT_EQ(p.op, PredicateOp::EQ);
  EXPECT_EQ(p.column_name, "user_id");
  EXPECT_EQ(p.value, "abc123");
}

TEST(IndexHintFilterTest, ParseInPredicate) {
  auto p = IndexHintFilter::parsePredicate(
      R"({"op":"in","column":"region","values":["us","eu","apac"]})");
  EXPECT_EQ(p.op, PredicateOp::IN);
  EXPECT_EQ(p.column_name, "region");
  ASSERT_EQ(p.values.size(), 3u);
  EXPECT_EQ(p.values[0], "us");
  EXPECT_EQ(p.values[1], "eu");
  EXPECT_EQ(p.values[2], "apac");
}

TEST(IndexHintFilterTest, ParseRangePredicate) {
  auto p = IndexHintFilter::parsePredicate(
      R"({"op":"range","column":"amount","min":100,"max":999})");
  EXPECT_EQ(p.op, PredicateOp::RANGE);
  EXPECT_EQ(p.column_name, "amount");
  EXPECT_EQ(p.range_min, 100);
  EXPECT_EQ(p.range_max, 999);
}

TEST(IndexHintFilterTest, ParseRangePredicateNegativeBounds) {
  auto p = IndexHintFilter::parsePredicate(
      R"({"op":"range","column":"delta","min":-50,"max":-10})");
  EXPECT_EQ(p.op, PredicateOp::RANGE);
  EXPECT_EQ(p.range_min, -50);
  EXPECT_EQ(p.range_max, -10);
}

// --- Robustness: cases that broke the legacy regex parser ------------------

TEST(IndexHintFilterTest, ParseEqWithWhitespace) {
  // Legacy parser already handled this, but verify still works.
  auto p = IndexHintFilter::parsePredicate(
      "{ \"op\" : \"eq\" , \"column\" : \"k\" , \"value\" : \"v\" }");
  EXPECT_EQ(p.op, PredicateOp::EQ);
  EXPECT_EQ(p.column_name, "k");
  EXPECT_EQ(p.value, "v");
}

TEST(IndexHintFilterTest, ParseEqWithEscapedQuoteInValue) {
  // Legacy regex would terminate at the first inner `"` — break.
  auto p = IndexHintFilter::parsePredicate(
      R"({"op":"eq","column":"name","value":"O\"Brien"})");
  EXPECT_EQ(p.op, PredicateOp::EQ);
  EXPECT_EQ(p.column_name, "name");
  EXPECT_EQ(p.value, "O\"Brien");
}

TEST(IndexHintFilterTest, ParseEqWithBackslashInValue) {
  auto p = IndexHintFilter::parsePredicate(
      R"({"op":"eq","column":"path","value":"C:\\Users\\joe"})");
  EXPECT_EQ(p.op, PredicateOp::EQ);
  EXPECT_EQ(p.value, "C:\\Users\\joe");
}

TEST(IndexHintFilterTest, ParseEqWithNestedObjectBeforeOp) {
  // Legacy regex: `find("op")` matches inside the nested "operator" string.
  auto p = IndexHintFilter::parsePredicate(
      R"({"context":{"operator":"scan","stage":"leaf"},"op":"eq","column":"k","value":"v"})");
  EXPECT_EQ(p.op, PredicateOp::EQ);
  EXPECT_EQ(p.column_name, "k");
  EXPECT_EQ(p.value, "v");
}

TEST(IndexHintFilterTest, ParseRangeWithExtraFields) {
  // Realistic: predicate emitter adds metadata fields the parser must skip.
  auto p = IndexHintFilter::parsePredicate(
      R"({"op":"range","column":"ts","min":1000,"max":2000,"hint_id":"h7","priority":3})");
  EXPECT_EQ(p.op, PredicateOp::RANGE);
  EXPECT_EQ(p.range_min, 1000);
  EXPECT_EQ(p.range_max, 2000);
}

TEST(IndexHintFilterTest, ParseInWithKeyNamedValueElsewhere) {
  // Legacy: `find("values")` could collide with a sibling field whose key
  // contains "values" — simdjson key matching is exact.
  auto p = IndexHintFilter::parsePredicate(
      R"({"op":"in","column":"c","cached_values_hash":"x","values":["a","b"]})");
  EXPECT_EQ(p.op, PredicateOp::IN);
  ASSERT_EQ(p.values.size(), 2u);
  EXPECT_EQ(p.values[0], "a");
  EXPECT_EQ(p.values[1], "b");
}

// --- Malformed input: parser must return UNSUPPORTED, not crash ------------

TEST(IndexHintFilterTest, ParseMalformedJsonReturnsUnsupported) {
  auto p = IndexHintFilter::parsePredicate("{not even json");
  EXPECT_EQ(p.op, PredicateOp::UNSUPPORTED);
}

TEST(IndexHintFilterTest, ParseEmptyStringReturnsUnsupported) {
  auto p = IndexHintFilter::parsePredicate("");
  EXPECT_EQ(p.op, PredicateOp::UNSUPPORTED);
}

TEST(IndexHintFilterTest, ParseMissingColumnReturnsUnsupported) {
  auto p = IndexHintFilter::parsePredicate(
      R"({"op":"eq","value":"v"})");
  EXPECT_EQ(p.op, PredicateOp::UNSUPPORTED);
}

TEST(IndexHintFilterTest, ParseMissingValueReturnsUnsupported) {
  auto p = IndexHintFilter::parsePredicate(
      R"({"op":"eq","column":"k"})");
  EXPECT_EQ(p.op, PredicateOp::UNSUPPORTED);
}

TEST(IndexHintFilterTest, ParseEmptyValuesArrayReturnsUnsupported) {
  auto p = IndexHintFilter::parsePredicate(
      R"({"op":"in","column":"k","values":[]})");
  EXPECT_EQ(p.op, PredicateOp::UNSUPPORTED);
}

TEST(IndexHintFilterTest, ParseUnknownOpReturnsUnsupported) {
  auto p = IndexHintFilter::parsePredicate(
      R"({"op":"like","column":"k","value":"%foo%"})");
  EXPECT_EQ(p.op, PredicateOp::UNSUPPORTED);
}

TEST(IndexHintFilterTest, ParseRangeWithMissingBoundsDefaultsToZero) {
  // Legacy contract: extractJsonInt() returned 0 on missing key.
  auto p = IndexHintFilter::parsePredicate(
      R"({"op":"range","column":"k"})");
  EXPECT_EQ(p.op, PredicateOp::RANGE);
  EXPECT_EQ(p.range_min, 0);
  EXPECT_EQ(p.range_max, 0);
}

TEST(IndexHintFilterTest, ParseInWithNonStringElementsFiltersThem) {
  // Mixed types: only string elements are extracted; rest skipped.
  auto p = IndexHintFilter::parsePredicate(
      R"({"op":"in","column":"k","values":["a",123,"b",null,"c"]})");
  EXPECT_EQ(p.op, PredicateOp::IN);
  ASSERT_EQ(p.values.size(), 3u);
  EXPECT_EQ(p.values[0], "a");
  EXPECT_EQ(p.values[1], "b");
  EXPECT_EQ(p.values[2], "c");
}

TEST(IndexHintFilterTest, ParseValueWithUnicode) {
  auto p = IndexHintFilter::parsePredicate(
      R"({"op":"eq","column":"name","value":"日本語"})");
  EXPECT_EQ(p.op, PredicateOp::EQ);
  EXPECT_EQ(p.value, "日本語");
}

}  // namespace
}  // namespace pangrova::index
