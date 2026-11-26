#include <gtest/gtest.h>
#include <openssl/sha.h>

#include <filesystem>
#include <fstream>
#include <unistd.h>

#include "indexing/index/IndexHintFilter.h"
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

void bindPublishedArtifact(pangrova::protocol::v2::IndexEvalHintDetail* hint,
                           const std::string& bytes) {
  unsigned char digest[SHA256_DIGEST_LENGTH];
  SHA256(reinterpret_cast<const unsigned char*>(bytes.data()), bytes.size(), digest);
  hint->set_index_size_bytes(bytes.size());
  hint->set_index_sha256(digest, sizeof(digest));
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

  std::vector<protocol::v2::IndexEvalHintDetail> hints(1);
  auto* hint = &hints.front();
  hint->set_index_type("BLOOM_FILTER");
  bindPublishedArtifact(hint, artifact);
  hint->set_index_file_path(path.string());
  hint->set_data_file_path("/data/file.parquet");
  hint->mutable_predicate()->set_column("status");
  auto* value = hint->mutable_predicate()->mutable_equal_to()->mutable_value();
  value->set_string_value("ACTIVE");
  EXPECT_TRUE(IndexHintFilter::shouldKeepTypedSplit(
      &hints, "/data/file.parquet"));
  value->set_string_value("X");
  EXPECT_TRUE(IndexHintFilter::shouldKeepTypedSplit(
      &hints, "/data/file.parquet"));
  value->set_string_value("INACTIVE");
  EXPECT_FALSE(IndexHintFilter::shouldKeepTypedSplit(
      &hints, "/data/file.parquet"));

  // A same-length, CRC-valid Bloom that omits ACTIVE would discard matching rows.
  value->set_string_value("ACTIVE");
  const auto replacement = emptyBloomArtifact();
  ASSERT_EQ(replacement.size(), artifact.size());
  { std::ofstream out(path, std::ios::binary);
    out.write(replacement.data(), replacement.size()); }
  EXPECT_TRUE(IndexHintFilter::shouldKeepTypedSplit(
      &hints, "/data/file.parquet"));
  bindPublishedArtifact(hint, replacement);
  EXPECT_FALSE(IndexHintFilter::shouldKeepTypedSplit(
      &hints, "/data/file.parquet"));

  // An unknown SBBF version must never become a negative proof.
  auto future_bloom = bloom;
  future_bloom[7] = 2;
  const auto future_artifact = bloomArtifact(future_bloom);
  bindPublishedArtifact(hint, future_artifact);
  { std::ofstream out(path, std::ios::binary);
    out.write(future_artifact.data(), future_artifact.size()); }
  EXPECT_TRUE(IndexHintFilter::shouldKeepTypedSplit(
      &hints, "/data/file.parquet"));
  std::filesystem::remove(path);
}

TEST(IndexHintFilterTest, CanonicalTypedBloomHintPrunesOnlyValidatedNegative) {
  facebook::velox::filesystems::registerLocalFileSystem();
  auto path = std::filesystem::temp_directory_path() /
      ("pangrova-index-hint-" + std::to_string(::getpid()) + ".idx");
  const auto artifact = emptyBloomArtifact();
  { std::ofstream out(path, std::ios::binary); out.write(artifact.data(), artifact.size()); }

  std::vector<protocol::v2::IndexEvalHintDetail> hints(1);
  auto* hint = &hints.front();
  hint->set_index_type("BLOOM_FILTER");
  bindPublishedArtifact(hint, artifact);
  hint->set_index_file_path(path.string());
  hint->set_data_file_path("/data/file.parquet");
  hint->mutable_predicate()->set_column("user_id");
  hint->mutable_predicate()->mutable_equal_to()->mutable_value()->set_int64_value(1);

  bool evaluated = false;
  EXPECT_FALSE(IndexHintFilter::shouldKeepTypedSplit(
      &hints, "/data/file.parquet", &evaluated));
  EXPECT_TRUE(evaluated);
  evaluated = false;
  EXPECT_TRUE(IndexHintFilter::shouldKeepTypedSplit(
      &hints, "/other/file.parquet", &evaluated));
  EXPECT_FALSE(evaluated);

  hint->set_index_size_bytes(artifact.size() + 1);
  EXPECT_TRUE(IndexHintFilter::shouldKeepTypedSplit(
      &hints, "/data/file.parquet"));
  hint->clear_index_size_bytes();
  EXPECT_TRUE(IndexHintFilter::shouldKeepTypedSplit(
      &hints, "/data/file.parquet"));
  hint->set_index_size_bytes(artifact.size());
  hint->clear_index_sha256();
  EXPECT_TRUE(IndexHintFilter::shouldKeepTypedSplit(
      &hints, "/data/file.parquet"));

  std::string corrupt = artifact;
  corrupt.back() ^= 1;
  bindPublishedArtifact(hint, corrupt);
  { std::ofstream out(path, std::ios::binary); out.write(corrupt.data(), corrupt.size()); }
  EXPECT_TRUE(IndexHintFilter::shouldKeepTypedSplit(
      &hints, "/data/file.parquet"));
  std::filesystem::remove(path);
}

}  // namespace
}  // namespace pangrova::index
