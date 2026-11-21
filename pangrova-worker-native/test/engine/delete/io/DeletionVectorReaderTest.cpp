// DeletionVectorReaderTest: Unit tests for DeletionVectorReader.
// Tests Iceberg v3 Puffin format parsing and Paimon raw Roaring Bitmap reading.
// Validates: Requirements R5.4, R9.2

#include <gtest/gtest.h>

#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <unistd.h>

#include <roaring.hh>

#include "engine/delete/io/DeletionVectorReader.h"

namespace pangrova::delete_filter::io {
namespace {

// RAII temporary file: writes content on construction, removes on destruction.
class TempFile {
 public:
  explicit TempFile(const std::string& content) {
    // Use mkstemp for safe temp file creation.
    char tmpl[] = "/tmp/pangrova_dv_test_XXXXXX";
    int fd = mkstemp(tmpl);
    if (fd < 0) {
      throw std::runtime_error("Failed to create temp file");
    }
    path_ = tmpl;
    auto written = write(fd, content.data(), content.size());
    close(fd);
    if (written != static_cast<ssize_t>(content.size())) {
      throw std::runtime_error("Failed to write temp file");
    }
  }

  ~TempFile() {
    std::remove(path_.c_str());
  }

  const std::string& path() const { return path_; }

 private:
  std::string path_;
};

std::string sdkPuffin(const std::string& name) {
  const auto path = std::filesystem::path(__FILE__).parent_path().parent_path()
      / ("fixtures/iceberg-sdk-" + name + "-dv.puffin");
  std::ifstream input(path, std::ios::binary);
  if (!input) throw std::runtime_error("Missing SDK fixture: " + path.string());
  return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

// Helper: build a raw Paimon DV file (just serialized Roaring Bitmap).
std::string buildPaimonDVFile(const roaring::Roaring& bitmap) {
  auto bitmapSize = bitmap.getSizeInBytes();
  std::string data(bitmapSize, '\0');
  bitmap.write(data.data());
  return data;
}

// Actual pinned Iceberg BaseDVFileWriter output, including a 64-bit position.
TEST(DeletionVectorReaderTest, ReadsSdkDeletionVectorContainer) {
  const auto path = std::filesystem::path(__FILE__).parent_path().parent_path()
      / "fixtures/iceberg-sdk-multiple-dv.puffin";
  const auto result = DeletionVectorReader::readIcebergDV(path.string(), "s3://fixture/a.parquet");
  EXPECT_EQ(result.cardinality(), 1);
  EXPECT_TRUE(result.contains(1U));
}

TEST(DeletionVectorReaderTest, SelectsSdkBlobByDataFileAndPreserves64BitPositions) {
  TempFile fixture(sdkPuffin("multiple"));
  const auto bitmap = DeletionVectorReader::readIcebergDV(fixture.path(), "s3://fixture/b.parquet");
  EXPECT_EQ(bitmap.cardinality(), 2);
  EXPECT_TRUE(bitmap.contains(uint64_t{2}));
  EXPECT_TRUE(bitmap.contains((uint64_t{1} << 32) + 7));
  EXPECT_FALSE(bitmap.contains(uint64_t{1}));
  EXPECT_FALSE(bitmap.contains(uint64_t{7}));
  EXPECT_THROW(DeletionVectorReader::readIcebergDV(fixture.path()), std::runtime_error);
  EXPECT_THROW(DeletionVectorReader::readIcebergDV(fixture.path(), "s3://fixture/other.parquet"),
               std::runtime_error);
}

TEST(DeletionVectorReaderTest, RejectsSdkPayloadCorruption) {
  auto bytes = sdkPuffin("multiple");
  // The SDK receipt fixes the first blob at offset 4, length 42. Flip its
  // CRC, leaving its bitmap bytes and footer intact.
  bytes.at(45) ^= 1;
  TempFile fixture(bytes);
  EXPECT_THROW(DeletionVectorReader::readIcebergDV(fixture.path(), "s3://fixture/a.parquet"),
               std::runtime_error);
}

TEST(DeletionVectorReaderTest, RejectsSdkLengthCorruption) {
  auto bytes = sdkPuffin("multiple");
  bytes.at(7) ^= 1;
  TempFile fixture(bytes);
  EXPECT_THROW(DeletionVectorReader::readIcebergDV(fixture.path(), "s3://fixture/a.parquet"),
               std::runtime_error);
}

TEST(DeletionVectorReaderTest, RejectsUnknownFooterFlags) {
  auto bytes = sdkPuffin("multiple");
  bytes.at(bytes.size() - 8) = 2;
  TempFile fixture(bytes);
  EXPECT_THROW(DeletionVectorReader::readIcebergDV(fixture.path(), "s3://fixture/a.parquet"),
               std::runtime_error);
}

// --- Iceberg Puffin format tests ---

TEST(DeletionVectorReaderTest, ReadIcebergDV_BasicBitmap) {
  roaring::Roaring expected;
  expected.add(1);
  expected.add(5);
  expected.add(100);
  expected.add(9999);

  auto puffinContent = sdkPuffin("basic");
  TempFile tmpFile(puffinContent);

  auto result = DeletionVectorReader::readIcebergDV(tmpFile.path());

  EXPECT_EQ(result.cardinality(), 4);
  EXPECT_TRUE(result.contains(1U));
  EXPECT_TRUE(result.contains(5U));
  EXPECT_TRUE(result.contains(100U));
  EXPECT_TRUE(result.contains(9999U));
  EXPECT_FALSE(result.contains(0U));
  EXPECT_FALSE(result.contains(2U));
}

TEST(DeletionVectorReaderTest, ReadIcebergDV_EmptyBitmap) {
  roaring::Roaring expected;
  auto puffinContent = sdkPuffin("empty");
  TempFile tmpFile(puffinContent);

  auto result = DeletionVectorReader::readIcebergDV(tmpFile.path());

  EXPECT_TRUE(result.isEmpty());
  EXPECT_EQ(result.cardinality(), 0);
}

TEST(DeletionVectorReaderTest, ReadIcebergDV_LargeBitmap) {
  roaring::Roaring expected;
  for (uint32_t i = 0; i < 10000; i += 3) {
    expected.add(i);
  }

  auto puffinContent = sdkPuffin("large");
  TempFile tmpFile(puffinContent);

  auto result = DeletionVectorReader::readIcebergDV(tmpFile.path());

  EXPECT_EQ(result.cardinality(), expected.cardinality());
  EXPECT_TRUE(result == roaring::Roaring64Map(expected));
}

TEST(DeletionVectorReaderTest, ReadIcebergDV_InvalidMagic) {
  std::string badContent = "BAD1xxxx";
  TempFile tmpFile(badContent);

  EXPECT_THROW(
      DeletionVectorReader::readIcebergDV(tmpFile.path()),
      std::runtime_error);
}

TEST(DeletionVectorReaderTest, ReadIcebergDV_FileTooSmall) {
  std::string tinyContent = "PU";
  TempFile tmpFile(tinyContent);

  EXPECT_THROW(
      DeletionVectorReader::readIcebergDV(tmpFile.path()),
      std::runtime_error);
}

TEST(DeletionVectorReaderTest, ReadIcebergDV_NonExistentFile) {
  EXPECT_THROW(
      DeletionVectorReader::readIcebergDV("/nonexistent/path/dv.puffin"),
      std::runtime_error);
}

// --- Paimon raw format tests ---

TEST(DeletionVectorReaderTest, ReadPaimonDV_BasicBitmap) {
  roaring::Roaring expected;
  expected.add(0);
  expected.add(7);
  expected.add(42);
  expected.add(1000);

  auto rawContent = buildPaimonDVFile(expected);
  TempFile tmpFile(rawContent);

  auto result = DeletionVectorReader::readRawRoaringBitmap(tmpFile.path());

  EXPECT_EQ(result.cardinality(), 4);
  EXPECT_TRUE(result.contains(0U));
  EXPECT_TRUE(result.contains(7U));
  EXPECT_TRUE(result.contains(42U));
  EXPECT_TRUE(result.contains(1000U));
  EXPECT_FALSE(result.contains(1U));
}

TEST(DeletionVectorReaderTest, ReadPaimonDV_EmptyBitmap) {
  roaring::Roaring expected;
  auto rawContent = buildPaimonDVFile(expected);
  TempFile tmpFile(rawContent);

  auto result = DeletionVectorReader::readRawRoaringBitmap(tmpFile.path());

  EXPECT_TRUE(result.isEmpty());
}

TEST(DeletionVectorReaderTest, ReadPaimonDV_LargeBitmap) {
  roaring::Roaring expected;
  for (uint32_t i = 0; i < 50000; i += 7) {
    expected.add(i);
  }

  auto rawContent = buildPaimonDVFile(expected);
  TempFile tmpFile(rawContent);

  auto result = DeletionVectorReader::readRawRoaringBitmap(tmpFile.path());

  EXPECT_EQ(result.cardinality(), expected.cardinality());
  EXPECT_TRUE(result == expected);
}

TEST(DeletionVectorReaderTest, ReadPaimonDV_NonExistentFile) {
  EXPECT_THROW(
      DeletionVectorReader::readRawRoaringBitmap("/nonexistent/path/dv.bin"),
      std::runtime_error);
}

// --- Cross-format consistency test ---

TEST(DeletionVectorReaderTest, IcebergAndPaimonProduceSameBitmap) {
  // Both formats should produce identical Roaring Bitmaps for the same
  // deleted row positions. This validates the shared utility contract.
  roaring::Roaring expected;
  expected.add(10);
  expected.add(20);
  expected.add(30);
  expected.add(500);
  expected.add(65535);

  auto puffinContent = sdkPuffin("cross");
  TempFile icebergFile(puffinContent);

  auto rawContent = buildPaimonDVFile(expected);
  TempFile paimonFile(rawContent);

  auto icebergResult = DeletionVectorReader::readIcebergDV(icebergFile.path());
  auto paimonResult = DeletionVectorReader::readRawRoaringBitmap(paimonFile.path());

  EXPECT_TRUE(icebergResult == roaring::Roaring64Map(paimonResult));
  EXPECT_EQ(icebergResult.cardinality(), expected.cardinality());
}

}  // namespace
}  // namespace pangrova::delete_filter::io
