// DeletionVectorReaderTest: Unit tests for DeletionVectorReader.
// Tests Iceberg v3 Puffin format parsing and Paimon raw Roaring Bitmap reading.
// Validates: Requirements R5.4, R9.2

#include <gtest/gtest.h>

#include <cstring>
#include <string>
#include <unistd.h>

#include <roaring.hh>

#include "delete/io/DeletionVectorReader.h"

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

// Helper: encode a 4-byte little-endian integer.
std::string encodeInt32LE(int32_t val) {
  std::string result(4, '\0');
  result[0] = static_cast<char>(val & 0xFF);
  result[1] = static_cast<char>((val >> 8) & 0xFF);
  result[2] = static_cast<char>((val >> 16) & 0xFF);
  result[3] = static_cast<char>((val >> 24) & 0xFF);
  return result;
}

// Helper: build a minimal Puffin file containing a Roaring Bitmap blob.
// Layout: [magic(4)] [blob_data] [footer_payload] [footer_fixed(12)]
// Footer fixed: [footer_payload_size(4)] [flags(4)] [magic(4)]
std::string buildPuffinFile(const roaring::Roaring& bitmap) {
  // Serialize the bitmap.
  auto bitmapSize = bitmap.getSizeInBytes();
  std::string blobData(bitmapSize, '\0');
  bitmap.write(blobData.data());

  // Footer payload (minimal JSON — not parsed by our reader, but
  // included for format completeness).
  std::string footerPayload = R"({"blobs":[]})";

  // Build the file.
  std::string file;
  file.append(PuffinConstants::kMagic, PuffinConstants::kMagicSize);  // header magic
  file.append(blobData);                                               // blob data
  file.append(footerPayload);                                          // footer payload
  file.append(encodeInt32LE(static_cast<int32_t>(footerPayload.size()))); // footer_payload_size
  file.append(encodeInt32LE(0));                                       // flags
  file.append(PuffinConstants::kMagic, PuffinConstants::kMagicSize);  // footer magic
  return file;
}

// Helper: build a raw Paimon DV file (just serialized Roaring Bitmap).
std::string buildPaimonDVFile(const roaring::Roaring& bitmap) {
  auto bitmapSize = bitmap.getSizeInBytes();
  std::string data(bitmapSize, '\0');
  bitmap.write(data.data());
  return data;
}

// --- Iceberg Puffin format tests ---

TEST(DeletionVectorReaderTest, ReadIcebergDV_BasicBitmap) {
  roaring::Roaring expected;
  expected.add(1);
  expected.add(5);
  expected.add(100);
  expected.add(9999);

  auto puffinContent = buildPuffinFile(expected);
  TempFile tmpFile(puffinContent);

  auto result = DeletionVectorReader::readIcebergDV(tmpFile.path());

  EXPECT_EQ(result.cardinality(), 4);
  EXPECT_TRUE(result.contains(1));
  EXPECT_TRUE(result.contains(5));
  EXPECT_TRUE(result.contains(100));
  EXPECT_TRUE(result.contains(9999));
  EXPECT_FALSE(result.contains(0));
  EXPECT_FALSE(result.contains(2));
}

TEST(DeletionVectorReaderTest, ReadIcebergDV_EmptyBitmap) {
  roaring::Roaring expected;
  auto puffinContent = buildPuffinFile(expected);
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

  auto puffinContent = buildPuffinFile(expected);
  TempFile tmpFile(puffinContent);

  auto result = DeletionVectorReader::readIcebergDV(tmpFile.path());

  EXPECT_EQ(result.cardinality(), expected.cardinality());
  EXPECT_TRUE(result == expected);
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

  auto result = DeletionVectorReader::readPaimonDV(tmpFile.path());

  EXPECT_EQ(result.cardinality(), 4);
  EXPECT_TRUE(result.contains(0));
  EXPECT_TRUE(result.contains(7));
  EXPECT_TRUE(result.contains(42));
  EXPECT_TRUE(result.contains(1000));
  EXPECT_FALSE(result.contains(1));
}

TEST(DeletionVectorReaderTest, ReadPaimonDV_EmptyBitmap) {
  roaring::Roaring expected;
  auto rawContent = buildPaimonDVFile(expected);
  TempFile tmpFile(rawContent);

  auto result = DeletionVectorReader::readPaimonDV(tmpFile.path());

  EXPECT_TRUE(result.isEmpty());
}

TEST(DeletionVectorReaderTest, ReadPaimonDV_LargeBitmap) {
  roaring::Roaring expected;
  for (uint32_t i = 0; i < 50000; i += 7) {
    expected.add(i);
  }

  auto rawContent = buildPaimonDVFile(expected);
  TempFile tmpFile(rawContent);

  auto result = DeletionVectorReader::readPaimonDV(tmpFile.path());

  EXPECT_EQ(result.cardinality(), expected.cardinality());
  EXPECT_TRUE(result == expected);
}

TEST(DeletionVectorReaderTest, ReadPaimonDV_NonExistentFile) {
  EXPECT_THROW(
      DeletionVectorReader::readPaimonDV("/nonexistent/path/dv.bin"),
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

  auto puffinContent = buildPuffinFile(expected);
  TempFile icebergFile(puffinContent);

  auto rawContent = buildPaimonDVFile(expected);
  TempFile paimonFile(rawContent);

  auto icebergResult = DeletionVectorReader::readIcebergDV(icebergFile.path());
  auto paimonResult = DeletionVectorReader::readPaimonDV(paimonFile.path());

  EXPECT_TRUE(icebergResult == paimonResult);
  EXPECT_EQ(icebergResult.cardinality(), expected.cardinality());
}

}  // namespace
}  // namespace pangrova::delete_filter::io
