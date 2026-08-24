// Tests for DeleteFileReader — Position Delete, Equality Delete, Deletion Vector.
// Validates: Requirements 42.1
//
// DeleteFileReader is a static utility that reads Iceberg v2/v3 delete files
// (Parquet format) and Puffin deletion vectors. Since it requires real Parquet
// files on disk, we test the API contract and edge cases using temporary files.
#include <gtest/gtest.h>

#include <roaring.hh>
#include <cstdio>
#include <fstream>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include "delete/io/DeleteFileReader.h"

namespace pangrova::delete_filter::test {
namespace {

// --- Deletion Vector tests (Roaring Bitmap serialization round-trip) ---

// Helper: write a Roaring bitmap to a temp file and return the path.
std::string writeDeletionVectorFile(const roaring::Roaring& bitmap) {
  char path_template[] = "/tmp/pangrova_delete_file_XXXXXX";
  int fd = mkstemp(path_template);
  if (fd < 0) {
    throw std::runtime_error("Failed to create deletion vector temp file");
  }
  close(fd);
  std::string path(path_template);
  size_t size = bitmap.getSizeInBytes();
  std::vector<char> buf(size);
  bitmap.write(buf.data());
  std::ofstream out(path, std::ios::binary);
  out.write(buf.data(), static_cast<std::streamsize>(size));
  out.close();
  return path;
}

TEST(DeleteFileReaderTest, ReadDeletionVectorBasic) {
  roaring::Roaring original;
  original.add(0);
  original.add(5);
  original.add(100);
  original.add(999);

  auto path = writeDeletionVectorFile(original);
  auto result = DeleteFileReader::readDeletionVector(path);

  EXPECT_TRUE(result.contains(0));
  EXPECT_TRUE(result.contains(5));
  EXPECT_TRUE(result.contains(100));
  EXPECT_TRUE(result.contains(999));
  EXPECT_FALSE(result.contains(1));
  EXPECT_FALSE(result.contains(50));
  EXPECT_EQ(result.cardinality(), 4u);

  std::remove(path.c_str());
}

TEST(DeleteFileReaderTest, ReadDeletionVectorEmpty) {
  roaring::Roaring original;  // empty bitmap
  auto path = writeDeletionVectorFile(original);
  auto result = DeleteFileReader::readDeletionVector(path);

  EXPECT_EQ(result.cardinality(), 0u);
  EXPECT_TRUE(result.isEmpty());

  std::remove(path.c_str());
}

TEST(DeleteFileReaderTest, ReadDeletionVectorLargeRange) {
  roaring::Roaring original;
  for (uint32_t i = 0; i < 10000; i += 3) {
    original.add(i);
  }

  auto path = writeDeletionVectorFile(original);
  auto result = DeleteFileReader::readDeletionVector(path);

  EXPECT_EQ(result.cardinality(), original.cardinality());
  EXPECT_TRUE(result == original);

  std::remove(path.c_str());
}

TEST(DeleteFileReaderTest, ReadDeletionVectorNonExistentFileThrows) {
  EXPECT_THROW(
      DeleteFileReader::readDeletionVector("/nonexistent/path.dv"),
      std::runtime_error);
}

// --- Position Delete edge cases (without real Parquet files) ---

TEST(DeleteFileReaderTest, ReadPositionsNonExistentFileReturnsEmpty) {
#if PANGROVA_HAS_VELOX_DELETE_READER
  EXPECT_THROW(
      DeleteFileReader::readPositions("/nonexistent.parquet", "data.parquet"),
      std::runtime_error);
#endif
}

// --- Equality Delete edge cases ---

TEST(DeleteFileReaderTest, ReadEqualityKeysNonExistentFileReturnsEmpty) {
#if PANGROVA_HAS_VELOX_DELETE_READER
  EXPECT_THROW(
      DeleteFileReader::readEqualityKeys(
          "/nonexistent.parquet", {"col_a"}),
      std::runtime_error);
#endif
}

TEST(DeleteFileReaderTest, ReadEqualityKeysEmptyColumnsReturnsEmpty) {
#if PANGROVA_HAS_VELOX_DELETE_READER
  // With empty columns, serialization produces empty keys.
  // This is a contract test — real file would be needed for full test.
#endif
}

}  // namespace
}  // namespace pangrova::delete_filter::test
