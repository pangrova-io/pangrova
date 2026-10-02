#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <cstdint>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <unistd.h>

#include "TestMemoryHelper.h"
#include "velox/connector/IcebergMetadataJsonReader.h"
#include "velox/type/Type.h"
#include "velox/vector/FlatVector.h"

namespace pangrova::velox::test {
namespace {

class MetadataFile final {
 public:
  explicit MetadataFile(const std::string& contents) {
    char path[] = "/tmp/pangrova-iceberg-metadata-XXXXXX";
    const int descriptor = mkstemp(path);
    if (descriptor < 0) {
      throw std::runtime_error("Cannot create metadata test file");
    }
    close(descriptor);
    path_ = path;
    std::ofstream output(path_, std::ios::binary | std::ios::trunc);
    output << contents;
    output.close();
    size_ = contents.size();
  }

  ~MetadataFile() { std::filesystem::remove(path_); }
  const std::string& path() const { return path_; }
  uint64_t size() const { return size_; }

 private:
  std::string path_;
  uint64_t size_ = 0;
};

constexpr auto kMetadata = R"({
  "format-version": 2,
  "current-snapshot-id": 12,
  "snapshots": [
    {"snapshot-id": 10, "timestamp-ms": 1000,
     "summary": {"operation":"append","added-data-files":"1"}},
    {"snapshot-id": 11, "parent-snapshot-id": 10, "timestamp-ms": 2000,
     "summary": {"operation":"append"}},
    {"snapshot-id": 12, "parent-snapshot-id": 10, "timestamp-ms": 3000,
     "summary": {"operation":"delete"}}
  ],
  "snapshot-log": [
    {"timestamp-ms":1000,"snapshot-id":10},
    {"timestamp-ms":2000,"snapshot-id":11},
    {"timestamp-ms":3000,"snapshot-id":12}
  ]
})";

TEST(IcebergMetadataJsonReaderTest, StreamsSnapshotsFromPinnedFile) {
  auto pool = pangrova::test::testLeafPool("MetadataSnapshots");
  MetadataFile file(kMetadata);
  auto type = facebook::velox::ROW(
      {"snapshot_id", "parent_id", "timestamp_ms", "operation", "summary"},
      {facebook::velox::BIGINT(), facebook::velox::BIGINT(),
       facebook::velox::BIGINT(), facebook::velox::VARCHAR(),
       facebook::velox::VARCHAR()});
  IcebergMetadataJsonReader reader("file:" + file.path(), file.size(),
      split::SystemTableKind::ICEBERG_SNAPSHOTS, type, pool.get());
  auto first = reader.next(2);
  ASSERT_NE(first, nullptr);
  ASSERT_EQ(first->size(), 2);
  EXPECT_EQ(first->childAt(0)->asFlatVector<int64_t>()->valueAt(0), 10);
  EXPECT_TRUE(first->childAt(1)->isNullAt(0));
  EXPECT_EQ(first->childAt(3)->asFlatVector<facebook::velox::StringView>()
                ->valueAt(0).str(), "append");
  auto second = reader.next(2);
  ASSERT_NE(second, nullptr);
  ASSERT_EQ(second->size(), 1);
  EXPECT_EQ(second->childAt(0)->asFlatVector<int64_t>()->valueAt(0), 12);
  EXPECT_EQ(reader.next(2), nullptr);
}

TEST(IcebergMetadataJsonReaderTest, ComputesCurrentSnapshotAncestry) {
  auto pool = pangrova::test::testLeafPool("MetadataHistory");
  MetadataFile file(kMetadata);
  auto type = facebook::velox::ROW(
      {"made_current_at", "snapshot_id", "parent_id", "is_current_ancestor"},
      {facebook::velox::BIGINT(), facebook::velox::BIGINT(),
       facebook::velox::BIGINT(), facebook::velox::BOOLEAN()});
  IcebergMetadataJsonReader reader(file.path(), file.size(),
      split::SystemTableKind::ICEBERG_HISTORY, type, pool.get());
  auto rows = reader.next(10);
  ASSERT_NE(rows, nullptr);
  ASSERT_EQ(rows->size(), 3);
  auto ancestors = rows->childAt(3)->asFlatVector<bool>();
  EXPECT_TRUE(ancestors->valueAt(0));
  EXPECT_FALSE(ancestors->valueAt(1));
  EXPECT_TRUE(ancestors->valueAt(2));
  EXPECT_EQ(rows->childAt(0)->asFlatVector<int64_t>()->valueAt(2), 3000);
  EXPECT_EQ(reader.next(10), nullptr);
}

TEST(IcebergMetadataJsonReaderTest, RejectsChangedPinnedObjectLength) {
  auto pool = pangrova::test::testLeafPool("MetadataLength");
  MetadataFile file(kMetadata);
  auto type = facebook::velox::ROW(
      {"snapshot_id"}, {facebook::velox::BIGINT()});
  EXPECT_THROW(IcebergMetadataJsonReader(file.path(), file.size() + 1,
      split::SystemTableKind::ICEBERG_SNAPSHOTS, type, pool.get()),
      std::runtime_error);
}

TEST(IcebergMetadataJsonReaderTest, EmptyTableWithoutSnapshotArraysReturnsNoRows) {
  auto pool = pangrova::test::testLeafPool("EmptyMetadata");
  MetadataFile file(R"({"format-version":2,"current-snapshot-id":-1})");
  auto type = facebook::velox::ROW(
      {"snapshot_id"}, {facebook::velox::BIGINT()});
  IcebergMetadataJsonReader reader(file.path(), file.size(),
      split::SystemTableKind::ICEBERG_SNAPSHOTS, type, pool.get());
  EXPECT_EQ(reader.next(10), nullptr);
}

}  // namespace
}  // namespace pangrova::velox::test
