#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <openssl/evp.h>

#include "TestMemoryHelper.h"
#include "velox/connector/PaimonSnapshotJsonReader.h"
#include "velox/type/Type.h"
#include "velox/vector/FlatVector.h"

namespace pangrova::velox::test {
namespace {

class SnapshotFile final {
 public:
  explicit SnapshotFile(const std::string& contents) {
    char path[] = "/tmp/pangrova-paimon-snapshot-XXXXXX";
    const int descriptor = mkstemp(path);
    if (descriptor < 0) {
      throw std::runtime_error("Cannot create Paimon snapshot test file");
    }
    close(descriptor);
    path_ = path;
    std::ofstream output(path_, std::ios::binary | std::ios::trunc);
    output << contents;
    output.close();
    size_ = contents.size();
  }

  ~SnapshotFile() { std::filesystem::remove(path_); }
  const std::string& path() const { return path_; }
  uint64_t size() const { return size_; }

 private:
  std::string path_;
  uint64_t size_ = 0;
};

constexpr auto kSnapshot = R"({"version":3,"id":7,"schemaId":2,
  "commitUser":"writer","commitIdentifier":9,"commitKind":"APPEND",
  "timeMillis":1700000000000,"baseManifestList":"manifest-list-base",
  "deltaManifestList":"manifest-list-delta","changelogManifestList":null,
  "totalRecordCount":3,"deltaRecordCount":2,
  "changelogRecordCount":null,"watermark":null,"nextRowId":null})";

std::string digest(const std::string& bytes) {
  std::string output(32, '\0');
  unsigned int size = 0;
  if (EVP_Digest(bytes.data(), bytes.size(),
          reinterpret_cast<unsigned char*>(output.data()), &size,
          EVP_sha256(), nullptr) != 1 || size != 32) {
    throw std::runtime_error("Cannot hash test snapshot");
  }
  return output;
}

TEST(PaimonSnapshotJsonReaderTest, ReadsPinnedSnapshotAndProjectedColumns) {
  auto pool = pangrova::test::testLeafPool("PaimonSnapshot");
  SnapshotFile file(kSnapshot);
  auto type = facebook::velox::ROW(
      {"snapshot_id", "schema_id", "commit_kind", "commit_time",
       "total_record_count"},
      {facebook::velox::BIGINT(), facebook::velox::BIGINT(),
       facebook::velox::VARCHAR(), facebook::velox::TIMESTAMP(),
       facebook::velox::BIGINT()});
  PaimonSnapshotJsonReader reader("file:" + file.path(), file.size(),
      7, digest(kSnapshot), type, pool.get());
  auto row = reader.next(1);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(row->size(), 1);
  EXPECT_EQ(row->childAt(0)->asFlatVector<int64_t>()->valueAt(0), 7);
  EXPECT_EQ(row->childAt(1)->asFlatVector<int64_t>()->valueAt(0), 2);
  EXPECT_EQ(row->childAt(2)->asFlatVector<facebook::velox::StringView>()
                ->valueAt(0).str(), "APPEND");
  EXPECT_EQ(row->childAt(3)->asFlatVector<facebook::velox::Timestamp>()
                ->valueAt(0).toMillis(), 1700000000000);
  EXPECT_EQ(row->childAt(4)->asFlatVector<int64_t>()->valueAt(0), 3);
  EXPECT_EQ(reader.next(1), nullptr);
}

TEST(PaimonSnapshotJsonReaderTest, ExposesFixedPaimon141SchemaAndNulls) {
  auto pool = pangrova::test::testLeafPool("PaimonSnapshotFullSchema");
  SnapshotFile file(kSnapshot);
  auto type = facebook::velox::ROW(
      {"snapshot_id", "schema_id", "commit_user", "commit_identifier",
       "commit_kind", "commit_time", "base_manifest_list",
       "delta_manifest_list", "changelog_manifest_list", "total_record_count",
       "delta_record_count", "changelog_record_count", "watermark", "next_row_id"},
      {facebook::velox::BIGINT(), facebook::velox::BIGINT(),
       facebook::velox::VARCHAR(), facebook::velox::BIGINT(),
       facebook::velox::VARCHAR(), facebook::velox::TIMESTAMP(),
       facebook::velox::VARCHAR(), facebook::velox::VARCHAR(),
       facebook::velox::VARCHAR(), facebook::velox::BIGINT(),
       facebook::velox::BIGINT(), facebook::velox::BIGINT(),
       facebook::velox::BIGINT(), facebook::velox::BIGINT()});
  PaimonSnapshotJsonReader reader(file.path(), file.size(),
      7, digest(kSnapshot), type, pool.get());
  auto row = reader.next(1);
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(row->childrenSize(), 14);
  EXPECT_EQ(row->childAt(2)->asFlatVector<facebook::velox::StringView>()
                ->valueAt(0).str(), "writer");
  EXPECT_EQ(row->childAt(3)->asFlatVector<int64_t>()->valueAt(0), 9);
  EXPECT_EQ(row->childAt(6)->asFlatVector<facebook::velox::StringView>()
                ->valueAt(0).str(), "manifest-list-base");
  EXPECT_EQ(row->childAt(10)->asFlatVector<int64_t>()->valueAt(0), 2);
  for (int index : {8, 11, 12, 13}) {
    EXPECT_TRUE(row->childAt(index)->isNullAt(0));
  }
}

TEST(PaimonSnapshotJsonReaderTest, RejectsObjectLengthAndSnapshotIdDrift) {
  auto pool = pangrova::test::testLeafPool("PaimonSnapshotDrift");
  SnapshotFile file(kSnapshot);
  auto type = facebook::velox::ROW(
      {"snapshot_id"}, {facebook::velox::BIGINT()});
  EXPECT_THROW(PaimonSnapshotJsonReader(file.path(), file.size() + 1,
      7, digest(kSnapshot), type, pool.get()), std::runtime_error);
  PaimonSnapshotJsonReader reader(file.path(), file.size(),
      8, digest(kSnapshot), type, pool.get());
  EXPECT_THROW(reader.next(1), std::runtime_error);
}

TEST(PaimonSnapshotJsonReaderTest, RejectsSameLengthContentReplacement) {
  auto pool = pangrova::test::testLeafPool("PaimonSnapshotChecksum");
  SnapshotFile file(kSnapshot);
  auto type = facebook::velox::ROW(
      {"snapshot_id"}, {facebook::velox::BIGINT()});
  PaimonSnapshotJsonReader reader(file.path(), file.size(),
      7, digest(std::string(kSnapshot) + "different"), type, pool.get());
  EXPECT_THROW(reader.next(1), std::runtime_error);
}

TEST(PaimonSnapshotJsonReaderTest, ReadsTagFileWithNullableTtlFields) {
  auto pool = pangrova::test::testLeafPool("PaimonTag");
  SnapshotFile file(kSnapshot);
  auto type = facebook::velox::ROW(
      {"tag_name", "snapshot_id", "schema_id", "commit_time",
       "record_count", "create_time", "time_retained"},
      {facebook::velox::VARCHAR(), facebook::velox::BIGINT(),
       facebook::velox::BIGINT(), facebook::velox::TIMESTAMP(),
       facebook::velox::BIGINT(), facebook::velox::TIMESTAMP(),
       facebook::velox::VARCHAR()});
  PaimonSnapshotJsonReader reader(file.path(), file.size(),
      0, digest(kSnapshot), type, pool.get(), "v1");
  auto row = reader.next(1);
  ASSERT_NE(row, nullptr);
  EXPECT_EQ(row->childAt(0)->asFlatVector<facebook::velox::StringView>()
                ->valueAt(0).str(), "v1");
  EXPECT_EQ(row->childAt(1)->asFlatVector<int64_t>()->valueAt(0), 7);
  EXPECT_EQ(row->childAt(3)->asFlatVector<facebook::velox::Timestamp>()
                ->valueAt(0).toMillis(), 1700000000000);
  EXPECT_TRUE(row->childAt(5)->isNullAt(0));
  EXPECT_TRUE(row->childAt(6)->isNullAt(0));
}

TEST(PaimonSnapshotJsonReaderTest, ReadsTagFileWithTtlFields) {
  constexpr auto tagJson = R"({
    "id" : 7,
    "schemaId" : 2,
    "timeMillis" : 1700000000000,
    "totalRecordCount" : 3,
    "tagCreateTime" : [ 2023, 11, 14, 22, 13, 20, 123000000 ],
    "tagTimeRetained" : 172800.000000000
  })";
  auto pool = pangrova::test::testLeafPool("PaimonTagTtl");
  SnapshotFile file(tagJson);
  auto type = facebook::velox::ROW(
      {"create_time", "time_retained"},
      {facebook::velox::TIMESTAMP(), facebook::velox::VARCHAR()});
  PaimonSnapshotJsonReader reader(file.path(), file.size(),
      0, digest(tagJson), type, pool.get(), "v2");
  auto row = reader.next(1);
  ASSERT_NE(row, nullptr);
  EXPECT_EQ(row->childAt(0)->asFlatVector<facebook::velox::Timestamp>()
                ->valueAt(0).toMillis(), 1700000000123);
  EXPECT_EQ(row->childAt(1)->asFlatVector<facebook::velox::StringView>()
                ->valueAt(0).str(), "PT48H");
}

TEST(PaimonSnapshotJsonReaderTest, ReadsFractionalTagRetention) {
  constexpr auto tagJson = R"({"id":7,"schemaId":2,"timeMillis":1700000000000,
    "tagTimeRetained":3661.125000000})";
  auto pool = pangrova::test::testLeafPool("PaimonTagFractionalTtl");
  SnapshotFile file(tagJson);
  auto type = facebook::velox::ROW(
      {"time_retained"}, {facebook::velox::VARCHAR()});
  PaimonSnapshotJsonReader reader(file.path(), file.size(),
      0, digest(tagJson), type, pool.get(), "v3");
  auto row = reader.next(1);
  ASSERT_NE(row, nullptr);
  EXPECT_EQ(row->childAt(0)->asFlatVector<facebook::velox::StringView>()
                ->valueAt(0).str(), "PT1H1M1.125S");
}

TEST(PaimonSnapshotJsonReaderTest, ReadsNegativeTagRetentionLikePaimonSdk) {
  constexpr auto tagJson = R"({"id":7,"schemaId":2,"timeMillis":1700000000000,
    "tagTimeRetained":-3600.500000000})";
  auto pool = pangrova::test::testLeafPool("PaimonTagNegativeTtl");
  SnapshotFile file(tagJson);
  auto type = facebook::velox::ROW(
      {"time_retained"}, {facebook::velox::VARCHAR()});
  PaimonSnapshotJsonReader reader(file.path(), file.size(),
      0, digest(tagJson), type, pool.get(), "negative");
  auto row = reader.next(1);
  ASSERT_NE(row, nullptr);
  EXPECT_EQ(row->childAt(0)->asFlatVector<facebook::velox::StringView>()
                ->valueAt(0).str(), "PT-1H-0.5S");
}

TEST(PaimonSnapshotJsonReaderTest, ReadsPinnedSchemaOptionsInBoundedBatches) {
  constexpr auto schemaJson = R"({"id":0,"fields":[],
    "options":{"scan.mode":"latest","bucket":"1"}})";
  auto pool = pangrova::test::testLeafPool("PaimonOptions");
  SnapshotFile file(schemaJson);
  auto type = facebook::velox::ROW(
      {"key", "value"}, {facebook::velox::VARCHAR(), facebook::velox::VARCHAR()});
  PaimonSnapshotJsonReader reader(file.path(), file.size(),
      0, digest(schemaJson), type, pool.get(), "", 0);
  auto first = reader.next(1);
  ASSERT_NE(first, nullptr);
  ASSERT_EQ(first->size(), 1);
  EXPECT_EQ(first->childAt(0)->asFlatVector<facebook::velox::StringView>()
                ->valueAt(0).str(), "bucket");
  EXPECT_EQ(first->childAt(1)->asFlatVector<facebook::velox::StringView>()
                ->valueAt(0).str(), "1");
  auto second = reader.next(1);
  ASSERT_NE(second, nullptr);
  ASSERT_EQ(second->size(), 1);
  EXPECT_EQ(second->childAt(0)->asFlatVector<facebook::velox::StringView>()
                ->valueAt(0).str(), "scan.mode");
  EXPECT_EQ(reader.next(1), nullptr);
  PaimonSnapshotJsonReader wrong(file.path(), file.size(),
      0, digest(schemaJson), type, pool.get(), "", 1);
  EXPECT_THROW(wrong.next(1), std::runtime_error);
}

}  // namespace
}  // namespace pangrova::velox::test
