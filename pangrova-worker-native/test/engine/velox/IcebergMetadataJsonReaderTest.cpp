#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <cstdint>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <unistd.h>

#include "TestMemoryHelper.h"
#include "engine/velox/connector/IcebergMetadataJsonReader.h"
#include "velox/type/Type.h"
#include "velox/type/Timestamp.h"
#include "velox/vector/FlatVector.h"

namespace pangrova::velox::test {
namespace {

class MetadataFile final {
 public:
  explicit MetadataFile(const std::string& contents, bool spaced = false) {
    char path[] = "/tmp/pangrova-iceberg-metadata-XXXXXX";
    const int descriptor = mkstemp(path);
    if (descriptor < 0) {
      throw std::runtime_error("Cannot create metadata test file");
    }
    close(descriptor);
    path_ = path;
    if (spaced) {
      const auto renamed = path_ + " with space";
      std::filesystem::rename(path_, renamed);
      path_ = renamed;
    }
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
      {"snapshot_id", "parent_id", "committed_at", "operation", "summary", "manifest_list"},
      {facebook::velox::BIGINT(), facebook::velox::BIGINT(),
       facebook::velox::TIMESTAMP(), facebook::velox::VARCHAR(),
       facebook::velox::MAP(facebook::velox::VARCHAR(), facebook::velox::VARCHAR()),
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
  EXPECT_EQ(first->childAt(2)->asFlatVector<facebook::velox::Timestamp>()->valueAt(0),
      facebook::velox::Timestamp::fromMillis(1000));
  auto* summary = first->childAt(4)->as<facebook::velox::MapVector>();
  ASSERT_EQ(summary->sizeAt(0), 1);
  EXPECT_EQ(summary->mapKeys()->asFlatVector<facebook::velox::StringView>()->valueAt(0).str(),
      "added-data-files");
  EXPECT_EQ(summary->mapValues()->asFlatVector<facebook::velox::StringView>()->valueAt(0).str(), "1");
  EXPECT_EQ(summary->sizeAt(1), 0);
  EXPECT_FALSE(summary->isNullAt(1));
  EXPECT_TRUE(first->childAt(5)->isNullAt(0));
  auto second = reader.next(2);
  ASSERT_NE(second, nullptr);
  ASSERT_EQ(second->size(), 1);
  EXPECT_EQ(second->childAt(0)->asFlatVector<int64_t>()->valueAt(0), 12);
  EXPECT_EQ(reader.next(2), nullptr);
}

TEST(IcebergMetadataJsonReaderTest, SnapshotSummaryAndTimestampMatchSdkBoundarySemantics) {
  auto pool = pangrova::test::testLeafPool("SnapshotSemantics");
  MetadataFile file(R"({"current-snapshot-id":3,"snapshots":[
    {"snapshot-id":1,"timestamp-ms":-1,"manifests":[]},
    {"snapshot-id":2,"timestamp-ms":0,"summary":{},"manifests":[]},
    {"snapshot-id":3,"timestamp-ms":9223372036854775,
     "manifest-list":"file:/a\\b\"中.avro","summary":{"escaped\"中":"v\\x"}}
  ]})");
  auto type = facebook::velox::ROW({"summary", "operation", "manifest_list", "committed_at"},
      {facebook::velox::MAP(facebook::velox::VARCHAR(), facebook::velox::VARCHAR()),
       facebook::velox::VARCHAR(), facebook::velox::VARCHAR(), facebook::velox::TIMESTAMP()});
  IcebergMetadataJsonReader reader(file.path(), file.size(),
      split::SystemTableKind::ICEBERG_SNAPSHOTS, type, pool.get());
  auto rows = reader.next(10);
  ASSERT_EQ(rows->size(), 3);
  EXPECT_TRUE(rows->childAt(0)->isNullAt(0));
  EXPECT_TRUE(rows->childAt(0)->isNullAt(1));
  EXPECT_TRUE(rows->childAt(1)->isNullAt(0));
  EXPECT_TRUE(rows->childAt(1)->isNullAt(1));
  EXPECT_EQ(rows->childAt(1)->asFlatVector<facebook::velox::StringView>()->valueAt(2).str(), "overwrite");
  EXPECT_EQ(rows->childAt(2)->asFlatVector<facebook::velox::StringView>()->valueAt(2).str(),
      "file:/a\\b\"中.avro");
  auto* map = rows->childAt(0)->as<facebook::velox::MapVector>();
  EXPECT_EQ(map->mapKeys()->asFlatVector<facebook::velox::StringView>()->valueAt(0).str(), "escaped\"中");
  EXPECT_EQ(map->mapValues()->asFlatVector<facebook::velox::StringView>()->valueAt(0).str(), "v\\x");
  EXPECT_EQ(rows->childAt(3)->asFlatVector<facebook::velox::Timestamp>()->valueAt(0),
      facebook::velox::Timestamp::fromMillis(-1));
  EXPECT_EQ(rows->childAt(3)->asFlatVector<facebook::velox::Timestamp>()->valueAt(2),
      facebook::velox::Timestamp::fromMillis(9223372036854775));
}

TEST(IcebergMetadataJsonReaderTest, RejectsInvalidSnapshotSummaryAndDeclaredTypes) {
  auto pool = pangrova::test::testLeafPool("InvalidSnapshotSummary");
  for (const auto* summary : {"null", "[]", "1", R"({"operation":null})",
      R"({"operation":1})", R"({"bad":null})", R"({"bad":3})",
      R"({"bad":"a","bad":"b"})"}) {
    MetadataFile file(std::string(R"({"current-snapshot-id":1,"snapshots":[
      {"snapshot-id":1,"timestamp-ms":0,"summary":)") + summary + "}]}");
    EXPECT_ANY_THROW({
      IcebergMetadataJsonReader reader(file.path(), file.size(),
          split::SystemTableKind::ICEBERG_SNAPSHOTS, facebook::velox::ROW({}, {}), pool.get());
      reader.next(1);
    }) << summary;
  }
  MetadataFile file(kMetadata);
  for (const auto& type : {
      facebook::velox::ROW({"timestamp_ms"}, {facebook::velox::BIGINT()}),
      facebook::velox::ROW({"committed_at"}, {facebook::velox::BIGINT()}),
      facebook::velox::ROW({"summary"}, {facebook::velox::VARCHAR()}),
      facebook::velox::ROW({"summary"}, {facebook::velox::MAP(facebook::velox::BIGINT(), facebook::velox::VARCHAR())})}) {
    EXPECT_THROW(IcebergMetadataJsonReader(file.path(), file.size(),
        split::SystemTableKind::ICEBERG_SNAPSHOTS, type, pool.get()), std::runtime_error);
  }
}

TEST(IcebergMetadataJsonReaderTest, SnapshotBatchesRemainBoundedWhenCallerRequestsHugeBatch) {
  auto pool = pangrova::test::testLeafPool("BoundedSnapshots");
  std::string json = R"({"current-snapshot-id":1,"snapshots":[)";
  for (int i = 0; i < 5000; ++i) {
    if (i != 0) json += ',';
    json += "{\"snapshot-id\":" + std::to_string(i + 1) + ",\"timestamp-ms\":0}";
  }
  MetadataFile file(json + "]}");
  IcebergMetadataJsonReader reader(file.path(), file.size(),
      split::SystemTableKind::ICEBERG_SNAPSHOTS,
      facebook::velox::ROW({"snapshot_id"}, {facebook::velox::BIGINT()}), pool.get());
  ASSERT_EQ(reader.next(UINT64_MAX)->size(), 4096);
  ASSERT_EQ(reader.next(UINT64_MAX)->size(), 904);
  EXPECT_EQ(reader.next(UINT64_MAX), nullptr);
}

TEST(IcebergMetadataJsonReaderTest, DecodesEscapedSpaceAndRejectsMalformedFileUri) {
  auto pool = pangrova::test::testLeafPool("MetadataEncodedUri");
  MetadataFile file(kMetadata, true);
  auto type = facebook::velox::ROW({"snapshot_id"}, {facebook::velox::BIGINT()});
  auto uri = "file:" + file.path();
  uri.replace(uri.find("with space"), 10, "with%20space");
  IcebergMetadataJsonReader reader(uri, file.size(),
      split::SystemTableKind::ICEBERG_SNAPSHOTS, type, pool.get());
  ASSERT_NE(reader.next(1), nullptr);
  uri.replace(uri.find("%20"), 3, "%2");
  EXPECT_THROW(IcebergMetadataJsonReader(uri, file.size(),
      split::SystemTableKind::ICEBERG_SNAPSHOTS, type, pool.get()),
      std::invalid_argument);
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

facebook::velox::RowTypePtr refsType() {
  return facebook::velox::ROW(
      {"name", "type", "snapshot_id", "max_reference_age_in_ms",
       "min_snapshots_to_keep", "max_snapshot_age_in_ms"},
      {facebook::velox::VARCHAR(), facebook::velox::VARCHAR(),
       facebook::velox::BIGINT(), facebook::velox::BIGINT(),
       facebook::velox::INTEGER(), facebook::velox::BIGINT()});
}

TEST(IcebergMetadataJsonReaderTest, StreamsRefsWithEscapedNamesAndNullableRetention) {
  auto pool = pangrova::test::testLeafPool("MetadataRefs");
  MetadataFile file(R"({"current-snapshot-id":12,"snapshots":[],"refs":{
    "main":{"snapshot-id":12,"type":"branch"},
    "audit\"\\\u4e2d":{"snapshot-id":11,"type":"bRaNcH",
      "max-ref-age-ms":9000000000,"min-snapshots-to-keep":3,"max-snapshot-age-ms":7000},
    "release":{"snapshot-id":10,"type":"tag","max-ref-age-ms":8000}
  }})");
  IcebergMetadataJsonReader reader(file.path(), file.size(),
      split::SystemTableKind::ICEBERG_REFS, refsType(), pool.get());
  auto first = reader.next(1);
  ASSERT_NE(first, nullptr);
  EXPECT_EQ(first->childAt(0)->asFlatVector<facebook::velox::StringView>()->valueAt(0).str(), "main");
  EXPECT_TRUE(first->childAt(3)->isNullAt(0));
  EXPECT_TRUE(first->childAt(4)->isNullAt(0));
  EXPECT_TRUE(first->childAt(5)->isNullAt(0));
  auto rest = reader.next(1);
  ASSERT_NE(rest, nullptr);
  EXPECT_EQ(rest->childAt(0)->asFlatVector<facebook::velox::StringView>()->valueAt(0).str(), "audit\"\\中");
  EXPECT_EQ(rest->childAt(1)->asFlatVector<facebook::velox::StringView>()->valueAt(0).str(), "BRANCH");
  EXPECT_EQ(rest->childAt(3)->asFlatVector<int64_t>()->valueAt(0), 9000000000);
  EXPECT_EQ(rest->childAt(4)->asFlatVector<int32_t>()->valueAt(0), 3);
  EXPECT_EQ(rest->childAt(5)->asFlatVector<int64_t>()->valueAt(0), 7000);
  auto tag = reader.next(3);
  ASSERT_NE(tag, nullptr);
  ASSERT_EQ(tag->size(), 1);
  EXPECT_EQ(tag->childAt(1)->asFlatVector<facebook::velox::StringView>()->valueAt(0).str(), "TAG");
  EXPECT_EQ(tag->childAt(2)->asFlatVector<int64_t>()->valueAt(0), 10);
  EXPECT_TRUE(tag->childAt(4)->isNullAt(0));
  EXPECT_EQ(reader.next(1), nullptr);
}

TEST(IcebergMetadataJsonReaderTest, RefsHonorAbsentVersusExplicitEmptyMap) {
  auto pool = pangrova::test::testLeafPool("LegacyMetadataRefs");
  MetadataFile legacy(kMetadata);
  IcebergMetadataJsonReader reader(legacy.path(), legacy.size(),
      split::SystemTableKind::ICEBERG_REFS, refsType(), pool.get());
  auto main = reader.next(1);
  ASSERT_NE(main, nullptr);
  ASSERT_EQ(main->size(), 1);
  EXPECT_EQ(main->childAt(2)->asFlatVector<int64_t>()->valueAt(0), 12);
  EXPECT_EQ(reader.next(1), nullptr);
  for (const auto& document : {
      R"({"current-snapshot-id":-1})",
      R"({"current-snapshot-id":12,"snapshots":[],"refs":{}})"}) {
    MetadataFile empty(document);
    IcebergMetadataJsonReader emptyReader(empty.path(), empty.size(),
        split::SystemTableKind::ICEBERG_REFS, refsType(), pool.get());
    EXPECT_EQ(emptyReader.next(10), nullptr);
  }
}

TEST(IcebergMetadataJsonReaderTest, RejectsMalformedRefsAndInvalidRetentionEvenWithCountProjection) {
  auto pool = pangrova::test::testLeafPool("InvalidMetadataRefs");
  auto countType = facebook::velox::ROW({}, {});
  for (const auto* refs : {
      "null", "[]", R"({"a":3})", R"({"a":{"type":"tag"}})",
      R"({"a":{"snapshot-id":1,"type":"bad"}})",
      R"({"a":{"snapshot-id":1,"type":"tag","min-snapshots-to-keep":1}})",
      R"({"a":{"snapshot-id":1,"type":"branch","min-snapshots-to-keep":2147483648}})",
      R"({"a":{"snapshot-id":1,"type":"branch","max-ref-age-ms":0}})",
      R"({"a":{"snapshot-id":1,"type":"branch"},})",
      R"({"a":{"snapshot-id":1,"type":"tag"},"a":{"snapshot-id":2,"type":"tag"}})"}) {
    MetadataFile file(std::string("{\"current-snapshot-id\":-1,\"refs\":") + refs + "}");
    EXPECT_ANY_THROW({
      IcebergMetadataJsonReader reader(file.path(), file.size(),
          split::SystemTableKind::ICEBERG_REFS, countType, pool.get());
      while (reader.next(1)) {}
    }) << refs;
  }
  MetadataFile file(R"({"current-snapshot-id":-1,"refs":{}})");
  auto wrong = facebook::velox::ROW({"min_snapshots_to_keep"}, {facebook::velox::BIGINT()});
  EXPECT_THROW(IcebergMetadataJsonReader(file.path(), file.size(),
      split::SystemTableKind::ICEBERG_REFS, wrong, pool.get()), std::runtime_error);
}

TEST(IcebergMetadataJsonReaderTest, RefsStreamAcrossReadChunksWithProjectedColumns) {
  auto pool = pangrova::test::testLeafPool("LargeMetadataRefs");
  std::string json = R"({"current-snapshot-id":-1,"refs":{)";
  for (int i = 0; i < 2000; ++i) {
    if (i != 0) json += ',';
    json += "\"reference_with_long_name_" + std::to_string(i) + "\":{\"snapshot-id\":" +
        std::to_string(i + 1) + ",\"type\":\"tag\"}";
  }
  MetadataFile file(json + "}}");
  auto type = facebook::velox::ROW({"snapshot_id", "name"},
      {facebook::velox::BIGINT(), facebook::velox::VARCHAR()});
  IcebergMetadataJsonReader reader(file.path(), file.size(),
      split::SystemTableKind::ICEBERG_REFS, type, pool.get());
  int count = 0;
  while (auto batch = reader.next(17)) {
    for (int i = 0; i < batch->size(); ++i) {
      EXPECT_EQ(batch->childAt(0)->asFlatVector<int64_t>()->valueAt(i), ++count);
      EXPECT_EQ(batch->childAt(1)->asFlatVector<facebook::velox::StringView>()->valueAt(i).str(),
          "reference_with_long_name_" + std::to_string(count - 1));
    }
  }
  EXPECT_EQ(count, 2000);
}

}  // namespace
}  // namespace pangrova::velox::test
