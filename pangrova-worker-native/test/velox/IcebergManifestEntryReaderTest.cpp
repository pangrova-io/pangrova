#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include <unistd.h>

#include "openssl/evp.h"

#include "TestMemoryHelper.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connector/IcebergManifestEntryReader.h"
#include "velox/connector/IcebergManifestListReader.h"
#include "velox/type/Type.h"
#include "velox/vector/FlatVector.h"

namespace pangrova::velox::test {
namespace {

constexpr auto kListSchema = R"({"type":"record","name":"manifest_file","fields":[)"
    R"({"name":"manifest_path","type":"string"},)"
    R"({"name":"manifest_length","type":"long"},)"
    R"({"name":"partition_spec_id","type":"int"},)"
    R"({"name":"content","type":"int"}]})";
constexpr auto kChildSchema = R"({"type":"record","name":"manifest_entry","fields":[)"
    R"({"name":"status","type":"int"},)"
    R"({"name":"data_file","type":{"type":"record","name":"data_file","fields":[)"
    R"({"name":"content","type":"int"},)"
    R"({"name":"file_path","type":"string"},)"
    R"({"name":"file_format","type":"string"},)"
    R"({"name":"spec_id","type":["null","int"]},)"
    R"({"name":"record_count","type":"long"},)"
    R"({"name":"file_size_in_bytes","type":"long"},)"
    R"({"name":"split_offsets","type":["null",{"type":"array","items":"long"}]},)"
    R"({"name":"equality_ids","type":["null",{"type":"array","items":"int"}]},)"
    R"({"name":"sort_order_id","type":["null","int"]}]}}]})";
constexpr auto kChildMapSchema = R"({"type":"record","name":"manifest_entry","fields":[)"
    R"({"name":"status","type":"int"},)"
    R"({"name":"data_file","field-id":2,"type":{"type":"record","name":"data_file","fields":[)"
    R"({"name":"content","type":"int"},)"
    R"({"name":"file_path","type":"string"},)"
    R"({"name":"file_format","type":"string"},)"
    R"({"name":"spec_id","type":["null","int"]},)"
    R"({"name":"record_count","type":"long"},)"
    R"({"name":"file_size_in_bytes","type":"long"},)"
    R"({"name":"column_sizes","field-id":108,"type":["null",)"
    R"({"type":"array","logicalType":"map","items":)"
    R"({"type":"record","name":"k117_v118","fields":[)"
    R"({"name":"key","type":"int","field-id":117},)"
    R"({"name":"value","type":"long","field-id":118}]}}]},)"
    R"({"name":"split_offsets","type":["null",{"type":"array","items":"long"}]},)"
    R"({"name":"equality_ids","type":["null",{"type":"array","items":"int"}]},)"
    R"({"name":"sort_order_id","type":["null","int"]}]}}]})";

void appendLong(std::string& bytes, int64_t value) {
  auto encoded = (static_cast<uint64_t>(value) << 1) ^
      static_cast<uint64_t>(value >> 63);
  while ((encoded & ~uint64_t{0x7f}) != 0) {
    bytes.push_back(static_cast<char>((encoded & 0x7f) | 0x80));
    encoded >>= 7;
  }
  bytes.push_back(static_cast<char>(encoded));
}

void appendBytes(std::string& bytes, const std::string& value) {
  appendLong(bytes, value.size());
  bytes += value;
}

void appendMap(std::string& bytes,
    const std::vector<std::pair<std::string, std::string>>& entries) {
  appendLong(bytes, entries.size());
  for (const auto& [key, value] : entries) {
    appendBytes(bytes, key);
    appendBytes(bytes, value);
  }
  appendLong(bytes, 0);
}

std::string container(const std::string& schema,
    const std::vector<std::pair<std::string, std::string>>& metadata,
    int64_t rowCount, const std::string& rows) {
  std::string bytes("Obj\x01", 4);
  std::vector<std::pair<std::string, std::string>> header = metadata;
  header.emplace_back("avro.schema", schema);
  header.emplace_back("avro.codec", "null");
  appendMap(bytes, header);
  const std::string sync(16, 'f');
  bytes += sync;
  appendLong(bytes, rowCount);
  appendLong(bytes, rows.size());
  bytes += rows;
  bytes += sync;
  return bytes;
}

std::string digest(const std::string& bytes) {
  std::array<unsigned char, 32> hash{};
  unsigned int length = 0;
  if (EVP_Digest(bytes.data(), bytes.size(), hash.data(), &length,
          EVP_sha256(), nullptr) != 1 || length != hash.size()) {
    throw std::runtime_error("Cannot hash Iceberg files fixture");
  }
  return std::string(reinterpret_cast<const char*>(hash.data()), hash.size());
}

class AvroFile final {
 public:
  explicit AvroFile(const std::string& bytes, bool spaced = false) {
    char name[] = "/tmp/pangrova-files-manifest-XXXXXX";
    const auto descriptor = mkstemp(name);
    if (descriptor < 0) throw std::runtime_error("Cannot create Avro fixture");
    close(descriptor);
    path_ = name;
    if (spaced) {
      const auto renamed = path_ + " with space";
      std::filesystem::rename(path_, renamed);
      path_ = renamed;
    }
    std::ofstream stream(path_, std::ios::binary | std::ios::trunc);
    stream.write(bytes.data(), bytes.size());
  }
  ~AvroFile() { std::filesystem::remove(path_); }
  const std::string& path() const { return path_; }

 private:
  std::string path_;
};

std::string encodedLocalUri(const AvroFile& file) {
  std::string uri = "file:" + file.path();
  for (size_t offset = 0; (offset = uri.find(' ', offset)) != std::string::npos;
      offset += 3) {
    uri.replace(offset, 1, "%20");
  }
  return uri;
}

template <typename T>
void appendOptionalArray(std::string& row,
    const std::optional<std::vector<T>>& values) {
  if (!values) {
    appendLong(row, 0);
    return;
  }
  appendLong(row, 1);
  if (!values->empty()) {
    appendLong(row, values->size());
    for (const auto value : *values) appendLong(row, value);
  }
  appendLong(row, 0);
}

std::string childRow(int32_t status, const std::string& path, int32_t content = 0,
    std::optional<std::vector<int64_t>> offsets = std::nullopt,
    std::optional<std::vector<int32_t>> equalityIds = std::nullopt,
    std::optional<int32_t> sortOrderId = std::nullopt) {
  std::string row;
  appendLong(row, status);
  appendLong(row, content);
  appendBytes(row, path);
  appendBytes(row, "PARQUET");
  appendLong(row, 0);  // null spec ID inherited from manifest
  appendLong(row, 3);
  appendLong(row, 4096);
  appendOptionalArray(row, offsets);
  appendOptionalArray(row, equalityIds);
  if (sortOrderId) {
    appendLong(row, 1);
    appendLong(row, *sortOrderId);
  } else {
    appendLong(row, 0);
  }
  return row;
}

using MetricMap = std::vector<std::pair<int32_t, int64_t>>;

std::string childMapRow(const std::string& path,
    const std::optional<MetricMap>& columnSizes) {
  std::string row;
  appendLong(row, 1);  // added entry
  appendLong(row, 0);  // data content
  appendBytes(row, path);
  appendBytes(row, "PARQUET");
  appendLong(row, 0);  // null spec ID inherited from manifest
  appendLong(row, 3);
  appendLong(row, 4096);
  appendLong(row, columnSizes ? 1 : 0);
  if (columnSizes) {
    if (!columnSizes->empty()) {
      appendLong(row, columnSizes->size());
      for (const auto& [key, value] : *columnSizes) {
        appendLong(row, key);
        appendLong(row, value);
      }
    }
    appendLong(row, 0);
  }
  appendLong(row, 0);  // null split offsets
  appendLong(row, 0);  // null equality IDs
  appendLong(row, 0);  // null sort order ID
  return row;
}

std::string listRow(const AvroFile& child, uint64_t length) {
  std::string row;
  appendBytes(row, child.path());
  appendLong(row, length);
  appendLong(row, 0);
  appendLong(row, 0);
  return row;
}

std::string listRow(const std::string& childUri, uint64_t length,
    int32_t content = 0) {
  std::string row;
  appendBytes(row, childUri);
  appendLong(row, length);
  appendLong(row, 0);
  appendLong(row, content);
  return row;
}

auto projectedType() {
  return facebook::velox::ROW(
      {"content", "file_path", "file_format", "spec_id",
       "record_count", "file_size_in_bytes"},
      {facebook::velox::INTEGER(), facebook::velox::VARCHAR(),
       facebook::velox::VARCHAR(), facebook::velox::INTEGER(),
       facebook::velox::BIGINT(), facebook::velox::BIGINT()});
}

auto arrayProjectionType() {
  return facebook::velox::ROW(
      {"file_path", "split_offsets", "equality_ids", "sort_order_id"},
      {facebook::velox::VARCHAR(),
       facebook::velox::ARRAY(facebook::velox::BIGINT()),
       facebook::velox::ARRAY(facebook::velox::INTEGER()),
       facebook::velox::INTEGER()});
}

auto mapProjectionType() {
  return facebook::velox::ROW(
      {"file_path", "column_sizes"},
      {facebook::velox::VARCHAR(),
       facebook::velox::MAP(facebook::velox::INTEGER(), facebook::velox::BIGINT())});
}

TEST(IcebergManifestEntryReaderTest, PreservesNullEmptyAndPopulatedColumnSizes) {
  facebook::velox::filesystems::registerLocalFileSystem();
  auto pool = pangrova::test::testLeafPool("IcebergFilesMap");
  const auto childBytes = container(kChildMapSchema,
      {{"format-version", "2"}, {"partition-spec-id", "0"}, {"content", "data"}},
      3, childMapRow("file:/data/null.parquet", std::nullopt) +
          childMapRow("file:/data/empty.parquet", MetricMap{}) +
          childMapRow("file:/data/values.parquet", MetricMap{{1, 11}, {2, 22}}));
  AvroFile child(childBytes);
  const auto listBytes = container(kListSchema,
      {{"format-version", "2"}, {"snapshot-id", "7"}, {"sequence-number", "1"}},
      1, listRow(child, childBytes.size()));
  AvroFile list(listBytes);
  IcebergManifestEntryReader reader(child.path(), childBytes.size(), digest(childBytes),
      list.path(), listBytes.size(), digest(listBytes), 7, 1, 2, 0, 0,
      mapProjectionType(), pool.get());
  auto batch = reader.next(10);
  ASSERT_NE(batch, nullptr);
  ASSERT_EQ(batch->size(), 3);
  auto* maps = batch->childAt(1)->as<facebook::velox::MapVector>();
  ASSERT_NE(maps, nullptr);
  EXPECT_TRUE(maps->isNullAt(0));
  EXPECT_FALSE(maps->isNullAt(1));
  EXPECT_EQ(maps->sizeAt(1), 0);
  EXPECT_FALSE(maps->isNullAt(2));
  EXPECT_EQ(maps->sizeAt(2), 2);
  EXPECT_EQ(maps->mapKeys()->asFlatVector<int32_t>()->valueAt(maps->offsetAt(2)), 1);
  EXPECT_EQ(maps->mapValues()->asFlatVector<int64_t>()->valueAt(maps->offsetAt(2)), 11);
  EXPECT_EQ(maps->mapKeys()->asFlatVector<int32_t>()->valueAt(maps->offsetAt(2) + 1), 2);
  EXPECT_EQ(maps->mapValues()->asFlatVector<int64_t>()->valueAt(maps->offsetAt(2) + 1), 22);
}

TEST(IcebergManifestEntryReaderTest, RejectsMalformedColumnSizes) {
  facebook::velox::filesystems::registerLocalFileSystem();
  auto pool = pangrova::test::testLeafPool("IcebergFilesMapReject");
  for (const auto& metrics : {MetricMap{{1, 11}, {1, 22}}, MetricMap{{-1, 11}},
                               MetricMap{{1, -1}}}) {
    const auto childBytes = container(kChildMapSchema,
        {{"format-version", "2"}, {"partition-spec-id", "0"}, {"content", "data"}},
        1, childMapRow("file:/data/invalid.parquet", metrics));
    AvroFile child(childBytes);
    const auto listBytes = container(kListSchema,
        {{"format-version", "2"}, {"snapshot-id", "7"}, {"sequence-number", "1"}},
        1, listRow(child, childBytes.size()));
    AvroFile list(listBytes);
    IcebergManifestEntryReader reader(child.path(), childBytes.size(), digest(childBytes),
        list.path(), listBytes.size(), digest(listBytes), 7, 1, 2, 0, 0,
        mapProjectionType(), pool.get());
    EXPECT_THROW(reader.next(10), std::runtime_error);
  }
  std::string wrongSchema = kChildMapSchema;
  auto id = wrongSchema.find("\"field-id\":117");
  ASSERT_NE(id, std::string::npos);
  wrongSchema.replace(id, std::string("\"field-id\":117").size(), "\"field-id\":999");
  const auto childBytes = container(wrongSchema,
      {{"format-version", "2"}, {"partition-spec-id", "0"}, {"content", "data"}},
      1, childMapRow("file:/data/invalid.parquet", MetricMap{{1, 11}}));
  AvroFile child(childBytes);
  const auto listBytes = container(kListSchema,
      {{"format-version", "2"}, {"snapshot-id", "7"}, {"sequence-number", "1"}},
      1, listRow(child, childBytes.size()));
  AvroFile list(listBytes);
  EXPECT_THROW(IcebergManifestEntryReader(child.path(), childBytes.size(), digest(childBytes),
      list.path(), listBytes.size(), digest(listBytes), 7, 1, 2, 0, 0,
      mapProjectionType(), pool.get()), std::runtime_error);
}

TEST(IcebergManifestEntryReaderTest, PreservesNullEmptyAndPopulatedArrays) {
  facebook::velox::filesystems::registerLocalFileSystem();
  auto pool = pangrova::test::testLeafPool("IcebergFilesArrays");
  const auto childBytes = container(kChildSchema,
      {{"format-version", "2"}, {"partition-spec-id", "0"}, {"content", "data"}},
      3, childRow(1, "file:/data/one.parquet", 0, std::vector<int64_t>{4, 40},
             std::nullopt, 0) +
          childRow(1, "file:/data/two.parquet", 0, std::vector<int64_t>{}) +
          childRow(1, "file:/data/three.parquet"));
  AvroFile child(childBytes);
  const auto listBytes = container(kListSchema,
      {{"format-version", "2"}, {"snapshot-id", "7"}, {"sequence-number", "1"}},
      1, listRow(child, childBytes.size()));
  AvroFile list(listBytes);
  IcebergManifestEntryReader reader(child.path(), childBytes.size(), digest(childBytes),
      list.path(), listBytes.size(), digest(listBytes), 7, 1, 2, 0, 0,
      arrayProjectionType(), pool.get());
  auto batch = reader.next(10);
  ASSERT_NE(batch, nullptr);
  ASSERT_EQ(batch->size(), 3);
  auto* offsets = batch->childAt(1)->as<facebook::velox::ArrayVector>();
  ASSERT_NE(offsets, nullptr);
  ASSERT_EQ(offsets->sizeAt(0), 2);
  EXPECT_EQ(offsets->elements()->asFlatVector<int64_t>()->valueAt(
      offsets->offsetAt(0)), 4);
  EXPECT_EQ(offsets->elements()->asFlatVector<int64_t>()->valueAt(
      offsets->offsetAt(0) + 1), 40);
  EXPECT_EQ(offsets->sizeAt(1), 0);
  EXPECT_TRUE(offsets->isNullAt(2));
  auto* equality = batch->childAt(2)->as<facebook::velox::ArrayVector>();
  ASSERT_NE(equality, nullptr);
  EXPECT_TRUE(equality->isNullAt(0));
  EXPECT_TRUE(equality->isNullAt(1));
  EXPECT_TRUE(equality->isNullAt(2));
  auto* sortOrder = batch->childAt(3)->asFlatVector<int32_t>();
  ASSERT_NE(sortOrder, nullptr);
  EXPECT_EQ(sortOrder->valueAt(0), 0);
  EXPECT_TRUE(sortOrder->isNullAt(1));
  EXPECT_TRUE(sortOrder->isNullAt(2));
}

TEST(IcebergManifestEntryReaderTest, ReadsLiveScalarProjectionAndSkipsDeleted) {
  facebook::velox::filesystems::registerLocalFileSystem();
  auto pool = pangrova::test::testLeafPool("IcebergFilesLive");
  const auto childBytes = container(kChildSchema,
      {{"format-version", "2"}, {"partition-spec-id", "0"}, {"content", "data"}},
      2, childRow(1, "file:/data/live.parquet") +
          childRow(2, "file:/data/deleted.parquet"));
  AvroFile child(childBytes);
  const auto listBytes = container(kListSchema,
      {{"format-version", "2"}, {"snapshot-id", "7"}, {"sequence-number", "1"}},
      1, listRow(child, childBytes.size()));
  AvroFile list(listBytes);
  IcebergManifestEntryReader reader(child.path(), childBytes.size(), digest(childBytes),
      list.path(), listBytes.size(), digest(listBytes), 7, 1, 2, 0, 0,
      projectedType(), pool.get());
  auto batch = reader.next(10);
  ASSERT_NE(batch, nullptr);
  ASSERT_EQ(batch->size(), 1);
  EXPECT_EQ(batch->childAt(0)->asFlatVector<int32_t>()->valueAt(0), 0);
  EXPECT_EQ(batch->childAt(1)->asFlatVector<facebook::velox::StringView>()
                ->valueAt(0).str(), "file:/data/live.parquet");
  EXPECT_EQ(batch->childAt(2)->asFlatVector<facebook::velox::StringView>()
                ->valueAt(0).str(), "PARQUET");
  EXPECT_EQ(batch->childAt(3)->asFlatVector<int32_t>()->valueAt(0), 0);
  EXPECT_EQ(batch->childAt(4)->asFlatVector<int64_t>()->valueAt(0), 3);
  EXPECT_EQ(batch->childAt(5)->asFlatVector<int64_t>()->valueAt(0), 4096);
  EXPECT_EQ(reader.next(10), nullptr);
}

TEST(IcebergManifestEntryReaderTest, RejectsParentOrChildMutation) {
  facebook::velox::filesystems::registerLocalFileSystem();
  auto pool = pangrova::test::testLeafPool("IcebergFilesIdentity");
  const auto childBytes = container(kChildSchema,
      {{"format-version", "2"}, {"partition-spec-id", "0"}, {"content", "data"}},
      1, childRow(1, "file:/data/live.parquet"));
  AvroFile child(childBytes);
  const auto listBytes = container(kListSchema,
      {{"format-version", "2"}, {"snapshot-id", "7"}, {"sequence-number", "1"}},
      1, listRow(child, childBytes.size()));
  AvroFile list(listBytes);
  EXPECT_THROW(IcebergManifestEntryReader(child.path(), childBytes.size(),
      digest(childBytes + "changed"), list.path(), listBytes.size(), digest(listBytes),
      7, 1, 2, 0, 0, projectedType(), pool.get()), std::runtime_error);
  EXPECT_THROW(IcebergManifestEntryReader(child.path(), childBytes.size(),
      digest(childBytes), list.path(), listBytes.size(), digest(listBytes + "changed"),
      7, 1, 2, 0, 0, projectedType(), pool.get()), std::runtime_error);
}

TEST(IcebergManifestEntryReaderTest, RejectsUnsupportedNestedProjection) {
  facebook::velox::filesystems::registerLocalFileSystem();
  auto pool = pangrova::test::testLeafPool("IcebergFilesUnsupported");
  const auto childBytes = container(kChildSchema,
      {{"format-version", "2"}, {"partition-spec-id", "0"}, {"content", "data"}},
      1, childRow(1, "file:/data/live.parquet"));
  AvroFile child(childBytes);
  const auto listBytes = container(kListSchema,
      {{"format-version", "2"}, {"snapshot-id", "7"}, {"sequence-number", "1"}},
      1, listRow(child, childBytes.size()));
  AvroFile list(listBytes);
  auto type = facebook::velox::ROW({"column_sizes"},
      {facebook::velox::MAP(facebook::velox::INTEGER(), facebook::velox::BIGINT())});
  EXPECT_THROW(IcebergManifestEntryReader(child.path(), childBytes.size(),
      digest(childBytes), list.path(), listBytes.size(), digest(listBytes),
      7, 1, 2, 0, 0, type, pool.get()), std::runtime_error);
  auto wrongElementType = facebook::velox::ROW({"split_offsets"},
      {facebook::velox::ARRAY(facebook::velox::INTEGER())});
  EXPECT_THROW(IcebergManifestEntryReader(child.path(), childBytes.size(),
      digest(childBytes), list.path(), listBytes.size(), digest(listBytes),
      7, 1, 2, 0, 0, wrongElementType, pool.get()), std::runtime_error);
}

TEST(IcebergManifestEntryReaderTest, ReadsPositionDeleteManifest) {
  facebook::velox::filesystems::registerLocalFileSystem();
  auto pool = pangrova::test::testLeafPool("IcebergFilesDelete");
  const auto childBytes = container(kChildSchema,
      {{"format-version", "2"}, {"partition-spec-id", "0"}, {"content", "deletes"}},
      2, childRow(1, "file:/data/position-delete.parquet", 1) +
          childRow(1, "file:/data/equality-delete.parquet", 2,
              std::nullopt, std::vector<int32_t>{1}));
  AvroFile child(childBytes);
  const auto listBytes = container(kListSchema,
      {{"format-version", "2"}, {"snapshot-id", "7"}, {"sequence-number", "1"}},
      1, listRow(child.path(), childBytes.size(), 1));
  AvroFile list(listBytes);
  IcebergManifestEntryReader reader(child.path(), childBytes.size(), digest(childBytes),
      list.path(), listBytes.size(), digest(listBytes), 7, 1, 2, 0, 1,
      facebook::velox::ROW({"content", "file_path", "equality_ids"},
          {facebook::velox::INTEGER(), facebook::velox::VARCHAR(),
           facebook::velox::ARRAY(facebook::velox::INTEGER())}), pool.get());
  auto batch = reader.next(2);
  ASSERT_NE(batch, nullptr);
  ASSERT_EQ(batch->size(), 2);
  EXPECT_EQ(batch->childAt(0)->asFlatVector<int32_t>()->valueAt(0), 1);
  EXPECT_EQ(batch->childAt(0)->asFlatVector<int32_t>()->valueAt(1), 2);
  EXPECT_EQ(batch->childAt(1)->asFlatVector<facebook::velox::StringView>()
                ->valueAt(0).str(), "file:/data/position-delete.parquet");
  auto* equality = batch->childAt(2)->as<facebook::velox::ArrayVector>();
  ASSERT_NE(equality, nullptr);
  EXPECT_TRUE(equality->isNullAt(0));
  ASSERT_EQ(equality->sizeAt(1), 1);
  EXPECT_EQ(equality->elements()->asFlatVector<int32_t>()->valueAt(
      equality->offsetAt(1)), 1);
  EXPECT_EQ(reader.next(2), nullptr);
}

TEST(IcebergManifestEntryReaderTest, BothReadersOpenEncodedLocalUriWithSpaces) {
  facebook::velox::filesystems::registerLocalFileSystem();
  auto pool = pangrova::test::testLeafPool("IcebergFilesEncodedUri");
  const auto childBytes = container(kChildSchema,
      {{"format-version", "2"}, {"partition-spec-id", "0"}, {"content", "data"}},
      1, childRow(1, "file:/data/live.parquet"));
  AvroFile child(childBytes, true);
  const auto childUri = encodedLocalUri(child);
  const auto listBytes = container(kListSchema,
      {{"format-version", "2"}, {"snapshot-id", "7"}, {"sequence-number", "1"}},
      1, listRow(childUri, childBytes.size()));
  AvroFile list(listBytes, true);
  const auto listUri = encodedLocalUri(list);
  auto emptyType = facebook::velox::ROW({}, {});
  EXPECT_NO_THROW(IcebergManifestListReader(listUri, listBytes.size(), 7, 1, 2,
      digest(listBytes), {}, emptyType, pool.get()));
  IcebergManifestEntryReader reader(childUri, childBytes.size(), digest(childBytes),
      listUri, listBytes.size(), digest(listBytes), 7, 1, 2, 0, 0,
      projectedType(), pool.get());
  ASSERT_NE(reader.next(1), nullptr);
}

}  // namespace
}  // namespace pangrova::velox::test
