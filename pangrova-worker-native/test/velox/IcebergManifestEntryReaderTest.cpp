#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <filesystem>
#include <fstream>
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
    R"({"name":"file_size_in_bytes","type":"long"}]}}]})";

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

std::string childRow(int32_t status, const std::string& path, int32_t content = 0) {
  std::string row;
  appendLong(row, status);
  appendLong(row, content);
  appendBytes(row, path);
  appendBytes(row, "PARQUET");
  appendLong(row, 0);  // null spec ID inherited from manifest
  appendLong(row, 3);
  appendLong(row, 4096);
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
}

TEST(IcebergManifestEntryReaderTest, ReadsPositionDeleteManifest) {
  facebook::velox::filesystems::registerLocalFileSystem();
  auto pool = pangrova::test::testLeafPool("IcebergFilesDelete");
  const auto childBytes = container(kChildSchema,
      {{"format-version", "2"}, {"partition-spec-id", "0"}, {"content", "deletes"}},
      1, childRow(1, "file:/data/position-delete.parquet", 1));
  AvroFile child(childBytes);
  const auto listBytes = container(kListSchema,
      {{"format-version", "2"}, {"snapshot-id", "7"}, {"sequence-number", "1"}},
      1, listRow(child.path(), childBytes.size(), 1));
  AvroFile list(listBytes);
  IcebergManifestEntryReader reader(child.path(), childBytes.size(), digest(childBytes),
      list.path(), listBytes.size(), digest(listBytes), 7, 1, 2, 0, 1,
      projectedType(), pool.get());
  auto batch = reader.next(1);
  ASSERT_NE(batch, nullptr);
  ASSERT_EQ(batch->size(), 1);
  EXPECT_EQ(batch->childAt(0)->asFlatVector<int32_t>()->valueAt(0), 1);
  EXPECT_EQ(batch->childAt(1)->asFlatVector<facebook::velox::StringView>()
                ->valueAt(0).str(), "file:/data/position-delete.parquet");
  EXPECT_EQ(reader.next(1), nullptr);
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
