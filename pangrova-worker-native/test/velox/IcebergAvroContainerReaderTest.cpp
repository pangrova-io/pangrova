#include <gtest/gtest.h>

#include <array>
#include <filesystem>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <string>
#include <unistd.h>

#include "openssl/evp.h"
#include "zlib.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connector/IcebergAvroContainerReader.h"

namespace pangrova::velox::test {
namespace {

constexpr auto kSchema = R"({"type":"record","name":"manifest_file","fields":[)"
    R"({"name":"manifest_path","type":"string"},)"
    R"({"name":"added_files_count","type":"int"},)"
    R"({"name":"partitions","type":["null",{"type":"array",)"
    R"("items":{"type":"record","name":"field_summary","fields":[)"
    R"({"name":"contains_null","type":"boolean"},)"
    R"({"name":"lower_bound","type":["null","bytes"]})"
    R"(]}}]}]})";

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

std::string container(const std::string& codec, const std::string& row) {
  std::string bytes("Obj\x01", 4);
  appendLong(bytes, 2);
  appendBytes(bytes, "avro.schema");
  appendBytes(bytes, kSchema);
  appendBytes(bytes, "avro.codec");
  appendBytes(bytes, codec);
  appendLong(bytes, 0);
  const std::string sync(16, 's');
  bytes += sync;
  appendLong(bytes, 1);
  std::string encoded = row;
  if (codec == "deflate") {
    z_stream stream{};
    if (deflateInit2(&stream, Z_DEFAULT_COMPRESSION, Z_DEFLATED,
            -MAX_WBITS, MAX_MEM_LEVEL, Z_DEFAULT_STRATEGY) != Z_OK) {
      throw std::runtime_error("Cannot initialize Avro test deflate");
    }
    encoded.resize(compressBound(row.size()));
    stream.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(row.data()));
    stream.avail_in = static_cast<uInt>(row.size());
    stream.next_out = reinterpret_cast<Bytef*>(encoded.data());
    stream.avail_out = static_cast<uInt>(encoded.size());
    const auto result = deflate(&stream, Z_FINISH);
    deflateEnd(&stream);
    if (result != Z_STREAM_END) {
      throw std::runtime_error("Cannot deflate Avro test row");
    }
    encoded.resize(stream.total_out);
  }
  appendLong(bytes, encoded.size());
  bytes += encoded;
  bytes += sync;
  return bytes;
}

std::string sha256(const std::string& bytes) {
  std::array<unsigned char, 32> digest{};
  unsigned int size = 0;
  if (EVP_Digest(bytes.data(), bytes.size(), digest.data(), &size,
          EVP_sha256(), nullptr) != 1 || size != digest.size()) {
    throw std::runtime_error("Cannot hash Avro test fixture");
  }
  return std::string(reinterpret_cast<const char*>(digest.data()), digest.size());
}

class AvroFile final {
 public:
  explicit AvroFile(const std::string& bytes) {
    char path[] = "/tmp/pangrova-manifest-list-XXXXXX";
    const int descriptor = mkstemp(path);
    if (descriptor < 0) throw std::runtime_error("Cannot create Avro test file");
    close(descriptor);
    path_ = path;
    std::ofstream stream(path_, std::ios::binary | std::ios::trunc);
    stream.write(bytes.data(), bytes.size());
    stream.close();
  }

  ~AvroFile() { std::filesystem::remove(path_); }

  std::shared_ptr<facebook::velox::ReadFile> open() const {
    facebook::velox::filesystems::registerLocalFileSystem();
    return facebook::velox::filesystems::getFileSystem(path_, nullptr)
        ->openFileForRead(path_);
  }

 private:
  std::string path_;
};

std::string oneRow() {
  std::string row;
  appendBytes(row, "file:/data/manifest.avro");
  appendLong(row, 3);
  appendLong(row, 1);  // union branch: non-null array
  appendLong(row, 1);  // one partition summary
  row.push_back(1);   // contains_null
  appendLong(row, 1);  // union branch: non-null byte bound
  appendBytes(row, "low");
  appendLong(row, 0);  // end of array
  return row;
}

std::string rowWithFalseCollectionBlockSize() {
  std::string row;
  appendBytes(row, "file:/data/manifest.avro");
  appendLong(row, 3);
  appendLong(row, 1);   // union branch: non-null array
  appendLong(row, -1);  // one element with an explicit encoded byte length
  appendLong(row, 0);   // false length: the element below has actual bytes
  row.push_back(1);
  appendLong(row, 1);
  appendBytes(row, "low");
  appendLong(row, 0);
  return row;
}

TEST(IcebergAvroContainerReaderTest, ReadsPinnedNestedManifestRecord) {
  const auto bytes = container("null", oneRow());
  AvroFile file(bytes);
  IcebergAvroContainerReader reader(file.open(), bytes.size(), sha256(bytes));
  auto record = reader.nextRecord();
  ASSERT_TRUE(record.has_value());
  EXPECT_EQ((*record)["manifest_path"].asString(), "file:/data/manifest.avro");
  EXPECT_EQ((*record)["added_files_count"].asInt(), 3);
  EXPECT_TRUE((*record)["partitions"][0]["contains_null"].asBool());
  EXPECT_EQ((*record)["partitions"][0]["lower_bound"].asString(), "low");
  EXPECT_FALSE(reader.nextRecord().has_value());
}

TEST(IcebergAvroContainerReaderTest, ReadsDefaultDeflateCodec) {
  const auto bytes = container("deflate", oneRow());
  AvroFile file(bytes);
  IcebergAvroContainerReader reader(file.open(), bytes.size(), sha256(bytes));
  const auto record = reader.nextRecord();
  ASSERT_TRUE(record.has_value());
  EXPECT_EQ((*record)["manifest_path"].asString(), "file:/data/manifest.avro");
  EXPECT_EQ((*record)["partitions"][0]["lower_bound"].asString(), "low");
  EXPECT_FALSE(reader.nextRecord().has_value());
}

TEST(IcebergAvroContainerReaderTest, RejectsChangedObjectAndUnsupportedCodec) {
  auto bytes = container("null", oneRow());
  AvroFile file(bytes);
  bytes.back() = 'x';
  EXPECT_THROW(IcebergAvroContainerReader(file.open(), bytes.size(), sha256(bytes)),
      std::runtime_error);

  const auto unsupported = container("snappy", oneRow());
  AvroFile unsupportedFile(unsupported);
  EXPECT_THROW(IcebergAvroContainerReader(unsupportedFile.open(),
      unsupported.size(), sha256(unsupported)), std::runtime_error);
}

TEST(IcebergAvroContainerReaderTest, RejectsFalseExplicitCollectionBlockSize) {
  const auto bytes = container("null", rowWithFalseCollectionBlockSize());
  AvroFile file(bytes);
  IcebergAvroContainerReader reader(file.open(), bytes.size(), sha256(bytes));
  EXPECT_THROW(reader.nextRecord(), std::runtime_error);
}

}  // namespace
}  // namespace pangrova::velox::test
