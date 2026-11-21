#include <gtest/gtest.h>

#include <array>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>
#include <unistd.h>

#include "openssl/evp.h"
#include "zlib.h"
#include "zstd.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "engine/velox/connector/AvroContainerReader.h"

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

std::string container(const std::string& codec, const std::string& row,
    bool contentSize = true, const std::function<void(std::string&)>& mutate = {}) {
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
  if (codec == "zstandard") {
    std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)> context(ZSTD_createCCtx(), ZSTD_freeCCtx);
    if (!context || ZSTD_isError(ZSTD_CCtx_setParameter(context.get(), ZSTD_c_contentSizeFlag, contentSize))) {
      throw std::runtime_error("Cannot initialize Avro test Zstandard encoder");
    }
    encoded.resize(ZSTD_compressBound(row.size()));
    const auto length = ZSTD_compress2(context.get(), encoded.data(), encoded.size(), row.data(), row.size());
    if (ZSTD_isError(length)) throw std::runtime_error("Cannot compress Avro test Zstandard block");
    encoded.resize(length);
  }
  if (mutate) mutate(encoded);
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
  AvroContainerReader reader(file.open(), bytes.size(), sha256(bytes));
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
  AvroContainerReader reader(file.open(), bytes.size(), sha256(bytes));
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
  EXPECT_THROW(AvroContainerReader(file.open(), bytes.size(), sha256(bytes)),
      std::runtime_error);

  const auto unsupported = container("snappy", oneRow());
  AvroFile unsupportedFile(unsupported);
  EXPECT_THROW(AvroContainerReader(unsupportedFile.open(),
      unsupported.size(), sha256(unsupported)), std::runtime_error);
}

TEST(IcebergAvroContainerReaderTest, RejectsFalseExplicitCollectionBlockSize) {
  const auto bytes = container("null", rowWithFalseCollectionBlockSize());
  AvroFile file(bytes);
  AvroContainerReader reader(file.open(), bytes.size(), sha256(bytes));
  EXPECT_THROW(reader.nextRecord(), std::runtime_error);
}

class AvroZstandardCodecTest : public ::testing::TestWithParam<bool> {};

TEST_P(AvroZstandardCodecTest, ReadsCanonicalFramesWithKnownOrUnknownDecodedSize) {
  const auto bytes = container("zstandard", oneRow(), GetParam());
  AvroFile file(bytes);
  AvroContainerReader reader(file.open(), bytes.size(), sha256(bytes));
  const auto row = reader.nextRecord();
  ASSERT_TRUE(row.has_value());
  EXPECT_EQ((*row)["manifest_path"].asString(), "file:/data/manifest.avro");
  EXPECT_EQ((*row)["partitions"][0]["lower_bound"].asString(), "low");
  EXPECT_FALSE(reader.nextRecord().has_value());
}
INSTANTIATE_TEST_SUITE_P(ContentSize, AvroZstandardCodecTest, ::testing::Bool());

TEST(AvroZstandardBoundaryTest, RejectsTruncatedCorruptAndTrailingNonFrameBytes) {
  const std::vector<std::function<void(std::string&)>> mutations{
      [](auto& bytes) { bytes.resize(bytes.size() - 1); },
      [](auto& bytes) { bytes[0] = 0; },
      [](auto& bytes) { bytes += "invalid trailing data"; }};
  for (const auto& mutation : mutations) {
    const auto bytes = container("zstandard", oneRow(), false, mutation);
    AvroFile file(bytes);
    AvroContainerReader reader(file.open(), bytes.size(), sha256(bytes));
    EXPECT_THROW(reader.nextRecord(), std::runtime_error);
  }
}

TEST(AvroZstandardBoundaryTest, RejectsBlocksExpandingBeyondTheExistingDecodedBudget) {
  const auto bytes = container("zstandard", std::string(65 * 1024 * 1024, 'x'), false);
  AvroFile file(bytes);
  AvroContainerReader reader(file.open(), bytes.size(), sha256(bytes));
  try {
    reader.nextRecord();
    FAIL() << "Oversized Zstandard output must be rejected";
  } catch (const std::runtime_error& failure) {
    EXPECT_NE(std::string(failure.what()).find("bounded read limit"), std::string::npos);
  }
}

class PaimonSdkAvroCompatibilityTest : public ::testing::TestWithParam<std::string> {};

TEST_P(PaimonSdkAvroCompatibilityTest, ReadsActualDefaultCompressedSdkMetadata) {
  const auto path = std::filesystem::path(__FILE__).parent_path().parent_path().parent_path() /
      "resources" / "paimon" / GetParam();
  std::ifstream input(path, std::ios::binary);
  ASSERT_TRUE(input.good());
  const std::string bytes((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
  AvroFile file(bytes);
  AvroContainerReader reader(file.open(), bytes.size(), sha256(bytes));
  EXPECT_EQ(reader.metadataValue("avro.codec"), "zstandard");
  const bool entry = GetParam().find("manifest-entry") != std::string::npos;
  int count = 0;
  while (auto record = reader.nextRecord()) {
    ++count;
    EXPECT_EQ((*record)["_VERSION"].asInt(), 2);
    if (entry) {
      EXPECT_EQ((*record)["_KIND"].asInt(), 0);
      EXPECT_EQ((*record)["_BUCKET"].asInt(), 0);
      const auto& data = (*record)["_FILE"];
      EXPECT_EQ(data["_ROW_COUNT"].asInt(), 1);
      EXPECT_GT(data["_FILE_SIZE"].asInt(), 0);
      EXPECT_EQ(data["_SCHEMA_ID"].asInt(), 0);
      EXPECT_EQ(data["_VALUE_STATS"]["_NULL_COUNTS"].size(), 3);
      EXPECT_EQ(data["_FILE_NAME"].asString().rfind(".parquet"), data["_FILE_NAME"].asString().size() - 8);
    } else {
      EXPECT_EQ((*record)["_NUM_DELETED_FILES"].asInt(), 0);
      EXPECT_GT((*record)["_NUM_ADDED_FILES"].asInt(), 0);
      EXPECT_EQ((*record)["_FILE_NAME"].asString().find("manifest-"), 0);
    }
  }
  EXPECT_EQ(count, entry ? 2 : 1);
}
INSTANTIATE_TEST_SUITE_P(PhysicalObjects, PaimonSdkAvroCompatibilityTest, ::testing::Values(
    "append-manifest-entry.avro", "append-manifest-list.avro",
    "primary-key-manifest-entry.avro", "primary-key-manifest-list.avro"));

}  // namespace
}  // namespace pangrova::velox::test
