#include <gtest/gtest.h>

#include <array>
#include <filesystem>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <string>
#include <unistd.h>

#include "TestMemoryHelper.h"
#include "folly/json.h"
#include "openssl/evp.h"
#include "velox/connector/PaimonFilesMetadataReader.h"
#include "velox/type/Type.h"
#include "velox/vector/FlatVector.h"

namespace pangrova::velox::test {
namespace {
using namespace facebook::velox;

std::string read(const std::filesystem::path& path) {
  std::ifstream stream(path, std::ios::binary);
  if (!stream) throw std::runtime_error("Cannot read Paimon metadata fixture");
  return {std::istreambuf_iterator<char>(stream), std::istreambuf_iterator<char>()};
}
std::string digest(const std::string& bytes) {
  std::string value(32, '\0');
  unsigned int size = 0;
  if (EVP_Digest(bytes.data(), bytes.size(), reinterpret_cast<unsigned char*>(value.data()),
          &size, EVP_sha256(), nullptr) != 1 || size != value.size())
    throw std::runtime_error("Cannot hash Paimon test metadata");
  return value;
}
std::string unhex(const std::string& hex) {
  std::string bytes;
  protocol::v2::PinnedMetadataObject object;
  for (size_t index = 0; index < hex.size(); index += 2)
    bytes += static_cast<char>(std::stoul(hex.substr(index, 2), nullptr, 16));
  return bytes;
}
class Snapshot final {
 public:
  explicit Snapshot(const std::string& contents) {
    char name[] = "/tmp/pangrova-paimon-files-XXXXXX";
    const int descriptor = mkstemp(name);
    if (descriptor < 0) throw std::runtime_error("Cannot create pinned snapshot test file");
    close(descriptor);
    path = name;
    std::ofstream output(path, std::ios::binary);
    output << contents;
  }
  ~Snapshot() { std::filesystem::remove(path); }
  std::string path;
};

RowTypePtr allColumns() {
  return ROW({"partition", "bucket", "file_path", "file_format", "schema_id", "level",
      "record_count", "file_size_in_bytes", "min_key", "max_key", "null_value_counts",
      "min_value_stats", "max_value_stats", "min_sequence_number", "max_sequence_number",
      "creation_time", "deleteRowCount", "file_source", "first_row_id", "write_cols"},
      {VARCHAR(), INTEGER(), VARCHAR(), VARCHAR(), BIGINT(), INTEGER(), BIGINT(), BIGINT(),
       VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(), BIGINT(), BIGINT(), TIMESTAMP(),
       BIGINT(), VARCHAR(), BIGINT(), ARRAY(VARCHAR())});
}

class PaimonFilesMetadataReaderTest : public ::testing::TestWithParam<std::string> {
 protected:
  std::filesystem::path fixtures = std::filesystem::path(__FILE__).parent_path().parent_path()
      / "resources" / "paimon";
  std::unique_ptr<Snapshot> snapshot;
  folly::dynamic oracle;
  protocol::v2::PaimonFilesReadContext context;
  std::string manifest;
  std::string bytes;
  protocol::v2::PinnedMetadataObject object;
  std::shared_ptr<facebook::velox::memory::MemoryPool> pool = pangrova::test::testLeafPool("PaimonFiles");

  void SetUp() override {
    oracle = folly::parseJson(read(fixtures / (GetParam() + "-files-oracle.json")));
    snapshot = std::make_unique<Snapshot>(oracle["snapshot"].asString());
    context.mutable_snapshot()->set_uri("file:" + snapshot->path);
    context.mutable_snapshot()->set_length(oracle["snapshot"].asString().size());
    context.mutable_snapshot()->set_sha256(digest(oracle["snapshot"].asString()));
    auto* schema = context.add_schemas();
    schema->set_schema_json(oracle["schema"].asString());
    for (const auto& row : oracle["rows"]) {
      auto* selected = context.add_files();
      selected->set_identifier_sha256(unhex(row["identifier"].asString()));
      selected->set_file_uri(row["file_path"].asString());
    }
    manifest = (fixtures / (GetParam() + "-manifest-entry.avro")).string();
    bytes = read(manifest);
    object.set_uri(manifest);
    object.set_length(bytes.size());
    object.set_sha256(digest(bytes));
  }
  PaimonFilesMetadataReader reader(const RowTypePtr& type) {
    return PaimonFilesMetadataReader(object, 1,
        context.SerializeAsString(), type, pool.get());
  }
};

TEST_P(PaimonFilesMetadataReaderTest, AllTwentyColumnsMatchTheActualSdkIncludingNulls) {
  auto type = allColumns();
  auto input = reader(type);
  size_t count = 0;
  while (auto batch = input.next(1)) {
    ASSERT_EQ(batch->size(), 1);
    auto uri = batch->childAt(2)->asFlatVector<StringView>()->valueAt(0).str();
    const folly::dynamic* expected = nullptr;
    for (const auto& row : oracle["rows"]) if (row["file_path"] == uri) expected = &row;
    ASSERT_NE(expected, nullptr);
    for (uint32_t column = 0; column < type->size(); ++column) {
      SCOPED_TRACE(type->nameOf(column));
      const auto& value = (*expected)[type->nameOf(column)];
      const auto vector = batch->childAt(column);
      ASSERT_EQ(vector->isNullAt(0), value.isNull());
      if (value.isNull()) continue;
      switch (vector->typeKind()) {
        case TypeKind::VARCHAR: EXPECT_EQ(vector->asFlatVector<StringView>()->valueAt(0).str(), value.asString()); break;
        case TypeKind::INTEGER: EXPECT_EQ(vector->asFlatVector<int32_t>()->valueAt(0), value.asInt()); break;
        case TypeKind::BIGINT: EXPECT_EQ(vector->asFlatVector<int64_t>()->valueAt(0), value.asInt()); break;
        case TypeKind::TIMESTAMP: EXPECT_EQ(vector->asFlatVector<Timestamp>()->valueAt(0).toMillis(), value.asInt()); break;
        case TypeKind::ARRAY: {
          auto* array = vector->as<ArrayVector>();
          ASSERT_EQ(array->sizeAt(0), value.size());
          for (size_t index = 0; index < value.size(); ++index) {
            auto position = array->offsetAt(0) + index;
            ASSERT_EQ(array->elements()->isNullAt(position), value[index].isNull());
            if (!value[index].isNull())
              EXPECT_EQ(array->elements()->asFlatVector<StringView>()->valueAt(position).str(), value[index].asString());
          }
          break;
        }
        default: FAIL() << "Unexpected Paimon metadata type";
      }
    }
    ++count;
  }
  EXPECT_EQ(count, oracle["rows"].size());
}

TEST_P(PaimonFilesMetadataReaderTest, ProjectsSelectedColumnsWithoutChangingActiveFileMembership) {
  auto input = reader(ROW({"partition", "record_count"}, {VARCHAR(), BIGINT()}));
  auto batch = input.next(100);
  ASSERT_NE(batch, nullptr);
  EXPECT_EQ(batch->size(), oracle["rows"].size());
  EXPECT_EQ(input.next(100), nullptr);
}

TEST_P(PaimonFilesMetadataReaderTest, RejectsUnresolvedAndDuplicateSelectedIdentities) {
  context.mutable_files(0)->set_identifier_sha256(std::string(32, 'x'));
  auto input = reader(ROW({"record_count"}, {BIGINT()}));
  EXPECT_THROW(input.next(100), std::runtime_error);
  *context.add_files() = context.files(0);
  EXPECT_THROW(reader(allColumns()), std::runtime_error);
}

TEST_P(PaimonFilesMetadataReaderTest, RejectsSnapshotReplacementAndSchemaDrift) {
  auto changed = oracle["snapshot"].asString();
  changed[0] = '[';
  { std::ofstream output(snapshot->path, std::ios::binary | std::ios::trunc); output << changed; }
  EXPECT_THROW(reader(allColumns()), std::runtime_error);
  { std::ofstream output(snapshot->path, std::ios::binary | std::ios::trunc); output << oracle["snapshot"].asString(); }
  context.mutable_schemas(0)->set_schema_id(1);
  EXPECT_THROW(reader(allColumns()), std::runtime_error);
}

TEST_P(PaimonFilesMetadataReaderTest, RejectsOutputTypeAndManifestHashDrift) {
  EXPECT_THROW(reader(ROW({"bucket"}, {BIGINT()})), std::runtime_error);
  object.set_sha256(std::string(32, 'x'));
  EXPECT_THROW(reader(allColumns()), std::runtime_error);
}

TEST_P(PaimonFilesMetadataReaderTest, NewlyAddedFieldsHaveAllNullStatisticsByFieldIdentity) {
  auto schema = folly::parseJson(oracle["schema"].asString());
  schema["id"] = 1;
  schema["fields"].push_back(folly::dynamic::object("id", 3)("name", "added")("type", "BIGINT"));
  auto* evolved = context.add_schemas();
  evolved->set_schema_id(1);
  evolved->set_schema_json(folly::toJson(schema));
  context.set_read_schema_id(1);
  auto input = reader(ROW({"record_count", "null_value_counts", "min_value_stats"}, {BIGINT(), VARCHAR(), VARCHAR()}));
  auto batch = input.next(100);
  ASSERT_NE(batch, nullptr);
  for (int row = 0; row < batch->size(); ++row) {
    EXPECT_EQ(batch->childAt(1)->asFlatVector<StringView>()->valueAt(row).str().find("{added=1, "), 0);
    EXPECT_EQ(batch->childAt(2)->asFlatVector<StringView>()->valueAt(row).str().find("{added=null, "), 0);
  }
}

INSTANTIATE_TEST_SUITE_P(ActualSdk, PaimonFilesMetadataReaderTest,
    ::testing::Values(std::string("append"), std::string("primary-key"), std::string("projected")),
    [](const ::testing::TestParamInfo<std::string>& info) {
      return info.param == "append" ? "Append" : info.param == "primary-key" ? "PrimaryKey" : "Projected";
    });

}  // namespace
}  // namespace pangrova::velox::test
