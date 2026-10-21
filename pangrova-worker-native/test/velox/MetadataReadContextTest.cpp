#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <unistd.h>

#include "TestMemoryHelper.h"
#include "openssl/evp.h"
#include "resource/TaskObjectReadBudget.h"
#include "velox/connector/MetadataReadContext.h"
#include "velox/connector/PangrovaVeloxConnector.h"
#include "velox/type/Type.h"

namespace pangrova::velox::test {
namespace {
using namespace facebook::velox;

constexpr auto kSnapshot = R"({"version":3,"id":7,"schemaId":0,"commitUser":"writer",)"
    R"("commitIdentifier":1,"commitKind":"APPEND","timeMillis":1700000000000,)"
    R"("baseManifestList":"base","deltaManifestList":"delta","totalRecordCount":0,"deltaRecordCount":0})";

class MetadataReadContextTest : public ::testing::Test {
 protected:
  std::string path;
  std::string sha;
  std::shared_ptr<facebook::velox::memory::MemoryPool> pool = pangrova::test::testLeafPool("MetadataIoOwner");
  RowTypePtr type = ROW({"snapshot_id"}, {BIGINT()});
  void SetUp() override {
    char name[] = "/tmp/pangrova-metadata-budget-XXXXXX";
    int descriptor = mkstemp(name);
    ASSERT_GE(descriptor, 0);
    close(descriptor);
    path = name;
    { std::ofstream output(path, std::ios::binary); output << kSnapshot; }
    sha.resize(32);
    unsigned int size = 0;
    ASSERT_EQ(EVP_Digest(kSnapshot, std::char_traits<char>::length(kSnapshot),
        reinterpret_cast<unsigned char*>(sha.data()), &size, EVP_sha256(), nullptr), 1);
    ASSERT_EQ(size, 32);
  }
  void TearDown() override { std::filesystem::remove(path); }
  std::shared_ptr<PangrovaConnectorSplit> source() {
    split::SplitConfig config;
    config.file_path = "file:" + path;
    config.file_format = split::FileFormatType::METADATA_JSON;
    config.length = std::char_traits<char>::length(kSnapshot);
    config.system_table = split::SystemTableKind::PAIMON_SNAPSHOTS;
    config.system_table_snapshot_id = 7;
    config.system_table_object_sha256 = sha;
    return std::make_shared<PangrovaConnectorSplit>("paimon", std::move(config));
  }
};

TEST_F(MetadataReadContextTest, TaskMetadataCannotOpenWithoutItsAdmittedOwner) {
  PangrovaVeloxDataSource input(pool.get(), type, type);
  EXPECT_THROW(input.addSplit(source()), std::invalid_argument);
  EXPECT_THROW(MetadataReadContext(nullptr, {}), std::invalid_argument);
}

TEST_F(MetadataReadContextTest, ActualDataSourceRejectsZeroReadGrantWithoutConsumingBytes) {
  auto budget = std::make_shared<pangrova::resource::TaskObjectReadBudget>(0);
  PangrovaVeloxDataSource input(pool.get(), type, type);
  input.setReadBudget(budget);
  input.addSplit(source());
  ContinueFuture future;
  EXPECT_THROW(input.next(1, future), std::runtime_error);
  EXPECT_EQ(budget->requestedBytes(), 0);
  EXPECT_EQ(budget->activeReads(), 0);
}

TEST_F(MetadataReadContextTest, IcebergJsonIndexingUsesItsActualBackendGrant) {
  const std::string document = R"({"format-version":2,"current-snapshot-id":7,)"
      R"("snapshots":[{"snapshot-id":7,"timestamp-ms":1700000000000,"summary":{"operation":"append"},)"
      R"("manifest-list":"file:/manifest"}],"snapshot-log":[]})";
  { std::ofstream output(path, std::ios::binary | std::ios::trunc); output << document; }
  auto budget = std::make_shared<pangrova::resource::TaskObjectReadBudget>(0);
  PangrovaVeloxDataSource input(pool.get(), type, type);
  input.setReadBudget(budget);
  auto split = source();
  split->splitConfig.system_table = pangrova::split::SystemTableKind::ICEBERG_SNAPSHOTS;
  split->splitConfig.length = document.size();
  try {
    input.addSplit(split);
    FAIL() << "JSON indexing must be charged to the task";
  } catch (const std::runtime_error& failure) {
    EXPECT_NE(std::string(failure.what()).find("requested-read grant exhausted"), std::string::npos);
  }
  EXPECT_EQ(budget->requestedBytes(), 0);
  EXPECT_EQ(budget->activeReads(), 0);
}

TEST_F(MetadataReadContextTest, ConsecutiveMetadataSplitsShareOneTaskGrant) {
  const auto length = std::char_traits<char>::length(kSnapshot);
  auto budget = std::make_shared<pangrova::resource::TaskObjectReadBudget>(length);
  PangrovaVeloxDataSource input(pool.get(), type, type);
  input.setReadBudget(budget);
  ContinueFuture future;
  input.addSplit(source());
  auto first = input.next(1, future);
  ASSERT_TRUE(first.has_value());
  ASSERT_NE(first.value(), nullptr);
  EXPECT_EQ(first.value()->size(), 1);
  EXPECT_EQ(budget->requestedBytes(), length);
  input.addSplit(source());
  EXPECT_THROW(input.next(1, future), std::runtime_error);
  EXPECT_EQ(budget->requestedBytes(), length);
  EXPECT_EQ(budget->activeReads(), 0);
}

TEST_F(MetadataReadContextTest, LocalDataSourceCancellationStopsItsMetadataReader) {
  auto budget = std::make_shared<pangrova::resource::TaskObjectReadBudget>(4096);
  PangrovaVeloxDataSource input(pool.get(), type, type);
  input.setReadBudget(budget);
  input.addSplit(source());
  input.cancel();
  ContinueFuture future;
  EXPECT_THROW(input.next(1, future), std::runtime_error);
  EXPECT_EQ(budget->requestedBytes(), 0);
  EXPECT_EQ(budget->activeReads(), 0);
}

TEST_F(MetadataReadContextTest, ReleasedTaskOwnerRejectsOpenBeforeResolvingAnInvalidLocation) {
  auto budget = std::make_shared<pangrova::resource::TaskObjectReadBudget>(4096);
  MetadataReadContext reads(budget, {});
  auto file = reads.open("file:" + path);
  auto size = file->size();
  budget->stopAndDrain();
  EXPECT_EQ(file->size(), size); // An immutable cached size needs no new backend operation.
  try {
    reads.open("unknown-scheme://not-a-real-backend/object");
    FAIL() << "A released owner must not open a backend";
  } catch (const std::runtime_error& failure) {
    EXPECT_NE(std::string(failure.what()).find("cancelled or released"), std::string::npos);
  }
  EXPECT_EQ(budget->activeReads(), 0);
}

}  // namespace
}  // namespace pangrova::velox::test
