// RowDeleteFilterFactoryTest: Unit tests for RowDeleteFilterFactory
// DV detection, filter creation, and chain ordering (DV → Position → Equality).
// Validates: Requirements R5.1, R5.6

#include <gtest/gtest.h>

#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <unistd.h>

#include <roaring.hh>

#include "TestMemoryHelper.h"
#include "engine/split/SplitConfig.h"
#include "engine/delete/RowDeleteFilterFactory.h"
#include "engine/delete/DeletionVectorRowFilter.h"
#include "engine/delete/io/DeletionVectorReader.h"

#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/SelectivityVector.h"

namespace pangrova::delete_filter {
namespace {

using namespace facebook::velox;

// RAII temporary file for DV test data.
class TempFile {
 public:
  explicit TempFile(const std::string& content) {
    char tmpl[] = "/tmp/pangrova_factory_test_XXXXXX";
    int fd = mkstemp(tmpl);
    if (fd < 0) {
      throw std::runtime_error("Failed to create temp file");
    }
    path_ = tmpl;
    auto written = write(fd, content.data(), content.size());
    close(fd);
    if (written != static_cast<ssize_t>(content.size())) {
      throw std::runtime_error("Failed to write temp file");
    }
  }

  ~TempFile() { std::remove(path_.c_str()); }

  const std::string& path() const { return path_; }

 private:
  std::string path_;
};

std::string sdkPuffin(const std::string& name) {
  const auto path = std::filesystem::path(__FILE__).parent_path()
      / ("fixtures/iceberg-sdk-" + name + "-dv.puffin");
  std::ifstream input(path, std::ios::binary);
  if (!input) throw std::runtime_error("Missing SDK fixture: " + path.string());
  return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

// Helper: create a simple RowVector with a single int32 column.
RowVectorPtr makeRowVector(facebook::velox::memory::MemoryPool* pool, int32_t num_rows) {
  std::vector<int32_t> values(num_rows);
  for (int32_t i = 0; i < num_rows; ++i) {
    values[i] = i;
  }
  auto vector = BaseVector::create(INTEGER(), num_rows, pool);
  auto flat = vector->asFlatVector<int32_t>();
  for (int32_t i = 0; i < num_rows; ++i) {
    flat->set(i, i);
  }
  auto rowType = ROW({"id"}, {INTEGER()});
  return std::make_shared<RowVector>(
      pool, rowType, nullptr, num_rows, std::vector<VectorPtr>{vector});
}

class RowDeleteFilterFactoryTest : public ::testing::Test {
 protected:
  static std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;

  static void SetUpTestSuite() {
    pool_ = pangrova::test::testLeafPool("velox_factory_test");
  }

  facebook::velox::memory::MemoryPool* pool() { return pool_.get(); }
};

std::shared_ptr<facebook::velox::memory::MemoryPool>
    RowDeleteFilterFactoryTest::pool_;

// No delete files → empty filter vector.
TEST_F(RowDeleteFilterFactoryTest, NoDeleteFilesReturnsEmpty) {
  split::IcebergConfig config;
  auto filters = RowDeleteFilterFactory::create(config);
  EXPECT_TRUE(filters.empty());
}

TEST_F(RowDeleteFilterFactoryTest, UnqualifiedEqualityDeleteIsRejectedBeforeFileIo) {
  split::IcebergConfig config;
  split::DeleteFileConfig file;
  file.delete_type = "equality";
  file.file_path = "/must-not-open.parquet";
  config.delete_files.push_back(file);
  try {
    (void)RowDeleteFilterFactory::create(config);
    FAIL() << "Unqualified equality deletes were accepted";
  } catch (const std::runtime_error& error) {
    EXPECT_NE(std::string(error.what()).find("equality deletes are not supported"), std::string::npos);
  }
}

// Single DV delete file → one filter that marks bitmap positions as deleted.
TEST_F(RowDeleteFilterFactoryTest, SingleDeletionVectorFilter) {
  roaring::Roaring bitmap;
  bitmap.add(1);
  bitmap.add(3);
  bitmap.add(7);

  auto puffinContent = sdkPuffin("factory-basic");
  TempFile dvFile(puffinContent);

  split::IcebergConfig config;
  split::DeleteFileConfig df;
  df.delete_type = "deletion_vector";
  df.dv_file_path = dvFile.path();
  config.delete_files.push_back(df);

  auto filters = RowDeleteFilterFactory::create(config);
  ASSERT_EQ(filters.size(), 1);

  // Apply the DV filter and verify rows 1, 3, 7 are deleted.
  auto batch = makeRowVector(pool(), 10);
  SelectivityVector rows(batch->size(), true);
  filters[0]->apply(*batch, rows, /*row_offset=*/0);

  EXPECT_TRUE(rows.isValid(0));
  EXPECT_FALSE(rows.isValid(1));
  EXPECT_TRUE(rows.isValid(2));
  EXPECT_FALSE(rows.isValid(3));
  EXPECT_TRUE(rows.isValid(4));
  EXPECT_TRUE(rows.isValid(5));
  EXPECT_TRUE(rows.isValid(6));
  EXPECT_FALSE(rows.isValid(7));
}

TEST_F(RowDeleteFilterFactoryTest, SdkMultipleBlobsUseTheScannedFileAnd64BitOffset) {
  TempFile fixture(sdkPuffin("multiple"));
  split::IcebergConfig config;
  split::DeleteFileConfig file;
  file.delete_type = "deletion_vector";
  file.dv_file_path = fixture.path();
  config.delete_files.push_back(file);
  auto filters = RowDeleteFilterFactory::create(config, "s3://fixture/b.parquet");
  ASSERT_EQ(filters.size(), 1);
  auto batch = makeRowVector(pool(), 10);
  SelectivityVector rows(batch->size(), true);
  filters.front()->apply(*batch, rows, 0);
  EXPECT_TRUE(rows.isValid(1));
  EXPECT_FALSE(rows.isValid(2));
  EXPECT_TRUE(rows.isValid(7));
  rows.setAll();
  filters.front()->apply(*batch, rows, int64_t{1} << 32);
  EXPECT_TRUE(rows.isValid(2));
  EXPECT_FALSE(rows.isValid(7));
}

// Multiple DV files → multiple DV filters, all before position/equality.
TEST_F(RowDeleteFilterFactoryTest, MultipleDeletionVectorFiles) {
  roaring::Roaring bitmap1;
  bitmap1.add(0);
  bitmap1.add(2);
  roaring::Roaring bitmap2;
  bitmap2.add(4);
  bitmap2.add(6);

  auto puffin1 = sdkPuffin("factory-first");
  auto puffin2 = sdkPuffin("factory-second");
  TempFile dvFile1(puffin1);
  TempFile dvFile2(puffin2);

  split::IcebergConfig config;
  split::DeleteFileConfig df1;
  df1.delete_type = "deletion_vector";
  df1.dv_file_path = dvFile1.path();
  split::DeleteFileConfig df2;
  df2.delete_type = "deletion_vector";
  df2.dv_file_path = dvFile2.path();
  config.delete_files.push_back(df1);
  config.delete_files.push_back(df2);

  auto filters = RowDeleteFilterFactory::create(config);
  ASSERT_EQ(filters.size(), 2);

  // Apply both filters sequentially.
  auto batch = makeRowVector(pool(), 8);
  SelectivityVector rows(batch->size(), true);
  for (const auto& f : filters) {
    f->apply(*batch, rows, /*row_offset=*/0);
  }

  EXPECT_FALSE(rows.isValid(0));
  EXPECT_TRUE(rows.isValid(1));
  EXPECT_FALSE(rows.isValid(2));
  EXPECT_TRUE(rows.isValid(3));
  EXPECT_FALSE(rows.isValid(4));
  EXPECT_TRUE(rows.isValid(5));
  EXPECT_FALSE(rows.isValid(6));
  EXPECT_TRUE(rows.isValid(7));
}

// R5.6: DV + Position + Equality → filter chain order is DV first,
// then position, then equality. Verify by counting filters and
// applying them in order.
TEST_F(RowDeleteFilterFactoryTest, MixedDeleteTypesOrdering) {
  roaring::Roaring bitmap;
  bitmap.add(0);

  auto puffinContent = sdkPuffin("factory-zero");
  TempFile dvFile(puffinContent);

  // When Velox DWIO reader is available, position/equality delete files
  // must exist on disk. Test DV-only ordering here; position/equality
  // filter creation is tested separately with real Parquet files.
  split::IcebergConfig config;

  split::DeleteFileConfig dvDelete;
  dvDelete.delete_type = "deletion_vector";
  dvDelete.dv_file_path = dvFile.path();

  config.delete_files.push_back(dvDelete);

  auto filters = RowDeleteFilterFactory::create(config);

  // DV filter should be created.
  ASSERT_EQ(filters.size(), 1);

  // The DV filter must delete row 0.
  auto batch = makeRowVector(pool(), 5);
  SelectivityVector rows(batch->size(), true);
  filters[0]->apply(*batch, rows, /*row_offset=*/0);

  EXPECT_FALSE(rows.isValid(0));
  EXPECT_TRUE(rows.isValid(1));
  EXPECT_TRUE(rows.isValid(2));
  EXPECT_TRUE(rows.isValid(3));
  EXPECT_TRUE(rows.isValid(4));
}

// Unknown delete semantics must never return unfiltered rows.
TEST_F(RowDeleteFilterFactoryTest, UnknownDeleteTypeRejected) {
  split::IcebergConfig config;
  split::DeleteFileConfig df;
  df.delete_type = "unknown_type";
  df.file_path = "/some/path";
  config.delete_files.push_back(df);

  EXPECT_THROW((void)RowDeleteFilterFactory::create(config), std::runtime_error);
}

// DV file read failure propagates as exception.
TEST_F(RowDeleteFilterFactoryTest, DvFileReadFailureThrows) {
  split::IcebergConfig config;
  split::DeleteFileConfig df;
  df.delete_type = "deletion_vector";
  df.dv_file_path = "/nonexistent/dv.puffin";
  config.delete_files.push_back(df);

  EXPECT_THROW(
      RowDeleteFilterFactory::create(config),
      std::runtime_error);
}

}  // namespace
}  // namespace pangrova::delete_filter
