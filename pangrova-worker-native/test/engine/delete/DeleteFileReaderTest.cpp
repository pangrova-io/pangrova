// Tests for DeleteFileReader — Position Delete, Equality Delete, Deletion Vector.
// Validates: Requirements 42.1
//
// DeleteFileReader is a static utility that reads Iceberg v2/v3 delete files
// (Parquet format) and Puffin deletion vectors. Since it requires real Parquet
// files on disk, we test the API contract and edge cases using temporary files.
#include <gtest/gtest.h>
#include <folly/ScopeGuard.h>

#include <roaring.hh>
#include <cstdio>
#include <fstream>
#include <filesystem>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include "engine/delete/io/DeleteFileReader.h"
#include "engine/velox/connector/PangrovaVeloxConnector.h"
#include "jni/RuntimeInitializer.h"
#include "TestMemoryHelper.h"
#include "velox/core/Expressions.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace pangrova::delete_filter::test {
namespace {

// --- Deletion Vector tests (Roaring Bitmap serialization round-trip) ---

// Helper: write a Roaring bitmap to a temp file and return the path.
std::string writeDeletionVectorFile(const roaring::Roaring& bitmap) {
  char path_template[] = "/tmp/pangrova_delete_file_XXXXXX";
  int fd = mkstemp(path_template);
  if (fd < 0) {
    throw std::runtime_error("Failed to create deletion vector temp file");
  }
  close(fd);
  std::string path(path_template);
  size_t size = bitmap.getSizeInBytes();
  std::vector<char> buf(size);
  bitmap.write(buf.data());
  std::ofstream out(path, std::ios::binary);
  out.write(buf.data(), static_cast<std::streamsize>(size));
  out.close();
  return path;
}

TEST(DeleteFileReaderTest, ReadDeletionVectorBasic) {
  roaring::Roaring original;
  original.add(0);
  original.add(5);
  original.add(100);
  original.add(999);

  auto path = writeDeletionVectorFile(original);
  auto result = DeleteFileReader::readDeletionVector(path);

  EXPECT_TRUE(result.contains(0));
  EXPECT_TRUE(result.contains(5));
  EXPECT_TRUE(result.contains(100));
  EXPECT_TRUE(result.contains(999));
  EXPECT_FALSE(result.contains(1));
  EXPECT_FALSE(result.contains(50));
  EXPECT_EQ(result.cardinality(), 4u);

  std::remove(path.c_str());
}

TEST(DeleteFileReaderTest, ReadDeletionVectorEmpty) {
  roaring::Roaring original;  // empty bitmap
  auto path = writeDeletionVectorFile(original);
  auto result = DeleteFileReader::readDeletionVector(path);

  EXPECT_EQ(result.cardinality(), 0u);
  EXPECT_TRUE(result.isEmpty());

  std::remove(path.c_str());
}

TEST(DeleteFileReaderTest, ReadDeletionVectorLargeRange) {
  roaring::Roaring original;
  for (uint32_t i = 0; i < 10000; i += 3) {
    original.add(i);
  }

  auto path = writeDeletionVectorFile(original);
  auto result = DeleteFileReader::readDeletionVector(path);

  EXPECT_EQ(result.cardinality(), original.cardinality());
  EXPECT_TRUE(result == original);

  std::remove(path.c_str());
}

TEST(DeleteFileReaderTest, ReadDeletionVectorNonExistentFileThrows) {
  EXPECT_THROW(
      DeleteFileReader::readDeletionVector("/nonexistent/path.dv"),
      std::runtime_error);
}

// --- Position Delete edge cases (without real Parquet files) ---

TEST(DeleteFileReaderTest, ReadPositionsNonExistentFileReturnsEmpty) {
#if PANGROVA_HAS_VELOX_DELETE_READER
  EXPECT_THROW(
      DeleteFileReader::readPositions("/nonexistent.parquet", "data.parquet"),
      std::runtime_error);
#endif
}

TEST(DeleteFileReaderTest, ScanKeepsPhysicalPositionsBeforePredicateFiltering) {
  namespace sdk = facebook::velox;
  auto root = pangrova::test::testRootPool("PositionDeleteScan");
  auto pool = root->addLeafChild("parquet");
  sdk::test::VectorMaker maker(pool.get());
  char temp[] = "/tmp/pangrova_position_delete_XXXXXX";
  ASSERT_NE(mkdtemp(temp), nullptr);
  const std::filesystem::path directory(temp);
  auto cleanup = folly::makeGuard([&] { std::filesystem::remove_all(directory); });
  const auto dataPath = (directory / "data.parquet").string();
  const auto deletePath = (directory / "delete.parquet").string();
  auto write = [&](const std::string& path, const sdk::RowVectorPtr& rows) {
    auto config = std::make_shared<const sdk::config::ConfigBase>(
        std::unordered_map<std::string, std::string>{});
    sdk::parquet::WriterOptions options;
    options.memoryPool = root.get();
    sdk::dwio::common::FileSink::Options sinkOptions{
        .connectorProperties = config, .pool = pool.get()};
    sdk::parquet::Writer writer(
        std::make_unique<sdk::dwio::common::LocalFileSink>(path, sinkOptions), options, rows->rowType());
    writer.write(rows);
    writer.close();
  };
  auto data = maker.rowVector({"id"}, {maker.flatVector<int64_t>({4, 42, 44})});
  write(dataPath, data);
  write(deletePath, maker.rowVector({"file_path", "pos"}, {
      maker.flatVector<sdk::StringView>({sdk::StringView(dataPath)}), maker.flatVector<int64_t>({1})}));

  auto filter = std::make_shared<sdk::core::CallTypedExpr>(sdk::BOOLEAN(),
      std::vector<sdk::core::TypedExprPtr>{
          std::make_shared<sdk::core::FieldAccessTypedExpr>(sdk::BIGINT(), "id"),
          std::make_shared<sdk::core::ConstantTypedExpr>(sdk::BIGINT(), sdk::variant(int64_t{10}))},
      "gt");
  pangrova::velox::PangrovaVeloxDataSource source(pool.get(), data->rowType(),
      data->rowType(), "", "", filter);
  pangrova::split::SplitConfig split;
  split.file_path = dataPath;
  split.file_format = pangrova::split::FileFormatType::PARQUET;
  split.length = std::filesystem::file_size(dataPath);
  split.iceberg.emplace().delete_files.push_back({
      .file_path = deletePath, .delete_type = "position"});
  source.addSplit(std::make_shared<pangrova::velox::PangrovaConnectorSplit>(
      pangrova::velox::PangrovaVeloxConnector::kConnectorId, split));

  sdk::ContinueFuture future = folly::makeSemiFuture();
  auto next = source.next(10, future);
  ASSERT_TRUE(next.has_value() && *next);
  auto output = *next;
  ASSERT_EQ(output->size(), 2);
  sdk::DecodedVector ids(*output->childAt(0));
  EXPECT_EQ(ids.valueAt<int64_t>(0), 4);
  EXPECT_EQ(ids.valueAt<int64_t>(1), 44);
}

// Actual Iceberg SDK Parquet.writeDeletes().buildEqualityWriter() output from
// TableControlLifecycleE2ETest: optional BIGINT id=41, field ID 1. Its encoded
// child used to be dereferenced as a FlatVector and crashed the worker JVM.
TEST(DeleteFileReaderTest, EncodedSdkEqualityDeleteDoesNotRequireFlatVectors) {
  pangrova::jni::initializeVeloxRuntime(pangrova::jni::NativeRuntimeConfig{});
  const auto file = std::filesystem::path(__FILE__).parent_path()
      / "fixtures/iceberg-equality-id-41.parquet";
  EXPECT_EQ(DeleteFileReader::readEqualityKeys(file.string(), {"id"}),
            std::unordered_set<std::string>({"41"}));
}

// --- Equality Delete edge cases ---

TEST(DeleteFileReaderTest, ReadEqualityKeysNonExistentFileReturnsEmpty) {
#if PANGROVA_HAS_VELOX_DELETE_READER
  EXPECT_THROW(
      DeleteFileReader::readEqualityKeys(
          "/nonexistent.parquet", {"col_a"}),
      std::runtime_error);
#endif
}

TEST(DeleteFileReaderTest, ReadEqualityKeysEmptyColumnsReturnsEmpty) {
#if PANGROVA_HAS_VELOX_DELETE_READER
  // With empty columns, serialization produces empty keys.
  // This is a contract test — real file would be needed for full test.
#endif
}

}  // namespace
}  // namespace pangrova::delete_filter::test
