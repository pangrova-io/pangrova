#include "engine/velox/connector/PangrovaVeloxConnector.h"

#include "TestMemoryHelper.h"
#include "engine/resource/TaskObjectReadBudget.h"
#include "engine/velox/filter/PendingFilterRegistry.h"
#include "velox/common/file/File.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"

#include <folly/ScopeGuard.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace pangrova::velox::test {
namespace {
namespace sdk = facebook::velox;

void writeParquet(const std::string& path, const sdk::RowVectorPtr& rows,
                  sdk::memory::MemoryPool& root, sdk::memory::MemoryPool& pool) {
  auto config = std::make_shared<const sdk::config::ConfigBase>(
      std::unordered_map<std::string, std::string>{});
  sdk::parquet::WriterOptions options;
  options.memoryPool = &root;
  options.compressionKind = sdk::common::CompressionKind_NONE;
  options.enableDictionary = false;
  options.flushPolicyFactory = [] {
    return std::make_unique<sdk::parquet::DefaultFlushPolicy>(3, std::numeric_limits<int64_t>::max());
  };
  sdk::dwio::common::FileSink::Options sinkOptions{
      .connectorProperties = config, .pool = &pool};
  sdk::parquet::Writer writer(
      std::make_unique<sdk::dwio::common::LocalFileSink>(path, sinkOptions), options, rows->rowType());
  writer.write(rows);
  writer.close();
}

TEST(ParquetRangeScanTest, AdjacentRangesKeepFileGlobalDeletePositions) {
  char temp[] = "/tmp/pangrova-parquet-ranges-XXXXXX";
  ASSERT_NE(mkdtemp(temp), nullptr);
  const std::filesystem::path directory(temp);
  auto cleanup = folly::makeGuard([&] { std::filesystem::remove_all(directory); });
  const auto dataPath = (directory / "range.parquet").string();
  const auto deletePath = (directory / "delete.parquet").string();
  auto root = pangrova::test::testRootPool("ParquetRangeScan");
  auto pool = root->addLeafChild("file");
  sdk::test::VectorMaker maker(pool.get());
  const std::string payload(256 * 1024, 'x');
  auto data = maker.rowVector({"id", "payload"}, {
      maker.flatVector<int64_t>({10, 11, 12, 20, 21, 22}),
      maker.flatVector<sdk::StringView>(std::vector<sdk::StringView>(6, sdk::StringView(payload)))});
  writeParquet(dataPath, data, *root, *pool);
  writeParquet(deletePath, maker.rowVector({"file_path", "pos"}, {
      maker.flatVector<sdk::StringView>({sdk::StringView(dataPath), sdk::StringView(dataPath)}),
      maker.flatVector<int64_t>({1, 4})}), *root, *pool);

  sdk::parquet::ParquetReader metadata(
      std::make_unique<sdk::dwio::common::BufferedInput>(
          std::make_shared<sdk::LocalReadFile>(dataPath), *pool),
      sdk::dwio::common::ReaderOptions(pool.get()));
  const auto file = metadata.fileMetaData();
  ASSERT_EQ(file.numRowGroups(), 2);
  const auto group = file.rowGroup(1);
  const auto column = group.columnChunk(0);
  const auto boundary = group.hasFileOffset() && group.fileOffset() != 0
      ? group.fileOffset()
      : column.hasDictionaryPageOffset() ? column.dictionaryPageOffset() : column.dataPageOffset();
  const auto size = static_cast<int64_t>(std::filesystem::file_size(dataPath));
  ASSERT_GT(boundary, 0);
  ASSERT_LT(boundary, size);

  auto read = [&](int64_t start, int64_t length) {
    PangrovaVeloxDataSource source(pool.get(), data->rowType(), data->rowType());
    auto budget = std::make_shared<resource::TaskObjectReadBudget>(std::numeric_limits<uint64_t>::max());
    source.setReadBudget(budget);
    split::SplitConfig split;
    split.file_path = dataPath;
    split.file_format = split::FileFormatType::PARQUET;
    split.start_offset = start;
    split.length = length;
    split.iceberg.emplace().delete_files.push_back({
        .file_path = deletePath, .delete_type = "position"});
    source.addSplit(std::make_shared<PangrovaConnectorSplit>(
        PangrovaVeloxConnector::kConnectorId, split));
    std::vector<int64_t> ids;
    sdk::ContinueFuture future = folly::makeSemiFuture();
    for (auto batch = source.next(2, future); batch && *batch; batch = source.next(2, future)) {
      sdk::DecodedVector decoded(*(*batch)->childAt(0));
      (*batch)->childAt(1)->loadedVector();
      for (sdk::vector_size_t row = 0; row < (*batch)->size(); ++row) {
        ids.push_back(decoded.valueAt<int64_t>(row));
      }
    }
    return std::pair{std::move(ids), budget->requestedBytes()};
  };

  const auto left = read(0, boundary);
  const auto right = read(boundary, size - boundary);
  EXPECT_EQ(left.first, (std::vector<int64_t>{10, 12}));
  EXPECT_EQ(right.first, (std::vector<int64_t>{20, 22}));
  EXPECT_LT(left.second, static_cast<uint64_t>(size));
  EXPECT_LT(right.second, static_cast<uint64_t>(size));
  RecordProperty("object_bytes", std::to_string(size));
  RecordProperty("left_requested_bytes", std::to_string(left.second));
  RecordProperty("right_requested_bytes", std::to_string(right.second));
  EXPECT_EQ(read(0, size).first, (std::vector<int64_t>{10, 12, 20, 22}));
  EXPECT_TRUE(read(size - 1, 1).first.empty());
  EXPECT_THROW(read(boundary, size), std::runtime_error);
}

TEST(ParquetRangeScanTest, RunningScanAppliesLatePredicateToOnlySubsequentBatches) {
  char temp[] = "/tmp/pangrova-parquet-late-filter-XXXXXX";
  ASSERT_NE(mkdtemp(temp), nullptr);
  const std::filesystem::path directory(temp);
  auto cleanup = folly::makeGuard([&] { std::filesystem::remove_all(directory); });
  const auto path = (directory / "scan.parquet").string();
  auto root = pangrova::test::testRootPool("ParquetLateRuntimeFilter");
  auto pool = root->addLeafChild("scan");
  sdk::test::VectorMaker maker(pool.get());
  auto data = maker.rowVector({"id"}, {maker.flatVector<int64_t>({10, 11, 12, 20, 21, 22})});
  writeParquet(path, data, *root, *pool);

  auto read = [&](bool inject) {
    const std::string task = inject ? "late-parquet-probe" : "baseline-parquet-probe";
    const std::string scan = "node_0";
    auto& registry = PendingFilterRegistry::instance();
    auto remove = folly::makeGuard([&] { registry.remove(task); });
    PangrovaVeloxDataSource source(pool.get(), data->rowType(), data->rowType(), task, scan);
    split::SplitConfig split;
    split.file_path = path;
    split.file_format = split::FileFormatType::PARQUET;
    split.length = static_cast<int64_t>(std::filesystem::file_size(path));
    source.addSplit(std::make_shared<PangrovaConnectorSplit>(
        PangrovaVeloxConnector::kConnectorId, split));

    std::vector<int64_t> ids;
    size_t batches = 0;
    sdk::ContinueFuture future = folly::makeSemiFuture();
    for (auto batch = source.next(2, future); batch && *batch; batch = source.next(2, future)) {
      sdk::DecodedVector decoded(*(*batch)->childAt(0));
      for (sdk::vector_size_t row = 0; row < (*batch)->size(); ++row) {
        ids.push_back(decoded.valueAt<int64_t>(row));
      }
      if (++batches == 1 && inject) {
        EXPECT_EQ(ids, (std::vector<int64_t>{10, 11}));
        registry.push(task, scan, 0, std::make_shared<sdk::common::BigintRange>(20, 21, false));
      }
    }
    return ids;
  };

  EXPECT_EQ(read(false), (std::vector<int64_t>{10, 11, 12, 20, 21, 22}));
  EXPECT_EQ(read(true), (std::vector<int64_t>{10, 11, 20, 21}));
}

}  // namespace
}  // namespace pangrova::velox::test
