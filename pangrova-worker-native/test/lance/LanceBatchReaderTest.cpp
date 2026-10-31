// LanceBatchReaderTest: GoogleTest unit tests for LanceBatchReader.
//
// Tests cover:
//   - Construction validation (null pool, null type, empty path)
//   - Reading pre-created .lance files with correct row counts
//   - Column projection (only requested columns returned)
//   - Predicate execution is covered by LanceFilterExecutionTest (real Velox Task).
//   - VECTOR(dim) type mapping to FixedSizeList<Float32>
//   - Exhaustion semantics (hasNext/next after end-of-dataset)
//
// Requirements: 1.3, 1.6, 1.8, 1.9

#include <gtest/gtest.h>

#include <array>
#include <barrier>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <iterator>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <unistd.h>

#include <arrow/c/abi.h>
#include <openssl/evp.h>

#include "TestMemoryHelper.h"
#include "lance/LanceBatchReader.h"
#include "LanceTestDataset.h"
#include "LanceDatasetTestFixture.h"
#include "lance/LanceBatchWriter.h"
#include "engine/delete/PaimonSnapshotDeletionFilter.h"
#include "engine/velox/connector/PangrovaVeloxConnector.h"
#include "engine/velox/connector/PangrovaTableHandle.h"

#include "lance_c_ffi.h"
#include "pangrova/protocol/v2/result.pb.h"
#include "pangrova/protocol/v2/task.pb.h"

#include "velox/type/Type.h"
#include "velox/type/Filter.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/arrow/Bridge.h"

namespace pangrova::lance::test {
namespace {

using namespace facebook::velox;
using pangrova::test::PartitionedTestDatasetReader;

// ---------------------------------------------------------------------------
// Test fixture: creates temporary Lance datasets for reader tests.
// ---------------------------------------------------------------------------

class LanceBatchReaderTest : public LanceDatasetTestFixture {};

// ---------------------------------------------------------------------------
// Construction validation
// ---------------------------------------------------------------------------

TEST_F(LanceBatchReaderTest, RejectsNullMemoryPool) {
  auto outputType = ROW({"id"}, {BIGINT()});
  EXPECT_THROW(
      PartitionedTestDatasetReader(nullptr, outputType, "/tmp/not-opened.lance", 1, {}),
      std::invalid_argument);
}

TEST_F(LanceBatchReaderTest, RejectsNullOutputType) {
  EXPECT_THROW(
      PartitionedTestDatasetReader(pool_.get(), nullptr, "/tmp/not-opened.lance", 1, {}),
      std::invalid_argument);
}

TEST_F(LanceBatchReaderTest, RejectsEmptyFilePath) {
  auto outputType = ROW({"id"}, {BIGINT()});
  EXPECT_THROW(
      PartitionedTestDatasetReader(pool_.get(), outputType, "", 1, {}),
      std::invalid_argument);
}

TEST_F(LanceBatchReaderTest, RejectsNonexistentFile) {
  auto outputType = ROW({"id"}, {BIGINT()});
  EXPECT_THROW(
      PartitionedTestDatasetReader(
          pool_.get(), outputType, "/nonexistent/path/data.lance", 1, {}),
      std::runtime_error);
}

TEST_F(LanceBatchReaderTest, RejectsUnpinnedOrMissingVersion) {
  auto path = writeSimpleDataset(2);
  auto outputType = ROW({"id", "name"}, {BIGINT(), VARCHAR()});
  EXPECT_THROW(
      PartitionedTestDatasetReader(pool_.get(), outputType, path, 0, {}),
      std::invalid_argument);
  EXPECT_THROW(
      PartitionedTestDatasetReader(pool_.get(), outputType, path, 2, {}),
      std::runtime_error);
}

TEST_F(LanceBatchReaderTest, RequiredFfiAbiAndRuntimeAreReady) {
  EXPECT_EQ(
      lance_ffi_abi_version(),
      static_cast<uint32_t>(PANGROVA_LANCE_C_FFI_ABI_VERSION));
  EXPECT_EQ(lance_ffi_runtime_init(), 0);
}


// ---------------------------------------------------------------------------
// Basic read: correct row count and values
// ---------------------------------------------------------------------------

TEST_F(LanceBatchReaderTest, ReadsAllRowsFromSimpleDataset) {
  auto path = writeSimpleDataset(5);
  auto outputType = ROW({"id", "name"}, {BIGINT(), VARCHAR()});

  PartitionedTestDatasetReader reader(pool_.get(), outputType, path, datasetVersion(path), {});

  EXPECT_TRUE(reader.hasNext());

  int64_t totalRows = 0;
  while (reader.hasNext()) {
    auto batch = reader.next(1024);
    if (!batch) {
      break;
    }
    totalRows += batch->size();
  }

  EXPECT_EQ(totalRows, 5);
  EXPECT_FALSE(reader.hasNext());
}

TEST_F(LanceBatchReaderTest, ReadsCorrectValuesFromSimpleDataset) {
  auto path = writeSimpleDataset(3);
  auto outputType = ROW({"id", "name"}, {BIGINT(), VARCHAR()});

  PartitionedTestDatasetReader reader(pool_.get(), outputType, path, datasetVersion(path), {});

  auto batch = reader.next(1024);
  ASSERT_NE(batch, nullptr);
  ASSERT_EQ(batch->size(), 3);

  auto idCol = batch->childAt(0)->asFlatVector<int64_t>();
  EXPECT_EQ(idCol->valueAt(0), 1);
  EXPECT_EQ(idCol->valueAt(1), 2);
  EXPECT_EQ(idCol->valueAt(2), 3);

  auto nameCol = batch->childAt(1)->asFlatVector<StringView>();
  EXPECT_EQ(nameCol->valueAt(0).str(), "row_1");
  EXPECT_EQ(nameCol->valueAt(1).str(), "row_2");
  EXPECT_EQ(nameCol->valueAt(2).str(), "row_3");
}

// ---------------------------------------------------------------------------
// Column projection
// ---------------------------------------------------------------------------

TEST_F(LanceBatchReaderTest, ColumnProjectionReadsOnlyRequestedColumns) {
  auto path = writeFilterableDataset();
  auto outputType = ROW({"id", "score"}, {BIGINT(), DOUBLE()});

  PartitionedTestDatasetReader reader(
      pool_.get(), outputType, path, datasetVersion(path), {"id", "score"});

  auto batch = reader.next(1024);
  ASSERT_NE(batch, nullptr);
  EXPECT_EQ(batch->childrenSize(), 2);
  EXPECT_EQ(batch->size(), 10);

  auto idCol = batch->childAt(0)->asFlatVector<int64_t>();
  EXPECT_EQ(idCol->valueAt(0), 1);

  auto scoreCol = batch->childAt(1)->asFlatVector<double>();
  EXPECT_EQ(scoreCol->valueAt(0), 50.0);
}

TEST_F(LanceBatchReaderTest, MissingProjectedColumnsAreDeferredToEvolution) {
  auto path = writeSimpleDataset(2);
  auto requestedType = ROW({"id", "added"}, {BIGINT(), DOUBLE()});

  PartitionedTestDatasetReader reader(
      pool_.get(), requestedType, path, datasetVersion(path), {"id", "added"});
  ASSERT_EQ(reader.outputType()->size(), 1);
  EXPECT_EQ(reader.outputType()->nameOf(0), "id");

  auto batch = reader.next(1024);
  ASSERT_NE(batch, nullptr);
  EXPECT_EQ(batch->size(), 2);
  EXPECT_EQ(batch->childrenSize(), 1);
}

TEST_F(
    LanceBatchReaderTest,
    ConnectorAppliesPhysicalRenamePromotionDefaultAndPartition) {
  auto path = (tmpDir_ / "schema_evolution.lance").string();
  auto physicalType = ROW({"old_id", "unused"}, {INTEGER(), VARCHAR()});
  auto ids = BaseVector::create(INTEGER(), 2, pool_.get());
  ids->asFlatVector<int32_t>()->set(0, 17);
  ids->asFlatVector<int32_t>()->set(1, 29);
  auto unused = BaseVector::create(VARCHAR(), 2, pool_.get());
  unused->asFlatVector<StringView>()->set(0, StringView("a"));
  unused->asFlatVector<StringView>()->set(1, StringView("b"));
  auto input = std::make_shared<RowVector>(
      pool_.get(), physicalType, nullptr, 2,
      std::vector<VectorPtr>{ids, unused});
  auto writerConfig = createConfig(path, "schema-evolution", physicalType);
  LanceBatchWriter writer(pool_, physicalType, writerConfig);
  writer.writeBatch(input);
  auto fragment = writer.finish();
  EXPECT_EQ(commitFragment(writerConfig, fragment), 1);

  auto outputType = ROW(
      {"id", "added", "region"}, {BIGINT(), DOUBLE(), VARCHAR()});
  pangrova::velox::PangrovaVeloxDataSource dataSource(
      pool_.get(), outputType, outputType);
  pangrova::split::SplitConfig split;
  split.split_id = "lance-evolution";
  split.connector_id = "lance";
  split.file_path = path;
  split.file_format = pangrova::split::FileFormatType::LANCE;
  split.source_snapshot = "1";
  split.storage_binding = pangrova::test::testStorageBinding(path);
  split.file_scan_detail = pangrova::test::testReadDetail(
      pangrova::test::testScanPage(path, 1, split.storage_binding).partitions(0));
  split.partition_values["region"] = "us-east";
  split.column_mappings = {
      {.source_id = 1,
       .target_id = 10,
       .source_name = "old_id",
       .target_name = "id"},
      {.source_id = 2,
       .target_id = 20,
       .source_name = "not_yet_written",
       .target_name = "added",
       .default_value = "1.5"}};
  split.type_promotions = {
      {.field_id = 10,
       .source_type = "integer",
       .target_type = "bigint",
       .column_name = "id"}};
  dataSource.addSplit(
      std::make_shared<pangrova::velox::PangrovaConnectorSplit>(
          "lance", std::move(split)));

  auto future = ContinueFuture::makeEmpty();
  auto result = dataSource.next(1024, future);
  ASSERT_TRUE(result.has_value());
  ASSERT_NE(result.value(), nullptr);
  auto batch = result.value();
  ASSERT_TRUE(batch->type()->equivalent(*outputType));
  ASSERT_EQ(batch->size(), 2);
  EXPECT_EQ(batch->childAt(0)->asFlatVector<int64_t>()->valueAt(0), 17);
  EXPECT_EQ(batch->childAt(0)->asFlatVector<int64_t>()->valueAt(1), 29);
  EXPECT_DOUBLE_EQ(
      batch->childAt(1)->as<SimpleVector<double>>()->valueAt(0), 1.5);
  EXPECT_EQ(
      batch->childAt(2)->as<SimpleVector<StringView>>()
          ->valueAt(0)
          .str(),
      "us-east");
}

TEST_F(LanceBatchReaderTest, ConnectorRejectsUnpinnedSnapshot) {
  auto path = writeSimpleDataset(1);
  auto outputType = ROW({"id", "name"}, {BIGINT(), VARCHAR()});
  pangrova::velox::PangrovaVeloxDataSource dataSource(
      pool_.get(), outputType, outputType);
  pangrova::split::SplitConfig split;
  split.split_id = "lance-unpinned";
  split.connector_id = "lance";
  split.file_path = path;
  split.file_format = pangrova::split::FileFormatType::LANCE;
  split.source_snapshot = "latest";
  EXPECT_THROW(
      dataSource.addSplit(
          std::make_shared<pangrova::velox::PangrovaConnectorSplit>(
              "lance", std::move(split))),
      std::invalid_argument);
}

TEST_F(LanceBatchReaderTest, ProjectionPreservesPunctuationAndRejectsDuplicates) {
  auto path = (tmpDir_ / "punctuation.lance").string();
  auto storedType = ROW({"id", "payload,raw"}, {BIGINT(), VARCHAR()});
  auto ids = BaseVector::create(BIGINT(), 1, pool_.get());
  ids->asFlatVector<int64_t>()->set(0, 42);
  auto payloads = BaseVector::create(VARCHAR(), 1, pool_.get());
  payloads->asFlatVector<StringView>()->set(0, StringView("kept"));
  auto batch = std::make_shared<RowVector>(
      pool_.get(), storedType, nullptr, 1,
      std::vector<VectorPtr>{ids, payloads});
  auto config = createConfig(path, "punctuation", storedType);
  LanceBatchWriter writer(pool_, storedType, config);
  writer.writeBatch(batch);
  auto fragment = writer.finish();
  EXPECT_EQ(commitFragment(config, fragment), 1);

  auto projectedType = ROW({"payload,raw"}, {VARCHAR()});
  PartitionedTestDatasetReader reader(
      pool_.get(), projectedType, path, datasetVersion(path), {"payload,raw"});
  auto projected = reader.next(1024);
  ASSERT_NE(projected, nullptr);
  ASSERT_EQ(projected->childrenSize(), 1);
  EXPECT_EQ(
      projected->childAt(0)->asFlatVector<StringView>()->valueAt(0).str(),
      "kept");

  EXPECT_THROW(
      PartitionedTestDatasetReader(
          pool_.get(), ROW({"id", "id"}, {BIGINT(), BIGINT()}), path,
          datasetVersion(path),
          {"id", "id"}),
      std::runtime_error);
}


// ---------------------------------------------------------------------------
// VECTOR payload
// ---------------------------------------------------------------------------

TEST_F(LanceBatchReaderTest, ReadsVectorPayloadWithStableDimension) {
  auto path = writeVectorDataset();
  auto vecType = ARRAY(REAL());
  auto outputType = ROW({"id", "embedding"}, {BIGINT(), vecType});

  PartitionedTestDatasetReader reader(pool_.get(), outputType, path, datasetVersion(path), {});

  auto batch = reader.next(1024);
  ASSERT_NE(batch, nullptr);
  ASSERT_EQ(batch->size(), 3);

  auto idCol = batch->childAt(0)->asFlatVector<int64_t>();
  EXPECT_EQ(idCol->valueAt(0), 1);
  EXPECT_EQ(idCol->valueAt(1), 2);
  EXPECT_EQ(idCol->valueAt(2), 3);

  auto embCol = batch->childAt(1)->as<ArrayVector>();
  ASSERT_NE(embCol, nullptr);
  EXPECT_EQ(embCol->sizeAt(0), 4);
  EXPECT_EQ(embCol->sizeAt(1), 4);
  EXPECT_EQ(embCol->sizeAt(2), 4);

  // First row embedding: [0.0, 0.1, 0.2, 0.3]
  auto elements = embCol->elements()->asFlatVector<float>();
  EXPECT_FLOAT_EQ(elements->valueAt(0), 0.0f);
  EXPECT_FLOAT_EQ(elements->valueAt(1), 0.1f);
  EXPECT_FLOAT_EQ(elements->valueAt(2), 0.2f);
  EXPECT_FLOAT_EQ(elements->valueAt(3), 0.3f);
}

// ---------------------------------------------------------------------------
// Exhaustion semantics
// ---------------------------------------------------------------------------

TEST_F(LanceBatchReaderTest, NextReturnsNullAfterExhaustion) {
  auto path = writeSimpleDataset(2);
  auto outputType = ROW({"id", "name"}, {BIGINT(), VARCHAR()});

  PartitionedTestDatasetReader reader(pool_.get(), outputType, path, datasetVersion(path), {});

  while (reader.hasNext()) {
    auto batch = reader.next(1024);
    if (!batch) {
      break;
    }
  }

  EXPECT_FALSE(reader.hasNext());
  EXPECT_EQ(reader.next(1024), nullptr);
}

TEST_F(LanceBatchReaderTest, BatchSizeLimitsRowsPerCall) {
  auto path = writeSimpleDataset(10);
  auto outputType = ROW({"id", "name"}, {BIGINT(), VARCHAR()});

  PartitionedTestDatasetReader reader(pool_.get(), outputType, path, datasetVersion(path), {});

  auto batch = reader.next(3);
  ASSERT_NE(batch, nullptr);
  EXPECT_LE(batch->size(), 3);
}

TEST_F(LanceBatchReaderTest, ChangingBatchLimitPreservesRowsAndBackpressure) {
  auto path = writeSimpleDataset(10);
  auto outputType = ROW({"id", "name"}, {BIGINT(), VARCHAR()});
  PartitionedTestDatasetReader reader(pool_.get(), outputType, path, datasetVersion(path), {});

  int64_t expectedId = 1;
  for (uint64_t limit : {7ULL, 1ULL, 2ULL}) {
    auto batch = reader.next(limit);
    ASSERT_NE(batch, nullptr);
    ASSERT_EQ(batch->size(), limit);
    auto ids = batch->childAt(0)->asFlatVector<int64_t>();
    for (facebook::velox::vector_size_t row = 0; row < batch->size(); ++row) {
      EXPECT_EQ(ids->valueAt(row), expectedId++);
    }
  }
  EXPECT_EQ(expectedId, 11);
  EXPECT_EQ(reader.next(1), nullptr);
  EXPECT_FALSE(reader.hasNext());
}


TEST_F(LanceBatchReaderTest, DatasetPhysicalPositionsReachConnectorWithExactSourceIdentity) {
  const auto path = writeSimpleDataset(5);
  const std::string source(pangrova::velox::kSourceObjectIdColumnName);
  const std::string position(pangrova::velox::kPhysicalRowPositionColumnName);
  const std::vector<RowTypePtr> types{
      ROW({"id", source, position}, {BIGINT(), VARCHAR(), BIGINT()}),
      ROW({position, "id", source}, {BIGINT(), BIGINT(), VARCHAR()}),
      ROW({source, "region", position, "id"}, {VARCHAR(), VARCHAR(), BIGINT(), BIGINT()})};
  for (const auto& type : types) {
    SCOPED_TRACE(type->toString());
    pangrova::velox::PangrovaVeloxDataSource dataSource(pool_.get(), type, type);
    pangrova::split::SplitConfig split;
    split.split_id = "dataset-partition";
    split.connector_id = "lance";
    split.file_path = path;
    split.file_format = pangrova::split::FileFormatType::LANCE;
    split.source_snapshot = "1";
    split.storage_binding = pangrova::test::testStorageBinding(path);
    const auto partition = pangrova::test::testScanPage(path, 1, split.storage_binding).partitions(0);
    split.file_scan_detail = pangrova::test::testReadDetail(partition);
    split.data_object_id = "table-a:1:" + std::to_string(partition.partition_id());
    const auto expectedSource = split.data_object_id;
    if (type->containsChild("region")) split.partition_values.emplace("region", "test-region");
    dataSource.addSplit(std::make_shared<pangrova::velox::PangrovaConnectorSplit>("lance", std::move(split)));
    dataSource.addDynamicFilter(type->getChildIdx("id"), std::make_shared<common::BigintRange>(2, 4, false));
    auto future = ContinueFuture::makeEmpty();
    const auto result = dataSource.next(1024, future);
    ASSERT_TRUE(result.has_value());
    ASSERT_NE(result.value(), nullptr);
    const auto rows = result.value();
    ASSERT_EQ(rows->size(), 3);
    ASSERT_EQ(rows->childrenSize(), type->size());
    ASSERT_TRUE(rows->type()->equivalent(*type));
    for (vector_size_t row = 0; row < rows->size(); ++row) {
      EXPECT_EQ(rows->childAt(type->getChildIdx("id"))->as<SimpleVector<int64_t>>()->valueAt(row), row + 2);
      EXPECT_EQ(rows->childAt(type->getChildIdx(source))->as<SimpleVector<StringView>>()->valueAt(row).str(), expectedSource);
      EXPECT_EQ(rows->childAt(type->getChildIdx(position))->as<SimpleVector<int64_t>>()->valueAt(row), row + 1);
      if (type->containsChild("region")) {
        EXPECT_EQ(rows->childAt(type->getChildIdx("region"))->as<SimpleVector<StringView>>()->valueAt(row).str(), "test-region");
      }
    }
  }
}

TEST_F(LanceBatchReaderTest, DatasetPhysicalPositionsRejectManifestBoundsAndTypeDrift) {
  const auto path = writeSimpleDataset(5);
  const auto storage = pangrova::test::testStorageBinding(path);
  auto partition = pangrova::test::testScanPage(path, 1, storage).partitions(0);
  const std::string position(pangrova::velox::kPhysicalRowPositionColumnName);
  const auto type = ROW({"id", position}, {BIGINT(), BIGINT()});
  auto drift = partition;
  drift.mutable_manifest_sha256()->set_digest(std::string(32, 'x'));
  EXPECT_THROW((LanceBatchReader(pool_.get(), type, path, 1, {"id", position}, storage,
      pangrova::test::testReadDetail(drift), 0)), std::runtime_error);
  drift = partition;
  drift.set_physical_rows(partition.physical_rows() + 1);
  EXPECT_THROW((LanceBatchReader(pool_.get(), type, path, 1, {"id", position}, storage,
      pangrova::test::testReadDetail(drift), 0)), std::runtime_error);
  const auto scalar = ROW({"id", position}, {BIGINT(), VARCHAR()});
  EXPECT_THROW((LanceBatchReader(pool_.get(), scalar, path, 1, {"id", position}, storage,
      pangrova::test::testReadDetail(partition), 0)), std::invalid_argument);
}

TEST_F(LanceBatchReaderTest, RejectsUnboundedBatchRequest) {
  auto path = writeSimpleDataset(2);
  auto outputType = ROW({"id", "name"}, {BIGINT(), VARCHAR()});
  PartitionedTestDatasetReader reader(pool_.get(), outputType, path, datasetVersion(path), {});
  EXPECT_THROW(
      reader.next(PANGROVA_LANCE_C_FFI_MAX_BATCH_ROWS + 1ULL),
      std::runtime_error);

  auto first = reader.next(1);
  ASSERT_NE(first, nullptr);
  EXPECT_EQ(first->childAt(0)->asFlatVector<int64_t>()->valueAt(0), 1);
  EXPECT_THROW(
      reader.next(PANGROVA_LANCE_C_FFI_MAX_BATCH_ROWS + 1ULL),
      std::runtime_error);
  auto second = reader.next(1);
  ASSERT_NE(second, nullptr);
  EXPECT_EQ(second->childAt(0)->asFlatVector<int64_t>()->valueAt(0), 2);
}

TEST_F(LanceBatchReaderTest, SdkDeletionContainerUsesExactSharedRangeCrcAndCardinality) {
  roaring::Roaring bitmap;
  bitmap.add(1);
  bitmap.add(3);
  std::string portable(bitmap.getSizeInBytes(), '\0');
  bitmap.write(portable.data());
  const auto integer = [](uint32_t value) {
    std::string bytes(4, '\0');
    for (size_t index = 0; index < 4; ++index) bytes[index] = static_cast<char>(value >> ((3 - index) * 8));
    return bytes;
  };
  std::string body = integer(1581511376u) + portable;
  boost::crc_32_type crc;
  crc.process_bytes(body.data(), body.size());
  const std::string payload = integer(body.size()) + body + integer(crc.checksum());
  const auto file = (tmpDir_ / "shared-deletion-index.bin").string();
  {
    std::ofstream output(file, std::ios::binary);
    output << std::string(13, 'p') << payload << std::string(9, 's');
  }
  protocol::v2::DeletionVectorReference reference;
  reference.set_opaque_location(file);
  reference.set_byte_offset(13);
  reference.set_byte_length(body.size());
  reference.set_cardinality(2);
  reference.set_physical_rows(5);
  const auto storage = pangrova::test::testStorageBinding(file);
  delete_filter::PaimonSnapshotDeletionFilter filter(reference, storage);
  auto row = std::make_shared<RowVector>(pool_.get(), ROW({"id"}, {BIGINT()}), nullptr, 5,
      std::vector<VectorPtr>{BaseVector::create(BIGINT(), 5, pool_.get())});
  SelectivityVector selected(5);
  filter.apply(*row, selected, 0);
  EXPECT_EQ(selected.countSelected(), 3);
  EXPECT_FALSE(selected.isValid(1));
  EXPECT_FALSE(selected.isValid(3));
  reference.set_cardinality(3);
  EXPECT_THROW(delete_filter::PaimonSnapshotDeletionFilter(reference, storage), std::invalid_argument);
  reference.set_cardinality(2);
  reference.set_physical_rows(3);
  EXPECT_THROW(delete_filter::PaimonSnapshotDeletionFilter(reference, storage), std::invalid_argument);
  reference.set_physical_rows(5);
  {
    std::fstream output(file, std::ios::binary | std::ios::in | std::ios::out);
    output.seekp(13 + payload.size() - 1);
    output.put(static_cast<char>(payload.back() ^ 1));
  }
  EXPECT_THROW(delete_filter::PaimonSnapshotDeletionFilter(reference, storage), std::invalid_argument);
}

}  // namespace
}  // namespace pangrova::lance::test
