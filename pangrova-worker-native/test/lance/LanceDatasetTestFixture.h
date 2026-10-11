// Shared fixture: real Lance writes and catalog-pinned commits; no fake data source.
#pragma once

#include <gtest/gtest.h>

#include <algorithm>
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

#include <arrow/buffer.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/ipc/writer.h>
#include <openssl/evp.h>

#include "../TestMemoryHelper.h"
#include "lance/LanceBatchReader.h"
#include "LanceTestDataset.h"
#include "lance/LanceBatchWriter.h"
#include "delete/PaimonSnapshotDeletionFilter.h"
#include "velox/connector/PangrovaVeloxConnector.h"
#include "velox/connector/PangrovaTableHandle.h"

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
using namespace facebook::velox;

class LanceDatasetTestFixture : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    pool_ = pangrova::test::testLeafPool("LanceDatasetTestFixture");
  }

  static void TearDownTestSuite() {
    pool_.reset();
  }

  void SetUp() override {
    tmpDir_ = pangrova::test::LanceManagedSourceTestAuthority::warehouse() /
        ("lance_reader_test_" + std::to_string(::getpid()) + "_" +
         ::testing::UnitTest::GetInstance()->current_test_info()->test_suite_name() + "_" +
         ::testing::UnitTest::GetInstance()->current_test_info()->name());
    std::filesystem::create_directories(tmpDir_);
  }

  void TearDown() override {
    std::filesystem::remove_all(tmpDir_);
  }

  LanceWriterConfig createConfig(
      const std::string& path,
      const std::string& writeId,
      const RowTypePtr& outputType,
      LanceWriteOperation operation = LanceWriteOperation::kCreate,
      uint64_t expectedVersion = 0,
      int32_t partitionId = 0) const {
    LanceWriterConfig config;
    config.outputPath = path;
    config.storageBinding = pangrova::test::testStorageBinding(path);
    config.operation = operation;
    config.expectedReadVersion = expectedVersion;
    auto empty = BaseVector::create(outputType, 0, pool_.get());
    ArrowSchema schema;
    exportToArrow(empty, schema);
    const auto fingerprintRc = lance_ffi_schema_fingerprint(
        &schema, config.schemaFenceSha256.data(),
        config.schemaFenceSha256.size());
    if (fingerprintRc != 0) {
      if (schema.release) schema.release(&schema);
      throw std::runtime_error("failed to fingerprint test Lance schema");
    }
    auto imported = arrow::ImportSchema(&schema);
    if (!imported.ok()) throw std::runtime_error(imported.status().ToString());
    auto ipc = arrow::ipc::SerializeSchema(**imported, arrow::default_memory_pool());
    if (!ipc.ok()) throw std::runtime_error(ipc.status().ToString());
    config.writeId = writeId;
    config.tenantId = "tenant-a";
    config.generation = "generation-1";
    pangrova::test::LanceManagedSourceTestAuthority::allocate(config,
        std::string(reinterpret_cast<const char*>((*ipc)->data()), (*ipc)->size()), partitionId);
    return config;
  }

  static uint64_t commitFragments(
      const LanceWriterConfig& config,
      const std::vector<std::vector<uint8_t>>& fragments,
      bool recovering = false,
      uint64_t knownCommittedVersion = 0,
      const std::vector<std::string>& stagingPrefixes = {}) {
    // The old recovery arguments are assertions against actual PG truth only.
    // They never authorize SDK filesystem lookup or physical publication.
    if (!recovering && knownCommittedVersion != 0) {
      throw std::runtime_error("A known version requires an explicit recovery attempt");
    }
    return pangrova::test::LanceManagedSourceTestAuthority::commit(
        config, fragments, knownCommittedVersion, stagingPrefixes);
  }

  static uint64_t commitFragment(
      const LanceWriterConfig& config,
      const std::vector<uint8_t>& fragment) {
    return commitFragments(config, {fragment});
  }

  static void discardFragments(
      const LanceWriterConfig& config,
      const std::vector<std::vector<uint8_t>>& fragments,
      const std::vector<std::string>& stagingPrefixes = {}) {
    pangrova::test::LanceManagedSourceTestAuthority::cleanup(config, fragments, stagingPrefixes);
  }

  static pangrova::protocol::v2::LanceWriteFragment parseFragment(
      const std::vector<uint8_t>& bytes) {
    pangrova::protocol::v2::LanceWriteFragment fragment;
    if (!fragment.ParseFromArray(
            bytes.data(), static_cast<int>(bytes.size()))) {
      throw std::runtime_error("failed to parse Lance write fragment");
    }
    return fragment;
  }

  static uint64_t datasetVersion(const std::string& path) {
    return pangrova::test::LanceManagedSourceTestAuthority::reference(path, 0).version();
  }

  static std::array<uint8_t, 32> datasetSchemaFence(const std::string& path) {
    const auto actual = pangrova::test::LanceManagedSourceTestAuthority::reference(path, 0).physical_schema_sha256().digest();
    if (actual.size() != 32) throw std::runtime_error("PG accepted schema is malformed");
    std::array<uint8_t, 32> result{};
    std::copy(actual.begin(), actual.end(), result.begin());
    return result;
  }

  static std::pair<uint64_t, std::array<uint8_t, 32>> datasetInfo(const std::string& path) {
    const auto accepted = pangrova::test::LanceManagedSourceTestAuthority::reference(path, 0);
    const auto storage = pangrova::test::testStorageBinding(path);
    const auto page = pangrova::test::testScanPage(path, accepted.version(), storage);
    if (page.version() != accepted.version() || page.physical_schema_sha256().SerializeAsString() != accepted.physical_schema_sha256().SerializeAsString()) {
      throw std::runtime_error("Actual SDK schema differs from authoritative accepted source");
    }
    return {page.version(), datasetSchemaFence(path)};
  }

  // Write a dataset with id (BIGINT) and name (VARCHAR) columns.
  std::string writeSimpleDataset(int numRows) {
    auto path = (tmpDir_ / "simple.lance").string();
    auto outputType = ROW({"id", "name"}, {BIGINT(), VARCHAR()});

    auto config = createConfig(path, "write-simple", outputType);

    LanceBatchWriter writer(pool_, outputType, config);

    auto idVector = BaseVector::create(BIGINT(), numRows, pool_.get());
    auto nameVector = BaseVector::create(VARCHAR(), numRows, pool_.get());
    auto idFlat = idVector->asFlatVector<int64_t>();
    auto nameFlat = nameVector->asFlatVector<StringView>();

    for (int i = 0; i < numRows; ++i) {
      idFlat->set(i, static_cast<int64_t>(i + 1));
      std::string name = "row_" + std::to_string(i + 1);
      nameFlat->set(i, StringView(name));
    }

    auto batch = std::make_shared<RowVector>(
        pool_.get(), outputType, nullptr, numRows,
        std::vector<VectorPtr>{idVector, nameVector});

    writer.writeBatch(batch);
    auto fragment = writer.finish();
    EXPECT_EQ(commitFragment(config, fragment), 1);
    return path;
  }

  // Write a dataset with id, category (VARCHAR), and score (DOUBLE).
  std::string writeFilterableDataset() {
    auto path = (tmpDir_ / "filterable.lance").string();
    auto outputType =
        ROW({"id", "category", "score"}, {BIGINT(), VARCHAR(), DOUBLE()});

    auto config = createConfig(path, "write-filterable", outputType);

    LanceBatchWriter writer(pool_, outputType, config);

    constexpr int numRows = 10;
    auto idVec = BaseVector::create(BIGINT(), numRows, pool_.get());
    auto catVec = BaseVector::create(VARCHAR(), numRows, pool_.get());
    auto scoreVec = BaseVector::create(DOUBLE(), numRows, pool_.get());

    auto idFlat = idVec->asFlatVector<int64_t>();
    auto catFlat = catVec->asFlatVector<StringView>();
    auto scoreFlat = scoreVec->asFlatVector<double>();

    const char* categories[] = {"tech", "science", "art"};
    for (int i = 0; i < numRows; ++i) {
      idFlat->set(i, static_cast<int64_t>(i + 1));
      catFlat->set(i, StringView(categories[i % 3]));
      scoreFlat->set(i, 50.0 + i * 10.0);
    }

    auto batch = std::make_shared<RowVector>(
        pool_.get(), outputType, nullptr, numRows,
        std::vector<VectorPtr>{idVec, catVec, scoreVec});

    writer.writeBatch(batch);
    auto fragment = writer.finish();
    EXPECT_EQ(commitFragment(config, fragment), 1);
    return path;
  }

  // Write a dataset with id (BIGINT) and embedding (ARRAY<REAL> dim=4).
  // The canonical task binding enforces VECTOR dimensions before artifact
  // construction; the Lance storage bridge preserves the array payload.
  std::string writeVectorDataset() {
    auto path = (tmpDir_ / "vector.lance").string();
    auto vecType = ARRAY(REAL());
    auto outputType = ROW({"id", "embedding"}, {BIGINT(), vecType});

    auto config = createConfig(path, "write-vector", outputType);

    LanceBatchWriter writer(pool_, outputType, config);

    constexpr int numRows = 3;
    auto idVec = BaseVector::create(BIGINT(), numRows, pool_.get());
    auto idFlat = idVec->asFlatVector<int64_t>();
    for (int i = 0; i < numRows; ++i) {
      idFlat->set(i, static_cast<int64_t>(i + 1));
    }

    // Build FixedSizeArray vector for embeddings.
    auto elementsVec =
        BaseVector::create(REAL(), numRows * 4, pool_.get());
    auto elemFlat = elementsVec->asFlatVector<float>();
    for (int i = 0; i < numRows * 4; ++i) {
      elemFlat->set(i, static_cast<float>(i) * 0.1f);
    }

    auto offsets =
        AlignedBuffer::allocate<vector_size_t>(numRows, pool_.get());
    auto sizes =
        AlignedBuffer::allocate<vector_size_t>(numRows, pool_.get());
    auto rawOffsets = offsets->asMutable<vector_size_t>();
    auto rawSizes = sizes->asMutable<vector_size_t>();
    for (int i = 0; i < numRows; ++i) {
      rawOffsets[i] = i * 4;
      rawSizes[i] = 4;
    }

    auto arrayVec = std::make_shared<ArrayVector>(
        pool_.get(), vecType, nullptr, numRows,
        offsets, sizes, elementsVec);

    auto batch = std::make_shared<RowVector>(
        pool_.get(), outputType, nullptr, numRows,
        std::vector<VectorPtr>{idVec, arrayVec});

    writer.writeBatch(batch);
    auto fragment = writer.finish();
    EXPECT_EQ(commitFragment(config, fragment), 1);
    return path;
  }

  // Fully qualified: the project's pangrova::memory namespace shadows the
  // `memory` alias pulled in by `using namespace facebook::velox`.
  inline static std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
  std::filesystem::path tmpDir_;
};


}  // namespace pangrova::lance::test
