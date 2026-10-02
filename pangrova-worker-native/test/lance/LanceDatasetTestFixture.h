// Shared fixture: real Lance writes and catalog-pinned commits; no fake data source.
#pragma once

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
    tmpDir_ = std::filesystem::temp_directory_path() /
        ("lance_reader_test_" + std::to_string(::getpid()));
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
    if (schema.release) {
      schema.release(&schema);
    }
    if (fingerprintRc != 0) {
      throw std::runtime_error("failed to fingerprint test Lance schema");
    }
    config.writeId = writeId;
    config.tenantId = "tenant-a";
    config.generation = "generation-1";
    config.transactionStagingPrefix =
        "_pangrova/write-staging/" + writeId + "/task-" +
        std::to_string(partitionId);
    pangrova::protocol::v2::TaskAttemptId attempt;
    attempt.set_query_or_job_id("query");
    attempt.set_stage_id(1);
    attempt.set_partition_id(partitionId);
    attempt.set_plan_epoch(1);
    attempt.set_attempt_id(0);
    attempt.set_fencing_token(1);
    attempt.set_task_id("task-" + std::to_string(partitionId));
    attempt.set_control_epoch(1);
    auto* owner = attempt.mutable_execution_owner();
    owner->set_owner_id("coordinator");
    owner->set_owner_incarnation("incarnation-1");
    owner->set_fencing_token(1);
    std::string serialized;
    if (!attempt.SerializeToString(&serialized)) {
      throw std::runtime_error("failed to serialize test TaskAttemptId");
    }
    config.taskAttempt.assign(serialized.begin(), serialized.end());
    return config;
  }

  static uint64_t commitFragments(
      const LanceWriterConfig& config,
      const std::vector<std::vector<uint8_t>>& fragments,
      bool recovering = false,
      uint64_t knownCommittedVersion = 0,
      const std::vector<std::string>& stagingPrefixes = {}) {
    if (!stagingPrefixes.empty() && stagingPrefixes.size() != fragments.size()) {
      throw std::invalid_argument("one staging prefix is required per fragment");
    }
    std::vector<LanceCCommitFragment> views;
    views.reserve(fragments.size());
    for (size_t index = 0; index < fragments.size(); ++index) {
      const auto& fragment = fragments[index];
      const auto& prefix = stagingPrefixes.empty()
          ? config.transactionStagingPrefix
          : stagingPrefixes[index];
      views.push_back({fragment.data(), fragment.size(), prefix.c_str()});
    }
    uint64_t committedVersion = 0;
    std::array<uint8_t, 32> manifestDigest{};
    const auto deadline = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count() + 60'000;
    LanceStorageContext storage(config.storageBinding);
    int32_t rc = lance_ffi_commit_fragments(storage.get(),
        config.outputPath.c_str(),
        config.writeId.c_str(),
        config.tenantId.c_str(),
        config.generation.c_str(),
        static_cast<int32_t>(config.operation),
        config.expectedReadVersion,
        config.schemaFenceSha256.data(),
        config.schemaFenceSha256.size(),
        views.data(),
        views.size(),
        8,
        recovering ? 1 : 0,
        knownCommittedVersion,
        static_cast<uint64_t>(deadline),
        &committedVersion,
        manifestDigest.data(),
        manifestDigest.size());
    if (rc != 0) {
      const char* error = lance_ffi_last_error();
      throw std::runtime_error(
          std::string("Lance test commit failed") +
          (error == nullptr ? "" : std::string(": ") + error));
    }
    bool matchesPersistedManifest = false;
    for (const auto& entry : std::filesystem::directory_iterator(
             std::filesystem::path(config.outputPath) / "_versions")) {
      if (!entry.is_regular_file() || entry.path().extension() != ".manifest") continue;
      std::ifstream file(entry.path(), std::ios::binary);
      const std::string bytes((std::istreambuf_iterator<char>(file)), {});
      std::array<uint8_t, 32> actual{};
      unsigned int size = 0;
      EXPECT_EQ(EVP_Digest(bytes.data(), bytes.size(), actual.data(), &size,
                          EVP_sha256(), nullptr), 1);
      EXPECT_EQ(size, 32u);
      matchesPersistedManifest = matchesPersistedManifest || actual == manifestDigest;
    }
    EXPECT_TRUE(matchesPersistedManifest)
        << "Lance C ABI must return SHA-256 of persisted manifest bytes, not a schema or tag digest";
    return committedVersion;
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
    if (!stagingPrefixes.empty() && stagingPrefixes.size() != fragments.size()) {
      throw std::invalid_argument("one staging prefix is required per fragment");
    }
    std::vector<LanceCCommitFragment> views;
    views.reserve(fragments.size());
    for (size_t index = 0; index < fragments.size(); ++index) {
      const auto& prefix = stagingPrefixes.empty()
          ? config.transactionStagingPrefix
          : stagingPrefixes[index];
      views.push_back(
          {fragments[index].data(), fragments[index].size(), prefix.c_str()});
    }
    LanceStorageContext storage(config.storageBinding);
    if (lance_ffi_discard_fragments(storage.get(),
            config.outputPath.c_str(),
            config.writeId.c_str(),
            config.tenantId.c_str(),
            config.generation.c_str(),
            static_cast<int32_t>(config.operation),
            config.expectedReadVersion,
            config.schemaFenceSha256.data(),
            config.schemaFenceSha256.size(),
            views.data(),
            views.size()) != 0) {
      const char* error = lance_ffi_last_error();
      throw std::runtime_error(
          std::string("Lance test discard failed") +
          (error == nullptr ? "" : std::string(": ") + error));
    }
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
    uint64_t version = 0;
    LanceStorageContext storage(pangrova::test::testStorageBinding(path));
    if (lance_ffi_dataset_version(storage.get(), path.c_str(), &version) != 0) {
      const char* error = lance_ffi_last_error();
      throw std::runtime_error(
          std::string("Lance dataset version lookup failed") +
          (error == nullptr ? "" : std::string(": ") + error));
    }
    return version;
  }

  static std::array<uint8_t, 32> datasetSchemaFence(
      const std::string& path) {
    std::array<uint8_t, 32> fingerprint{};
    LanceStorageContext storage(pangrova::test::testStorageBinding(path));
    if (lance_ffi_dataset_schema_fingerprint(
            storage.get(), path.c_str(), fingerprint.data(), fingerprint.size()) != 0) {
      const char* error = lance_ffi_last_error();
      throw std::runtime_error(
          std::string("Lance dataset schema lookup failed") +
          (error == nullptr ? "" : std::string(": ") + error));
    }
    return fingerprint;
  }

  static std::pair<uint64_t, std::array<uint8_t, 32>> datasetInfo(
      const std::string& path) {
    uint64_t version = 0;
    std::array<uint8_t, 32> fingerprint{};
    LanceStorageContext storage(pangrova::test::testStorageBinding(path));
    if (lance_ffi_dataset_info(
            storage.get(), path.c_str(), datasetVersion(path), &version, fingerprint.data(), fingerprint.size()) !=
        0) {
      throw std::runtime_error("Lance dataset info lookup failed");
    }
    return {version, fingerprint};
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
