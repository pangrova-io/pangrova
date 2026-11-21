// Real Lance publication / transaction component oracles.
#include "LanceDatasetTestFixture.h"

namespace pangrova::lance::test {
namespace {
using pangrova::test::PartitionedTestDatasetReader;
class LanceCommitExecutionTest : public LanceDatasetTestFixture {};

TEST_F(LanceCommitExecutionTest, WriterRejectsSchemaFenceDrift) {
  auto path = (tmpDir_ / "schema_drift.lance").string();
  auto boundType = ROW({"id"}, {BIGINT()});
  auto actualType = ROW({"id", "payload"}, {BIGINT(), VARCHAR()});
  auto config = createConfig(path, "schema-drift", boundType);

  EXPECT_THROW(
      LanceBatchWriter(pool_, actualType, config),
      std::runtime_error);
}

TEST_F(LanceCommitExecutionTest, WriterReturnsUncommittedMetadataAndReportsUsage) {
  auto path = (tmpDir_ / "writer_contract.lance").string();
  auto outputType = ROW({"id"}, {BIGINT()});
  auto idVector = BaseVector::create(BIGINT(), 2, pool_.get());
  auto idFlat = idVector->asFlatVector<int64_t>();
  idFlat->set(0, 7);
  idFlat->set(1, 9);
  auto batch = std::make_shared<RowVector>(
      pool_.get(), outputType, nullptr, 2,
      std::vector<VectorPtr>{idVector});

  auto config = createConfig(path, "writer-contract", outputType);
  LanceBatchWriter writer(pool_, outputType, config);
  writer.writeBatch(batch);
  EXPECT_EQ(writer.stats().rowsWritten, 2);
  EXPECT_EQ(writer.stats().batchesWritten, 1);
  EXPECT_GT(writer.stats().bytesWritten, 0);
  auto fragment = writer.finish();
  EXPECT_FALSE(fragment.empty());
  EXPECT_LE(fragment.size(), 64U * 1024U);
  EXPECT_THROW(writer.finish(), std::runtime_error);

  const auto metadata = parseFragment(fragment);
  EXPECT_EQ(metadata.contract_version(), 2U);
  EXPECT_EQ(metadata.dataset_uri(), path);
  EXPECT_EQ(metadata.write_id(), config.writeId);
  EXPECT_EQ(metadata.rows_written(), 2U);
  ASSERT_TRUE(metadata.has_transaction());
  const auto& transaction = metadata.transaction();
  EXPECT_EQ(transaction.tenant_id(), config.tenantId);
  EXPECT_EQ(transaction.write_id(), config.writeId);
  EXPECT_EQ(transaction.generation(), config.generation);
  EXPECT_EQ(transaction.task_attempt().SerializeAsString(),
            metadata.task_attempt().SerializeAsString());
  EXPECT_EQ(transaction.sha256().algorithm(),
            pangrova::protocol::v2::Checksum::ALGORITHM_SHA256);
  EXPECT_EQ(transaction.sha256().digest().size(), 32U);
  EXPECT_GT(transaction.size_bytes(), 0U);
  EXPECT_LE(transaction.size_bytes(), 64U * 1024U * 1024U);
  EXPECT_EQ(
      transaction.relative_object_key().find(
          config.transactionStagingPrefix + "/transaction-"),
      0U);
  EXPECT_EQ(transaction.relative_object_key().find("://"), std::string::npos);
  EXPECT_TRUE(std::filesystem::exists(
      std::filesystem::path(path) / transaction.relative_object_key()));

  // Worker finish writes immutable data objects but never a live manifest.
  EXPECT_THROW(
      PartitionedTestDatasetReader(pool_.get(), outputType, path, 1, {}),
      std::runtime_error);
  EXPECT_EQ(commitFragment(config, fragment), 1);

  PartitionedTestDatasetReader reader(pool_.get(), outputType, path, datasetVersion(path), {});
  auto written = reader.next(1024);
  ASSERT_NE(written, nullptr);
  EXPECT_EQ(written->size(), 2);
}

TEST_F(LanceCommitExecutionTest, FinishedEmptyWriterPublishesDeclaredSchema) {
  auto path = (tmpDir_ / "empty_writer.lance").string();
  auto outputType = ROW({"id"}, {BIGINT()});

  auto config = createConfig(path, "empty-writer", outputType);
  LanceBatchWriter writer(pool_, outputType, config);
  auto fragment = writer.finish();
  EXPECT_FALSE(fragment.empty());
  EXPECT_EQ(commitFragment(config, fragment), 1);

  PartitionedTestDatasetReader reader(pool_.get(), outputType, path, datasetVersion(path), {});
  EXPECT_EQ(reader.next(1024), nullptr);
}

TEST_F(LanceCommitExecutionTest, UnfinishedWriterDoesNotPublishDataset) {
  auto path = (tmpDir_ / "cancelled_writer.lance").string();
  auto outputType = ROW({"id"}, {BIGINT()});
  auto idVector = BaseVector::create(BIGINT(), 1, pool_.get());
  idVector->asFlatVector<int64_t>()->set(0, 7);
  auto batch = std::make_shared<RowVector>(
      pool_.get(), outputType, nullptr, 1,
      std::vector<VectorPtr>{idVector});

  {
    auto config = createConfig(path, "cancelled-writer", outputType);
    LanceBatchWriter writer(pool_, outputType, config);
    writer.writeBatch(batch);
  }

  EXPECT_THROW(PartitionedTestDatasetReader(pool_.get(), outputType, path, 1, {}),
               std::runtime_error);
}

TEST_F(LanceCommitExecutionTest, AtomicCommitMergesWorkersAndIsIdempotent) {
  auto path = (tmpDir_ / "distributed_create.lance").string();
  auto outputType = ROW({"id"}, {BIGINT()});
  auto firstConfig = createConfig(
      path,
      "distributed-create",
      outputType,
      LanceWriteOperation::kCreate,
      0,
      0);
  auto secondConfig = createConfig(
      path,
      "distributed-create",
      outputType,
      LanceWriteOperation::kCreate,
      0,
      1);

  auto makeBatch = [&](int64_t value) {
    auto ids = BaseVector::create(BIGINT(), 1, pool_.get());
    ids->asFlatVector<int64_t>()->set(0, value);
    return std::make_shared<RowVector>(
        pool_.get(), outputType, nullptr, 1,
        std::vector<VectorPtr>{ids});
  };

  LanceBatchWriter first(pool_, outputType, firstConfig);
  first.writeBatch(makeBatch(10));
  auto firstFragment = first.finish();
  LanceBatchWriter second(pool_, outputType, secondConfig);
  second.writeBatch(makeBatch(20));
  auto secondFragment = second.finish();

  EXPECT_THROW(
      PartitionedTestDatasetReader(pool_.get(), outputType, path, 1, {}),
      std::runtime_error);
  EXPECT_EQ(
      commitFragments(
          firstConfig, {firstFragment, secondFragment}, false, 0,
          {firstConfig.transactionStagingPrefix,
           secondConfig.transactionStagingPrefix}),
      1);
  EXPECT_EQ(datasetVersion(path), 1);
  EXPECT_EQ(datasetSchemaFence(path), firstConfig.schemaFenceSha256);
  EXPECT_EQ(
      datasetInfo(path),
      std::make_pair(uint64_t{1}, firstConfig.schemaFenceSha256));
  const auto firstMetadata = parseFragment(firstFragment);
  const auto secondMetadata = parseFragment(secondFragment);
  discardFragments(
      firstConfig,
      {firstFragment, secondFragment},
      {firstConfig.transactionStagingPrefix,
       secondConfig.transactionStagingPrefix});
  EXPECT_FALSE(std::filesystem::exists(
      std::filesystem::path(path) /
      firstMetadata.transaction().relative_object_key()));
  EXPECT_FALSE(std::filesystem::exists(
      std::filesystem::path(path) /
      secondMetadata.transaction().relative_object_key()));
  // Cleanup is idempotent and recovery checks the durable committed version
  // before attempting to read sidecars that were already reclaimed.
  discardFragments(
      firstConfig,
      {firstFragment, secondFragment},
      {firstConfig.transactionStagingPrefix,
       secondConfig.transactionStagingPrefix});
  // Recovery uses the connector's durable commit-journal version and checks
  // exactly that manifest instead of scanning table history.
  EXPECT_EQ(
      commitFragments(
          firstConfig, {firstFragment, secondFragment}, true, 1,
          {firstConfig.transactionStagingPrefix,
           secondConfig.transactionStagingPrefix}),
      1);

  PartitionedTestDatasetReader reader(pool_.get(), outputType, path, datasetVersion(path), {});
  int64_t rows = 0;
  while (auto batch = reader.next(1024)) {
    rows += batch->size();
  }
  EXPECT_EQ(rows, 2);
}

TEST_F(LanceCommitExecutionTest, CommitRejectsFenceAndPayloadCorruption) {
  auto path = (tmpDir_ / "fenced_create.lance").string();
  auto outputType = ROW({"id"}, {BIGINT()});
  auto ids = BaseVector::create(BIGINT(), 1, pool_.get());
  ids->asFlatVector<int64_t>()->set(0, 7);
  auto batch = std::make_shared<RowVector>(
      pool_.get(), outputType, nullptr, 1,
      std::vector<VectorPtr>{ids});
  auto config = createConfig(path, "fenced-create", outputType);
  LanceBatchWriter writer(pool_, outputType, config);
  writer.writeBatch(batch);
  auto fragment = writer.finish();

  auto wrongFence = config;
  wrongFence.schemaFenceSha256[0] ^= 0xff;
  EXPECT_THROW(commitFragment(wrongFence, fragment), std::runtime_error);

  std::vector<uint8_t> malformed{0xff, 0xff, 0xff};
  EXPECT_THROW(commitFragment(config, malformed), std::runtime_error);

  EXPECT_THROW(
      commitFragments(
          config, {fragment}, false, 0, {"other/write/task-0"}),
      std::runtime_error);

  auto arbitraryUri = parseFragment(fragment);
  arbitraryUri.mutable_transaction()->set_relative_object_key(
      "https://attacker.invalid/transaction.pb");
  const auto arbitraryUriBytes = arbitraryUri.SerializeAsString();
  EXPECT_THROW(
      commitFragment(
          config,
          std::vector<uint8_t>(
              arbitraryUriBytes.begin(), arbitraryUriBytes.end())),
      std::runtime_error);

  auto wrongChecksum = parseFragment(fragment);
  wrongChecksum.mutable_transaction()->mutable_sha256()->set_digest(
      std::string(32, '\xff'));
  const auto wrongChecksumBytes = wrongChecksum.SerializeAsString();
  EXPECT_THROW(
      commitFragment(
          config,
          std::vector<uint8_t>(
              wrongChecksumBytes.begin(), wrongChecksumBytes.end())),
      std::runtime_error);
  EXPECT_EQ(commitFragment(config, fragment), 1);
}

TEST_F(LanceCommitExecutionTest, AppendAndOverwriteHonorVersionAndModeFences) {
  auto path = writeSimpleDataset(2);
  auto outputType = ROW({"id", "name"}, {BIGINT(), VARCHAR()});
  auto makeBatch = [&](int64_t id, const std::string& name) {
    auto ids = BaseVector::create(BIGINT(), 1, pool_.get());
    auto names = BaseVector::create(VARCHAR(), 1, pool_.get());
    ids->asFlatVector<int64_t>()->set(0, id);
    names->asFlatVector<StringView>()->set(0, StringView(name));
    return std::make_shared<RowVector>(
        pool_.get(), outputType, nullptr, 1,
        std::vector<VectorPtr>{ids, names});
  };

  auto appendConfig = createConfig(
      path, "append-v1", outputType, LanceWriteOperation::kAppend, 1);
  LanceBatchWriter append(pool_, outputType, appendConfig);
  append.writeBatch(makeBatch(3, "row_3"));
  auto appendFragment = append.finish();
  EXPECT_EQ(commitFragment(appendConfig, appendFragment), 2);
  EXPECT_EQ(
      commitFragments(appendConfig, {appendFragment}, true, 2), 2);

  auto overwriteConfig = createConfig(
      path, "overwrite-v2", outputType, LanceWriteOperation::kOverwrite, 2);
  LanceBatchWriter overwrite(pool_, outputType, overwriteConfig);
  overwrite.writeBatch(makeBatch(99, "replacement"));
  auto overwriteFragment = overwrite.finish();
  EXPECT_EQ(commitFragment(overwriteConfig, overwriteFragment), 3);

  PartitionedTestDatasetReader original(pool_.get(), outputType, path, 1, {});
  auto originalBatch = original.next(1024);
  ASSERT_NE(originalBatch, nullptr);
  EXPECT_EQ(originalBatch->size(), 2);
  EXPECT_EQ(original.next(1024), nullptr);

  PartitionedTestDatasetReader appended(pool_.get(), outputType, path, 2, {});
  std::vector<int64_t> appendedIds;
  while (auto appendedBatch = appended.next(1024)) {
    for (int32_t row = 0; row < appendedBatch->size(); ++row) {
      appendedIds.push_back(
          appendedBatch->childAt(0)->asFlatVector<int64_t>()->valueAt(row));
    }
  }
  EXPECT_EQ(appendedIds, (std::vector<int64_t>{1, 2, 3}));

  PartitionedTestDatasetReader reader(pool_.get(), outputType, path, 3, {});
  auto batch = reader.next(1024);
  ASSERT_NE(batch, nullptr);
  ASSERT_EQ(batch->size(), 1);
  EXPECT_EQ(batch->childAt(0)->asFlatVector<int64_t>()->valueAt(0), 99);
  EXPECT_EQ(reader.next(1024), nullptr);

  auto staleOverwrite = createConfig(
      path, "overwrite-stale-v2", outputType,
      LanceWriteOperation::kOverwrite, 2);
  LanceBatchWriter staleOverwriteWriter(pool_, outputType, staleOverwrite);
  staleOverwriteWriter.writeBatch(makeBatch(100, "stale-overwrite"));
  auto staleOverwriteFragment = staleOverwriteWriter.finish();
  EXPECT_THROW(
      commitFragment(staleOverwrite, staleOverwriteFragment),
      std::runtime_error);
  EXPECT_EQ(datasetVersion(path), 3);

  // A base which was never accepted is rejected by the actual PG allocation
  // before a writer can open it. Keep the complete stale-write oracle bounded
  // by the failure assertion, including later physical stages if admission
  // ever permits this invalid base.
  EXPECT_THROW(
      ([&] {
        auto stale = createConfig(
            path, "append-stale", outputType, LanceWriteOperation::kAppend, 99);
        LanceBatchWriter staleWriter(pool_, outputType, stale);
        staleWriter.writeBatch(makeBatch(100, "stale"));
        staleWriter.finish();
      }()),
      std::runtime_error);
  EXPECT_EQ(datasetVersion(path), 3);
}

TEST_F(LanceCommitExecutionTest, ConcurrentOverwritesPublishOnlyOneVersion) {
  auto path = writeSimpleDataset(1);
  auto outputType = ROW({"id", "name"}, {BIGINT(), VARCHAR()});
  auto makeBatch = [&](int64_t id) {
    auto ids = BaseVector::create(BIGINT(), 1, pool_.get());
    auto names = BaseVector::create(VARCHAR(), 1, pool_.get());
    ids->asFlatVector<int64_t>()->set(0, id);
    names->asFlatVector<StringView>()->set(0, StringView("replacement"));
    return std::make_shared<RowVector>(
        pool_.get(), outputType, nullptr, 1,
        std::vector<VectorPtr>{ids, names});
  };
  auto firstConfig = createConfig(
      path, "overwrite-race-a", outputType, LanceWriteOperation::kOverwrite,
      1, 0);
  auto secondConfig = createConfig(
      path, "overwrite-race-b", outputType, LanceWriteOperation::kOverwrite,
      1, 1);
  LanceBatchWriter first(pool_, outputType, firstConfig);
  LanceBatchWriter second(pool_, outputType, secondConfig);
  first.writeBatch(makeBatch(10));
  second.writeBatch(makeBatch(20));
  auto firstFragment = first.finish();
  auto secondFragment = second.finish();
  std::barrier start(2);
  auto attempt = [&start](const LanceWriterConfig& config,
                          const std::vector<uint8_t>& fragment) {
    start.arrive_and_wait();
    try {
      commitFragment(config, fragment);
      return true;
    } catch (const std::runtime_error&) {
      return false;
    }
  };
  auto firstCommit = std::async(
      std::launch::async, attempt, firstConfig, firstFragment);
  auto secondCommit = std::async(
      std::launch::async, attempt, secondConfig, secondFragment);
  EXPECT_NE(firstCommit.get(), secondCommit.get());
  EXPECT_EQ(datasetVersion(path), 2);
  PartitionedTestDatasetReader reader(pool_.get(), outputType, path, datasetVersion(path), {});
  auto batch = reader.next(1024);
  ASSERT_NE(batch, nullptr);
  ASSERT_EQ(batch->size(), 1);
  EXPECT_EQ(reader.next(1024), nullptr);
}

}  // namespace
}  // namespace pangrova::lance::test
