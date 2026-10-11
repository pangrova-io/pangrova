#include <gtest/gtest.h>

#include <array>
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>
#include <unordered_set>

#include <arrow/buffer.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/ipc/writer.h>
#include <openssl/evp.h>

#include "../TestMemoryHelper.h"
#include "google/protobuf/unknown_field_set.h"
#include "jni/command/NativeCommandDispatcher.h"
#include "lance/LanceBatchReader.h"
#include "lance/LanceBatchWriter.h"
#include "../lance/LanceTestDataset.h"
#include "lance_c_ffi.h"
#include "pangrova/protocol/v2/runtime.pb.h"
#include "velox/vector/arrow/Bridge.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/DecodedVector.h"

namespace pangrova::jni::test {
namespace {
namespace proto = protocol::v2;
using namespace facebook::velox;

uint64_t deadline() {
  return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count()) + 60'000;
}

std::string digest(const std::string& bytes) {
  std::array<uint8_t, 32> output{};
  unsigned int size = 0;
  EXPECT_EQ(EVP_Digest(bytes.data(), bytes.size(), output.data(), &size, EVP_sha256(), nullptr), 1);
  EXPECT_EQ(size, 32u);
  return {reinterpret_cast<const char*>(output.data()), output.size()};
}

void checksum(proto::Checksum* checksum, const std::string& bytes) {
  checksum->set_algorithm(proto::Checksum::ALGORITHM_SHA256);
  checksum->set_digest(digest(bytes));
}

template <typename Request>
proto::RuntimeCommandResponse dispatch(proto::RuntimeCommandType type, const Request& request) {
  proto::RuntimeCommand command;
  command.mutable_protocol()->set_major(2);
  command.set_command_type(type);
  command.mutable_payload()->PackFrom(request);
  proto::ObjectStoreExecutionBinding binding;
  if constexpr (std::is_same_v<Request, proto::PrepareDistributedWriteRequest>) {
    binding.ParseFromString(pangrova::test::testStorageBinding(request.output_binding().lance().dataset_uri(),
        request.output_binding().lance().catalog(), request.security().tenant_id()));
  } else if constexpr (std::is_same_v<Request, proto::DatasetScanPartitionsRequest>) {
    binding.ParseFromString(pangrova::test::testStorageBinding(request.dataset_uri(),
        request.catalog(), request.security().tenant_id()));
  } else {
    *binding.mutable_catalog() = request.catalog();
    binding.set_tenant_id(request.security().tenant_id());
    binding.set_warehouse_uri(pangrova::test::LanceManagedSourceTestAuthority::warehouse().string());
  }
  *command.add_worker_local_storage_bindings() = binding;
  const auto serialized = command.SerializeAsString();
  const auto bytes = dispatchCommand({serialized.begin(), serialized.end()});
  proto::RuntimeCommandResponse response;
  EXPECT_TRUE(response.ParseFromArray(bytes.data(), bytes.size()));
  return response;
}

void catalog(proto::CatalogExecutionBinding* value) {
  *value = pangrova::test::LanceManagedSourceTestAuthority::catalog();
}

proto::DatasetSchemaFingerprintRequest schemaRequest() {
  proto::DatasetSchemaFingerprintRequest request;
  catalog(request.mutable_catalog());
  request.mutable_security()->set_tenant_id("tenant-a");
  request.mutable_security()->set_principal("lance-fixture-writer");
  request.set_deadline_epoch_ms(deadline());
  request.mutable_schema()->add_names("id");
  auto* type = request.mutable_schema()->mutable_struct_()->add_types();
  type->mutable_i64()->set_nullability(substrait::Type_Nullability_NULLABILITY_NULLABLE);
  auto* logical = request.add_column_types();
  logical->set_canonical_sql("BIGINT");
  *logical->mutable_substrait_type() = *type;
  return request;
}

bool isPersistedManifestDigest(const std::filesystem::path& dataset,
    const proto::AcceptedSourceSnapshotRef& expected) {
  std::ifstream input(dataset / expected.manifest_relative_path(), std::ios::binary);
  if (!input) return false;
  const std::string bytes((std::istreambuf_iterator<char>(input)), {});
  return bytes.size() == expected.size_bytes() && digest(bytes) == expected.manifest_sha256().digest();
}

class NativeDatasetControlTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = pangrova::test::testLeafPool("NativeDatasetControlTest");
    directory_ = pangrova::test::LanceManagedSourceTestAuthority::warehouse() /
        ("pangrova-dataset-control-" + std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(directory_);
  }

  void TearDown() override {
    preparation_actor_.reset();
    pool_.reset();
    std::error_code ignored;
    std::filesystem::remove_all(directory_, ignored);
  }

  lance::LanceWriterConfig allocationConfig(const std::string& writeId,
      proto::LanceWriteOperation operation, uint64_t base, int32_t partition) {
    const auto response = dispatch(proto::RUNTIME_COMMAND_TYPE_DATASET_SCHEMA_FINGERPRINT, schemaRequest());
    EXPECT_TRUE(response.success()) << response.error().message();
    proto::DatasetSchemaFingerprintResult fingerprint;
    EXPECT_TRUE(response.payload().UnpackTo(&fingerprint));
    lance::LanceWriterConfig config;
    config.outputPath = (directory_ / "dataset.lance").string();
    config.storageBinding = pangrova::test::testStorageBinding(config.outputPath);
    config.writeId = writeId; config.tenantId = "tenant-a"; config.generation = "generation-1";
    config.operation = static_cast<lance::LanceWriteOperation>(operation); config.expectedReadVersion = base;
    std::copy(fingerprint.physical_schema_sha256().digest().begin(),
        fingerprint.physical_schema_sha256().digest().end(), config.schemaFenceSha256.begin());
    const auto empty = BaseVector::create(ROW({"id"}, {BIGINT()}), 0, pool_.get());
    ArrowSchema physical{}; exportToArrow(empty, physical);
    auto schema = arrow::ImportSchema(&physical);
    if (!schema.ok()) throw std::runtime_error(schema.status().ToString());
    auto ipc = arrow::ipc::SerializeSchema(**schema, arrow::default_memory_pool());
    if (!ipc.ok()) throw std::runtime_error(ipc.status().ToString());
    pangrova::test::LanceManagedSourceTestAuthority::allocate(config,
        std::string(reinterpret_cast<const char*>((*ipc)->data()), (*ipc)->size()), partition);
    return config;
  }

  proto::PrepareDistributedWriteRequest publication(const std::string& writeId,
      proto::LanceWriteOperation operation = proto::LANCE_WRITE_OPERATION_CREATE, uint64_t base = 0) {
    const auto config = allocationConfig(writeId, operation, base, 0);
    proto::TaskAttemptId issued;
    EXPECT_TRUE(issued.ParseFromArray(config.taskAttempt.data(), config.taskAttempt.size()));
    proto::PrepareDistributedWriteRequest request;
    request.mutable_security()->set_tenant_id("tenant-a");
    request.mutable_security()->set_principal("lance-fixture-writer");
    *request.mutable_output_binding() = pangrova::test::LanceManagedSourceTestAuthority::allocationBinding(config);
    request.set_execution_id(issued.query_or_job_id()); request.set_deadline_epoch_ms(deadline());
    request.set_control_epoch(issued.control_epoch());
    *request.mutable_execution_owner() = issued.execution_owner();
    *request.mutable_intent_execution_owner() = issued.execution_owner();
    return request;
  }

  void addWinner(proto::PrepareDistributedWriteRequest& request, int32_t partition) {
    const auto& output = request.output_binding().lance();
    auto config = allocationConfig(output.write_id(), output.operation(), output.expected_read_version(), partition);
    const auto type = ROW({"id"}, {BIGINT()});
    lance::LanceBatchWriter writer(pool_, type, config);
    auto values = BaseVector::create(BIGINT(), 2, pool_.get());
    values->asFlatVector<int64_t>()->set(0, partition * 10);
    values->asFlatVector<int64_t>()->set(1, partition * 10 + 1);
    auto rows = std::make_shared<RowVector>(pool_.get(), type, nullptr, 2, std::vector<VectorPtr>{values});
    writer.writeBatch(rows);
    const auto bytes = writer.finish();
    auto* winner = request.add_fragments();
    EXPECT_TRUE(winner->mutable_task_attempt()->ParseFromArray(config.taskAttempt.data(), config.taskAttempt.size()));
    winner->set_commit_metadata(bytes.data(), bytes.size());
    checksum(winner->mutable_sha256(), winner->commit_metadata());
  }

  void seal(proto::PrepareDistributedWriteRequest& request) {
    if (!request.commit_fingerprint().empty()) return;
    std::vector<std::vector<uint8_t>> fragments;
    for (const auto& winner : request.fragments()) {
      fragments.emplace_back(winner.commit_metadata().begin(), winner.commit_metadata().end());
    }
    if (!preparation_actor_) preparation_actor_ = std::make_unique<pangrova::test::LanceSdkPreparationActor>();
    request = pangrova::test::LanceManagedSourceTestAuthority::issue(
        pangrova::test::LanceManagedSourceTestAuthority::reserve(request.execution_id(), fragments), *preparation_actor_);
  }

  proto::PrepareDistributedWriteResult prepare(proto::PrepareDistributedWriteRequest& request) {
    seal(request);
    if (!invoked_.insert(request.invocation_id()).second || !preparation_actor_->matches(request)) {
      request = pangrova::test::LanceManagedSourceTestAuthority::issue(request, *preparation_actor_);
      EXPECT_TRUE(invoked_.insert(request.invocation_id()).second);
    }
    const auto response = preparation_actor_->prepare(request);
    EXPECT_TRUE(response.success()) << response.error().message();
    proto::PrepareDistributedWriteResult result;
    EXPECT_TRUE(response.payload().UnpackTo(&result)); EXPECT_FALSE(response.has_error());
    return result;
  }

  proto::AcceptedSourceSnapshotRef accept(const proto::PrepareDistributedWriteRequest& request,
      const proto::PrepareDistributedWriteResult& prepared) {
    const auto accepted = pangrova::test::LanceManagedSourceTestAuthority::registerPrepared(request.execution_id(), prepared);
    if (!accepted) throw std::runtime_error("Actual PG did not accept complete SDK candidate");
    EXPECT_EQ(accepted->SerializeAsString(), prepared.publication().candidate().SerializeAsString());
    return *accepted;
  }

  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
  std::filesystem::path directory_;
  std::unordered_set<std::string> invoked_;
  std::unique_ptr<pangrova::test::LanceSdkPreparationActor> preparation_actor_;
};

TEST_F(NativeDatasetControlTest, ExactPgIssuanceRunsOnItsListeningSdkActorBeforeAcceptedRead) {
  auto request = publication("actual-sdk-actor");
  addWinner(request, 0);
  seal(request);
  const auto endpoint = preparation_actor_->endpoint();
  ASSERT_TRUE(std::filesystem::exists(endpoint));
  const auto result = prepare(request);
  ASSERT_EQ(result.outcome(), proto::WRITE_PREPARATION_OUTCOME_PREPARED);
  ASSERT_TRUE(result.sdk_joined());
  EXPECT_EQ(result.invocation_id(), request.invocation_id());
  const auto accepted = accept(request, result);
  EXPECT_TRUE(isPersistedManifestDigest(directory_ / "dataset.lance", accepted));
  pangrova::test::PartitionedTestDatasetReader reader(pool_.get(), ROW({"id"}, {BIGINT()}),
      accepted.dataset_uri(), accepted.version(), {});
  std::vector<int64_t> rows;
  while (auto batch = reader.next(8192)) {
    const DecodedVector values(*batch->childAt(0));
    for (vector_size_t row = 0; row < batch->size(); ++row) rows.push_back(values.valueAt<int64_t>(row));
  }
  EXPECT_EQ(rows, (std::vector<int64_t>{0, 1}));
  preparation_actor_.reset();
  EXPECT_FALSE(std::filesystem::exists(endpoint));
}

TEST_F(NativeDatasetControlTest, SchemaCommandUsesExecutionTypesAndRealPhysicalArrowFingerprint) {
  auto request = schemaRequest();
  request.mutable_schema()->add_names("embedding");
  auto* vector = request.add_column_types();
  vector->set_canonical_sql("VECTOR(4)");
  auto* list = vector->mutable_substrait_type()->mutable_list();
  list->set_nullability(substrait::Type_Nullability_NULLABILITY_NULLABLE);
  list->mutable_type()->mutable_fp32()->set_nullability(substrait::Type_Nullability_NULLABILITY_REQUIRED);
  vector->add_constraints()->set_name("dimension");
  vector->mutable_constraints(0)->set_integer_value(4);
  vector->add_constraints()->set_name("element_type");
  vector->mutable_constraints(1)->set_string_value("FLOAT32");
  *request.mutable_schema()->mutable_struct_()->add_types() = vector->substrait_type();
  auto response = dispatch(proto::RUNTIME_COMMAND_TYPE_DATASET_SCHEMA_FINGERPRINT, request);
  ASSERT_TRUE(response.success()) << response.error().message();
  proto::DatasetSchemaFingerprintResult result;
  ASSERT_TRUE(response.payload().UnpackTo(&result));
  const auto empty = BaseVector::create(ROW({"id", "embedding"}, {BIGINT(), ARRAY(REAL())}), 0, pool_.get());
  ArrowSchema physical{};
  exportToArrow(empty, physical);
  std::array<uint8_t, 32> expected{};
  const int rc = lance_ffi_schema_fingerprint(&physical, expected.data(), expected.size());
  if (physical.release) physical.release(&physical);
  ASSERT_EQ(rc, 0);
  EXPECT_EQ(result.physical_schema_sha256().algorithm(), proto::Checksum::ALGORITHM_SHA256);
  EXPECT_EQ(result.physical_schema_sha256().digest(),
            std::string(reinterpret_cast<const char*>(expected.data()), expected.size()));
  vector->mutable_constraints(0)->set_integer_value(8);
  EXPECT_FALSE(dispatch(proto::RUNTIME_COMMAND_TYPE_DATASET_SCHEMA_FINGERPRINT, request).success());
  vector->mutable_constraints(0)->set_integer_value(4);
  vector->set_canonical_sql("VECTOR(4,DOUBLE)");
  vector->mutable_constraints(1)->set_string_value("FLOAT64");
  vector->mutable_substrait_type()->mutable_list()->mutable_type()->mutable_fp64()
      ->set_nullability(substrait::Type_Nullability_NULLABILITY_REQUIRED);
  *request.mutable_schema()->mutable_struct_()->mutable_types(1) = vector->substrait_type();
  const auto double_response = dispatch(proto::RUNTIME_COMMAND_TYPE_DATASET_SCHEMA_FINGERPRINT, request);
  ASSERT_TRUE(double_response.success()) << double_response.error().message();
  proto::DatasetSchemaFingerprintResult double_result;
  ASSERT_TRUE(double_response.payload().UnpackTo(&double_result));
  EXPECT_NE(double_result.physical_schema_sha256().digest(), result.physical_schema_sha256().digest());
  const auto double_empty = BaseVector::create(
      ROW({"id", "embedding"}, {BIGINT(), ARRAY(DOUBLE())}), 0, pool_.get());
  ArrowSchema double_physical{};
  exportToArrow(double_empty, double_physical);
  const auto double_rc = lance_ffi_schema_fingerprint(&double_physical, expected.data(), expected.size());
  if (double_physical.release) double_physical.release(&double_physical);
  ASSERT_EQ(double_rc, 0);
  EXPECT_EQ(double_result.physical_schema_sha256().digest(),
            std::string(reinterpret_cast<const char*>(expected.data()), expected.size()));
}

TEST_F(NativeDatasetControlTest, NestedNamesFollowStandardDepthFirstOrder) {
  auto request = schemaRequest();
  for (const auto* name : {"items", "quantity", "description"}) request.mutable_schema()->add_names(name);
  auto* nested = request.mutable_schema()->mutable_struct_()->add_types();
  auto* fields = nested->mutable_list()->mutable_type()->mutable_struct_();
  fields->add_types()->mutable_i32();
  fields->add_types()->mutable_string();
  auto* logical = request.add_column_types();
  logical->set_canonical_sql("ARRAY<ROW<quantity INTEGER,description VARCHAR>>");
  *logical->mutable_substrait_type() = *nested;
  const auto response = dispatch(proto::RUNTIME_COMMAND_TYPE_DATASET_SCHEMA_FINGERPRINT, request);
  ASSERT_TRUE(response.success()) << response.error().message();
  proto::DatasetSchemaFingerprintResult result;
  ASSERT_TRUE(response.payload().UnpackTo(&result));
  const auto empty = BaseVector::create(
      ROW({"id", "items"}, {BIGINT(), ARRAY(ROW({"quantity", "description"}, {INTEGER(), VARCHAR()}))}),
      0, pool_.get());
  ArrowSchema physical{};
  exportToArrow(empty, physical);
  std::array<uint8_t, 32> expected{};
  const auto rc = lance_ffi_schema_fingerprint(&physical, expected.data(), expected.size());
  if (physical.release) physical.release(&physical);
  ASSERT_EQ(rc, 0);
  EXPECT_EQ(result.physical_schema_sha256().digest(),
            std::string(reinterpret_cast<const char*>(expected.data()), expected.size()));
  request.mutable_schema()->set_names(3, "quantity");
  EXPECT_FALSE(dispatch(proto::RUNTIME_COMMAND_TYPE_DATASET_SCHEMA_FINGERPRINT, request).success());
}

TEST_F(NativeDatasetControlTest, SchemaCommandRejectsIncompleteTypedMetadata) {
  const std::vector<std::function<void(proto::DatasetSchemaFingerprintRequest&)>> mutations{
      [](auto& r) { r.set_deadline_epoch_ms(0); },
      [](auto& r) { r.mutable_security()->clear_tenant_id(); },
      [](auto& r) { r.mutable_catalog()->set_catalog_revision(0); },
      [](auto& r) { r.mutable_schema()->clear_names(); },
      [](auto& r) { r.mutable_schema()->add_names("unused"); },
      [](auto& r) { r.clear_column_types(); },
      [](auto& r) { r.mutable_column_types(0)->mutable_substrait_type()->mutable_fp64(); }};
  for (const auto& mutate : mutations) {
    auto request = schemaRequest();
    mutate(request);
    const auto response = dispatch(proto::RUNTIME_COMMAND_TYPE_DATASET_SCHEMA_FINGERPRINT, request);
    EXPECT_FALSE(response.success());
    EXPECT_EQ(response.error().error_class(), proto::ERROR_CLASS_INVALID_REQUEST);
    EXPECT_FALSE(response.has_payload());
  }
}

TEST_F(NativeDatasetControlTest, PublishesAllWinningFragmentsAndReturnsPersistedManifestDigest) {
  auto request = publication("create-1"); addWinner(request, 0); addWinner(request, 1);
  auto result = prepare(request);
  ASSERT_EQ(result.outcome(), proto::WRITE_PREPARATION_OUTCOME_PREPARED);
  EXPECT_EQ(result.commit_fingerprint(), request.commit_fingerprint());
  const auto recovered = prepare(request);
  const auto accepted = accept(request, result);
  EXPECT_EQ(accepted.version(), 1u);
  EXPECT_TRUE(isPersistedManifestDigest(directory_ / "dataset.lance", accepted));
  EXPECT_NE(accepted.manifest_sha256().digest(), request.output_binding().lance().schema_fence_sha256().digest());
  EXPECT_FALSE(std::filesystem::exists(directory_ / "dataset.lance" / "_versions"));
  pangrova::test::PartitionedTestDatasetReader reader(pool_.get(), ROW({"id"}, {BIGINT()}),
      request.output_binding().lance().dataset_uri(), accepted.version(), {});
  int64_t rows = 0; while (auto batch = reader.next(8192)) rows += batch->size(); EXPECT_EQ(rows, 4);
  EXPECT_EQ(recovered.outcome(), proto::WRITE_PREPARATION_OUTCOME_PREPARED);
  EXPECT_EQ(recovered.publication().candidate().version(), accepted.version());
  EXPECT_EQ(recovered.publication().candidate().manifest_sha256().digest(), accepted.manifest_sha256().digest());
  EXPECT_EQ(pangrova::test::LanceManagedSourceTestAuthority::reference(accepted.dataset_uri(), accepted.version()).SerializeAsString(),
      accepted.SerializeAsString());
  // A forged manifest proof cannot claim a successful read/publication acknowledgement.
  proto::DatasetScanPartitionsRequest forged;
  *forged.mutable_catalog() = accepted.catalog(); *forged.mutable_security() = request.security();
  forged.set_dataset_uri(accepted.dataset_uri()); forged.set_version(accepted.version());
  *forged.mutable_accepted_snapshot() = accepted;
  forged.mutable_accepted_snapshot()->mutable_manifest_sha256()->set_digest(std::string(32, 'x'));
  *forged.mutable_physical_schema_sha256() = accepted.physical_schema_sha256();
  forged.set_page_size(1); forged.set_deadline_epoch_ms(deadline());
  const auto mismatched = dispatch(proto::RUNTIME_COMMAND_TYPE_DATASET_SCAN_PARTITIONS, forged);
  EXPECT_FALSE(mismatched.success()); EXPECT_FALSE(mismatched.has_payload());
}

TEST_F(NativeDatasetControlTest, EnumeratesPinnedManifestPagesAndReadsEachPartitionExactlyOnce) {
  auto create = publication("partition-pages");
  addWinner(create, 0);
  addWinner(create, 1);
  const auto prepared = prepare(create);
  ASSERT_EQ(prepared.outcome(), proto::WRITE_PREPARATION_OUTCOME_PREPARED);
  const auto published = accept(create, prepared);
  const auto& output = create.output_binding().lance();
  proto::DatasetScanPartitionsRequest request;
  *request.mutable_catalog() = output.catalog();
  *request.mutable_security() = create.security();
  request.set_dataset_uri(output.dataset_uri());
  request.set_version(published.version());
  *request.mutable_accepted_snapshot() = published;
  *request.mutable_physical_schema_sha256() = output.schema_fence_sha256();
  request.set_page_size(1);
  request.set_deadline_epoch_ms(deadline());
  auto response = dispatch(proto::RUNTIME_COMMAND_TYPE_DATASET_SCAN_PARTITIONS, request);
  ASSERT_TRUE(response.success()) << response.error().message();
  proto::DatasetScanPartitionsResult first;
  ASSERT_TRUE(response.payload().UnpackTo(&first));
  ASSERT_EQ(first.partitions_size(), 1);
  ASSERT_EQ(first.total_partitions(), 2u);
  ASSERT_TRUE(first.has_more());
  EXPECT_EQ(first.manifest_sha256().digest(), published.manifest_sha256().digest());
  request.set_has_cursor(true);
  request.set_after_partition_id(first.next_partition_id());
  response = dispatch(proto::RUNTIME_COMMAND_TYPE_DATASET_SCAN_PARTITIONS, request);
  ASSERT_TRUE(response.success()) << response.error().message();
  proto::DatasetScanPartitionsResult second;
  ASSERT_TRUE(response.payload().UnpackTo(&second));
  ASSERT_EQ(second.partitions_size(), 1);
  EXPECT_FALSE(second.has_more());
  EXPECT_EQ(second.total_partitions(), 2u);
  EXPECT_EQ(second.manifest_sha256().digest(), first.manifest_sha256().digest());
  EXPECT_GT(second.partitions(0).partition_id(), first.partitions(0).partition_id());
  const auto storage = pangrova::test::testStorageBinding(output.dataset_uri(), output.catalog());
  std::vector<int64_t> ids;
  for (const auto* page : {&first, &second}) {
    const auto& partition = page->partitions(0);
    lance::LanceBatchReader reader(pool_.get(), ROW({"id"}, {BIGINT()}), output.dataset_uri(),
        request.version(), {}, storage, pangrova::test::testReadDetail(partition), 0);
    uint64_t count = 0;
    while (auto rows = reader.next(1)) {
      for (vector_size_t row = 0; row < rows->size(); ++row) {
        ids.push_back(rows->childAt(0)->asFlatVector<int64_t>()->valueAt(row));
      }
      count += rows->size();
    }
    EXPECT_EQ(count, partition.physical_rows());
    EXPECT_EQ(count, 2u);
  }
  std::sort(ids.begin(), ids.end());
  EXPECT_EQ(ids, (std::vector<int64_t>{0, 1, 10, 11}));
  auto forged = first.partitions(0);
  forged.set_physical_rows(forged.physical_rows() + 1);
  EXPECT_THROW(lance::LanceBatchReader(pool_.get(), ROW({"id"}, {BIGINT()}), output.dataset_uri(),
      request.version(), {}, storage, pangrova::test::testReadDetail(forged), 0), std::runtime_error);
  forged = first.partitions(0);
  forged.set_partition_id(UINT64_MAX);
  EXPECT_THROW(lance::LanceBatchReader(pool_.get(), ROW({"id"}, {BIGINT()}), output.dataset_uri(),
      request.version(), {}, storage, pangrova::test::testReadDetail(forged), 0), std::runtime_error);
  forged = first.partitions(0);
  forged.mutable_manifest_sha256()->set_digest(std::string(32, 'x'));
  EXPECT_THROW(lance::LanceBatchReader(pool_.get(), ROW({"id"}, {BIGINT()}), output.dataset_uri(),
      request.version(), {}, storage, pangrova::test::testReadDetail(forged), 0), std::runtime_error);
  request.set_after_partition_id(UINT64_MAX);
  EXPECT_FALSE(dispatch(proto::RUNTIME_COMMAND_TYPE_DATASET_SCAN_PARTITIONS, request).success());
  request.set_page_size(257);
  EXPECT_FALSE(dispatch(proto::RUNTIME_COMMAND_TYPE_DATASET_SCAN_PARTITIONS, request).success());
}

TEST_F(NativeDatasetControlTest, MetadataCannotUseUnboundOrCrossScopeWorkerStorage) {
  const auto request = schemaRequest();
  proto::RuntimeCommand command;
  command.mutable_protocol()->set_major(2);
  command.set_command_type(proto::RUNTIME_COMMAND_TYPE_DATASET_SCHEMA_FINGERPRINT);
  command.mutable_payload()->PackFrom(request);
  auto execute = [&]() {
    const auto serialized = command.SerializeAsString();
    const auto bytes = dispatchCommand({serialized.begin(), serialized.end()});
    proto::RuntimeCommandResponse response;
    EXPECT_TRUE(response.ParseFromArray(bytes.data(), bytes.size()));
    return response;
  };
  EXPECT_FALSE(execute().success());
  auto* binding = command.add_worker_local_storage_bindings();
  *binding->mutable_catalog() = request.catalog();
  binding->set_tenant_id("other-tenant");
  binding->set_warehouse_uri(directory_.string());
  EXPECT_FALSE(execute().success());
  binding->set_tenant_id(request.security().tenant_id());
  binding->mutable_catalog()->set_catalog_revision(request.catalog().catalog_revision() + 1);
  EXPECT_FALSE(execute().success());
  *binding->mutable_catalog() = request.catalog();
  EXPECT_TRUE(execute().success());
  *command.add_worker_local_storage_bindings() = *binding;
  EXPECT_FALSE(execute().success());
}

TEST_F(NativeDatasetControlTest, AppendAndOverwritePublishOnlyExactBasePlusOne) {
  auto create = publication("create-1"); addWinner(create, 0);
  const auto first = accept(create, prepare(create)); ASSERT_EQ(first.version(), 1u);
  auto append = publication("append-1", proto::LANCE_WRITE_OPERATION_APPEND, 1); addWinner(append, 0);
  const auto appendPrepared = prepare(append);
  ASSERT_EQ(appendPrepared.outcome(), proto::WRITE_PREPARATION_OUTCOME_PREPARED);
  const auto appended = accept(append, appendPrepared); ASSERT_EQ(appended.version(), 2u);
  EXPECT_TRUE(isPersistedManifestDigest(directory_ / "dataset.lance", appended));
  auto overwrite = publication("overwrite-1", proto::LANCE_WRITE_OPERATION_OVERWRITE, 2); addWinner(overwrite, 0);
  const auto overwritePrepared = prepare(overwrite);
  ASSERT_EQ(overwritePrepared.outcome(), proto::WRITE_PREPARATION_OUTCOME_PREPARED);
  const auto overwritten = accept(overwrite, overwritePrepared); ASSERT_EQ(overwritten.version(), 3u);
  auto stale = publication("stale-append", proto::LANCE_WRITE_OPERATION_APPEND, 1); addWinner(stale, 0);
  // The actual PG base CAS rejects before issuing a preparation RPC; no fake Native COMMITTED/UNKNOWN is returned.
  EXPECT_THROW(seal(stale), std::runtime_error);
  const auto current = pangrova::test::LanceManagedSourceTestAuthority::reference(
      stale.output_binding().lance().dataset_uri(), 0);
  EXPECT_EQ(current.version(), 3u); EXPECT_EQ(current.SerializeAsString(), overwritten.SerializeAsString());
}

TEST_F(NativeDatasetControlTest, ValidationErrorsCannotPublishOrClaimAnOutcome) {
  auto original = publication("validate-1");
  addWinner(original, 0);
  seal(original);
  const std::vector<std::function<void(proto::PrepareDistributedWriteRequest&)>> mutations{
      [](auto& r) { r.set_deadline_epoch_ms(0); },
      [](auto& r) { r.clear_execution_owner(); },
      [](auto& r) { r.clear_intent_execution_owner(); },
      [](auto& r) { r.mutable_execution_owner()->set_fencing_token(r.intent_execution_owner().fencing_token() - 1); },
      [](auto& r) { r.mutable_execution_owner()->set_owner_incarnation("different-at-same-real-token"); },
      [](auto& r) { r.set_control_epoch(0); },
      [](auto& r) { r.mutable_security()->set_tenant_id("other-tenant"); },
      [](auto& r) { r.mutable_output_binding()->mutable_lance()->set_generation("other-generation"); },
      [](auto& r) { r.mutable_output_binding()->mutable_lance()->set_expected_read_version(1); },
      [](auto& r) { r.mutable_output_binding()->mutable_lance()->mutable_schema_fence_sha256()->set_digest(std::string(32, 'x')); },
      [](auto& r) { r.mutable_output_binding()->mutable_lance()->set_allocation_staging_prefix("../escape"); },
      [](auto& r) { r.mutable_output_binding()->mutable_lance()->set_transaction_staging_prefix("bound-attempt"); },
      [](auto& r) { r.mutable_fragments(0)->mutable_sha256()->set_digest(std::string(32, 'x')); },
      [](auto& r) { r.mutable_fragments(0)->mutable_task_attempt()->set_attempt_id(3); },
      [](auto& r) { r.mutable_fragments(0)->mutable_task_attempt()->set_control_epoch(r.control_epoch() + 1); },
      [](auto& r) { r.mutable_fragments(0)->mutable_task_attempt()->mutable_execution_owner()->set_owner_incarnation("wrong-real-winner-owner"); },
      [](auto& r) { *r.add_fragments() = r.fragments(0); },
      [](auto& r) { r.GetReflection()->MutableUnknownFields(&r)->AddVarint(9, 1); },
      [](auto& r) { r.GetReflection()->MutableUnknownFields(&r)->AddVarint(10, 1); r.GetReflection()->MutableUnknownFields(&r)->AddVarint(9, 1); },
      [](auto& r) { r.clear_fragments(); }};
  for (const auto& mutate : mutations) {
    auto request = original;
    mutate(request);
    const auto response = dispatch(proto::RUNTIME_COMMAND_TYPE_PREPARE_DISTRIBUTED_WRITE, request);
    EXPECT_FALSE(response.success());
    EXPECT_EQ(response.error().error_class(), proto::ERROR_CLASS_INVALID_REQUEST);
    EXPECT_FALSE(response.has_payload());
  }
  EXPECT_FALSE(std::filesystem::exists(directory_ / "dataset.lance" / "_versions"));
}

TEST_F(NativeDatasetControlTest, CurrentAuthorityDoesNotRewriteOriginalWinnerEvidence) {
  auto request = publication("owner-fence"); addWinner(request, 0); seal(request);
  const auto original = request.intent_execution_owner();
  const auto winnerBefore = request.fragments(0).SerializeAsString();
  const auto current = pangrova::test::LanceManagedSourceTestAuthority::handoff(request.execution_id());
  *request.mutable_execution_owner() = current;
  ASSERT_GT(request.execution_owner().fencing_token(), original.fencing_token());
  ASSERT_EQ(request.intent_execution_owner().SerializeAsString(), original.SerializeAsString());
  ASSERT_EQ(request.fragments(0).task_attempt().execution_owner().SerializeAsString(), original.SerializeAsString());
  ASSERT_LE(request.fragments(0).task_attempt().control_epoch(), request.control_epoch());
  auto future = request;
  future.mutable_fragments(0)->mutable_task_attempt()->set_control_epoch(request.control_epoch() + 1);
  auto response = dispatch(proto::RUNTIME_COMMAND_TYPE_PREPARE_DISTRIBUTED_WRITE, future);
  EXPECT_FALSE(response.success()); EXPECT_EQ(response.error().error_class(), proto::ERROR_CLASS_INVALID_REQUEST);
  EXPECT_NE(response.error().message().find("execution owner/control fence"), std::string::npos);
  auto wrongOwner = request;
  *wrongOwner.mutable_fragments(0)->mutable_task_attempt()->mutable_execution_owner() = request.execution_owner();
  response = dispatch(proto::RUNTIME_COMMAND_TYPE_PREPARE_DISTRIBUTED_WRITE, wrongOwner);
  EXPECT_FALSE(response.success()); EXPECT_NE(response.error().message().find("execution owner/control fence"), std::string::npos);
  const auto result = prepare(request); EXPECT_EQ(result.outcome(), proto::WRITE_PREPARATION_OUTCOME_PREPARED);
  const auto accepted = accept(request, result); EXPECT_EQ(accepted.version(), 1u);
  EXPECT_EQ(request.fragments(0).SerializeAsString(), winnerBefore);
}

TEST_F(NativeDatasetControlTest, RejectsOversizedMetadataAndTooManyFragmentsBeforeFfi) {
  auto request = publication("bounded-1");
  addWinner(request, 0);
  seal(request);
  const auto winner = request.fragments(0);
  while (request.fragments_size() <= 4096) *request.add_fragments() = winner;
  const auto many = dispatch(proto::RUNTIME_COMMAND_TYPE_PREPARE_DISTRIBUTED_WRITE, request);
  EXPECT_FALSE(many.success());
  EXPECT_EQ(many.error().error_class(), proto::ERROR_CLASS_INVALID_REQUEST);
  request = publication("bounded-2");
  request.mutable_output_binding()->mutable_lance()->mutable_output_handle()
      ->set_payload(std::string(16 * 1024 * 1024, 'x'));
  const auto oversized = dispatch(proto::RUNTIME_COMMAND_TYPE_PREPARE_DISTRIBUTED_WRITE, request);
  EXPECT_FALSE(oversized.success());
  EXPECT_EQ(oversized.error().error_class(), proto::ERROR_CLASS_INVALID_REQUEST);
  EXPECT_FALSE(std::filesystem::exists(directory_ / "dataset.lance" / "_versions"));
}

}  // namespace
}  // namespace pangrova::jni::test
