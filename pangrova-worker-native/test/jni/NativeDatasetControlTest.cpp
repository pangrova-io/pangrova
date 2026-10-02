#include <gtest/gtest.h>

#include <array>
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <string>
#include <type_traits>
#include <vector>

#include <arrow/c/abi.h>
#include <openssl/evp.h>

#include "../TestMemoryHelper.h"
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/io/zero_copy_stream_impl_lite.h"
#include "jni/command/NativeCommandDispatcher.h"
#include "lance/LanceBatchReader.h"
#include "lance/LanceBatchWriter.h"
#include "../lance/LanceTestDataset.h"
#include "lance_c_ffi.h"
#include "pangrova/protocol/v2/runtime.pb.h"
#include "velox/vector/arrow/Bridge.h"
#include "velox/vector/FlatVector.h"

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

std::string hex(const std::string& bytes) {
  constexpr char digits[] = "0123456789abcdef";
  std::string output;
  for (unsigned char value : bytes) {
    output.push_back(digits[value >> 4]);
    output.push_back(digits[value & 15]);
  }
  return output;
}

std::string canonical(const google::protobuf::Message& message) {
  std::string bytes(message.ByteSizeLong(), '\0');
  google::protobuf::io::ArrayOutputStream output(bytes.data(), bytes.size());
  google::protobuf::io::CodedOutputStream coded(&output);
  coded.SetSerializationDeterministic(true);
  EXPECT_TRUE(message.SerializeToCodedStream(&coded));
  EXPECT_FALSE(coded.HadError());
  return bytes;
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
  if constexpr (std::is_same_v<Request, proto::FinalizeDistributedWriteRequest>) {
    binding.ParseFromString(pangrova::test::testStorageBinding(request.output_binding().lance().dataset_uri(),
        request.output_binding().lance().catalog(), request.security().tenant_id()));
  } else if constexpr (std::is_same_v<Request, proto::DatasetScanPartitionsRequest>) {
    binding.ParseFromString(pangrova::test::testStorageBinding(request.dataset_uri(),
        request.catalog(), request.security().tenant_id()));
  } else {
    *binding.mutable_catalog() = request.catalog();
    binding.set_tenant_id(request.security().tenant_id());
    binding.set_warehouse_uri(std::filesystem::temp_directory_path().string());
  }
  *command.add_worker_local_storage_bindings() = binding;
  const auto serialized = command.SerializeAsString();
  const auto bytes = dispatchCommand({serialized.begin(), serialized.end()});
  proto::RuntimeCommandResponse response;
  EXPECT_TRUE(response.ParseFromArray(bytes.data(), bytes.size()));
  return response;
}

void catalog(proto::CatalogExecutionBinding* value) {
  value->set_catalog_name("lake");
  value->set_connector_provider("iceberg");
  value->set_catalog_incarnation_id("catalog-incarnation");
  value->set_catalog_revision(7);
  checksum(value->mutable_configuration_fingerprint(), "catalog-config");
}

proto::DatasetSchemaFingerprintRequest schemaRequest() {
  proto::DatasetSchemaFingerprintRequest request;
  catalog(request.mutable_catalog());
  request.mutable_security()->set_tenant_id("tenant-a");
  request.mutable_security()->set_principal("principal");
  request.set_deadline_epoch_ms(deadline());
  request.mutable_schema()->add_names("id");
  auto* type = request.mutable_schema()->mutable_struct_()->add_types();
  type->mutable_i64()->set_nullability(substrait::Type_Nullability_NULLABILITY_NULLABLE);
  auto* logical = request.add_column_types();
  logical->set_canonical_sql("BIGINT");
  *logical->mutable_substrait_type() = *type;
  return request;
}

bool isPersistedManifestDigest(const std::filesystem::path& dataset, const std::string& expected) {
  for (const auto& entry : std::filesystem::directory_iterator(dataset / "_versions")) {
    if (!entry.is_regular_file() || entry.path().extension() != ".manifest") continue;
    std::ifstream input(entry.path(), std::ios::binary);
    const std::string bytes((std::istreambuf_iterator<char>(input)), {});
    if (digest(bytes) == expected) return true;
  }
  return false;
}

class NativeDatasetControlTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = pangrova::test::testLeafPool("NativeDatasetControlTest");
    directory_ = std::filesystem::temp_directory_path() /
        ("pangrova-dataset-control-" + std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(directory_);
  }

  void TearDown() override {
    pool_.reset();
    std::error_code ignored;
    std::filesystem::remove_all(directory_, ignored);
  }

  proto::FinalizeDistributedWriteRequest publication(
      const std::string& write_id, proto::LanceWriteOperation operation = proto::LANCE_WRITE_OPERATION_CREATE,
      uint64_t base = 0) {
    const auto schema = schemaRequest();
    auto schema_response = dispatch(proto::RUNTIME_COMMAND_TYPE_DATASET_SCHEMA_FINGERPRINT, schema);
    EXPECT_TRUE(schema_response.success()) << schema_response.error().message();
    proto::DatasetSchemaFingerprintResult fingerprint;
    EXPECT_TRUE(schema_response.payload().UnpackTo(&fingerprint));
    proto::FinalizeDistributedWriteRequest request;
    *request.mutable_security() = schema.security();
    request.set_execution_id("execution-1");
    request.set_commit_fingerprint(hex(digest(write_id)));
    request.set_deadline_epoch_ms(deadline());
    request.set_control_epoch(8);
    auto* owner = request.mutable_execution_owner();
    owner->set_owner_id("coordinator-current");
    owner->set_owner_incarnation("current-incarnation");
    owner->set_fencing_token(9);
    auto* intent_owner = request.mutable_intent_execution_owner();
    intent_owner->set_owner_id("coordinator-original");
    intent_owner->set_owner_incarnation("original-incarnation");
    intent_owner->set_fencing_token(3);
    auto* allocation = request.mutable_output_binding()->mutable_lance();
    allocation->set_dataset_uri((directory_ / "dataset.lance").string());
    allocation->set_write_id(write_id);
    allocation->set_generation("generation-1");
    allocation->set_operation(operation);
    allocation->set_expected_read_version(base);
    allocation->set_allocation_staging_prefix("_pangrova/write-staging/" + write_id);
    allocation->set_transaction_staging_prefix(allocation->allocation_staging_prefix());
    *allocation->mutable_catalog() = schema.catalog();
    *allocation->mutable_schema_fence_sha256() = fingerprint.physical_schema_sha256();
    allocation->add_write_column_names("id");
    // The connector handle is opaque to Native, but its exact bytes are fenced.
    auto* handle = allocation->mutable_output_handle();
    handle->set_type_url("type.googleapis.com/pangrova.protocol.v2.LanceWriteBinding");
    handle->set_codec_version(1);
    handle->set_payload(canonical(*allocation));
    checksum(handle->mutable_payload_checksum(), handle->payload());
    handle->add_write_column_names("id");
    return request;
  }

  void addWinner(proto::FinalizeDistributedWriteRequest& request, int32_t partition) {
    const auto& allocation = request.output_binding().lance();
    proto::TaskAttemptId attempt;
    attempt.set_query_or_job_id(request.execution_id());
    attempt.set_stage_id(1);
    attempt.set_partition_id(partition);
    attempt.set_plan_epoch(3);
    attempt.set_attempt_id(2);
    attempt.set_fencing_token(5);
    attempt.set_control_epoch(2);
    attempt.set_task_id("task-" + std::to_string(partition));
    *attempt.mutable_execution_owner() = request.intent_execution_owner();
    const auto attempt_bytes = canonical(attempt);
    lance::LanceWriterConfig config;
    config.outputPath = allocation.dataset_uri();
    config.storageBinding = pangrova::test::testStorageBinding(allocation.dataset_uri(), allocation.catalog(),
        request.security().tenant_id());
    config.writeId = allocation.write_id();
    config.tenantId = request.security().tenant_id();
    config.generation = allocation.generation();
    config.operation = static_cast<lance::LanceWriteOperation>(allocation.operation());
    config.expectedReadVersion = allocation.expected_read_version();
    config.transactionStagingPrefix = allocation.allocation_staging_prefix() +
        "/attempt-" + hex(digest(attempt_bytes));
    config.taskAttempt.assign(attempt_bytes.begin(), attempt_bytes.end());
    std::copy(allocation.schema_fence_sha256().digest().begin(),
              allocation.schema_fence_sha256().digest().end(), config.schemaFenceSha256.begin());
    const auto type = ROW({"id"}, {BIGINT()});
    lance::LanceBatchWriter writer(pool_, type, config);
    auto values = BaseVector::create(BIGINT(), 2, pool_.get());
    values->asFlatVector<int64_t>()->set(0, partition * 10);
    values->asFlatVector<int64_t>()->set(1, partition * 10 + 1);
    auto rows = std::make_shared<RowVector>(pool_.get(), type, nullptr, 2, std::vector<VectorPtr>{values});
    writer.writeBatch(rows);
    const auto bytes = writer.finish();
    auto* winner = request.add_fragments();
    *winner->mutable_task_attempt() = attempt;
    winner->set_commit_metadata(bytes.data(), bytes.size());
    checksum(winner->mutable_sha256(), winner->commit_metadata());
  }

  proto::FinalizeDistributedWriteResult finalize(const proto::FinalizeDistributedWriteRequest& request) {
    const auto response = dispatch(proto::RUNTIME_COMMAND_TYPE_FINALIZE_DISTRIBUTED_WRITE, request);
    EXPECT_TRUE(response.success()) << response.error().message();
    proto::FinalizeDistributedWriteResult result;
    EXPECT_TRUE(response.payload().UnpackTo(&result));
    EXPECT_FALSE(response.has_error());
    return result;
  }

  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
  std::filesystem::path directory_;
};

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
  auto request = publication("create-1");
  addWinner(request, 0);
  addWinner(request, 1);
  auto result = finalize(request);
  ASSERT_EQ(result.outcome(), proto::WRITE_PUBLICATION_OUTCOME_COMMITTED);
  EXPECT_EQ(result.commit_fingerprint(), request.commit_fingerprint());
  EXPECT_EQ(result.published_version(), 1u);
  EXPECT_TRUE(isPersistedManifestDigest(directory_ / "dataset.lance", result.manifest_sha256().digest()));
  EXPECT_NE(result.manifest_sha256().digest(), request.output_binding().lance().schema_fence_sha256().digest());
  pangrova::test::PartitionedTestDatasetReader reader(pool_.get(), ROW({"id"}, {BIGINT()}),
      request.output_binding().lance().dataset_uri(), result.published_version(), {});
  int64_t rows = 0;
  while (auto batch = reader.next(8192)) rows += batch->size();
  EXPECT_EQ(rows, 4);

  request.set_recovering(true);
  request.set_known_published_version(result.published_version());
  *request.mutable_known_manifest_sha256() = result.manifest_sha256();
  auto recovered = finalize(request);
  EXPECT_EQ(recovered.outcome(), proto::WRITE_PUBLICATION_OUTCOME_COMMITTED);
  EXPECT_EQ(recovered.published_version(), result.published_version());
  EXPECT_EQ(recovered.manifest_sha256().digest(), result.manifest_sha256().digest());
  request.mutable_known_manifest_sha256()->set_digest(std::string(32, 'x'));
  auto mismatched = finalize(request);
  EXPECT_EQ(mismatched.outcome(), proto::WRITE_PUBLICATION_OUTCOME_UNKNOWN);
  EXPECT_EQ(mismatched.published_version(), 0u);
  EXPECT_FALSE(mismatched.has_manifest_sha256());
}

TEST_F(NativeDatasetControlTest, EnumeratesPinnedManifestPagesAndReadsEachPartitionExactlyOnce) {
  auto create = publication("partition-pages");
  addWinner(create, 0);
  addWinner(create, 1);
  const auto published = finalize(create);
  ASSERT_EQ(published.outcome(), proto::WRITE_PUBLICATION_OUTCOME_COMMITTED);
  const auto& output = create.output_binding().lance();
  proto::DatasetScanPartitionsRequest request;
  *request.mutable_catalog() = output.catalog();
  *request.mutable_security() = create.security();
  request.set_dataset_uri(output.dataset_uri());
  request.set_version(published.published_version());
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
  auto create = publication("create-1");
  addWinner(create, 0);
  ASSERT_EQ(finalize(create).published_version(), 1u);
  auto append = publication("append-1", proto::LANCE_WRITE_OPERATION_APPEND, 1);
  addWinner(append, 0);
  auto appended = finalize(append);
  ASSERT_EQ(appended.outcome(), proto::WRITE_PUBLICATION_OUTCOME_COMMITTED);
  ASSERT_EQ(appended.published_version(), 2u);
  EXPECT_TRUE(isPersistedManifestDigest(directory_ / "dataset.lance", appended.manifest_sha256().digest()));
  auto overwrite = publication("overwrite-1", proto::LANCE_WRITE_OPERATION_OVERWRITE, 2);
  addWinner(overwrite, 0);
  auto overwritten = finalize(overwrite);
  ASSERT_EQ(overwritten.outcome(), proto::WRITE_PUBLICATION_OUTCOME_COMMITTED);
  ASSERT_EQ(overwritten.published_version(), 3u);
  auto stale = publication("stale-append", proto::LANCE_WRITE_OPERATION_APPEND, 1);
  addWinner(stale, 0);
  const auto failed = finalize(stale);
  EXPECT_EQ(failed.outcome(), proto::WRITE_PUBLICATION_OUTCOME_UNKNOWN);
  EXPECT_FALSE(failed.has_manifest_sha256());
  uint64_t version = 0;
  lance::LanceStorageContext storage(pangrova::test::testStorageBinding(
      stale.output_binding().lance().dataset_uri(), stale.output_binding().lance().catalog()));
  ASSERT_EQ(lance_ffi_dataset_version(storage.get(), stale.output_binding().lance().dataset_uri().c_str(), &version), 0);
  EXPECT_EQ(version, 3u);
}

TEST_F(NativeDatasetControlTest, ValidationErrorsCannotPublishOrClaimAnOutcome) {
  auto original = publication("validate-1");
  addWinner(original, 0);
  const std::vector<std::function<void(proto::FinalizeDistributedWriteRequest&)>> mutations{
      [](auto& r) { r.set_deadline_epoch_ms(0); },
      [](auto& r) { r.clear_execution_owner(); },
      [](auto& r) { r.clear_intent_execution_owner(); },
      [](auto& r) { r.mutable_execution_owner()->set_fencing_token(2); },
      [](auto& r) { r.mutable_execution_owner()->set_fencing_token(3); },
      [](auto& r) { r.set_control_epoch(0); },
      [](auto& r) { r.mutable_security()->set_tenant_id("other-tenant"); },
      [](auto& r) { r.mutable_output_binding()->mutable_lance()->set_generation("other-generation"); },
      [](auto& r) { r.mutable_output_binding()->mutable_lance()->set_expected_read_version(1); },
      [](auto& r) { r.mutable_output_binding()->mutable_lance()->mutable_schema_fence_sha256()->set_digest(std::string(32, 'x')); },
      [](auto& r) { r.mutable_output_binding()->mutable_lance()->set_allocation_staging_prefix("../escape"); },
      [](auto& r) { r.mutable_output_binding()->mutable_lance()->set_transaction_staging_prefix("bound-attempt"); },
      [](auto& r) { r.mutable_fragments(0)->mutable_sha256()->set_digest(std::string(32, 'x')); },
      [](auto& r) { r.mutable_fragments(0)->mutable_task_attempt()->set_attempt_id(3); },
      [](auto& r) { r.mutable_fragments(0)->mutable_task_attempt()->set_control_epoch(9); },
      [](auto& r) { *r.mutable_fragments(0)->mutable_task_attempt()->mutable_execution_owner() = r.execution_owner(); },
      [](auto& r) { *r.add_fragments() = r.fragments(0); },
      [](auto& r) { r.set_known_published_version(1); },
      [](auto& r) { r.set_recovering(true); r.set_known_published_version(1); },
      [](auto& r) { r.clear_fragments(); }};
  for (const auto& mutate : mutations) {
    auto request = original;
    mutate(request);
    const auto response = dispatch(proto::RUNTIME_COMMAND_TYPE_FINALIZE_DISTRIBUTED_WRITE, request);
    EXPECT_FALSE(response.success());
    EXPECT_EQ(response.error().error_class(), proto::ERROR_CLASS_INVALID_REQUEST);
    EXPECT_FALSE(response.has_payload());
  }
  EXPECT_FALSE(std::filesystem::exists(directory_ / "dataset.lance" / "_versions"));
}

TEST_F(NativeDatasetControlTest, CurrentAuthorityDoesNotRewriteOriginalWinnerEvidence) {
  auto request = publication("owner-fence");
  addWinner(request, 0);
  ASSERT_EQ(request.execution_owner().fencing_token(), 9u);
  ASSERT_EQ(request.intent_execution_owner().fencing_token(), 3u);
  ASSERT_EQ(request.control_epoch(), 8u);
  ASSERT_EQ(request.fragments(0).task_attempt().control_epoch(), 2u);
  auto future = request;
  future.mutable_fragments(0)->mutable_task_attempt()->set_control_epoch(9);
  auto response = dispatch(proto::RUNTIME_COMMAND_TYPE_FINALIZE_DISTRIBUTED_WRITE, future);
  EXPECT_FALSE(response.success());
  EXPECT_EQ(response.error().error_class(), proto::ERROR_CLASS_INVALID_REQUEST);
  EXPECT_NE(response.error().message().find("execution owner/control fence"), std::string::npos);
  auto wrong_owner = request;
  *wrong_owner.mutable_fragments(0)->mutable_task_attempt()->mutable_execution_owner() = request.execution_owner();
  response = dispatch(proto::RUNTIME_COMMAND_TYPE_FINALIZE_DISTRIBUTED_WRITE, wrong_owner);
  EXPECT_FALSE(response.success());
  EXPECT_NE(response.error().message().find("execution owner/control fence"), std::string::npos);
  // Original metadata remains intact while a newer authority performs publication.
  const auto result = finalize(request);
  EXPECT_EQ(result.outcome(), proto::WRITE_PUBLICATION_OUTCOME_COMMITTED);
  EXPECT_EQ(result.published_version(), 1u);
}

TEST_F(NativeDatasetControlTest, RejectsOversizedMetadataAndTooManyFragmentsBeforeFfi) {
  auto request = publication("bounded-1");
  addWinner(request, 0);
  const auto winner = request.fragments(0);
  while (request.fragments_size() <= 4096) *request.add_fragments() = winner;
  const auto many = dispatch(proto::RUNTIME_COMMAND_TYPE_FINALIZE_DISTRIBUTED_WRITE, request);
  EXPECT_FALSE(many.success());
  EXPECT_EQ(many.error().error_class(), proto::ERROR_CLASS_INVALID_REQUEST);
  request = publication("bounded-2");
  request.mutable_output_binding()->mutable_lance()->mutable_output_handle()
      ->set_payload(std::string(16 * 1024 * 1024, 'x'));
  const auto oversized = dispatch(proto::RUNTIME_COMMAND_TYPE_FINALIZE_DISTRIBUTED_WRITE, request);
  EXPECT_FALSE(oversized.success());
  EXPECT_EQ(oversized.error().error_class(), proto::ERROR_CLASS_INVALID_REQUEST);
  EXPECT_FALSE(std::filesystem::exists(directory_ / "dataset.lance" / "_versions"));
}

}  // namespace
}  // namespace pangrova::jni::test
