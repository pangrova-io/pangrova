// Canonical v2 TaskRequest -> TaskExecutorConfig contract tests.
#include <gtest/gtest.h>

#include <openssl/evp.h>

#include <algorithm>
#include <array>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <google/protobuf/descriptor.pb.h>

#include "executor/driver/TaskExecutor.h"
#include "capability/NativeCapabilityCatalog.h"
#include "capability/LogicalTypeContract.h"
#include "jni/command/TaskRequestParser.h"
#include "jni/command/WorkerLocalStorage.h"
#include "jni/command/WorkerLocalCopyInput.h"
#include "../lance/LanceDatasetTestFixture.h"

#include "pangrova/protocol/v2/connector.pb.h"
#include "pangrova/protocol/v2/runtime.pb.h"
#include "pangrova/protocol/v2/task.pb.h"

namespace pangrova::jni::test {
namespace {

using executor::TaskExecutorConfig;

TEST(ParseTaskConfigTest, EmptyDataFailsClosed) {
  EXPECT_THROW(parseTaskConfig({}), std::runtime_error);
}

TEST(ParseTaskConfigTest, GarbageDataThrowsRuntimeError) {
  EXPECT_THROW(
      parseTaskConfig(std::vector<uint8_t>{0xFF, 0xFE, 0xFD}),
      std::runtime_error);
}

namespace proto = pangrova::protocol::v2;

void collectDescriptorFiles(
    const google::protobuf::FileDescriptor* descriptor,
    std::map<std::string, const google::protobuf::FileDescriptor*>& files) {
  if (!files.emplace(descriptor->name(), descriptor).second) {
    return;
  }
  for (int i = 0; i < descriptor->dependency_count(); ++i) {
    collectDescriptorFiles(descriptor->dependency(i), files);
  }
}

void clearDerivedFields(google::protobuf::DescriptorProto* message) {
  for (auto& field : *message->mutable_field()) {
    field.clear_json_name();
  }
  for (auto& extension : *message->mutable_extension()) {
    extension.clear_json_name();
  }
  for (auto& nested : *message->mutable_nested_type()) {
    clearDerivedFields(&nested);
  }
}

std::string descriptorHash() {
  std::map<std::string, const google::protobuf::FileDescriptor*> files;
  collectDescriptorFiles(proto::RuntimeCommand::descriptor()->file(), files);
  google::protobuf::FileDescriptorSet set;
  for (const auto& [name, descriptor] : files) {
    (void)name;
    auto* output = set.add_file();
    descriptor->CopyTo(output);
    for (auto& message : *output->mutable_message_type()) {
      clearDerivedFields(&message);
    }
    for (auto& extension : *output->mutable_extension()) {
      extension.clear_json_name();
    }
  }
  const auto bytes = set.SerializeAsString();
  std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
  unsigned int size = 0;
  EXPECT_EQ(EVP_Digest(
      bytes.data(), bytes.size(), digest.data(), &size,
      EVP_sha256(), nullptr), 1);
  return std::string(reinterpret_cast<const char*>(digest.data()), size);
}

std::string sha256(const std::string& bytes) {
  std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
  unsigned int size = 0;
  EXPECT_EQ(EVP_Digest(
      bytes.data(), bytes.size(), digest.data(), &size,
      EVP_sha256(), nullptr), 1);
  return std::string(reinterpret_cast<const char*>(digest.data()), size);
}

void appendLengthPrefixed(std::string& output, const std::string& value) {
  const auto size = static_cast<uint32_t>(value.size());
  output.push_back(static_cast<char>((size >> 24) & 0xffU));
  output.push_back(static_cast<char>((size >> 16) & 0xffU));
  output.push_back(static_cast<char>((size >> 8) & 0xffU));
  output.push_back(static_cast<char>(size & 0xffU));
  output.append(value);
}

std::string toHex(const std::string& bytes) {
  constexpr char kHex[] = "0123456789abcdef";
  std::string result;
  result.reserve(bytes.size() * 2);
  for (unsigned char byte : bytes) {
    result.push_back(kHex[byte >> 4]);
    result.push_back(kHex[byte & 0x0F]);
  }
  return result;
}

template <typename Artifact>
void signDependencySet(Artifact* artifact) {
  std::string canonical;
  for (const auto& dependency : artifact->dependencies()) {
    appendLengthPrefixed(canonical, dependency.kind());
    appendLengthPrefixed(canonical, dependency.id());
    appendLengthPrefixed(canonical, dependency.version());
  }
  artifact->mutable_dependency_set_checksum()->set_algorithm(
      proto::Checksum::ALGORITHM_SHA256);
  artifact->mutable_dependency_set_checksum()->set_digest(sha256(canonical));
}

template <typename Artifact>
void addArtifactProvenance(
    Artifact* artifact,
    proto::ArtifactExecutionKind execution_kind =
        proto::ARTIFACT_EXECUTION_KIND_BUILD) {
  artifact->set_execution_kind(execution_kind);
  artifact->set_empty_partition_policy(
      proto::ARTIFACT_EMPTY_PARTITION_POLICY_FORBID);
  auto* dependency = artifact->add_dependencies();
  dependency->set_kind("SOURCE_SNAPSHOT");
  dependency->set_id("catalog.schema.items");
  dependency->set_version("123");
  signDependencySet(artifact);
}

std::vector<uint8_t> serializeRaw(const google::protobuf::MessageLite& message) {
  std::vector<uint8_t> bytes(message.ByteSizeLong());
  EXPECT_TRUE(message.SerializeToArray(
      bytes.data(), static_cast<int>(bytes.size())));
  return bytes;
}

proto::TaskRequest validRequest() {
  proto::TaskRequest request;
  auto* protocol = request.mutable_protocol();
  protocol->set_major(2);
  protocol->set_minor(0);
  protocol->set_descriptor_set_sha256(descriptorHash());
  protocol->set_rollout_epoch("ai-native-function-resource-r3");
  auto* attempt = request.mutable_attempt();
  attempt->set_query_or_job_id("query-7");
  attempt->set_stage_id(3);
  attempt->set_partition_id(5);
  attempt->set_plan_epoch(11);
  attempt->set_attempt_id(2);
  attempt->set_fencing_token((uint64_t{11} << 32) + 3);
  attempt->set_task_id("task-42");
  attempt->set_control_epoch(9);
  auto* owner = attempt->mutable_execution_owner();
  owner->set_owner_id("coordinator-a");
  owner->set_owner_incarnation("process-a");
  owner->set_fencing_token(17);
  request.set_task_kind(proto::TASK_KIND_READ);
  request.mutable_security()->set_tenant_id("tenant-a");
  request.mutable_security()->set_principal("alice");
  request.set_substrait_plan("plan-bytes");
  request.mutable_substrait_plan_checksum()->set_algorithm(
      proto::Checksum::ALGORITHM_SHA256);
  request.mutable_substrait_plan_checksum()->set_digest(
      sha256(request.substrait_plan()));
  request.set_deadline_epoch_ms(4102444800000ULL);
  request.mutable_resource_grant()->set_grant_id("test-task-grant");
  request.mutable_resource_grant()->set_cpu_slots(2);
  request.mutable_resource_grant()->set_native_memory_bytes(1024 * 1024);
  return request;
}

TEST(ParseTaskConfigTest, CopyStagingDigestCannotBypassWorkerStaging) {
  auto request = validRequest();
  request.set_task_kind(proto::TASK_KIND_WRITE);
  auto* checksum = request.mutable_copy_source_staging_command_sha256();
  checksum->set_algorithm(proto::Checksum::ALGORITHM_SHA256);
  checksum->set_digest(std::string(32, '\x5a'));
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
  EXPECT_THROW(taskConfigFromRequest(request), std::runtime_error);
}

TEST(ParseTaskConfigTest, DirectResultRequiresPositiveExactSerializedBufferGrant) {
  auto request = validRequest();
  request.mutable_execution_options()->set_direct_result_output(true);
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
  request.mutable_resource_grant()->set_shuffle_buffer_bytes(1'048'576);
  const auto config = parseTaskConfig(serializeRaw(request));
  EXPECT_TRUE(config.is_result_task);
  EXPECT_EQ(config.resource_limit.max_shuffle_buffer_bytes, 1'048'576);
  request.mutable_execution_options()->set_direct_result_output(false);
  request.mutable_resource_grant()->set_shuffle_buffer_bytes(0);
  EXPECT_FALSE(parseTaskConfig(serializeRaw(request)).is_result_task);
}

proto::TaskRequest remoteShuffleRequest() {
  auto request = validRequest();
  request.mutable_resource_grant()->set_grant_id("remote-shuffle-grant");
  request.mutable_resource_grant()->set_native_memory_bytes(64ULL * 1024 * 1024);
  request.mutable_resource_grant()->set_shuffle_buffer_bytes(16ULL * 1024 * 1024);
  auto* shuffle = request.mutable_execution_options()->mutable_shuffle();
  shuffle->set_serde(proto::BATCH_SERDE_ARROW_BATCH);
  shuffle->set_compression_codec("none");
  shuffle->set_batch_size(1024);
  shuffle->set_persistence_mode(
      proto::SHUFFLE_PERSISTENCE_MODE_MEMORY_AND_REMOTE);
  shuffle->set_partition_count(8);
  auto* binding = shuffle->mutable_execution_binding();
  binding->set_shuffle_id("shuffle-1");
  binding->set_plan_epoch(request.attempt().plan_epoch());
  *binding->mutable_execution_owner() = request.attempt().execution_owner();
  auto* endpoint = binding->add_endpoints();
  endpoint->set_host("shuffle-a.internal");
  endpoint->set_port(9400);
  endpoint->set_partition_start(0);
  endpoint->set_partition_end(8);
  binding->mutable_write_capability()->set_token("signed-write-token");
  binding->mutable_write_capability()->set_expires_at_epoch_ms(
      4102444800000ULL);
  return request;
}

proto::DataSplit* addFileSplit(
    proto::TaskRequest& request,
    proto::FileFormat format = proto::FILE_FORMAT_PARQUET) {
  auto* split = request.add_splits();
  split->set_split_id("split-1");
  split->set_connector_id("iceberg");
  split->set_source_id("catalog.schema.items");
  split->set_source_snapshot("123");
  split->set_data_object_id("object-1");
  split->set_opaque_location("/data/part-0.parquet");
  split->set_start_offset(100);
  split->set_length(5000);
  proto::FileSplitDetail detail;
  detail.set_file_format(format);
  if (format == proto::FILE_FORMAT_LANCE) {
    split->set_connector_id("lance");
    split->set_start_offset(0);
    split->set_length(0);
    auto* catalog = split->mutable_catalog();
    catalog->set_catalog_name("lakehouse");
    catalog->set_connector_provider("lance");
    catalog->set_catalog_incarnation_id("catalog-incarnation-1");
    catalog->set_catalog_revision(7);
    catalog->mutable_configuration_fingerprint()->set_algorithm(proto::Checksum::ALGORITHM_SHA256);
    catalog->mutable_configuration_fingerprint()->set_digest(std::string(32, 'c'));
    auto* partition = detail.mutable_scan_partition();
    partition->set_partition_id(42);
    partition->set_physical_rows(100);
    partition->mutable_manifest_sha256()->set_algorithm(proto::Checksum::ALGORITHM_SHA256);
    partition->mutable_manifest_sha256()->set_digest(std::string(32, 'm'));
    partition->mutable_physical_schema_sha256()->set_algorithm(proto::Checksum::ALGORITHM_SHA256);
    partition->mutable_physical_schema_sha256()->set_digest(std::string(32, 's'));
    auto* storage = request.add_worker_local_storage_bindings();
    *storage->mutable_catalog() = *catalog;
    storage->set_tenant_id(request.security().tenant_id());
    storage->set_warehouse_uri("/data");
  }
  split->mutable_connector_detail()->PackFrom(detail);
  return split;
}

proto::OperatorBinding* addExactBinding(
    proto::TaskRequest& request,
    const std::string& plan_node_id = "topk-1",
    uint64_t candidate_limit = 25) {
  auto* binding = request.add_operator_bindings();
  binding->set_plan_node_id(plan_node_id);
  *binding->mutable_capability() =
      pangrova::capability::exactTopKRetrievalDescriptor().ref();
  auto* policy = binding->mutable_execution_policy();
  policy->set_coverage_policy(proto::COVERAGE_POLICY_REQUIRE_COMPLETE);
  policy->set_fallback_policy(proto::FALLBACK_POLICY_FORBID);
  policy->set_retry_class(proto::RETRY_CLASS_IDEMPOTENT);
  policy->set_deadline_epoch_ms(request.deadline_epoch_ms());
  auto* approximation = policy->mutable_approximation();
  approximation->set_profile_id("exact");
  approximation->set_candidate_limit(candidate_limit);
  approximation->set_exact_rescore_limit(candidate_limit);
  approximation->set_required_recall_at_k(1.0);
  approximation->set_recall_k(static_cast<uint32_t>(candidate_limit));
  binding->mutable_resources()->set_candidate_rows(candidate_limit);
  auto* grant = request.mutable_resource_grant();
  grant->set_grant_id("grant-1");
  grant->set_native_memory_bytes(1024 * 1024);
  grant->set_candidate_rows(candidate_limit);
  grant->set_candidate_bytes(1024 * 1024);
  return binding;
}

proto::OperatorBinding* addAiBinding(proto::TaskRequest& request) {
  auto* binding = request.add_operator_bindings();
  binding->set_plan_node_id("ai-invoke-1");
  *binding->mutable_capability() =
      pangrova::capability::aiTextGenerationDescriptor().ref();
  binding->mutable_capability()->clear_implementation_build_id();
  auto* model = binding->mutable_model()->mutable_model();
  model->set_id("catalog.llm");
  model->set_version("2026-09-12");
  model->set_signature_sha256(std::string(32, 'm'));
  auto* request_limits = binding->mutable_model()->mutable_request_limits();
  request_limits->set_max_input_tokens(1000);
  request_limits->set_max_output_tokens(500);
  request_limits->set_max_request_bytes(1000);
  request_limits->set_max_response_bytes(500);
  auto* output = binding->mutable_model()->mutable_output_type();
  output->set_canonical_sql("VARCHAR");
  output->mutable_substrait_type()->mutable_string()->set_nullability(
      substrait::Type_Nullability_NULLABILITY_NULLABLE);
  auto* policy = binding->mutable_execution_policy();
  policy->set_coverage_policy(proto::COVERAGE_POLICY_REQUIRE_COMPLETE);
  policy->set_fallback_policy(proto::FALLBACK_POLICY_FORBID);
  policy->set_retry_class(proto::RETRY_CLASS_PROVIDER_IDEMPOTENCY_KEY);
  policy->set_idempotency_key("query:stage:partition:ai-invoke-1");
  policy->set_deadline_epoch_ms(request.deadline_epoch_ms());
  auto* resources = binding->mutable_resources();
  resources->set_provider_concurrency(2);
  resources->set_provider_items(64);
  resources->set_provider_attempts(70);
  resources->set_provider_request_bytes(64'000);
  resources->set_provider_response_bytes(32'000);
  auto* grant = request.mutable_resource_grant();
  grant->set_grant_id("grant-ai-1");
  grant->set_cpu_slots(2);
  grant->set_native_memory_bytes(1024 * 1024);
  grant->set_provider_concurrency(2);
  grant->set_provider_items(64);
  grant->set_provider_attempts(70);
  grant->set_provider_request_bytes(64'000);
  grant->set_provider_response_bytes(32'000);
  return binding;
}

void addConnectorWriteBinding(proto::WriteTaskOptions* write) {
  auto* connector = write->mutable_output_binding()->mutable_connector();
  auto* catalog = connector->mutable_catalog();
  catalog->set_catalog_name("lakehouse");
  catalog->set_connector_provider("iceberg");
  catalog->set_catalog_incarnation_id("catalog-incarnation-1");
  catalog->set_catalog_revision(7);
  catalog->mutable_configuration_fingerprint()->set_algorithm(
      proto::Checksum::ALGORITHM_SHA256);
  catalog->mutable_configuration_fingerprint()->set_digest(
      std::string(32, '\x2a'));
  auto* handle = connector->mutable_output_handle();
  handle->set_type_url("type.pangrova.io/iceberg.OutputTableHandle");
  handle->set_codec_version(1);
  handle->set_payload("connector-owned-output-handle");
  handle->mutable_payload_checksum()->set_algorithm(
      proto::Checksum::ALGORITHM_SHA256);
  handle->mutable_payload_checksum()->set_digest(sha256(handle->payload()));
}

void addLanceCatalogFence(proto::TaskRequest& request) {
  auto* write = request.mutable_execution_options()->mutable_write();
  auto lance = write->output_binding().lance();
  addConnectorWriteBinding(write);
  auto connector = write->output_binding().connector();
  *lance.mutable_catalog() = connector.catalog();
  *lance.mutable_output_handle() = connector.output_handle();
  lance.add_write_column_names("id");
  lance.mutable_output_handle()->add_write_column_names("id");
  lance.set_allocation_staging_prefix("_pangrova/write-staging/write-intent-123");
  lance.set_transaction_staging_prefix(lance.allocation_staging_prefix() +
      "/attempt-" + toHex(sha256(request.attempt().SerializeAsString())));
  *write->mutable_output_binding()->mutable_lance() = lance;
  auto* storage = request.add_worker_local_storage_bindings();
  *storage->mutable_catalog() = lance.catalog();
  storage->set_tenant_id(request.security().tenant_id());
  storage->set_warehouse_uri("s3://warehouse");
  auto* s3 = storage->mutable_s3();
  s3->set_endpoint("https://storage.example.invalid");
  s3->set_region("test-region");
  s3->set_tls_enabled(true);
  s3->set_path_style_access(true);
  s3->set_access_key("fixture-access");
  s3->set_secret_key("fixture-secret");
}

void setEmbeddingOutputType(proto::OperatorBinding* binding,
                            uint32_t dimension) {
  auto* output = binding->mutable_model()->mutable_output_type();
  output->Clear();
  output->set_canonical_sql("VECTOR(" + std::to_string(dimension) + ")");
  auto* list = output->mutable_substrait_type()->mutable_list();
  list->set_nullability(
      substrait::Type_Nullability_NULLABILITY_NULLABLE);
  list->mutable_type()->mutable_fp32()->set_nullability(
      substrait::Type_Nullability_NULLABILITY_REQUIRED);
  auto* dimension_constraint = output->add_constraints();
  dimension_constraint->set_name("dimension");
  dimension_constraint->set_integer_value(dimension);
  auto* element_constraint = output->add_constraints();
  element_constraint->set_name("element_type");
  element_constraint->set_string_value("FLOAT32");
}

void signArtifact(proto::ArtifactBinding* artifact) {
  artifact->clear_binding_checksum();
  const auto digest = sha256(artifact->SerializeAsString());
  artifact->mutable_binding_checksum()->set_algorithm(
      proto::Checksum::ALGORITHM_SHA256);
  artifact->mutable_binding_checksum()->set_digest(
      digest);
}

void addArtifactObject(
    proto::ArtifactBinding* artifact,
    const std::string& role,
    uint32_t ordinal) {
  auto* object = artifact->add_split_objects();
  object->set_object_id("index-1:object-1:" + std::to_string(ordinal));
  object->set_opaque_uri(
      "s3://bucket/index-1/7/object-1/" + std::to_string(ordinal));
  object->set_size_bytes(4096);
  object->mutable_checksum()->set_algorithm(proto::Checksum::ALGORITHM_SHA256);
  object->mutable_checksum()->set_digest(std::string(32, '\0'));
  object->set_object_role(role);
  object->set_source_object_id("object-1");
  object->set_row_count(10);
  object->set_ordinal(ordinal);
  object->set_scope(proto::ARTIFACT_OBJECT_SCOPE_SOURCE_OBJECT);
}

void addValidArtifact(proto::OperatorBinding* binding) {
  auto* artifact = binding->add_artifacts();
  artifact->set_artifact_id("index-1");
  artifact->set_artifact_type("VECTOR_INDEX");
  artifact->set_generation("7");
  artifact->set_source_id("catalog.schema.items");
  artifact->set_source_snapshot("123");
  artifact->set_source_data_timestamp_ms(4102444700000ULL);
  artifact->set_artifact_created_at_epoch_ms(4102444750000ULL);
  artifact->set_manifest_uri("s3://bucket/index-1/7/manifest.pb");
  artifact->mutable_manifest_checksum()->set_algorithm(
      proto::Checksum::ALGORITHM_SHA256);
  artifact->mutable_manifest_checksum()->set_digest(std::string(32, '\0'));
  *artifact->mutable_format() = pangrova::capability::diskAnnArtifactFormat();
  *artifact->mutable_row_locator() =
      pangrova::capability::physicalRowLocatorContract();
  addArtifactProvenance(artifact);
  addArtifactObject(artifact, "DISKANN_GRAPH", 0);
  addArtifactObject(artifact, "RAW_VECTORS", 1);
  addArtifactObject(artifact, "ROW_LOCATORS", 2);
  signArtifact(artifact);
}

void addGeneratedValueProperties(
    google::protobuf::RepeatedPtrField<proto::NamedValue>* properties) {
  const auto add_string = [properties](
                              const std::string& name,
                              const std::string& value) {
    auto* property = properties->Add();
    property->set_name(name);
    property->mutable_value()->set_string_value(value);
  };
  add_string("target_column", "embedding");
  add_string("output_type", "VECTOR(2)");
  auto* dimension = properties->Add();
  dimension->set_name("vector_dimension");
  dimension->mutable_value()->set_int64_value(2);
  add_string("source_encoding", "ordered-unit-separator-v1");
  add_string("execution_mode", "EXECUTION_MODE_ASYNC");
  add_string("source_column.0", "description");
}

void addGeneratedValuesArtifact(proto::OperatorBinding* binding) {
  auto* artifact = binding->add_artifacts();
  artifact->set_artifact_id("generated-column-1");
  artifact->set_artifact_type("GENERATED_COLUMN_VALUES");
  artifact->set_generation("9");
  artifact->set_source_id("catalog.schema.items");
  artifact->set_source_snapshot("123");
  artifact->set_source_data_timestamp_ms(4102444700000ULL);
  artifact->set_artifact_created_at_epoch_ms(4102444750000ULL);
  artifact->set_manifest_uri(
      "s3://bucket/generated-column-1/9/manifest.pb");
  artifact->mutable_manifest_checksum()->set_algorithm(
      proto::Checksum::ALGORITHM_SHA256);
  artifact->mutable_manifest_checksum()->set_digest(std::string(32, '\0'));
  *artifact->mutable_format() =
      pangrova::capability::generatedValuesArtifactFormat();
  *artifact->mutable_row_locator() =
      pangrova::capability::physicalRowLocatorContract();
  addArtifactProvenance(artifact);
  addGeneratedValueProperties(artifact->mutable_properties());
  addArtifactObject(artifact, "GENERATED_VALUES", 0);
  addArtifactObject(artifact, "ROW_LOCATORS", 1);
  signArtifact(artifact);
}

TEST(ParseTaskConfigTest, ParsesCanonicalIdentityGovernanceAndPlan) {
  auto request = validRequest();
  auto config = parseTaskConfig(serializeRaw(request));
  EXPECT_EQ(config.task_id, "task-42");
  EXPECT_EQ(config.query_id, "query-7");
  EXPECT_EQ(config.stage_id, 3);
  EXPECT_EQ(config.partition_id, 5);
  EXPECT_EQ(config.plan_epoch, 11);
  EXPECT_EQ(config.attempt_id, 2);
  EXPECT_EQ(config.fencing_token, (uint64_t{11} << 32) + 3);
  EXPECT_EQ(config.control_epoch, 9);
  EXPECT_EQ(config.owner_id, "coordinator-a");
  EXPECT_EQ(config.owner_incarnation, "process-a");
  EXPECT_EQ(config.owner_fencing_token, 17);
  EXPECT_EQ(config.tenant_id, "tenant-a");
  EXPECT_EQ(config.principal, "alice");
  EXPECT_EQ(config.deadline_epoch_ms, 4102444800000LL);
  EXPECT_EQ(
      std::string(config.substrait_plan.begin(), config.substrait_plan.end()),
      "plan-bytes");
  proto::ExecutionCallbackContext callback;
  ASSERT_TRUE(callback.ParseFromString(config.callback_context));
  EXPECT_EQ(callback.protocol().SerializeAsString(), request.protocol().SerializeAsString());
  EXPECT_EQ(callback.attempt().SerializeAsString(), request.attempt().SerializeAsString());
  EXPECT_EQ(callback.security().SerializeAsString(), request.security().SerializeAsString());
  EXPECT_EQ(callback.resource_grant().SerializeAsString(), request.resource_grant().SerializeAsString());
  EXPECT_EQ(callback.deadline_epoch_ms(), request.deadline_epoch_ms());
  EXPECT_FALSE(callback.has_trace_context());
}

TEST(ParseTaskConfigTest, RejectsCallbackAuthorityOutsideTheJavaAccountingDomain) {
  auto request = validRequest();
  request.mutable_attempt()->set_plan_epoch(uint64_t{1} << 63);
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
  request = validRequest();
  request.mutable_attempt()->set_stage_id(-1);
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
  request = validRequest();
  request.mutable_attempt()->set_attempt_id(uint32_t{1} << 31);
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
  request = validRequest();
  request.mutable_attempt()->mutable_execution_owner()->set_fencing_token(uint64_t{1} << 63);
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
  request = validRequest();
  request.mutable_security()->set_principal("   ");
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
  request = validRequest();
  request.mutable_trace_context()->GetReflection()->MutableUnknownFields(request.mutable_trace_context())
      ->AddVarint(999, 1);
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
}

TEST(ParseTaskConfigTest, OrdinaryReadRequiresAnExplicitCpuAndNativeGrantWithoutProviderBudget) {
  auto request = validRequest();
  const auto config = parseTaskConfig(serializeRaw(request));
  EXPECT_EQ(config.resource_limit.max_provider_items, 0);
  EXPECT_EQ(config.resource_limit.max_provider_attempts, 0);
  EXPECT_EQ(config.resource_limit.max_provider_request_bytes, 0);
  EXPECT_EQ(config.resource_limit.max_provider_response_bytes, 0);
  request.clear_resource_grant();
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::invalid_argument);
  request = validRequest();
  request.mutable_resource_grant()->clear_cpu_slots();
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::invalid_argument);
}

TEST(ParseTaskConfigTest, ParsesIndexedResultVectorDimensions) {
  auto request = validRequest();
  request.mutable_execution_options()->set_direct_result_output(true);
  request.mutable_resource_grant()->set_shuffle_buffer_bytes(8 * 1024 * 1024);
  proto::OperatorBinding first;
  proto::OperatorBinding second;
  setEmbeddingOutputType(&first, 3);
  setEmbeddingOutputType(&second, 5);
  auto* first_column = request.add_result_column_types();
  first_column->set_column_index(0);
  *first_column->mutable_logical_type() = first.model().output_type();
  auto* second_column = request.add_result_column_types();
  second_column->set_column_index(2);
  *second_column->mutable_logical_type() = second.model().output_type();

  auto config = parseTaskConfig(serializeRaw(request));

  ASSERT_EQ(config.result_column_types.size(), 2U);
  EXPECT_EQ(config.result_column_types[0].column_index, 0U);
  EXPECT_EQ(config.result_column_types[0].dimension, 3U);
  EXPECT_EQ(config.result_column_types[1].column_index, 2U);
  EXPECT_EQ(config.result_column_types[1].dimension, 5U);
}

TEST(ParseTaskConfigTest, RetainsCompleteDoubleStorageVectorDeclarationWithoutWeakeningAiContract) {
  auto request = validRequest();
  request.mutable_execution_options()->set_direct_result_output(true);
  request.mutable_resource_grant()->set_shuffle_buffer_bytes(8 * 1024 * 1024);
  proto::OperatorBinding embedding;
  setEmbeddingOutputType(&embedding, 3);
  auto* column = request.add_result_column_types();
  column->set_column_index(0);
  auto* logical = column->mutable_logical_type();
  *logical = embedding.model().output_type();
  logical->set_canonical_sql("VECTOR(3,DOUBLE)");
  logical->mutable_substrait_type()->mutable_list()->mutable_type()->mutable_fp64()
      ->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  logical->mutable_constraints(1)->set_string_value("FLOAT64");
  const auto config = parseTaskConfig(serializeRaw(request));
  ASSERT_EQ(config.result_column_types.size(), 1U);
  EXPECT_EQ(config.result_column_types[0].dimension, 3U);
  EXPECT_EQ(config.result_column_types[0].logical_type.SerializeAsString(), logical->SerializeAsString());
  EXPECT_THROW(pangrova::capability::requireFloatVectorDimension(*logical), std::invalid_argument);
  logical->set_canonical_sql("VECTOR(3)");
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::invalid_argument);
}

TEST(ParseTaskConfigTest, RejectsResultTypesWithoutDirectOutputOrUniqueIndex) {
  auto request = validRequest();
  proto::OperatorBinding embedding;
  setEmbeddingOutputType(&embedding, 3);
  auto* column = request.add_result_column_types();
  column->set_column_index(0);
  *column->mutable_logical_type() = embedding.model().output_type();

  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
  request.mutable_execution_options()->set_direct_result_output(true);
  request.mutable_resource_grant()->set_shuffle_buffer_bytes(8 * 1024 * 1024);
  auto duplicate = *column;
  *request.add_result_column_types() = duplicate;
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
}

TEST(ParseTaskConfigTest, ParsesCanonicalInitialRuntimeFilters) {
  auto request = validRequest();
  auto* binding = request.add_initial_runtime_filters();
  binding->set_plan_node_id("scan-1");
  auto* filter = binding->add_filters();
  filter->set_column_name("id");
  filter->set_scope(proto::RUNTIME_FILTER_SCOPE_ROW_FILTER);
  filter->mutable_in_list()->add_values()->set_int64_value(7);

  auto config = parseTaskConfig(serializeRaw(request));
  ASSERT_EQ(config.initial_runtime_filters.size(), 1U);
  EXPECT_EQ(config.initial_runtime_filters.front().plan_node_id(), "scan-1");
  ASSERT_EQ(config.initial_runtime_filters.front().filters_size(), 1);
  EXPECT_EQ(
      config.initial_runtime_filters.front().filters(0).column_name(), "id");
}

TEST(ParseTaskConfigTest, RejectsIncompleteOrDuplicateInitialRuntimeFilters) {
  auto incomplete = validRequest();
  incomplete.add_initial_runtime_filters()->set_plan_node_id("scan-1");
  EXPECT_THROW(parseTaskConfig(serializeRaw(incomplete)), std::runtime_error);

  auto duplicate = validRequest();
  for (int i = 0; i < 2; ++i) {
    auto* binding = duplicate.add_initial_runtime_filters();
    binding->set_plan_node_id("scan-1");
    auto* filter = binding->add_filters();
    filter->set_column_name("id");
    filter->set_scope(proto::RUNTIME_FILTER_SCOPE_ROW_FILTER);
    filter->mutable_in_list()->add_values()->set_int64_value(7);
  }
  EXPECT_THROW(parseTaskConfig(serializeRaw(duplicate)), std::runtime_error);
}

TEST(ParseTaskConfigTest, RejectsProtocolAndPlanIntegrityDrift) {
  auto wrong_version = validRequest();
  wrong_version.mutable_protocol()->set_major(1);
  EXPECT_THROW(parseTaskConfig(serializeRaw(wrong_version)), std::runtime_error);

  auto wrong_descriptor = validRequest();
  wrong_descriptor.mutable_protocol()->set_descriptor_set_sha256("wrong");
  EXPECT_THROW(parseTaskConfig(serializeRaw(wrong_descriptor)), std::runtime_error);

  auto wrong_plan = validRequest();
  wrong_plan.set_substrait_plan("mutated-after-checksum");
  EXPECT_THROW(parseTaskConfig(serializeRaw(wrong_plan)), std::runtime_error);
}

TEST(ParseTaskConfigTest, RejectsMissingAttemptFenceAndDeadline) {
  auto missing_attempt = validRequest();
  missing_attempt.clear_attempt();
  EXPECT_THROW(parseTaskConfig(serializeRaw(missing_attempt)), std::runtime_error);

  auto missing_fence = validRequest();
  missing_fence.mutable_attempt()->set_fencing_token(0);
  EXPECT_THROW(parseTaskConfig(serializeRaw(missing_fence)), std::runtime_error);

  auto missing_deadline = validRequest();
  missing_deadline.set_deadline_epoch_ms(0);
  EXPECT_THROW(parseTaskConfig(serializeRaw(missing_deadline)), std::runtime_error);
}

TEST(ParseTaskConfigTest, ParsesGenericDataSplitAndTypedFileDetail) {
  auto request = validRequest();
  auto* split = addFileSplit(request);
  auto* partition = split->add_partition_values();
  partition->set_column_name("region");
  partition->mutable_value()->set_string_value("apac");

  proto::FileSplitDetail detail;
  ASSERT_TRUE(split->connector_detail().UnpackTo(&detail));
  auto* mapping = detail.mutable_schema_evolution()->add_column_mappings();
  mapping->set_source_field_id(1);
  mapping->set_target_field_id(2);
  mapping->set_source_name("old_col");
  mapping->set_target_name("new_col");
  (*detail.mutable_required_subfields())["payload"].add_paths("nested.value");
  auto* iceberg = detail.mutable_iceberg();
  iceberg->set_read_mode(proto::ICEBERG_READ_MODE_SNAPSHOT);
  iceberg->set_snapshot_id(12345);
  auto* delete_file = iceberg->add_delete_files();
  delete_file->set_opaque_location("/delete/positions.parquet");
  delete_file->set_delete_type(proto::DELETE_FILE_TYPE_POSITION);
  split->mutable_connector_detail()->PackFrom(detail);

  auto config = parseTaskConfig(serializeRaw(request));
  ASSERT_EQ(config.splits.size(), 1U);
  const auto& parsed = config.splits.front();
  EXPECT_EQ(parsed.split_id, "split-1");
  EXPECT_EQ(parsed.connector_id, "iceberg");
  EXPECT_EQ(parsed.table_name, "catalog.schema.items");
  EXPECT_EQ(parsed.file_path, "/data/part-0.parquet");
  EXPECT_EQ(parsed.file_format, split::FileFormatType::PARQUET);
  EXPECT_EQ(parsed.partition_values.at("region"), "apac");
  ASSERT_EQ(parsed.column_mappings.size(), 1U);
  EXPECT_EQ(parsed.column_mappings.front().target_name, "new_col");
  EXPECT_EQ(parsed.required_subfields.at("payload").front(), "nested.value");
  ASSERT_TRUE(parsed.iceberg.has_value());
  EXPECT_EQ(parsed.iceberg->snapshot_id, 12345);
  EXPECT_EQ(
      parsed.iceberg->delete_files.front().file_path,
      "/delete/positions.parquet");
}

TEST(ParseTaskConfigTest, ParsesPinnedIcebergMetadataSplitAndRejectsIdentityDrift) {
  auto request = validRequest();
  auto* data_split = addFileSplit(request, proto::FILE_FORMAT_METADATA_JSON);
  const std::string location = "s3://warehouse/db/orders/metadata/v1.metadata.json";
  data_split->set_source_id("db.orders$snapshots");
  data_split->set_source_snapshot(location);
  data_split->set_data_object_id(location);
  data_split->set_opaque_location(location);
  data_split->set_start_offset(0);
  data_split->set_length(4096);
  proto::FileSplitDetail detail;
  ASSERT_TRUE(data_split->connector_detail().UnpackTo(&detail));
  detail.mutable_system_table()->set_kind(
      proto::SystemTableSplitDetail::ICEBERG_SNAPSHOTS);
  data_split->mutable_connector_detail()->PackFrom(detail);

  auto parsed = parseTaskConfig(serializeRaw(request));
  ASSERT_EQ(parsed.splits.size(), 1U);
  EXPECT_EQ(parsed.splits.front().file_format,
      split::FileFormatType::METADATA_JSON);
  ASSERT_TRUE(parsed.splits.front().system_table.has_value());
  EXPECT_EQ(*parsed.splits.front().system_table,
      split::SystemTableKind::ICEBERG_SNAPSHOTS);

  data_split->set_source_id("db.orders$history");
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
  data_split->set_source_id("db.orders$snapshots");
  data_split->set_data_object_id("different-object");
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
  data_split->set_data_object_id(location);
  data_split->set_source_snapshot("latest");
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
}

TEST(ParseTaskConfigTest, ParsesPinnedIcebergRefsAndRejectsIdentityDrift) {
  auto request = validRequest();
  auto* data_split = addFileSplit(request, proto::FILE_FORMAT_METADATA_JSON);
  const std::string location = "s3://warehouse/db/orders/metadata/v1.metadata.json";
  data_split->set_source_id("db.orders$refs");
  data_split->set_source_snapshot(location);
  data_split->set_data_object_id(location);
  data_split->set_opaque_location(location);
  data_split->set_start_offset(0);
  data_split->set_length(4096);
  proto::FileSplitDetail detail;
  ASSERT_TRUE(data_split->connector_detail().UnpackTo(&detail));
  detail.mutable_system_table()->set_kind(
      proto::SystemTableSplitDetail::ICEBERG_REFS);
  data_split->mutable_connector_detail()->PackFrom(detail);

  auto parsed = parseTaskConfig(serializeRaw(request));
  ASSERT_EQ(parsed.splits.size(), 1U);
  EXPECT_EQ(parsed.splits.front().file_format,
      split::FileFormatType::METADATA_JSON);
  ASSERT_TRUE(parsed.splits.front().system_table.has_value());
  EXPECT_EQ(*parsed.splits.front().system_table,
      split::SystemTableKind::ICEBERG_REFS);

  data_split->set_source_id("db.orders$history");
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
  data_split->set_source_id("db.orders$refs");
  data_split->set_data_object_id("different-object");
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
  data_split->set_data_object_id(location);
  data_split->set_source_snapshot("latest");
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
}

TEST(ParseTaskConfigTest, PinsIcebergPartitionFieldIdsAndRejectsUnboundOrDuplicateFields) {
  auto request = validRequest();
  auto* data = addFileSplit(request, proto::FILE_FORMAT_METADATA_AVRO);
  data->set_source_id("db.orders$files");
  data->set_source_snapshot("s3://warehouse/list.avro");
  data->set_data_object_id("s3://warehouse/manifest.avro");
  data->set_opaque_location(data->data_object_id());
  data->set_start_offset(0);
  proto::FileSplitDetail detail;
  ASSERT_TRUE(data->connector_detail().UnpackTo(&detail));
  auto* system = detail.mutable_system_table();
  system->set_kind(proto::SystemTableSplitDetail::ICEBERG_FILES);
  system->set_snapshot_id(7);
  system->set_object_sha256(std::string(32, 'a'));
  system->set_iceberg_format_version(2);
  system->set_iceberg_snapshot_sequence_number(1);
  system->set_iceberg_manifest_list_uri(data->source_snapshot());
  system->set_iceberg_manifest_list_length(1000);
  system->set_iceberg_manifest_list_sha256(std::string(32, 'b'));
  system->set_iceberg_child_spec_id(0);
  system->set_iceberg_child_content(0);
  auto* spec = system->add_iceberg_partition_specs();
  spec->set_spec_id(0);
  auto* field = spec->add_fields();
  field->set_transform("identity");
  field->set_result_type("long");
  field->set_field_id(1000);
  field->set_name("renamed_id");
  data->mutable_connector_detail()->PackFrom(detail);
  const auto parsed = parseTaskConfig(serializeRaw(request));
  const auto& fields = parsed.splits.at(0).system_table_iceberg_partition_specs.at(0).fields;
  ASSERT_EQ(fields.size(), 1);
  EXPECT_EQ(fields[0].field_id, 1000);
  EXPECT_EQ(fields[0].name, "renamed_id");

  field->set_field_id(0);
  data->mutable_connector_detail()->PackFrom(detail);
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
  field->set_field_id(1000);
  *spec->add_fields() = *field;
  data->mutable_connector_detail()->PackFrom(detail);
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
  spec->mutable_fields()->RemoveLast();
  system->set_iceberg_child_spec_id(1);
  data->mutable_connector_detail()->PackFrom(detail);
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
}

TEST(ParseTaskConfigTest, ParsesPinnedPaimonFileManifestAndRejectsMixedSourceKinds) {
  auto request = validRequest();
  auto* data = addFileSplit(request, proto::FILE_FORMAT_METADATA_AVRO);
  const std::string manifest = "s3://warehouse/db/orders/manifest/manifest-a";
  const std::string snapshot = "s3://warehouse/db/orders/snapshot/snapshot-7";
  data->set_connector_id("paimon");
  data->set_source_id("db.orders$files");
  data->set_source_snapshot(snapshot);
  data->set_data_object_id(manifest);
  data->set_opaque_location(manifest);
  data->set_start_offset(0);
  data->set_length(256);
  proto::FileSplitDetail detail;
  ASSERT_TRUE(data->connector_detail().UnpackTo(&detail));
  auto* system = detail.mutable_system_table();
  system->set_kind(proto::SystemTableSplitDetail::PAIMON_FILES);
  system->set_snapshot_id(7);
  system->set_object_sha256(std::string(32, 'b'));
  auto* context = system->mutable_paimon_files();
  context->mutable_snapshot()->set_uri(snapshot);
  context->mutable_snapshot()->set_length(128);
  context->mutable_snapshot()->set_sha256(std::string(32, 'c'));
  context->add_schemas()->set_schema_json("{}");
  auto* selected = context->add_files();
  selected->set_identifier_sha256(std::string(32, 'd'));
  selected->set_file_uri("s3://warehouse/db/orders/bucket-0/a.parquet");
  data->mutable_connector_detail()->PackFrom(detail);
  auto parsed = parseTaskConfig(serializeRaw(request));
  ASSERT_EQ(parsed.splits.size(), 1);
  EXPECT_EQ(parsed.splits[0].system_table, split::SystemTableKind::PAIMON_FILES);
  EXPECT_EQ(parsed.splits[0].system_table_paimon_files_context, context->SerializeAsString());

  system->set_iceberg_format_version(2);
  data->mutable_connector_detail()->PackFrom(detail);
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
  system->clear_iceberg_format_version();
  context->mutable_snapshot()->set_sha256("short");
  data->mutable_connector_detail()->PackFrom(detail);
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
  context->mutable_snapshot()->set_sha256(std::string(32, 'c'));
  data->mutable_connector_detail()->PackFrom(detail);
  data->set_source_snapshot("different-snapshot");
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
  data->set_source_snapshot(snapshot);
  system->set_kind(proto::SystemTableSplitDetail::ICEBERG_FILES);
  data->mutable_connector_detail()->PackFrom(detail);
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
}

TEST(ParseTaskConfigTest, ParsesPinnedPaimonSnapshotObjectAndRejectsKindDrift) {
  auto request = validRequest();
  auto* data_split = addFileSplit(request, proto::FILE_FORMAT_METADATA_JSON);
  const std::string location = "s3://warehouse/db/orders/snapshot/snapshot-7";
  data_split->set_connector_id("paimon");
  data_split->set_source_id("db.orders$snapshots");
  data_split->set_source_snapshot(std::string(64, 'a'));
  data_split->set_data_object_id(location);
  data_split->set_opaque_location(location);
  data_split->set_start_offset(0);
  data_split->set_length(256);
  proto::FileSplitDetail detail;
  ASSERT_TRUE(data_split->connector_detail().UnpackTo(&detail));
  detail.mutable_system_table()->set_kind(
      proto::SystemTableSplitDetail::PAIMON_SNAPSHOTS);
  detail.mutable_system_table()->set_snapshot_id(7);
  detail.mutable_system_table()->set_object_sha256(std::string(32, 'b'));
  data_split->mutable_connector_detail()->PackFrom(detail);

  auto parsed = parseTaskConfig(serializeRaw(request));
  ASSERT_EQ(parsed.splits.size(), 1U);
  EXPECT_EQ(parsed.splits.front().system_table,
      split::SystemTableKind::PAIMON_SNAPSHOTS);
  EXPECT_EQ(parsed.splits.front().system_table_snapshot_id, 7);
  EXPECT_EQ(parsed.splits.front().system_table_object_sha256, std::string(32, 'b'));

  detail.mutable_system_table()->set_snapshot_id(0);
  data_split->mutable_connector_detail()->PackFrom(detail);
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
  detail.mutable_system_table()->set_snapshot_id(7);
  detail.mutable_system_table()->clear_object_sha256();
  data_split->mutable_connector_detail()->PackFrom(detail);
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
  detail.mutable_system_table()->set_object_sha256(std::string(32, 'b'));
  data_split->mutable_connector_detail()->PackFrom(detail);
  data_split->set_connector_id("iceberg");
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
}

TEST(ParseTaskConfigTest, ParsesPinnedPaimonTagAndRejectsIdentityDrift) {
  auto request = validRequest();
  auto* data_split = addFileSplit(request, proto::FILE_FORMAT_METADATA_JSON);
  const std::string location = "s3://warehouse/db/orders/tag/tag-v1";
  data_split->set_connector_id("paimon");
  data_split->set_source_id("db.orders$tags");
  data_split->set_source_snapshot(std::string(64, 'a'));
  data_split->set_data_object_id(location);
  data_split->set_opaque_location(location);
  data_split->set_start_offset(0);
  data_split->set_length(256);
  proto::FileSplitDetail detail;
  ASSERT_TRUE(data_split->connector_detail().UnpackTo(&detail));
  detail.mutable_system_table()->set_kind(proto::SystemTableSplitDetail::PAIMON_TAGS);
  detail.mutable_system_table()->set_tag_name("v1");
  detail.mutable_system_table()->set_object_sha256(std::string(32, 'b'));
  data_split->mutable_connector_detail()->PackFrom(detail);

  auto parsed = parseTaskConfig(serializeRaw(request));
  ASSERT_EQ(parsed.splits.size(), 1U);
  EXPECT_EQ(parsed.splits.front().system_table, split::SystemTableKind::PAIMON_TAGS);
  EXPECT_EQ(parsed.splits.front().system_table_tag_name, "v1");

  detail.mutable_system_table()->set_tag_name("v2");
  data_split->mutable_connector_detail()->PackFrom(detail);
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
  detail.mutable_system_table()->set_tag_name("v1");
  detail.mutable_system_table()->set_snapshot_id(7);
  data_split->mutable_connector_detail()->PackFrom(detail);
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
}

TEST(ParseTaskConfigTest, ParsesPinnedPaimonOptionsAndRejectsSchemaDrift) {
  auto request = validRequest();
  auto* data_split = addFileSplit(request, proto::FILE_FORMAT_METADATA_JSON);
  const std::string location = "s3://warehouse/db/orders/schema/schema-2";
  data_split->set_connector_id("paimon");
  data_split->set_source_id("db.orders$options");
  data_split->set_source_snapshot(std::string(64, 'a'));
  data_split->set_data_object_id(location);
  data_split->set_opaque_location(location);
  data_split->set_start_offset(0);
  data_split->set_length(256);
  proto::FileSplitDetail detail;
  ASSERT_TRUE(data_split->connector_detail().UnpackTo(&detail));
  detail.mutable_system_table()->set_kind(proto::SystemTableSplitDetail::PAIMON_OPTIONS);
  detail.mutable_system_table()->set_schema_id(2);
  detail.mutable_system_table()->set_object_sha256(std::string(32, 'b'));
  data_split->mutable_connector_detail()->PackFrom(detail);

  auto parsed = parseTaskConfig(serializeRaw(request));
  ASSERT_EQ(parsed.splits.size(), 1U);
  EXPECT_EQ(parsed.splits.front().system_table, split::SystemTableKind::PAIMON_OPTIONS);
  ASSERT_TRUE(parsed.splits.front().system_table_schema_id.has_value());
  EXPECT_EQ(parsed.splits.front().system_table_schema_id.value(), 2);

  detail.mutable_system_table()->set_schema_id(3);
  data_split->mutable_connector_detail()->PackFrom(detail);
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
}

TEST(ParseTaskConfigTest, ParsesPinnedPaimonSchemasAndRejectsWrongObject) {
  auto request = validRequest();
  auto* data_split = addFileSplit(request, proto::FILE_FORMAT_METADATA_JSON);
  const std::string location = "s3://warehouse/db/orders/schema/schema-0";
  data_split->set_connector_id("paimon");
  data_split->set_source_id("db.orders$schemas");
  data_split->set_source_snapshot(std::string(64, 'a'));
  data_split->set_data_object_id(location);
  data_split->set_opaque_location(location);
  data_split->set_start_offset(0);
  data_split->set_length(256);
  proto::FileSplitDetail detail;
  ASSERT_TRUE(data_split->connector_detail().UnpackTo(&detail));
  detail.mutable_system_table()->set_kind(proto::SystemTableSplitDetail::PAIMON_SCHEMAS);
  detail.mutable_system_table()->set_schema_id(0);
  detail.mutable_system_table()->set_object_sha256(std::string(32, 'b'));
  data_split->mutable_connector_detail()->PackFrom(detail);

  auto parsed = parseTaskConfig(serializeRaw(request));
  ASSERT_EQ(parsed.splits.size(), 1U);
  EXPECT_EQ(parsed.splits.front().system_table, split::SystemTableKind::PAIMON_SCHEMAS);
  EXPECT_EQ(parsed.splits.front().system_table_schema_id.value(), 0);

  detail.mutable_system_table()->set_schema_id(1);
  data_split->mutable_connector_detail()->PackFrom(detail);
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
}

TEST(ParseTaskConfigTest, ParsesLocalPaimonBranchDirectoryAndRejectsWrongIdentity) {
  auto request = validRequest();
  auto* data_split = addFileSplit(request, proto::FILE_FORMAT_METADATA_DIRECTORY);
  const std::string location = "file:/warehouse/db/orders/branch/branch-feature%20beta";
  data_split->set_connector_id("paimon");
  data_split->set_source_id("db.orders$branches");
  data_split->set_source_snapshot(std::string(64, 'a'));
  data_split->set_data_object_id(location);
  data_split->set_opaque_location(location);
  data_split->set_start_offset(0);
  data_split->set_length(0);
  proto::FileSplitDetail detail;
  ASSERT_TRUE(data_split->connector_detail().UnpackTo(&detail));
  detail.mutable_system_table()->set_kind(proto::SystemTableSplitDetail::PAIMON_BRANCHES);
  detail.mutable_system_table()->set_branch_name("feature beta");
  detail.mutable_system_table()->set_directory_mtime_ms(1700000000123);
  detail.mutable_system_table()->set_directory_device(2);
  detail.mutable_system_table()->set_directory_inode(42);
  detail.mutable_system_table()->set_directory_zone_offset_seconds(8 * 3600);
  data_split->mutable_connector_detail()->PackFrom(detail);

  auto parsed = parseTaskConfig(serializeRaw(request));
  ASSERT_EQ(parsed.splits.size(), 1U);
  EXPECT_EQ(parsed.splits.front().system_table, split::SystemTableKind::PAIMON_BRANCHES);
  EXPECT_EQ(parsed.splits.front().system_table_branch_name, "feature beta");
  EXPECT_EQ(parsed.splits.front().system_table_directory_mtime_ms, 1700000000123);
  EXPECT_EQ(parsed.splits.front().system_table_directory_device, 2);
  EXPECT_EQ(parsed.splits.front().system_table_directory_inode, 42);
  EXPECT_EQ(parsed.splits.front().system_table_directory_zone_offset_seconds, 8 * 3600);

  detail.mutable_system_table()->set_branch_name("other");
  data_split->mutable_connector_detail()->PackFrom(detail);
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
  detail.mutable_system_table()->set_branch_name("feature beta");
  detail.mutable_system_table()->set_object_sha256(std::string(32, 'x'));
  data_split->mutable_connector_detail()->PackFrom(detail);
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
  detail.mutable_system_table()->clear_object_sha256();
  detail.mutable_system_table()->set_directory_zone_offset_seconds(19 * 3600);
  data_split->mutable_connector_detail()->PackFrom(detail);
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
}

TEST(ParseTaskConfigTest, RejectsDuplicateDataSplitIdentity) {
  auto request = validRequest();
  auto* first = addFileSplit(request);
  auto* duplicate = request.add_splits();
  duplicate->CopyFrom(*first);
  duplicate->set_data_object_id("object-2");
  duplicate->set_opaque_location("/data/part-1.parquet");

  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
}

TEST(ParseTaskConfigTest, ParsesPaimonAndOrcDetails) {
  auto request = validRequest();
  auto* split = addFileSplit(request, proto::FILE_FORMAT_ORC);
  split->set_connector_id("paimon");
  proto::FileSplitDetail detail;
  ASSERT_TRUE(split->connector_detail().UnpackTo(&detail));
  auto* paimon = detail.mutable_paimon();
  paimon->mutable_merge_policy()->set_remove_record_on_delete(true);
  paimon->set_read_mode(proto::PAIMON_READ_MODE_CHANGELOG);
  paimon->set_merge_engine(proto::PAIMON_MERGE_ENGINE_PARTIAL_UPDATE);
  paimon->set_changelog_producer(proto::PAIMON_CHANGELOG_PRODUCER_INPUT);
  paimon->add_primary_keys("pk");
  paimon->set_needs_merge(true);
  auto* file = paimon->add_lsm_levels()->add_files();
  file->set_opaque_location("/data/l0.orc");
  file->set_file_format(proto::FILE_FORMAT_ORC);
  file->set_file_size(4096);
  file->mutable_scan()->set_physical_rows(100);
  file->mutable_scan()->set_row_end(100);
  file->mutable_scan()->mutable_schema_evolution();
  split->mutable_connector_detail()->PackFrom(detail);

  auto config = parseTaskConfig(serializeRaw(request));
  ASSERT_TRUE(config.splits.front().paimon.has_value());
  EXPECT_EQ(config.splits.front().file_format, split::FileFormatType::ORC);
  EXPECT_EQ(config.splits.front().paimon->merge_engine, "partial_update");
  EXPECT_TRUE(config.splits.front().paimon->changelog_mode);
  EXPECT_TRUE(config.splits.front().paimon->needs_merge);
  EXPECT_TRUE(config.splits.front().paimon->remove_record_on_delete);
  EXPECT_EQ(
      config.splits.front().paimon->lsm_levels.front().files.front().file_path,
      "/data/l0.orc");
}

TEST(ParseTaskConfigTest, ParsesCanonicalLanceSplit) {
  auto request = validRequest();
  auto* data_split = addFileSplit(request, proto::FILE_FORMAT_LANCE);
  data_split->set_opaque_location("/data/items.lance");

  const auto config = parseTaskConfig(serializeRaw(request));

  ASSERT_EQ(config.splits.size(), 1U);
  const auto& parsed = config.splits.front();
  EXPECT_EQ(parsed.file_format, split::FileFormatType::LANCE);
  EXPECT_EQ(parsed.file_path, "/data/items.lance");
  EXPECT_EQ(parsed.table_name, "catalog.schema.items");
  EXPECT_EQ(parsed.source_snapshot, "123");
  EXPECT_EQ(parsed.data_object_id, "object-1");
  EXPECT_FALSE(parsed.storage_binding.empty());
  proto::FileSplitDetail read;
  ASSERT_TRUE(read.ParseFromString(parsed.file_scan_detail));
  const auto& partition = read.scan_partition();
  EXPECT_EQ(partition.partition_id(), 42u);
  EXPECT_EQ(partition.physical_rows(), 100u);
}

TEST(ParseTaskConfigTest, LancePartitionAndWorkerLocalStorageFailClosed) {
  auto request = validRequest();
  auto* split = addFileSplit(request, proto::FILE_FORMAT_LANCE);
  split->set_opaque_location("/data/items.lance");
  const auto valid = request;
  request.clear_worker_local_storage_bindings();
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::invalid_argument);
  request = valid;
  request.mutable_worker_local_storage_bindings(0)->set_tenant_id("tenant-b");
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::invalid_argument);
  request = valid;
  request.mutable_splits(0)->set_opaque_location("/data-other/items.lance");
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::invalid_argument);
  request = valid;
  request.mutable_splits(0)->set_start_offset(1);
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
  request = valid;
  auto* duplicate = request.add_splits();
  *duplicate = request.splits(0);
  duplicate->set_split_id("different-split-id");
  duplicate->set_data_object_id("different-object-id");
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
}

TEST(ParseTaskConfigTest, BucketRootWarehouseRetainsExactCatalogContainment) {
  proto::ObjectStoreExecutionBinding binding;
  binding.set_warehouse_uri("s3://warehouse");
  EXPECT_NO_THROW(validateStorageLocation(binding, "s3://warehouse/db/items.lance"));
  binding.set_warehouse_uri("s3://warehouse/");
  EXPECT_NO_THROW(validateStorageLocation(binding, "s3://warehouse/db/items.lance"));
  for (const auto& invalid : {"s3://warehouse", "s3://warehouse/", "s3://other/db/items.lance",
                             "s3://warehouse/../other/items.lance", "s3://warehouse/%2e%2e/items.lance",
                             "s3://warehouse/db%2fitems.lance", "s3://warehouse/db%00items.lance",
                             "s3:///items.lance", "s3://"}) {
    EXPECT_THROW(validateStorageLocation(binding, invalid), std::invalid_argument) << invalid;
  }
  binding.set_warehouse_uri("s3://warehouse/catalog");
  EXPECT_NO_THROW(validateStorageLocation(binding, "s3://warehouse/catalog/db/items.lance"));
  EXPECT_THROW(validateStorageLocation(binding, "s3://warehouse/catalog-other/items.lance"),
               std::invalid_argument);
}

TEST(ParseTaskConfigTest, RejectsUnpinnedLanceSplit) {
  auto request = validRequest();
  auto* data_split = addFileSplit(request, proto::FILE_FORMAT_LANCE);
  data_split->set_source_snapshot("latest");
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);

  data_split->set_source_snapshot("0");
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
}

TEST(ParseTaskConfigTest, RejectsUntypedOrMalformedSplitDetail) {
  auto request = validRequest();
  auto* split = request.add_splits();
  split->set_split_id("split-1");
  split->set_connector_id("iceberg");
  split->set_opaque_location("/data/file.parquet");
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);

  auto unspecified = validRequest();
  addFileSplit(unspecified, proto::FILE_FORMAT_UNSPECIFIED);
  EXPECT_THROW(parseTaskConfig(serializeRaw(unspecified)), std::runtime_error);
}

TEST(ParseTaskConfigTest, ParsesResourceWriteShuffleAndResultOptions) {
  auto request = validRequest();
  auto* grant = request.mutable_resource_grant();
  grant->set_grant_id("grant-1");
  grant->set_native_memory_bytes(512ULL * 1024 * 1024);
  grant->set_shuffle_buffer_bytes(16ULL * 1024 * 1024);
  grant->set_cpu_slots(8);
  grant->set_spill_bytes(256ULL * 1024 * 1024);
  auto* options = request.mutable_execution_options();
  (*options->mutable_session_properties())["velox.max_drivers"] = "8";
  auto* write = options->mutable_write();
  write->set_target_table("lakehouse.db.orders");
  write->set_write_mode("OVERWRITE");
  write->set_file_format(proto::FILE_FORMAT_PARQUET);
  write->set_target_file_size(128LL * 1024 * 1024);
  write->add_partition_spec("day=2026-09-19");
  write->add_partition_spec("region=cn");
  addConnectorWriteBinding(write);
  auto* shuffle = options->mutable_shuffle();
  shuffle->set_serde(proto::BATCH_SERDE_ARROW_BATCH);
  shuffle->set_compression_codec("zstd");
  shuffle->set_batch_size(1024);
  shuffle->set_persistence_mode(
      proto::SHUFFLE_PERSISTENCE_MODE_MEMORY_AND_DISK);
  shuffle->set_partition_count(16);
  shuffle->add_partition_key_indices(2);
  options->set_direct_result_output(true);

  auto config = parseTaskConfig(serializeRaw(request));
  EXPECT_EQ(config.resource_limit.max_memory_bytes, 512LL * 1024 * 1024);
  EXPECT_EQ(config.resource_limit.max_shuffle_buffer_bytes, 16LL * 1024 * 1024);
  EXPECT_EQ(config.resource_limit.max_threads, 8);
  EXPECT_EQ(config.resource_limit.max_spill_bytes, 256LL * 1024 * 1024);
  EXPECT_EQ(config.session_properties.at("velox.max_drivers"), "8");
  EXPECT_EQ(config.write_config.target_table, "lakehouse.db.orders");
  EXPECT_EQ(config.write_config.write_mode, "OVERWRITE");
  EXPECT_EQ(config.write_config.partition_spec,
            std::vector<std::string>({"day=2026-09-19", "region=cn"}));
  EXPECT_EQ(config.write_config.file_format,
            split::FileFormatType::PARQUET);
  EXPECT_EQ(config.write_config.target_file_size, 128LL * 1024 * 1024);
  EXPECT_EQ(config.num_shuffle_partitions, 16);
  EXPECT_EQ(config.shuffle_batch_serde, proto::BATCH_SERDE_ARROW_BATCH);
  EXPECT_EQ(config.shuffle_compression_codec, "zstd");
  EXPECT_EQ(config.shuffle_batch_size, 1024);
  EXPECT_EQ(config.shuffle_persistence_mode,
            proto::SHUFFLE_PERSISTENCE_MODE_MEMORY_AND_DISK);
  EXPECT_FALSE(config.shuffle_execution.has_value());
  ASSERT_EQ(config.shuffle_partition_key_indices.size(), 1U);
  EXPECT_EQ(config.shuffle_partition_key_indices.front(), 2);
  EXPECT_TRUE(config.is_result_task);
}

TEST(ParseTaskConfigTest, ParsesAttemptBoundRemoteShuffleExecution) {
  auto request = remoteShuffleRequest();
  auto* shuffle = request.mutable_execution_options()->mutable_shuffle();
  shuffle->set_batch_size(2048);
  shuffle->set_partition_count(32);
  auto* binding = shuffle->mutable_execution_binding();
  binding->set_shuffle_id("shuffle-query-7-stage-3");
  binding->clear_endpoints();
  auto* tls_endpoint = binding->add_endpoints();
  tls_endpoint->set_host("shuffle-a.internal");
  tls_endpoint->set_port(9443);
  tls_endpoint->set_partition_start(0);
  tls_endpoint->set_partition_end(16);
  tls_endpoint->set_tls_identity("shuffle-a.internal");
  auto* plaintext_endpoint = binding->add_endpoints();
  plaintext_endpoint->set_host("10.0.0.12");
  plaintext_endpoint->set_port(9400);
  plaintext_endpoint->set_partition_start(16);
  plaintext_endpoint->set_partition_end(32);

  auto config = parseTaskConfig(serializeRaw(request));

  EXPECT_EQ(config.shuffle_persistence_mode,
            proto::SHUFFLE_PERSISTENCE_MODE_MEMORY_AND_REMOTE);
  ASSERT_TRUE(config.shuffle_execution.has_value());
  const auto& parsed = *config.shuffle_execution;
  EXPECT_EQ(parsed.shuffle_id, "shuffle-query-7-stage-3");
  EXPECT_EQ(parsed.plan_epoch, config.plan_epoch);
  EXPECT_EQ(parsed.owner_id, config.owner_id);
  EXPECT_EQ(parsed.owner_incarnation, config.owner_incarnation);
  EXPECT_EQ(parsed.owner_fencing_token, config.owner_fencing_token);
  ASSERT_EQ(parsed.endpoints.size(), 2U);
  EXPECT_EQ(parsed.endpoints[0].host, "shuffle-a.internal");
  EXPECT_EQ(parsed.endpoints[0].port, 9443);
  EXPECT_EQ(parsed.endpoints[0].partition_start, 0);
  EXPECT_EQ(parsed.endpoints[0].partition_end, 16);
  EXPECT_EQ(parsed.endpoints[0].tls_identity, "shuffle-a.internal");
  EXPECT_EQ(parsed.endpoints[1].host, "10.0.0.12");
  EXPECT_EQ(parsed.endpoints[1].port, 9400);
  EXPECT_TRUE(parsed.endpoints[1].tls_identity.empty());
}

TEST(ParseTaskConfigTest, RejectsShuffleWithoutResidentBufferGrant) {
  auto request = remoteShuffleRequest();
  request.mutable_resource_grant()->clear_shuffle_buffer_bytes();
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
}

TEST(ParseTaskConfigTest, ParsesConnectorOwnedLanceWriteBinding) {
  auto request = validRequest();
  request.set_task_kind(proto::TASK_KIND_WRITE);
  auto* write = request.mutable_execution_options()->mutable_write();
  write->set_target_table("lakehouse.db.vectors");
  write->set_write_mode("INSERT");
  write->set_file_format(proto::FILE_FORMAT_LANCE);
  auto* lance = write->mutable_output_binding()->mutable_lance();
  lance->set_dataset_uri("s3://warehouse/db/vectors.lance");
  lance->set_operation(proto::LANCE_WRITE_OPERATION_APPEND);
  lance->set_expected_read_version(41);
  lance->mutable_schema_fence_sha256()->set_algorithm(
      proto::Checksum::ALGORITHM_SHA256);
  lance->mutable_schema_fence_sha256()->set_digest(std::string(32, '\x5a'));
  lance->set_write_id("write-intent-123");
  lance->set_transaction_staging_prefix(
      "_pangrova/write-staging/write-intent-123/task-42-attempt-2");
  lance->set_generation("generation-9");

  addLanceCatalogFence(request);

  // Preserve the former positive-version APPEND as an explicit rejection:
  // a structural fixture cannot manufacture an accepted PostgreSQL base.
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::invalid_argument);
  lance = write->mutable_output_binding()->mutable_lance();
  lance->set_operation(proto::LANCE_WRITE_OPERATION_CREATE);
  lance->set_expected_read_version(0);
  auto config = parseTaskConfig(serializeRaw(request));
  ASSERT_TRUE(config.write_config.lance_output.has_value());
  const auto& binding = *config.write_config.lance_output;
  EXPECT_EQ(binding.dataset_uri, "s3://warehouse/db/vectors.lance");
  EXPECT_EQ(binding.operation, proto::LANCE_WRITE_OPERATION_CREATE);
  EXPECT_EQ(binding.expected_read_version, 0);
  EXPECT_TRUE(binding.accepted_base_snapshot.empty());
  EXPECT_EQ(binding.write_id, "write-intent-123");
  EXPECT_EQ(
      binding.transaction_staging_prefix,
      request.execution_options().write().output_binding().lance().transaction_staging_prefix());
  EXPECT_EQ(binding.write_column_names, std::vector<std::string>({"id"}));
  EXPECT_EQ(binding.generation, "generation-9");
  EXPECT_EQ(binding.schema_fence_sha256.front(), 0x5a);
  EXPECT_FALSE(binding.task_attempt.empty());
  proto::TaskAttemptId decoded;
  ASSERT_TRUE(decoded.ParseFromArray(
      binding.task_attempt.data(),
      static_cast<int>(binding.task_attempt.size())));
  EXPECT_EQ(decoded.SerializeAsString(), request.attempt().SerializeAsString());
}

TEST(ParseTaskConfigTest, LanceWriteBindingFailsClosed) {
  auto request = validRequest();
  request.set_task_kind(proto::TASK_KIND_WRITE);
  auto* write = request.mutable_execution_options()->mutable_write();
  write->set_target_table("lakehouse.db.vectors");
  write->set_write_mode("INSERT");
  write->set_file_format(proto::FILE_FORMAT_LANCE);
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);

  auto* lance = write->mutable_output_binding()->mutable_lance();
  lance->set_dataset_uri("s3://warehouse/db/vectors.lance");
  lance->set_operation(proto::LANCE_WRITE_OPERATION_APPEND);
  lance->set_expected_read_version(0);
  lance->mutable_schema_fence_sha256()->set_algorithm(
      proto::Checksum::ALGORITHM_SHA256);
  lance->mutable_schema_fence_sha256()->set_digest(std::string(32, '\x5a'));
  lance->set_write_id("write-intent-123");
  lance->set_transaction_staging_prefix(
      "_pangrova/write-staging/write-intent-123/task-42-attempt-2");
  lance->set_generation("generation-9");
  addLanceCatalogFence(request);
  lance = write->mutable_output_binding()->mutable_lance();
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);

  lance->set_expected_read_version(41);
  lance->set_operation(proto::LANCE_WRITE_OPERATION_OVERWRITE);
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);

  write->set_write_mode("OVERWRITE");
  lance->mutable_schema_fence_sha256()->set_digest("short");
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);

  lance->mutable_schema_fence_sha256()->set_digest(std::string(32, '\x5a'));
  lance->set_transaction_staging_prefix("../other-write");
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
}

TEST(ParseTaskConfigTest, LanceRejectsCatalogColumnsAndAttemptStagingMutation) {
  auto request = validRequest();
  request.set_task_kind(proto::TASK_KIND_WRITE);
  auto* write = request.mutable_execution_options()->mutable_write();
  write->set_write_mode("INSERT");
  write->set_file_format(proto::FILE_FORMAT_LANCE);
  auto* lance = write->mutable_output_binding()->mutable_lance();
  lance->set_dataset_uri("s3://warehouse/vectors.lance");
  lance->set_operation(proto::LANCE_WRITE_OPERATION_APPEND);
  lance->set_expected_read_version(1);
  lance->mutable_schema_fence_sha256()->set_algorithm(proto::Checksum::ALGORITHM_SHA256);
  lance->mutable_schema_fence_sha256()->set_digest(std::string(32, 'x'));
  lance->set_write_id("write-intent-123");
  lance->set_generation("generation-9");
  addLanceCatalogFence(request);
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::invalid_argument);
  lance = write->mutable_output_binding()->mutable_lance();
  lance->set_operation(proto::LANCE_WRITE_OPERATION_CREATE);
  lance->set_expected_read_version(0);
  auto valid = request;
  EXPECT_NO_THROW(parseTaskConfig(serializeRaw(valid)));
  request.mutable_execution_options()->mutable_write()->mutable_output_binding()
      ->mutable_lance()->mutable_catalog()->set_catalog_revision(0);
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
  request = valid;
  request.mutable_execution_options()->mutable_write()->mutable_output_binding()
      ->mutable_lance()->set_write_column_names(0, "other");
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
  request = valid;
  request.mutable_attempt()->set_attempt_id(request.attempt().attempt_id() + 1);
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
}

class ParseTaskConfigLanceWriteTest : public lance::test::LanceDatasetTestFixture {
 protected:
  std::vector<uint8_t> writeRows(const lance::LanceWriterConfig& config,
      const facebook::velox::RowTypePtr& type, const std::vector<int64_t>& values) {
    auto ids = facebook::velox::BaseVector::create(facebook::velox::BIGINT(), values.size(), pool_.get());
    for (size_t index = 0; index < values.size(); ++index) {
      ids->asFlatVector<int64_t>()->set(index, values[index]);
    }
    auto rows = std::make_shared<facebook::velox::RowVector>(pool_.get(), type, nullptr,
        values.size(), std::vector<facebook::velox::VectorPtr>{ids});
    lance::LanceBatchWriter writer(pool_, type, config);
    writer.writeBatch(rows);
    return writer.finish();
  }

  proto::TaskRequest requestFor(const lance::LanceWriterConfig& config) {
    auto request = validRequest();
    if (!request.mutable_attempt()->ParseFromArray(config.taskAttempt.data(), config.taskAttempt.size())) {
      throw std::runtime_error("Malformed actual PostgreSQL-issued task attempt");
    }
    request.set_task_kind(proto::TASK_KIND_WRITE);
    request.mutable_security()->set_principal("lance-fixture-writer");
    request.mutable_security()->set_tenant_id(config.tenantId);
    auto* write = request.mutable_execution_options()->mutable_write();
    write->set_write_mode("INSERT");
    write->set_file_format(proto::FILE_FORMAT_LANCE);
    *write->mutable_output_binding() = pangrova::test::LanceManagedSourceTestAuthority::allocationBinding(config);
    // The catalog binding is the immutable template; allocation also returns
    // the exact Java-derived prefix for this already-issued complete attempt.
    write->mutable_output_binding()->mutable_lance()->set_transaction_staging_prefix(config.transactionStagingPrefix);
    if (!request.add_worker_local_storage_bindings()->ParseFromString(config.storageBinding)) {
      throw std::runtime_error("Malformed actual catalog storage binding");
    }
    return request;
  }
};

TEST_F(ParseTaskConfigLanceWriteTest, PgAcceptedAppendBindingRoundTripsAndWritesExactHistoricalBase) {
  const auto type = facebook::velox::ROW({"id"}, {facebook::velox::BIGINT()});
  const auto path = (tmpDir_ / "append.lance").string();
  const auto create = createConfig(path, "parser-create", type);
  ASSERT_EQ(commitFragment(create, writeRows(create, type, {71, 72})), 1);
  const auto accepted = pangrova::test::LanceManagedSourceTestAuthority::reference(path, 1);
  const auto append = createConfig(path, "parser-append", type, lance::LanceWriteOperation::kAppend, 1);
  const auto request = requestFor(append);
  const auto parsed = parseTaskConfig(serializeRaw(request));
  ASSERT_TRUE(parsed.write_config.lance_output.has_value());
  const auto& binding = *parsed.write_config.lance_output;
  EXPECT_EQ(binding.dataset_uri, path);
  EXPECT_EQ(binding.operation, proto::LANCE_WRITE_OPERATION_APPEND);
  EXPECT_EQ(binding.expected_read_version, 1);
  EXPECT_EQ(binding.write_id, append.writeId);
  EXPECT_EQ(binding.transaction_staging_prefix, append.transactionStagingPrefix);
  EXPECT_EQ(binding.write_column_names, std::vector<std::string>({"id"}));
  EXPECT_EQ(binding.generation, append.generation);
  EXPECT_EQ(binding.schema_fence_sha256, append.schemaFenceSha256);
  EXPECT_EQ(binding.task_attempt, append.taskAttempt);
  EXPECT_EQ(binding.storage_binding, append.storageBinding);
  EXPECT_EQ(binding.accepted_base_snapshot, append.acceptedBaseSnapshot);
  proto::AcceptedSourceSnapshotRef base;
  ASSERT_TRUE(base.ParseFromString(binding.accepted_base_snapshot));
  EXPECT_EQ(base.SerializeAsString(), accepted.SerializeAsString());
  EXPECT_EQ(base.catalog().SerializeAsString(), request.execution_options().write().output_binding().lance().catalog().SerializeAsString());
  EXPECT_EQ(base.accepted_origin_catalog_revision(), accepted.accepted_origin_catalog_revision());
  EXPECT_EQ(base.tenant_id(), append.tenantId);
  EXPECT_EQ(base.table_id(), accepted.table_id());
  EXPECT_EQ(base.table_generation(), accepted.table_generation());
  EXPECT_EQ(base.version(), 1);
  EXPECT_EQ(base.manifest_sha256().digest(), accepted.manifest_sha256().digest());
  EXPECT_EQ(base.physical_schema_sha256().digest(), accepted.physical_schema_sha256().digest());
  EXPECT_EQ(base.manifest_relative_path(), accepted.manifest_relative_path());
  EXPECT_EQ(base.membership_id(), accepted.membership_id());
  auto missing = request;
  missing.mutable_execution_options()->mutable_write()->mutable_output_binding()->mutable_lance()->clear_base_snapshot();
  EXPECT_THROW(parseTaskConfig(serializeRaw(missing)), std::invalid_argument);
  auto different = request;
  different.mutable_execution_options()->mutable_write()->mutable_output_binding()->mutable_lance()->mutable_base_snapshot()->set_version(2);
  EXPECT_THROW(parseTaskConfig(serializeRaw(different)), std::invalid_argument);
  auto consumer = append;
  consumer.outputPath = binding.dataset_uri;
  consumer.operation = static_cast<lance::LanceWriteOperation>(binding.operation);
  consumer.expectedReadVersion = binding.expected_read_version;
  consumer.acceptedBaseSnapshot = binding.accepted_base_snapshot;
  consumer.schemaFenceSha256 = binding.schema_fence_sha256;
  consumer.writeId = binding.write_id;
  consumer.generation = binding.generation;
  consumer.transactionStagingPrefix = binding.transaction_staging_prefix;
  consumer.taskAttempt = binding.task_attempt;
  consumer.storageBinding = binding.storage_binding;
  ASSERT_EQ(commitFragment(consumer, writeRows(consumer, type, {73})), 2);
  EXPECT_EQ(pangrova::test::LanceManagedSourceTestAuthority::reference(path, 1).SerializeAsString(), accepted.SerializeAsString());
  std::vector<int64_t> actual;
  pangrova::test::PartitionedTestDatasetReader reader(pool_.get(), type, path, 2, {"id"});
  while (auto rows = reader.next(16)) {
    auto* ids = rows->childAt(0)->asFlatVector<int64_t>();
    for (facebook::velox::vector_size_t index = 0; index < rows->size(); ++index) actual.push_back(ids->valueAt(index));
  }
  std::sort(actual.begin(), actual.end());
  EXPECT_EQ(actual, std::vector<int64_t>({71, 72, 73}));
}

TEST(ParseTaskConfigTest, GenericWriteBindingFailsClosed) {
  auto request = validRequest();
  request.set_task_kind(proto::TASK_KIND_WRITE);
  auto* write = request.mutable_execution_options()->mutable_write();
  write->set_target_table("lakehouse.db.orders");
  write->set_write_mode("INSERT");
  write->set_file_format(proto::FILE_FORMAT_PARQUET);

  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);

  addConnectorWriteBinding(write);
  EXPECT_NO_THROW(parseTaskConfig(serializeRaw(request)));

  write->mutable_output_binding()
      ->mutable_connector()
      ->mutable_output_handle()
      ->set_payload("mutated-after-checksum");
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);

  addConnectorWriteBinding(write);
  write->mutable_output_binding()
      ->mutable_connector()
      ->mutable_catalog()
      ->set_catalog_revision(0);
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
}

TEST(ParseTaskConfigTest, RejectsRemoteShuffleWithoutCompleteBinding) {
  auto missing_binding = remoteShuffleRequest();
  missing_binding.mutable_execution_options()->mutable_shuffle()
      ->clear_execution_binding();
  EXPECT_THROW(
      parseTaskConfig(serializeRaw(missing_binding)), std::runtime_error);

  auto missing_shuffle_id = remoteShuffleRequest();
  missing_shuffle_id.mutable_execution_options()->mutable_shuffle()
      ->mutable_execution_binding()->clear_shuffle_id();
  EXPECT_THROW(
      parseTaskConfig(serializeRaw(missing_shuffle_id)), std::runtime_error);

  auto missing_endpoints = remoteShuffleRequest();
  missing_endpoints.mutable_execution_options()->mutable_shuffle()
      ->mutable_execution_binding()->clear_endpoints();
  EXPECT_THROW(
      parseTaskConfig(serializeRaw(missing_endpoints)), std::runtime_error);

  auto missing_owner = remoteShuffleRequest();
  missing_owner.mutable_execution_options()->mutable_shuffle()
      ->mutable_execution_binding()->clear_execution_owner();
  EXPECT_THROW(
      parseTaskConfig(serializeRaw(missing_owner)), std::runtime_error);

  auto invalid_endpoint = remoteShuffleRequest();
  invalid_endpoint.mutable_execution_options()->mutable_shuffle()
      ->mutable_execution_binding()->mutable_endpoints(0)->set_port(0);
  EXPECT_THROW(
      parseTaskConfig(serializeRaw(invalid_endpoint)), std::runtime_error);

  auto missing_capability = remoteShuffleRequest();
  missing_capability.mutable_execution_options()->mutable_shuffle()
      ->mutable_execution_binding()->clear_write_capability();
  EXPECT_THROW(
      parseTaskConfig(serializeRaw(missing_capability)), std::runtime_error);

  auto expired_capability = remoteShuffleRequest();
  expired_capability.mutable_execution_options()->mutable_shuffle()
      ->mutable_execution_binding()->mutable_write_capability()
      ->set_expires_at_epoch_ms(1);
  EXPECT_THROW(
      parseTaskConfig(serializeRaw(expired_capability)), std::runtime_error);

  auto coverage_gap = remoteShuffleRequest();
  coverage_gap.mutable_execution_options()->mutable_shuffle()
      ->mutable_execution_binding()->mutable_endpoints(0)
      ->set_partition_end(7);
  EXPECT_THROW(
      parseTaskConfig(serializeRaw(coverage_gap)), std::runtime_error);
}

TEST(ParseTaskConfigTest, RejectsRemoteShuffleAttemptIdentityDrift) {
  auto epoch_drift = remoteShuffleRequest();
  epoch_drift.mutable_execution_options()->mutable_shuffle()
      ->mutable_execution_binding()->set_plan_epoch(12);
  EXPECT_THROW(parseTaskConfig(serializeRaw(epoch_drift)), std::runtime_error);

  auto owner_id_drift = remoteShuffleRequest();
  owner_id_drift.mutable_execution_options()->mutable_shuffle()
      ->mutable_execution_binding()->mutable_execution_owner()
      ->set_owner_id("coordinator-b");
  EXPECT_THROW(
      parseTaskConfig(serializeRaw(owner_id_drift)), std::runtime_error);

  auto incarnation_drift = remoteShuffleRequest();
  incarnation_drift.mutable_execution_options()->mutable_shuffle()
      ->mutable_execution_binding()->mutable_execution_owner()
      ->set_owner_incarnation("process-b");
  EXPECT_THROW(
      parseTaskConfig(serializeRaw(incarnation_drift)), std::runtime_error);

  auto fence_drift = remoteShuffleRequest();
  fence_drift.mutable_execution_options()->mutable_shuffle()
      ->mutable_execution_binding()->mutable_execution_owner()
      ->set_fencing_token(18);
  EXPECT_THROW(parseTaskConfig(serializeRaw(fence_drift)), std::runtime_error);
}

TEST(ParseTaskConfigTest, RejectsNonRemoteShuffleExecutionBinding) {
  auto request = validRequest();
  auto* shuffle =
      request.mutable_execution_options()->mutable_shuffle();
  shuffle->set_serde(proto::BATCH_SERDE_ARROW_BATCH);
  shuffle->set_compression_codec("zstd");
  shuffle->set_batch_size(1024);
  shuffle->set_persistence_mode(
      proto::SHUFFLE_PERSISTENCE_MODE_MEMORY_AND_DISK);
  shuffle->set_partition_count(8);
  auto* binding = shuffle->mutable_execution_binding();
  binding->set_shuffle_id("unexpected-shuffle");
  binding->set_plan_epoch(request.attempt().plan_epoch());
  *binding->mutable_execution_owner() = request.attempt().execution_owner();
  auto* endpoint = binding->add_endpoints();
  endpoint->set_host("shuffle-a.internal");
  endpoint->set_port(9400);

  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
}

TEST(ParseTaskConfigTest, CanonicalShuffleCodecsAndSignedRowLimitAreAdmittedExactly) {
  for (const auto* codec : {"none", "lz4", "zstd"}) {
    auto request = remoteShuffleRequest();
    request.mutable_execution_options()->mutable_shuffle()->set_compression_codec(codec);
    const auto config = parseTaskConfig(serializeRaw(request));
    EXPECT_EQ(config.shuffle_compression_codec, codec);
    EXPECT_EQ(config.shuffle_batch_serde, proto::BATCH_SERDE_ARROW_BATCH);
    EXPECT_EQ(config.shuffle_batch_size, 1024);
  }
  for (const auto* codec : {"NONE", "LZ4", "ZSTD", "snappy", "", " none"}) {
    auto request = remoteShuffleRequest();
    request.mutable_execution_options()->mutable_shuffle()->set_compression_codec(codec);
    EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
  }
  for (const auto rows : {0U, 2147483648U, 4294967295U}) {
    auto request = remoteShuffleRequest();
    request.mutable_execution_options()->mutable_shuffle()->set_batch_size(rows);
    EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
  }
  for (const auto serde : {0, 1, 99}) {
    auto request = remoteShuffleRequest();
    request.mutable_execution_options()->mutable_shuffle()->set_serde(static_cast<proto::BatchSerde>(serde));
    EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
  }
  auto request = remoteShuffleRequest();
  request.mutable_execution_options()->mutable_shuffle()->set_batch_size(2147483647U);
  EXPECT_EQ(parseTaskConfig(serializeRaw(request)).shuffle_batch_size, 2147483647);
}

TEST(ParseTaskConfigTest, PreservesCanonicalOperatorBindingAndBudgets) {
  auto request = validRequest();
  auto* binding = addExactBinding(request);

  auto config = parseTaskConfig(serializeRaw(request));

  ASSERT_EQ(config.operator_bindings.size(), 1U);
  EXPECT_EQ(config.operator_bindings.at("topk-1").SerializeAsString(),
            binding->SerializeAsString());
  EXPECT_EQ(config.resource_limit.max_candidate_rows, 25);
  EXPECT_EQ(config.resource_limit.max_candidate_bytes, 1024 * 1024);
}

TEST(ParseTaskConfigTest, PreservesGovernedAiInvocationBudget) {
  auto request = validRequest();
  auto* binding = addAiBinding(request);

  auto config = parseTaskConfig(serializeRaw(request));

  ASSERT_EQ(config.operator_bindings.size(), 1U);
  EXPECT_EQ(config.operator_bindings.at("ai-invoke-1").SerializeAsString(),
            binding->SerializeAsString());
  EXPECT_EQ(config.resource_limit.max_provider_concurrency, 2);
  EXPECT_EQ(config.resource_limit.max_provider_items, 64);
  EXPECT_EQ(config.resource_limit.max_provider_request_bytes, 64'000);
  EXPECT_EQ(config.resource_limit.max_provider_response_bytes, 32'000);

  auto missing_operator_budget = request;
  missing_operator_budget.mutable_operator_bindings(0)
      ->mutable_resources()
      ->clear_provider_items();
  EXPECT_THROW(
      parseTaskConfig(serializeRaw(missing_operator_budget)),
      std::runtime_error);

  auto missing_output_contract = request;
  missing_output_contract.mutable_operator_bindings(0)
      ->mutable_model()->clear_output_type();
  EXPECT_THROW(
      parseTaskConfig(serializeRaw(missing_output_contract)),
      std::runtime_error);

  auto insufficient_grant = request;
  insufficient_grant.mutable_resource_grant()->set_provider_items(63);
  EXPECT_THROW(
      parseTaskConfig(serializeRaw(insufficient_grant)),
      std::runtime_error);
}

TEST(ParseTaskConfigTest, RejectsReuseOfNonDeterministicModelInvocations) {
  auto request = validRequest();
  request.mutable_security()->set_policy_context_hash("secured-plan-sha256");
  auto* binding = addAiBinding(request);
  auto* reuse = binding->mutable_reuse_policy();
  reuse->set_enabled(true);
  reuse->set_ttl_ms(60'000);
  reuse->set_max_entries(10'000);
  reuse->set_max_bytes(64 * 1024 * 1024);
  reuse->set_similarity_threshold(0.98);
  reuse->set_key_dimension(256);
  reuse->set_require_complete(true);

  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);

  auto exact_threshold = request;
  exact_threshold.mutable_operator_bindings(0)
      ->mutable_reuse_policy()->set_similarity_threshold(1.0);
  EXPECT_THROW(parseTaskConfig(serializeRaw(exact_threshold)), std::runtime_error);

  auto without_reuse = request;
  without_reuse.mutable_operator_bindings(0)->clear_reuse_policy();
  auto config = parseTaskConfig(serializeRaw(without_reuse));
  EXPECT_FALSE(config.operator_bindings.at("ai-invoke-1").has_reuse_policy());
  EXPECT_EQ(config.policy_context_hash, "secured-plan-sha256");
  EXPECT_FALSE(config.dependency_scope_hash.empty());
  EXPECT_FALSE(config.dependency_version_hash.empty());

  auto missing_policy_identity = request;
  missing_policy_identity.mutable_security()->clear_policy_context_hash();
  EXPECT_THROW(
      parseTaskConfig(serializeRaw(missing_policy_identity)),
      std::runtime_error);

  auto unsafe_threshold = request;
  unsafe_threshold.mutable_operator_bindings(0)
      ->mutable_reuse_policy()->set_similarity_threshold(0.5);
  EXPECT_THROW(
      parseTaskConfig(serializeRaw(unsafe_threshold)), std::runtime_error);

  auto incomplete = request;
  incomplete.mutable_operator_bindings(0)
      ->mutable_reuse_policy()->set_require_complete(false);
  EXPECT_THROW(parseTaskConfig(serializeRaw(incomplete)), std::runtime_error);
}

TEST(ParseTaskConfigTest, RejectsUnsafeOrUnbudgetedOperatorBindings) {
  auto duplicate = validRequest();
  addExactBinding(duplicate);
  addExactBinding(duplicate);
  duplicate.mutable_resource_grant()->set_candidate_rows(50);
  EXPECT_THROW(parseTaskConfig(serializeRaw(duplicate)), std::runtime_error);

  auto deadline_drift = validRequest();
  addExactBinding(deadline_drift)->mutable_execution_policy()
      ->set_deadline_epoch_ms(1);
  EXPECT_THROW(parseTaskConfig(serializeRaw(deadline_drift)), std::runtime_error);

  auto insufficient = validRequest();
  addExactBinding(insufficient, "topk-1", 25);
  insufficient.mutable_resource_grant()->set_candidate_rows(24);
  EXPECT_THROW(parseTaskConfig(serializeRaw(insufficient)), std::runtime_error);

  auto unknown = validRequest();
  addExactBinding(unknown)->mutable_capability()->set_capability_id(
      "pangrova.retrieval.unknown");
  EXPECT_THROW(parseTaskConfig(serializeRaw(unknown)), std::runtime_error);
}

TEST(ParseTaskConfigTest, ValidatesGenericArtifactFormatAndObjectIdentity) {
  auto request = validRequest();
  addFileSplit(request);
  auto* binding = addExactBinding(request);
  *binding->mutable_capability() =
      pangrova::capability::diskAnnRetrievalDescriptor().ref();
  addValidArtifact(binding);
  auto config = parseTaskConfig(serializeRaw(request));
  ASSERT_EQ(config.operator_bindings.at("topk-1").artifacts_size(), 1);

  auto missing_format = request;
  missing_format.mutable_operator_bindings(0)->mutable_artifacts(0)->clear_format();
  EXPECT_THROW(parseTaskConfig(serializeRaw(missing_format)), std::runtime_error);

  auto duplicate_ordinal = request;
  auto* artifact = duplicate_ordinal.mutable_operator_bindings(0)
                       ->mutable_artifacts(0);
  auto first = artifact->split_objects(0);
  auto* duplicate = artifact->add_split_objects();
  duplicate->CopyFrom(first);
  duplicate->set_object_id("index-1:part-1");
  signArtifact(artifact);
  EXPECT_THROW(parseTaskConfig(serializeRaw(duplicate_ordinal)), std::runtime_error);

  auto tampered = request;
  tampered.mutable_operator_bindings(0)
      ->mutable_artifacts(0)
      ->mutable_split_objects(0)
      ->set_row_count(11);
  EXPECT_THROW(parseTaskConfig(serializeRaw(tampered)), std::runtime_error);
}

TEST(ParseTaskConfigTest, ValidatesSharedArtifactDependencyCanonicalVector) {
  auto request = validRequest();
  addFileSplit(request);
  auto* binding = addExactBinding(request);
  *binding->mutable_capability() =
      pangrova::capability::diskAnnRetrievalDescriptor().ref();
  addValidArtifact(binding);
  auto* artifact = binding->mutable_artifacts(0);
  artifact->clear_dependencies();
  const std::vector<std::array<std::string, 3>> dependencies = {
      {"A", "id", "v1"},
      {"AA", "id", "v1"},
      {"B", "i", "v2"},
      {"B", "id", "v1"},
      {"\xEE\x80\x80", "prefix", "v1"},       // U+E000
      {"\xF0\x90\x80\x80", "prefix", "v1"},  // U+10000
  };
  for (const auto& [kind, id, version] : dependencies) {
    auto* dependency = artifact->add_dependencies();
    dependency->set_kind(kind);
    dependency->set_id(id);
    dependency->set_version(version);
  }
  signDependencySet(artifact);
  signArtifact(artifact);

  EXPECT_EQ(
      toHex(artifact->dependency_set_checksum().digest()),
      "9e4739f5cda65f336924f1d0fc609b448cb2c5080df0f6c3ab7065d48af5ab79");
  EXPECT_NO_THROW(parseTaskConfig(serializeRaw(request)));

  auto utf16_order = request;
  auto* reordered = utf16_order.mutable_operator_bindings(0)
                        ->mutable_artifacts(0);
  reordered->mutable_dependencies()->SwapElements(4, 5);
  signDependencySet(reordered);
  signArtifact(reordered);
  EXPECT_THROW(
      parseTaskConfig(serializeRaw(utf16_order)), std::runtime_error);
}

TEST(ParseTaskConfigTest, RejectsArtifactDependencyUnicodeControls) {
  auto request = validRequest();
  addFileSplit(request);
  auto* binding = addExactBinding(request);
  *binding->mutable_capability() =
      pangrova::capability::diskAnnRetrievalDescriptor().ref();
  addValidArtifact(binding);
  auto* artifact = binding->mutable_artifacts(0);

  artifact->mutable_dependencies(0)->set_id(std::string("id\0bad", 6));
  signDependencySet(artifact);
  signArtifact(artifact);
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);

  artifact->mutable_dependencies(0)->set_id("id\xC2\x85");  // U+0085
  signDependencySet(artifact);
  signArtifact(artifact);
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
}

TEST(ParseTaskConfigTest, RejectsArtifactWritesOutsideImplementedBackgroundCapability) {
  auto foreground = validRequest();
  auto* foreground_binding = addExactBinding(foreground);
  auto* output = foreground_binding->mutable_output_artifact();
  output->set_artifact_id("index-1");
  output->set_artifact_type("SEARCH_INDEX");
  output->set_generation("7");
  output->set_source_id("catalog.schema.items");
  output->set_source_snapshot("snapshot-7");
  output->set_staging_uri("s3://bucket/index-1/7/staging/page-0");
  output->mutable_format()->set_format_id("pangrova.index.inverted");
  output->mutable_format()->set_major_version(1);
  output->mutable_format()->set_schema_sha256(std::string(32, '\0'));
  output->set_publication_fencing_token(
      foreground.attempt().fencing_token());
  output->set_work_unit_id("page-0");
  output->set_idempotency_key("job-1:page-0");
  output->add_expected_source_object_ids("object-1");
  EXPECT_THROW(parseTaskConfig(serializeRaw(foreground)), std::runtime_error);

  auto background = validRequest();
  background.set_task_kind(proto::TASK_KIND_BACKGROUND_BUILD);
  addFileSplit(background);
  auto* background_binding = addExactBinding(background);
  background_binding->mutable_execution_policy()->clear_approximation();
  background_binding->mutable_execution_policy()->set_idempotency_key(
      "job-1:page-0");
  *background_binding->mutable_output_artifact() = *output;
  EXPECT_THROW(parseTaskConfig(serializeRaw(background)), std::runtime_error);
}

TEST(ParseTaskConfigTest, AcceptsFencedBudgetedDiskAnnBackgroundBuild) {
  auto request = validRequest();
  request.set_task_kind(proto::TASK_KIND_BACKGROUND_BUILD);
  addFileSplit(request);
  auto* grant = request.mutable_resource_grant();
  grant->set_grant_id("artifact-grant");
  grant->set_cpu_slots(2);
  grant->set_native_memory_bytes(64 * 1024 * 1024);
  grant->set_spill_bytes(64 * 1024 * 1024);
  grant->set_object_store_read_bytes(64 * 1024 * 1024);
  grant->set_object_store_write_bytes(64 * 1024 * 1024);

  auto* binding = request.add_operator_bindings();
  binding->set_plan_node_id("artifact-build-1");
  *binding->mutable_capability() =
      pangrova::capability::diskAnnArtifactBuildDescriptor().ref();
  auto* policy = binding->mutable_execution_policy();
  policy->set_coverage_policy(proto::COVERAGE_POLICY_REQUIRE_COMPLETE);
  policy->set_fallback_policy(proto::FALLBACK_POLICY_FORBID);
  policy->set_retry_class(proto::RETRY_CLASS_IDEMPOTENT);
  policy->set_deadline_epoch_ms(request.deadline_epoch_ms());
  policy->set_idempotency_key("job-1:page-0:generation-7");
  binding->mutable_resources()->set_object_store_read_bytes(
      64 * 1024 * 1024);
  binding->mutable_resources()->set_object_store_write_bytes(
      64 * 1024 * 1024);

  auto* output = binding->mutable_output_artifact();
  output->set_artifact_id("index-1");
  output->set_artifact_type("VECTOR_INDEX");
  output->set_generation("7");
  output->set_source_id("catalog.schema.items");
  output->set_source_snapshot("123");
  output->set_staging_uri("s3://bucket/index-1/7/staging/page-0");
  *output->mutable_format() = pangrova::capability::diskAnnArtifactFormat();
  *output->mutable_row_locator() =
      pangrova::capability::physicalRowLocatorContract();
  output->add_expected_source_object_ids("object-1");
  output->set_publication_fencing_token(request.attempt().fencing_token());
  output->set_work_unit_id("page-0");
  output->set_idempotency_key(policy->idempotency_key());
  output->set_work_kind(proto::ARTIFACT_WORK_KIND_SOURCE_MAP);
  addArtifactProvenance(output);
  auto* dimension = output->add_properties();
  dimension->set_name("dimension");
  dimension->mutable_value()->set_int64_value(768);

  const auto config = parseTaskConfig(serializeRaw(request));
  EXPECT_EQ(config.task_type, proto::TASK_KIND_BACKGROUND_BUILD);
  EXPECT_EQ(config.resource_limit.max_object_store_write_bytes,
            64 * 1024 * 1024);
  ASSERT_EQ(config.operator_bindings.size(), 1);
  EXPECT_TRUE(config.operator_bindings.at("artifact-build-1")
                  .has_output_artifact());

  auto export_request = request;
  export_request.set_task_kind(proto::TASK_KIND_BACKGROUND_EXPORT);
  export_request.mutable_operator_bindings(0)
      ->mutable_output_artifact()
      ->set_execution_kind(proto::ARTIFACT_EXECUTION_KIND_EXPORT);
  const auto export_config = parseTaskConfig(serializeRaw(export_request));
  EXPECT_EQ(export_config.task_type, proto::TASK_KIND_BACKGROUND_EXPORT);
}

TEST(ParseTaskConfigTest, AcceptsOnlyRelationalWorkUnitTabularExports) {
  auto request = validRequest();
  request.set_task_kind(proto::TASK_KIND_BACKGROUND_EXPORT);
  addFileSplit(request);
  auto* grant = request.mutable_resource_grant();
  grant->set_grant_id("tabular-export-grant");
  grant->set_cpu_slots(2);
  grant->set_native_memory_bytes(64 * 1024 * 1024);
  grant->set_spill_bytes(64 * 1024 * 1024);
  grant->set_object_store_read_bytes(64 * 1024 * 1024);
  grant->set_object_store_write_bytes(64 * 1024 * 1024);
  auto* binding = request.add_operator_bindings();
  binding->set_plan_node_id("tabular-export");
  *binding->mutable_capability() =
      pangrova::capability::tabularExportDescriptor().ref();
  auto* policy = binding->mutable_execution_policy();
  policy->set_coverage_policy(proto::COVERAGE_POLICY_REQUIRE_COMPLETE);
  policy->set_fallback_policy(proto::FALLBACK_POLICY_FORBID);
  policy->set_retry_class(proto::RETRY_CLASS_IDEMPOTENT);
  policy->set_deadline_epoch_ms(request.deadline_epoch_ms());
  policy->set_idempotency_key("job:stage:partition:generation");
  binding->mutable_resources()->set_object_store_write_bytes(64 * 1024 * 1024);
  binding->mutable_resources()->set_object_store_read_bytes(64 * 1024 * 1024);
  auto* output = binding->mutable_output_artifact();
  output->set_artifact_id("tabular-result");
  output->set_artifact_type("TABULAR_EXPORT");
  output->set_generation("7");
  output->set_source_id("authorized-relation");
  output->set_source_snapshot("exact-dependency-set");
  output->set_staging_uri("s3://bucket/exports/job/stage/partition");
  *output->mutable_format() = pangrova::capability::tabularExportFormat();
  output->set_publication_fencing_token(request.attempt().fencing_token());
  output->set_work_unit_id("stage-1-partition-0");
  output->set_output_scope_id(output->work_unit_id());
  output->set_idempotency_key(policy->idempotency_key());
  output->set_work_kind(proto::ARTIFACT_WORK_KIND_RELATIONAL_PARTITION);
  addArtifactProvenance(output);
  output->set_execution_kind(proto::ARTIFACT_EXECUTION_KIND_EXPORT);

  const auto config = parseTaskConfig(serializeRaw(request));
  ASSERT_EQ(config.operator_bindings.size(), 1);
  EXPECT_EQ(config.operator_bindings.at("tabular-export").output_artifact().work_kind(),
            proto::ARTIFACT_WORK_KIND_RELATIONAL_PARTITION);

  auto physical_locator = request;
  *physical_locator.mutable_operator_bindings(0)->mutable_output_artifact()->mutable_row_locator() =
      pangrova::capability::physicalRowLocatorContract();
  EXPECT_THROW(parseTaskConfig(serializeRaw(physical_locator)), std::runtime_error);
  auto unbounded_read = request;
  unbounded_read.mutable_resource_grant()->clear_object_store_read_bytes();
  EXPECT_THROW(parseTaskConfig(serializeRaw(unbounded_read)), std::runtime_error);
  auto fabricated_source = request;
  fabricated_source.mutable_operator_bindings(0)->mutable_output_artifact()
      ->add_expected_source_object_ids("object-1");
  EXPECT_THROW(parseTaskConfig(serializeRaw(fabricated_source)), std::runtime_error);
  auto wrong_scope = request;
  wrong_scope.mutable_operator_bindings(0)->mutable_output_artifact()->set_output_scope_id("other-partition");
  EXPECT_THROW(parseTaskConfig(serializeRaw(wrong_scope)), std::runtime_error);
  auto legacy_source_map = request;
  auto* legacy = legacy_source_map.mutable_operator_bindings(0)->mutable_output_artifact();
  legacy->set_work_kind(proto::ARTIFACT_WORK_KIND_SOURCE_MAP);
  legacy->set_source_id("catalog.schema.items");
  legacy->set_source_snapshot("123");
  legacy->clear_output_scope_id();
  legacy->add_expected_source_object_ids("object-1");
  EXPECT_THROW(parseTaskConfig(serializeRaw(legacy_source_map)), std::runtime_error);
}

TEST(ParseTaskConfigTest, AcceptsGovernedEmbeddingAndOneArtifactSink) {
  auto request = validRequest();
  request.set_task_kind(proto::TASK_KIND_BACKGROUND_BUILD);
  addFileSplit(request);
  auto* grant = request.mutable_resource_grant();
  grant->set_grant_id("embedding-artifact-grant");
  grant->set_cpu_slots(2);
  grant->set_native_memory_bytes(64 * 1024 * 1024);
  grant->set_spill_bytes(64 * 1024 * 1024);
  grant->set_object_store_read_bytes(64 * 1024 * 1024);
  grant->set_object_store_write_bytes(64 * 1024 * 1024);

  auto* invocation = addAiBinding(request);
  *invocation->mutable_capability() =
      pangrova::capability::aiEmbeddingDescriptor().ref();
  invocation->mutable_capability()->clear_implementation_build_id();
  setEmbeddingOutputType(invocation, 768);

  auto* sink = request.add_operator_bindings();
  sink->set_plan_node_id("artifact-build-1");
  *sink->mutable_capability() =
      pangrova::capability::diskAnnArtifactBuildDescriptor().ref();
  auto* policy = sink->mutable_execution_policy();
  policy->set_coverage_policy(proto::COVERAGE_POLICY_REQUIRE_COMPLETE);
  policy->set_fallback_policy(proto::FALLBACK_POLICY_FORBID);
  policy->set_retry_class(proto::RETRY_CLASS_IDEMPOTENT);
  policy->set_deadline_epoch_ms(request.deadline_epoch_ms());
  policy->set_idempotency_key("job-1:page-0:artifact-build-1");
  sink->mutable_resources()->set_object_store_read_bytes(64 * 1024 * 1024);
  sink->mutable_resources()->set_object_store_write_bytes(64 * 1024 * 1024);
  auto* output = sink->mutable_output_artifact();
  output->set_artifact_id("index-1");
  output->set_artifact_type("VECTOR_INDEX");
  output->set_generation("7");
  output->set_source_id("catalog.schema.items");
  output->set_source_snapshot("123");
  output->set_staging_uri("s3://bucket/index-1/7/staging/page-0");
  *output->mutable_format() = pangrova::capability::diskAnnArtifactFormat();
  *output->mutable_row_locator() =
      pangrova::capability::physicalRowLocatorContract();
  output->add_expected_source_object_ids("object-1");
  output->set_publication_fencing_token(request.attempt().fencing_token());
  output->set_work_unit_id("page-0");
  output->set_idempotency_key(policy->idempotency_key());
  output->set_work_kind(proto::ARTIFACT_WORK_KIND_SOURCE_MAP);
  addArtifactProvenance(output);
  auto* dimension = output->add_properties();
  dimension->set_name("dimension");
  dimension->mutable_value()->set_int64_value(768);

  const auto config = parseTaskConfig(serializeRaw(request));
  EXPECT_EQ(config.operator_bindings.size(), 2);
  EXPECT_TRUE(config.operator_bindings.at("ai-invoke-1").has_model());
  EXPECT_TRUE(config.operator_bindings.at("artifact-build-1")
                  .has_output_artifact());

  auto two_outputs = request;
  *two_outputs.mutable_operator_bindings(0)->mutable_output_artifact() =
      *output;
  EXPECT_THROW(
      parseTaskConfig(serializeRaw(two_outputs)), std::runtime_error);
}

TEST(ParseTaskConfigTest,
     AcceptsGeneratedValueModelBuildAndExactOverlayBindings) {
  auto build = validRequest();
  build.set_task_kind(proto::TASK_KIND_BACKGROUND_BUILD);
  addFileSplit(build);
  auto* grant = build.mutable_resource_grant();
  grant->set_grant_id("generated-values-build-grant");
  grant->set_cpu_slots(2);
  grant->set_native_memory_bytes(64 * 1024 * 1024);
  grant->set_spill_bytes(64 * 1024 * 1024);
  grant->set_object_store_read_bytes(64 * 1024 * 1024);
  grant->set_object_store_write_bytes(64 * 1024 * 1024);

  auto* model = addAiBinding(build);
  model->set_plan_node_id("generated-value-model-invocation");
  *model->mutable_capability() =
      pangrova::capability::aiEmbeddingDescriptor().ref();
  model->mutable_capability()->clear_implementation_build_id();
  setEmbeddingOutputType(model, 768);

  auto* sink = build.add_operator_bindings();
  sink->set_plan_node_id("artifact-build-generated-values");
  *sink->mutable_capability() =
      pangrova::capability::generatedValuesArtifactBuildDescriptor().ref();
  auto* policy = sink->mutable_execution_policy();
  policy->set_coverage_policy(proto::COVERAGE_POLICY_REQUIRE_COMPLETE);
  policy->set_fallback_policy(proto::FALLBACK_POLICY_FORBID);
  policy->set_retry_class(proto::RETRY_CLASS_IDEMPOTENT);
  policy->set_deadline_epoch_ms(build.deadline_epoch_ms());
  policy->set_idempotency_key("job-9:page-9:generation-9");
  sink->mutable_resources()->set_object_store_read_bytes(64 * 1024 * 1024);
  sink->mutable_resources()->set_object_store_write_bytes(64 * 1024 * 1024);
  auto* output = sink->mutable_output_artifact();
  output->set_artifact_id("generated-column-1");
  output->set_artifact_type("GENERATED_COLUMN_VALUES");
  output->set_generation("9");
  output->set_source_id("catalog.schema.items");
  output->set_source_snapshot("123");
  output->set_staging_uri("s3://bucket/generated-column-1/9/staging/page-9");
  *output->mutable_format() =
      pangrova::capability::generatedValuesArtifactFormat();
  *output->mutable_row_locator() =
      pangrova::capability::physicalRowLocatorContract();
  output->add_expected_source_object_ids("object-1");
  output->set_publication_fencing_token(build.attempt().fencing_token());
  output->set_work_unit_id("page-9");
  output->set_idempotency_key(policy->idempotency_key());
  output->set_work_kind(proto::ARTIFACT_WORK_KIND_SOURCE_MAP);
  addArtifactProvenance(output);
  addGeneratedValueProperties(output->mutable_properties());

  auto build_config = parseTaskConfig(serializeRaw(build));
  ASSERT_EQ(build_config.operator_bindings.size(), 2);
  EXPECT_TRUE(build_config.operator_bindings
                  .at("generated-value-model-invocation")
                  .has_model());
  EXPECT_TRUE(build_config.operator_bindings
                  .at("artifact-build-generated-values")
                  .has_output_artifact());

  auto overlay = validRequest();
  addFileSplit(overlay);
  auto* overlay_grant = overlay.mutable_resource_grant();
  overlay_grant->set_grant_id("generated-values-overlay-grant");
  overlay_grant->set_native_memory_bytes(64 * 1024 * 1024);
  overlay_grant->set_fill_keys(10);
  overlay_grant->set_object_store_read_bytes(64 * 1024 * 1024);
  auto* overlay_binding = overlay.add_operator_bindings();
  overlay_binding->set_plan_node_id("generated-values-overlay-1");
  *overlay_binding->mutable_capability() =
      pangrova::capability::generatedValuesOverlayDescriptor().ref();
  auto* overlay_policy = overlay_binding->mutable_execution_policy();
  overlay_policy->set_coverage_policy(proto::COVERAGE_POLICY_REQUIRE_COMPLETE);
  overlay_policy->set_fallback_policy(proto::FALLBACK_POLICY_FORBID);
  overlay_policy->set_retry_class(proto::RETRY_CLASS_IDEMPOTENT);
  overlay_policy->set_deadline_epoch_ms(overlay.deadline_epoch_ms());
  overlay_binding->mutable_resources()->set_fill_keys(10);
  overlay_binding->mutable_resources()->set_object_store_read_bytes(
      64 * 1024 * 1024);
  addGeneratedValuesArtifact(overlay_binding);

  auto overlay_config = parseTaskConfig(serializeRaw(overlay));
  ASSERT_EQ(overlay_config.operator_bindings.size(), 1);
  EXPECT_TRUE(pangrova::capability::isGeneratedValuesOverlay(
      overlay_config.operator_bindings
          .at("generated-values-overlay-1")
          .capability()));

  auto snapshot_mismatch = overlay;
  auto* artifact = snapshot_mismatch.mutable_operator_bindings(0)
                       ->mutable_artifacts(0);
  artifact->set_source_snapshot("124");
  signArtifact(artifact);
  EXPECT_THROW(
      parseTaskConfig(serializeRaw(snapshot_mismatch)), std::runtime_error);
}

proto::TaskRequest copyInputRequest() {
  auto request = validRequest();
  auto* file = addFileSplit(request);
  file->set_connector_id("copy");
  file->set_start_offset(0);
  file->set_opaque_location("s3://incoming/data.parquet");
  request.mutable_resource_grant()->set_object_store_read_bytes(15000);
  auto* digest = request.mutable_copy_source_staging_command_sha256();
  digest->set_algorithm(proto::Checksum::ALGORITHM_SHA256);
  digest->set_digest(std::string(32, 'c'));
  auto* local = request.mutable_worker_local_copy_input();
  local->set_split_id(file->split_id());
  local->set_source_version_id(file->source_snapshot());
  local->set_local_file_path("/tmp/copy-scan-private/source.parquet");
  local->set_content_sha256(std::string(32, 'h'));
  local->set_content_size(file->length());
  local->set_pre_native_read_bytes(file->length());
  return request;
}

TEST(ParseTaskConfigTest, CopySplitCannotUseOrdinaryAdmissionWithoutVerifiedLocalInput) {
  auto request = copyInputRequest();
  request.clear_copy_source_staging_command_sha256();
  request.clear_worker_local_copy_input();
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
  EXPECT_THROW(taskConfigFromRequest(request), std::runtime_error);
}

TEST(ParseTaskConfigTest, CopyPrivateInputCannotDisguiseAnotherConnector) {
  auto request = copyInputRequest();
  request.mutable_splits(0)->set_connector_id("iceberg");
  EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error);
  EXPECT_THROW(taskConfigFromRequest(request), std::runtime_error);
}

TEST(ParseTaskConfigTest, CopyPrivateInputOnlyBindsRelationalReadOrWriteTasks) {
  auto request = copyInputRequest();
  EXPECT_NO_THROW(validateWorkerCopyInput(request));
  request.set_task_kind(proto::TASK_KIND_WRITE);
  EXPECT_NO_THROW(validateWorkerCopyInput(request));
  request.set_task_kind(proto::TASK_KIND_SHUFFLE_READ);
  EXPECT_THROW(validateWorkerCopyInput(request), std::runtime_error);
}

TEST(ParseTaskConfigTest, VerifiedCopyBindsOnlyExecutionConfigAndDeductsPreNativeReads) {
  auto request = copyInputRequest();
  const auto encoded = request.SerializeAsString();
  const auto config = parseTaskConfig(serializeRaw(request));
  ASSERT_EQ(config.splits.size(), 1);
  EXPECT_EQ(config.splits.front().file_path, "/tmp/copy-scan-private/source.parquet");
  EXPECT_EQ(config.splits.front().expected_file_sha256, std::string(32, 'h'));
  EXPECT_EQ(config.resource_limit.max_object_store_read_bytes, 10000);
  EXPECT_EQ(request.SerializeAsString(), encoded);
}

TEST(ParseTaskConfigTest, WorkerCopyBindingPreservesOriginalCreationSealAndRestoresPrivateFields) {
  auto request = copyInputRequest();
  auto original = request;
  original.clear_worker_local_copy_input();
  const auto originalIdentity = creationIdentity(original).SerializeAsString();
  const auto encoded = request.SerializeAsString();
  EXPECT_EQ(creationIdentity(request).SerializeAsString(), originalIdentity);
  EXPECT_EQ(request.SerializeAsString(), encoded);
  request.mutable_worker_local_copy_input()->set_local_file_path("/tmp/copy-scan-other/source.parquet");
  EXPECT_EQ(creationIdentity(request).SerializeAsString(), originalIdentity);
  request.mutable_resource_grant()->set_object_store_read_bytes(15001);
  EXPECT_NE(creationIdentity(request).SerializeAsString(), originalIdentity);
}

TEST(ParseTaskConfigTest, CopyLocalInputRejectsVersionLengthDigestBudgetAndPathDrift) {
  const auto valid = copyInputRequest();
  for (int mutation = 0; mutation != 8; ++mutation) {
    auto request = valid;
    auto* local = request.mutable_worker_local_copy_input();
    switch (mutation) {
      case 0: local->set_source_version_id("other"); break;
      case 1: local->set_content_size(4999); break;
      case 2: local->set_content_sha256("bad"); break;
      case 3: local->set_pre_native_read_bytes(4999); break;
      case 4: request.mutable_resource_grant()->set_object_store_read_bytes(4999); break;
      case 5: local->set_local_file_path("/tmp/copy-scan-private/../source.parquet"); break;
      case 6: local->set_local_file_path("/tmp/unowned/source.parquet"); break;
      case 7: request.clear_copy_source_staging_command_sha256(); break;
    }
    EXPECT_THROW(parseTaskConfig(serializeRaw(request)), std::runtime_error) << mutation;
  }
}

}  // namespace
}  // namespace pangrova::jni::test
