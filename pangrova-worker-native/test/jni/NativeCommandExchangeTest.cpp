#include <gtest/gtest.h>
#include "velox/expression/FunctionRegistrySnapshot.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include <openssl/evp.h>

#include "executor/driver/TaskExecutor.h"
#include "jni/command/NativeCommandDispatcher.h"
#include "jni/command/StatusSerializer.h"
#include "jni/registry/HandleRegistry.h"
#include "pangrova/protocol/v2/runtime.pb.h"
#include "velox/exchange/PendingEndpointRegistry.h"

namespace pangrova::jni {
namespace {

namespace proto = pangrova::protocol::v2;

constexpr char kPlanNodeId[] = "external-exchange-input";
constexpr char kExchangeId[] = "exchange-9";

int64_t nowMillis() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

std::string sha256(const std::string& input) {
  std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
  unsigned int size = 0;
  EXPECT_EQ(EVP_Digest(input.data(), input.size(), digest.data(), &size,
                       EVP_sha256(), nullptr),
            1);
  EXPECT_EQ(size, 32u);
  return {reinterpret_cast<const char*>(digest.data()), size};
}

proto::PushExchangeBatchRequest pushRequest(
    int64_t sequence,
    const std::string& batch,
    uint32_t chunk_index,
    const std::string& batch_checksum) {
  proto::PushExchangeBatchRequest request;
  request.set_plan_node_id(kPlanNodeId);
  request.set_exchange_id(kExchangeId);
  auto* chunk = request.mutable_chunk();
  chunk->set_batch_sequence(sequence);
  chunk->set_chunk_index(chunk_index);
  chunk->set_chunk_count(static_cast<uint32_t>(
      1 + (batch.size() - 1) /
          pangrova::velox::kExternalShuffleChunkBytes));
  chunk->set_batch_size_bytes(batch.size());
  const auto offset =
      static_cast<size_t>(chunk_index) *
      pangrova::velox::kExternalShuffleChunkBytes;
  const auto size = std::min(
      pangrova::velox::kExternalShuffleChunkBytes, batch.size() - offset);
  chunk->set_data(batch.data() + offset, size);
  chunk->mutable_chunk_checksum()->set_algorithm(
      proto::Checksum::ALGORITHM_SHA256);
  chunk->mutable_chunk_checksum()->set_digest(sha256(chunk->data()));
  chunk->mutable_batch_checksum()->set_algorithm(
      proto::Checksum::ALGORITHM_SHA256);
  chunk->mutable_batch_checksum()->set_digest(batch_checksum);
  return request;
}

proto::RuntimeCommandResponse parseResponse(const std::vector<uint8_t>& bytes) {
  proto::RuntimeCommandResponse response;
  EXPECT_TRUE(response.ParseFromArray(bytes.data(), bytes.size()));
  return response;
}

class NativeCommandExchangeTest : public ::testing::Test {
protected:
  void SetUp() override {
    HandleRegistry::instance().destroyAll();
    pangrova::velox::PendingEndpointRegistry::instance().clear();
    deadline_epoch_ms_ = nowMillis() + 600'000;

    pangrova::executor::TaskExecutorConfig config;
    config.function_registry =
        facebook::velox::exec::FunctionRegistrySnapshot::capture();
    config.task_id = "consumer-task-3";
    config.query_id = "query-1";
    config.stage_id = 2;
    config.partition_id = 3;
    config.plan_epoch = 9;
    config.attempt_id = 4;
    config.fencing_token = 17;
    config.control_epoch = 6;
    config.owner_id = "coordinator-1";
    config.owner_incarnation = "incarnation-2";
    config.owner_fencing_token = 23;
    config.deadline_epoch_ms = deadline_epoch_ms_;
    config.resource_limit.max_memory_bytes = 1024LL * 1024 * 1024;
    bindContext(config);
    handle_ = HandleRegistry::instance().create(
        std::make_shared<pangrova::executor::TaskExecutor>(config));
  }

  void TearDown() override {
    if (handle_ > 0) {
      HandleRegistry::instance().release(handle_);
      handle_ = 0;
    }
    pangrova::velox::PendingEndpointRegistry::instance().clear();
    HandleRegistry::instance().destroyAll();
  }

  proto::RuntimeCommand command(proto::RuntimeCommandType type) const {
    proto::RuntimeCommand result;
    result.mutable_protocol()->set_major(2);
    result.mutable_protocol()->set_minor(0);
    result.mutable_protocol()->set_rollout_epoch("ai-native-function-resource-r3");
    result.set_command_type(type);
    auto* attempt = result.mutable_attempt();
    attempt->set_query_or_job_id("query-1");
    attempt->set_stage_id(2);
    attempt->set_partition_id(3);
    attempt->set_plan_epoch(9);
    attempt->set_attempt_id(4);
    attempt->set_fencing_token(17);
    attempt->set_task_id("consumer-task-3");
    attempt->set_control_epoch(6);
    auto* owner = attempt->mutable_execution_owner();
    owner->set_owner_id("coordinator-1");
    owner->set_owner_incarnation("incarnation-2");
    owner->set_fencing_token(23);
    auto* worker = attempt->mutable_worker_execution_identity();
    worker->set_admission_domain_id("00000000-0000-0000-0000-000000000001");
    worker->set_incarnation_id("00000000-0000-0000-0000-000000000002");
    return result;
  }

  void bindContext(pangrova::executor::TaskExecutorConfig& config) const {
    proto::ExecutionCallbackContext context;
    *context.mutable_attempt() = command(proto::RUNTIME_COMMAND_TYPE_CONNECT_EXCHANGE_SOURCE).attempt();
    config.callback_context = context.SerializeAsString();
  }

  template <typename Payload>
  proto::RuntimeCommandResponse dispatch(proto::RuntimeCommandType type,
                                         const Payload& payload) const {
    auto request = command(type);
    request.mutable_payload()->PackFrom(payload);
    std::vector<uint8_t> bytes(request.ByteSizeLong());
    EXPECT_TRUE(request.SerializeToArray(bytes.data(), bytes.size()));
    return parseResponse(dispatchCommandWithHandle(bytes, handle_));
  }

  proto::ConnectExchangeSourceRequest externalConnect() const {
    proto::ConnectExchangeSourceRequest request;
    request.set_plan_node_id(kPlanNodeId);
    request.set_exchange_id(kExchangeId);
    auto* external = request.mutable_external();
    external->set_shuffle_id("shuffle-1");
    auto* endpoint = external->add_endpoints();
    endpoint->set_host("shuffle.example.internal");
    endpoint->set_port(7443);
    endpoint->set_partition_start(0);
    endpoint->set_partition_end(4);
    endpoint->set_tls_identity("shuffle.example.internal");
    external->set_output_partition_id(3);
    external->set_plan_epoch(9);
    auto* owner = external->mutable_execution_owner();
    owner->set_owner_id("coordinator-1");
    owner->set_owner_incarnation("incarnation-2");
    owner->set_fencing_token(23);
    external->set_deadline_epoch_ms(deadline_epoch_ms_);
    external->set_partition_count(4);
    external->mutable_read_capability()->set_token("read-capability");
    external->mutable_read_capability()->set_expires_at_epoch_ms(
        deadline_epoch_ms_);
    external->mutable_commit_address()->set_host(
        "shuffle-control.example.internal");
    external->mutable_commit_address()->set_port(7443);
    external->mutable_commit_address()->set_tls_identity(
        "shuffle-control.example.internal");
    external->set_expected_producer_partitions(2);
    external->mutable_expected_producer_set_checksum()->set_algorithm(
        proto::Checksum::ALGORITHM_SHA256);
    external->mutable_expected_producer_set_checksum()->set_digest(
        std::string(32, 'p'));
    return request;
  }

  void connectAndSealExternal() const {
    auto connected = dispatch(
        proto::RUNTIME_COMMAND_TYPE_CONNECT_EXCHANGE_SOURCE, externalConnect());
    ASSERT_TRUE(connected.success()) << connected.error().message();
    proto::NoMoreExchangeSourcesRequest seal;
    seal.set_plan_node_id(kPlanNodeId);
    seal.set_exchange_id(kExchangeId);
    seal.set_expected_source_partitions(1);
    auto sealed =
        dispatch(proto::RUNTIME_COMMAND_TYPE_NO_MORE_EXCHANGE_SOURCES, seal);
    ASSERT_TRUE(sealed.success()) << sealed.error().message();
  }

  int64_t handle_ = 0;
  int64_t deadline_epoch_ms_ = 0;
};

TEST_F(NativeCommandExchangeTest, RegistersDirectTransportFromCanonicalOneof) {
  proto::ConnectExchangeSourceRequest request;
  request.set_plan_node_id("direct-input");
  request.set_exchange_id("direct-exchange");
  auto* direct = request.mutable_direct();
  direct->set_source_task_id("producer-task-0");
  direct->set_source_stage_id(1);
  direct->set_source_partition_id(0);
  direct->set_output_partition_id(3);
  direct->set_plan_epoch(9);
  direct->set_attempt_id(1);
  direct->set_fencing_token(7);
  direct->set_host("127.0.0.1");
  direct->set_port(32000);
  direct->set_signed_ticket("signed-ticket");
  direct->set_ticket_expires_at_epoch_ms(deadline_epoch_ms_);

  auto connected =
      dispatch(proto::RUNTIME_COMMAND_TYPE_CONNECT_EXCHANGE_SOURCE, request);
  ASSERT_TRUE(connected.success()) << connected.error().message();

  proto::NoMoreExchangeSourcesRequest seal;
  seal.set_plan_node_id("direct-input");
  seal.set_exchange_id("direct-exchange");
  seal.set_expected_source_partitions(1);
  auto sealed =
      dispatch(proto::RUNTIME_COMMAND_TYPE_NO_MORE_EXCHANGE_SOURCES, seal);
  ASSERT_TRUE(sealed.success()) << sealed.error().message();
  auto binding =
      pangrova::velox::PendingEndpointRegistry::instance().takeSealedInput(
          handle_, "direct-input");
  ASSERT_TRUE(binding.has_value());
  ASSERT_EQ(binding->sources.size(), 1);
  EXPECT_EQ(binding->sources.front().endpoint.source_task_id,
            "producer-task-0");
}

TEST_F(NativeCommandExchangeTest, ExternalPushAndFinishReachTakenIngress) {
  connectAndSealExternal();
  auto binding =
      pangrova::velox::PendingEndpointRegistry::instance().takeSealedInput(
          handle_, kPlanNodeId);
  ASSERT_TRUE(binding.has_value());
  ASSERT_EQ(binding->sources.size(), 1);
  auto endpoint =
      pangrova::velox::PendingEndpointRegistry::instance().takeEndpoint(
          binding->sources.front().runtime_source_id);
  ASSERT_TRUE(endpoint.has_value());
  ASSERT_NE(endpoint->external_ingress, nullptr);

  const std::string batch = "serialized-page";
  auto push = pushRequest(0, batch, 0, sha256(batch));
  auto pushed = dispatch(proto::RUNTIME_COMMAND_TYPE_PUSH_EXCHANGE_BATCH, push);
  ASSERT_TRUE(pushed.success()) << pushed.error().message();
  auto data = endpoint->external_ingress->take(std::chrono::seconds(1));
  ASSERT_EQ(data.kind,
            pangrova::velox::ExternalShuffleIngress::ResultKind::kData);
  EXPECT_EQ(data.data, "serialized-page");

  proto::FinishExchangeSourceRequest finish;
  finish.set_plan_node_id(kPlanNodeId);
  finish.set_exchange_id(kExchangeId);
  finish.set_last_sequence(0);
  auto finished =
      dispatch(proto::RUNTIME_COMMAND_TYPE_FINISH_EXCHANGE_SOURCE, finish);
  ASSERT_TRUE(finished.success()) << finished.error().message();
  auto terminal = endpoint->external_ingress->take(std::chrono::seconds(1));
  EXPECT_EQ(terminal.kind,
            pangrova::velox::ExternalShuffleIngress::ResultKind::kFinished);
}

TEST_F(NativeCommandExchangeTest, RejectsAnonymousOrMismatchedAttempt) {
  auto anonymous = command(proto::RUNTIME_COMMAND_TYPE_CONNECT_EXCHANGE_SOURCE);
  anonymous.clear_attempt();
  anonymous.mutable_payload()->PackFrom(externalConnect());
  std::vector<uint8_t> anonymous_bytes(anonymous.ByteSizeLong());
  ASSERT_TRUE(anonymous.SerializeToArray(anonymous_bytes.data(),
                                         anonymous_bytes.size()));
  auto missing =
      parseResponse(dispatchCommandWithHandle(anonymous_bytes, handle_));
  ASSERT_FALSE(missing.success());
  EXPECT_EQ(missing.error().code(), "MISSING_COMMAND_ATTEMPT");

  auto mismatched =
      command(proto::RUNTIME_COMMAND_TYPE_CONNECT_EXCHANGE_SOURCE);
  mismatched.mutable_attempt()->set_fencing_token(18);
  mismatched.mutable_payload()->PackFrom(externalConnect());
  std::vector<uint8_t> mismatch_bytes(mismatched.ByteSizeLong());
  ASSERT_TRUE(mismatched.SerializeToArray(mismatch_bytes.data(),
                                          mismatch_bytes.size()));
  auto fenced =
      parseResponse(dispatchCommandWithHandle(mismatch_bytes, handle_));
  ASSERT_FALSE(fenced.success());
  EXPECT_EQ(fenced.error().code(), "COMMAND_ATTEMPT_FENCE_MISMATCH");
}

TEST_F(NativeCommandExchangeTest, RejectsCommandForAnotherWorkerIncarnation) {
  auto request = command(proto::RUNTIME_COMMAND_TYPE_CONNECT_EXCHANGE_SOURCE);
  request.mutable_attempt()->mutable_worker_execution_identity()->set_incarnation_id(
      "00000000-0000-0000-0000-000000000099");
  request.mutable_payload()->PackFrom(externalConnect());
  const auto encoded = request.SerializeAsString();
  const auto response = parseResponse(dispatchCommandWithHandle(
      {encoded.begin(), encoded.end()}, handle_));
  EXPECT_FALSE(response.success());
  EXPECT_EQ(response.error().code(), "COMMAND_ATTEMPT_FENCE_MISMATCH");
}

TEST_F(NativeCommandExchangeTest, UsesDefaultBudgetForUnboundLocalExecutor) {
  HandleRegistry::instance().release(handle_);
  handle_ = 0;
  pangrova::executor::TaskExecutorConfig config;
  config.function_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  config.task_id = "consumer-task-3";
  config.query_id = "query-1";
  config.stage_id = 2;
  config.partition_id = 3;
  config.plan_epoch = 9;
  config.attempt_id = 4;
  config.fencing_token = 17;
  config.control_epoch = 6;
  config.owner_id = "coordinator-1";
  config.owner_incarnation = "incarnation-2";
  config.owner_fencing_token = 23;
  config.deadline_epoch_ms = deadline_epoch_ms_;
  bindContext(config);
  handle_ = HandleRegistry::instance().create(
      std::make_shared<pangrova::executor::TaskExecutor>(config));

  auto connected = dispatch(
      proto::RUNTIME_COMMAND_TYPE_CONNECT_EXCHANGE_SOURCE, externalConnect());
  ASSERT_TRUE(connected.success()) << connected.error().message();

  proto::PushExchangeBatchRequest push;
  push.set_plan_node_id(kPlanNodeId);
  push.set_exchange_id(kExchangeId);
  auto* chunk = push.mutable_chunk();
  chunk->set_batch_sequence(0);
  chunk->set_chunk_index(0);
  chunk->set_chunk_count(65);
  chunk->set_batch_size_bytes(64ULL * 1024 * 1024 + 1);
  chunk->set_data("x");
  chunk->mutable_chunk_checksum()->set_algorithm(
      proto::Checksum::ALGORITHM_SHA256);
  chunk->mutable_chunk_checksum()->set_digest(sha256(chunk->data()));
  chunk->mutable_batch_checksum()->set_algorithm(
      proto::Checksum::ALGORITHM_SHA256);
  chunk->mutable_batch_checksum()->set_digest(std::string(32, 'b'));
  auto rejected =
      dispatch(proto::RUNTIME_COMMAND_TYPE_PUSH_EXCHANGE_BATCH, push);
  ASSERT_FALSE(rejected.success());
  EXPECT_EQ(rejected.error().code(), "INVALID_EXCHANGE_CHUNK");
}

TEST_F(NativeCommandExchangeTest, RejectsMissingTransportAndCorruptBatch) {
  proto::ConnectExchangeSourceRequest missing_transport;
  missing_transport.set_plan_node_id(kPlanNodeId);
  missing_transport.set_exchange_id(kExchangeId);
  auto rejected = dispatch(proto::RUNTIME_COMMAND_TYPE_CONNECT_EXCHANGE_SOURCE,
                           missing_transport);
  ASSERT_FALSE(rejected.success());
  EXPECT_EQ(rejected.error().code(), "MISSING_EXCHANGE_TRANSPORT");

  connectAndSealExternal();
  const std::string batch = "corrupt-page";
  auto push = pushRequest(0, batch, 0, sha256(batch));
  push.mutable_chunk()->mutable_chunk_checksum()->set_digest(
      std::string(32, 'x'));
  auto corrupt =
      dispatch(proto::RUNTIME_COMMAND_TYPE_PUSH_EXCHANGE_BATCH, push);
  ASSERT_FALSE(corrupt.success());
  EXPECT_EQ(corrupt.error().error_class(), proto::ERROR_CLASS_DATA_CORRUPTION);
  EXPECT_EQ(corrupt.error().code(), "EXCHANGE_CHUNK_CHECKSUM_MISMATCH");
}

TEST_F(NativeCommandExchangeTest, ReassemblesBatchAcrossCanonicalChunks) {
  connectAndSealExternal();
  auto binding =
      pangrova::velox::PendingEndpointRegistry::instance().takeSealedInput(
          handle_, kPlanNodeId);
  ASSERT_TRUE(binding.has_value());
  auto endpoint =
      pangrova::velox::PendingEndpointRegistry::instance().takeEndpoint(
          binding->sources.front().runtime_source_id);
  ASSERT_TRUE(endpoint.has_value());

  const std::string batch(2 * 1024 * 1024 + 17, 'w');
  const auto checksum = sha256(batch);
  const auto chunk_count = static_cast<uint32_t>(
      1 + (batch.size() - 1) /
          pangrova::velox::kExternalShuffleChunkBytes);
  for (uint32_t index = 0; index < chunk_count; ++index) {
    auto pushed = dispatch(
        proto::RUNTIME_COMMAND_TYPE_PUSH_EXCHANGE_BATCH,
        pushRequest(0, batch, index, checksum));
    ASSERT_TRUE(pushed.success()) << pushed.error().message();
  }

  auto data = endpoint->external_ingress->take(std::chrono::seconds(1));
  ASSERT_EQ(data.kind,
            pangrova::velox::ExternalShuffleIngress::ResultKind::kData);
  EXPECT_EQ(data.data.size(), batch.size());
  EXPECT_EQ(data.data, batch);
}

TEST_F(NativeCommandExchangeTest, RejectsCorruptReassembledBatchAndChunkOrder) {
  connectAndSealExternal();
  const std::string batch(
      pangrova::velox::kExternalShuffleChunkBytes + 9, 'c');
  const auto checksum = sha256(batch);

  auto out_of_order = dispatch(
      proto::RUNTIME_COMMAND_TYPE_PUSH_EXCHANGE_BATCH,
      pushRequest(0, batch, 1, checksum));
  ASSERT_FALSE(out_of_order.success());
  EXPECT_EQ(out_of_order.error().code(), "INVALID_EXCHANGE_CHUNK_STATE");

  const std::string corrupt_checksum(32, 'x');
  auto first = dispatch(
      proto::RUNTIME_COMMAND_TYPE_PUSH_EXCHANGE_BATCH,
      pushRequest(0, batch, 0, corrupt_checksum));
  ASSERT_TRUE(first.success()) << first.error().message();
  auto second = dispatch(
      proto::RUNTIME_COMMAND_TYPE_PUSH_EXCHANGE_BATCH,
      pushRequest(0, batch, 1, corrupt_checksum));
  ASSERT_FALSE(second.success());
  EXPECT_EQ(second.error().error_class(), proto::ERROR_CLASS_DATA_CORRUPTION);
  EXPECT_EQ(second.error().code(), "EXCHANGE_BATCH_CHECKSUM_MISMATCH");
}

TEST_F(NativeCommandExchangeTest, PropagatesStructuredExternalFailure) {
  connectAndSealExternal();
  auto binding =
      pangrova::velox::PendingEndpointRegistry::instance().takeSealedInput(
          handle_, kPlanNodeId);
  ASSERT_TRUE(binding.has_value());
  auto endpoint =
      pangrova::velox::PendingEndpointRegistry::instance().takeEndpoint(
          binding->sources.front().runtime_source_id);
  ASSERT_TRUE(endpoint.has_value());

  proto::FinishExchangeSourceRequest invalid;
  invalid.set_plan_node_id(kPlanNodeId);
  invalid.set_exchange_id(kExchangeId);
  invalid.set_last_sequence(-1);
  invalid.mutable_failure()->set_error_class(proto::ERROR_CLASS_TRANSPORT);
  invalid.mutable_failure()->set_code("REMOTE_READ_FAILED");
  invalid.mutable_failure()->set_message("connection closed");
  auto invalid_response =
      dispatch(proto::RUNTIME_COMMAND_TYPE_FINISH_EXCHANGE_SOURCE, invalid);
  ASSERT_FALSE(invalid_response.success());
  EXPECT_EQ(invalid_response.error().code(), "INVALID_EXCHANGE_FAILURE");
  const auto executor = HandleRegistry::instance().acquire(handle_);
  EXPECT_FALSE(executor->lifecycleSnapshot().error.has_value());
  EXPECT_EQ(endpoint->external_ingress->take(std::chrono::microseconds(1)).kind,
            pangrova::velox::ExternalShuffleIngress::ResultKind::kPending);

  auto valid = invalid;
  valid.mutable_failure()->set_failed_component("remote-shuffle-client");
  valid.mutable_failure()->set_retryable(true);
  auto failure_response =
      dispatch(proto::RUNTIME_COMMAND_TYPE_FINISH_EXCHANGE_SOURCE, valid);
  ASSERT_TRUE(failure_response.success()) << failure_response.error().message();
  auto terminal = endpoint->external_ingress->take(std::chrono::seconds(1));
  ASSERT_EQ(terminal.kind,
            pangrova::velox::ExternalShuffleIngress::ResultKind::kFailed);
  EXPECT_EQ(terminal.error, valid.failure().message());
  const auto accepted = executor->lifecycleSnapshot();
  ASSERT_TRUE(accepted.error.has_value());
  EXPECT_EQ(accepted.error->SerializeAsString(), valid.failure().SerializeAsString());
  executor->notifyTaskFailed(terminal.error);
  const auto status = serializeTaskStatus(*executor);
  proto::TaskReport report;
  ASSERT_TRUE(report.ParseFromArray(status.data(), static_cast<int>(status.size())));
  EXPECT_EQ(report.state(), proto::TASK_STATE_FAILED);
  ASSERT_TRUE(report.has_error());
  EXPECT_EQ(report.error().SerializeAsString(), valid.failure().SerializeAsString());
}

TEST_F(NativeCommandExchangeTest, RejectsWrongPayloadAndUnknownCommand) {
  proto::NoMoreExchangeSourcesRequest wrong_payload;
  wrong_payload.set_plan_node_id(kPlanNodeId);
  wrong_payload.set_exchange_id(kExchangeId);
  wrong_payload.set_expected_source_partitions(1);
  auto malformed =
      dispatch(proto::RUNTIME_COMMAND_TYPE_PUSH_EXCHANGE_BATCH, wrong_payload);
  ASSERT_FALSE(malformed.success());
  EXPECT_EQ(malformed.error().code(), "INVALID_COMMAND_PAYLOAD");

  auto unknown = command(static_cast<proto::RuntimeCommandType>(999));
  std::vector<uint8_t> bytes(unknown.ByteSizeLong());
  ASSERT_TRUE(unknown.SerializeToArray(bytes.data(), bytes.size()));
  auto response = parseResponse(dispatchCommandWithHandle(bytes, handle_));
  ASSERT_FALSE(response.success());
  EXPECT_EQ(response.error().code(), "UNKNOWN_COMMAND_TYPE");
}

} // namespace
} // namespace pangrova::jni
