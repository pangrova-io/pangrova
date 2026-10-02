#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <openssl/evp.h>

#include "executor/queue/OutputQueue.h"
#include "jni/RuntimeInitializer.h"
#include "pangrova/protocol/v2/plan_extension.pb.h"
#include "shuffle/FlightShuffleServer.h"
#include "shuffle/ShuffleBatchSerializer.h"
#include "shuffle/SerializedShuffleQueue.h"
#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"
#include "velox/exec/ExchangeClient.h"
#include "velox/exec/ExchangeQueue.h"
#include "velox/exchange/PangrovaExchangeSource.h"
#include "velox/exchange/PendingEndpointRegistry.h"
#include "velox/plan/VeloxPlanConverter.h"
#include "velox/runner/VeloxTaskRunner.h"
#include "velox/vector/tests/utils/VectorMaker.h"

#include "../../TestMemoryHelper.h"

namespace pangrova::velox {
namespace {

constexpr int64_t kConsumerHandle = 9101;
constexpr int64_t kProducerHandle = 9201;
constexpr char kPlanNodeId[] = "exchange-input-native-e2e";
constexpr char kExchangeId[] = "native-e2e";
constexpr uint64_t kPlanEpoch = 9;
constexpr uint32_t kAttemptId = 0;
constexpr uint64_t kFencingToken = 38'654'705'665ULL;

std::vector<uint8_t> exchangePlan() {
  substrait::Plan plan;
  auto* root = plan.add_relations()->mutable_root();
  root->add_names("id");
  auto* extension = root->mutable_input()->mutable_extension_leaf();
  pangrova::protocol::v2::RelationDetailEnvelope envelope;
  envelope.set_schema_version(1);
  auto* detail = envelope.mutable_exchange_input();
  detail->set_plan_node_id(kPlanNodeId);
  detail->set_exchange_id(kExchangeId);
  detail->set_upstream_stage_id(1);
  detail->set_downstream_stage_id(0);
  detail->set_distribution(
      pangrova::protocol::v2::EXCHANGE_DISTRIBUTION_SINGLETON);
  detail->set_partition_count(1);
  detail->set_batch_serde(
      pangrova::protocol::v2::BATCH_SERDE_NATIVE_ROW);
  detail->set_require_complete(true);
  detail->mutable_output_schema()->add_names("id");
  detail->mutable_output_schema()->mutable_struct_()->add_types()->mutable_i64();
  extension->mutable_detail()->PackFrom(envelope);

  std::string bytes;
  EXPECT_TRUE(plan.SerializeToString(&bytes));
  return {bytes.begin(), bytes.end()};
}

PendingEndpoint localEndpoint(
    int port,
    int64_t producerHandle = kProducerHandle,
    int32_t sourcePartition = 0) {
  PendingEndpoint endpoint;
  endpoint.source_task_id = "producer-task-native-e2e";
  endpoint.source_stage_id = 1;
  endpoint.source_partition_id = sourcePartition;
  endpoint.output_partition_id = 0;
  endpoint.plan_epoch = kPlanEpoch;
  endpoint.attempt_id = kAttemptId;
  endpoint.fencing_token = kFencingToken;
  endpoint.host = "127.0.0.1";
  endpoint.port = port;
  endpoint.signed_ticket =
      std::to_string(producerHandle) + "\nquery-native-e2e\n1\n0\n" +
      std::to_string(kPlanEpoch) + "\n" + std::to_string(kAttemptId) +
      "\n" + std::to_string(kFencingToken);
  endpoint.ticket_expires_at_epoch_ms = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch()).count() +
      600'000);
  return endpoint;
}

std::string batchChecksum(const std::string& input) {
  std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
  unsigned int size = 0;
  EXPECT_EQ(
      EVP_Digest(
          input.data(), input.size(), digest.data(), &size,
          EVP_sha256(), nullptr),
      1);
  EXPECT_EQ(size, 32u);
  return {reinterpret_cast<const char*>(digest.data()), size};
}

ExternalShuffleChunk externalChunk(
    int64_t sequence,
    const std::string& batch,
    uint32_t index,
    const std::string& checksum) {
  ExternalShuffleChunk chunk;
  chunk.batch_sequence = sequence;
  chunk.chunk_index = index;
  chunk.chunk_count = static_cast<uint32_t>(
      1 + (batch.size() - 1) / kExternalShuffleChunkBytes);
  chunk.batch_size_bytes = batch.size();
  const auto offset = static_cast<size_t>(index) * kExternalShuffleChunkBytes;
  const auto size =
      std::min(kExternalShuffleChunkBytes, batch.size() - offset);
  chunk.data.assign(batch.data() + offset, size);
  chunk.batch_checksum = checksum;
  return chunk;
}

std::filesystem::path tlsResource(const char* filename) {
  return std::filesystem::path(__FILE__).parent_path().parent_path().parent_path() /
      "resources" / "tls" / filename;
}

std::shared_ptr<VeloxTaskRunner> makeConsumer(
    executor::OutputQueue* output_queue) {
  VeloxPlanConverter converter;
  auto plan = converter.convert(exchangePlan(), {});
  VeloxTaskRunnerConfig config;
  config.executor_handle = kConsumerHandle;
  config.task_id = "consumer-task-native-e2e";
  config.query_id = "query-native-e2e";
  config.max_drivers = 2;
  return std::make_shared<VeloxTaskRunner>(
      config, plan, std::vector<split::SplitConfig>{}, output_queue);
}

std::shared_ptr<PangrovaExchangeSource> makeDirectSource(
    int64_t consumerHandle,
    int port,
    const std::shared_ptr<facebook::velox::exec::ExchangeQueue>& queue,
    facebook::velox::memory::MemoryPool* pool,
    bool mutualTls = false) {
  if (mutualTls) {
    PangrovaExchangeSource::configureTransportSecurity(
        true,
        tlsResource("localhost-cert.pem").string(),
        tlsResource("localhost-key.pem").string(),
        tlsResource("localhost-cert.pem").string(),
        "localhost");
  } else {
    PangrovaExchangeSource::configureTransportSecurity(
        false, "", "", "", "");
  }
  auto& registry = PendingEndpointRegistry::instance();
  auto endpoint = localEndpoint(port);
  if (mutualTls) {
    endpoint.tls_identity = "localhost";
  }
  registry.registerSource(
      consumerHandle, kPlanNodeId, kExchangeId, std::move(endpoint));
  registry.sealInput(
      consumerHandle, kPlanNodeId, kExchangeId, 1);
  auto binding = registry.takeSealedInput(consumerHandle, kPlanNodeId);
  EXPECT_TRUE(binding.has_value());
  EXPECT_EQ(binding->sources.size(), 1);
  return std::make_shared<PangrovaExchangeSource>(
      binding->sources.front().runtime_source_id,
      0,
      queue,
      pool);
}

bool armRequest(const std::shared_ptr<PangrovaExchangeSource>& source) {
  std::lock_guard<std::mutex> lock(source->testingQueue()->mutex());
  return source->shouldRequestLocked();
}

struct TransportSecurityReset {
  ~TransportSecurityReset() {
    PangrovaExchangeSource::configureTransportSecurity(
        false, "", "", "", "");
  }
};

class NativeExchangeDataPlaneE2ETest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Runtime lifecycle tests intentionally exercise shutdown.  Every native
    // exchange E2E therefore establishes its own valid executor and memory
    // manager instead of depending on translation-unit initialization order.
    pangrova::test::testMemoryManager();
  }
};

TEST_F(NativeExchangeDataPlaneE2ETest, EmptyPartitionTraversesFlightAndVelox) {
  PendingEndpointRegistry::instance().clear();
  shuffle::SerializedShuffleQueue producerQueue(8);
  shuffle::FlightShuffleServerConfig serverConfig;
  serverConfig.port = 0;
  serverConfig.reuse_port = true;
  shuffle::FlightShuffleServer server(serverConfig);
  const int port = server.start();
  ASSERT_GT(port, 0);
  server.registerHandle(
      kProducerHandle, &producerQueue, "query-native-e2e", 1, 1,
      kPlanEpoch, kAttemptId, kFencingToken);
  ASSERT_TRUE(producerQueue.commit());
  // A zero-split producer can finish before the downstream stage starts. Its
  // signed ticket must still serve an empty committed stream after unregister.
  server.unregisterHandle(kProducerHandle);
  ASSERT_EQ(server.lingeringHandleCount(), 1);

  auto& registry = PendingEndpointRegistry::instance();
  registry.registerSource(
      kConsumerHandle, kPlanNodeId, kExchangeId, localEndpoint(port));
  registry.sealInput(kConsumerHandle, kPlanNodeId, kExchangeId, 1);

  executor::OutputQueue outputQueue(8);
  auto runner = makeConsumer(&outputQueue);

  ASSERT_NO_THROW(runner->start());
  EXPECT_TRUE(runner->waitForCompletion()) << runner->errorMessage();
  EXPECT_TRUE(runner->isFinished());

  server.unregisterHandle(kProducerHandle);
  EXPECT_EQ(server.lingeringHandleCount(), 0);
  server.stop();
  registry.discardExecutor(kConsumerHandle);
}

TEST_F(NativeExchangeDataPlaneE2ETest, NonEmptyBatchTraversesFlightAndVelox) {
  PendingEndpointRegistry::instance().clear();
  auto root_pool = pangrova::test::testRootPool(
      "NativeExchangeDataPlaneE2ETest");
  auto leaf_pool = root_pool->addLeafChild("producer");
  facebook::velox::test::VectorMaker maker(leaf_pool.get());
  auto input = maker.rowVector(
      {"id"}, {maker.flatVector<int64_t>({42, 7, 99})});
  shuffle::ShuffleBatchSerializer serializer(
      shuffle::ShuffleBatchSerializer::Config{"none", 10'000});

  shuffle::SerializedShuffleQueue producerQueue(8);
  shuffle::ShuffleBatch batch;
  batch.partition_id = 0;
  batch.data = serializer.serialize(input, leaf_pool.get());
  batch.row_count = input->size();
  ASSERT_TRUE(producerQueue.push(std::move(batch), std::chrono::seconds(1)));
  ASSERT_TRUE(producerQueue.commit());

  shuffle::FlightShuffleServerConfig serverConfig;
  serverConfig.port = 0;
  serverConfig.reuse_port = true;
  shuffle::FlightShuffleServer server(serverConfig);
  const int port = server.start();
  ASSERT_GT(port, 0);
  server.registerHandle(
      kProducerHandle, &producerQueue, "query-native-e2e", 1, 1,
      kPlanEpoch, kAttemptId, kFencingToken);
  // Producer completion must not destroy committed shuffle output. Exercise
  // the production ordering where the downstream DoGet opens only after the
  // producer has unregistered and its route moved to the lingering set.
  server.unregisterHandle(kProducerHandle);
  ASSERT_EQ(server.handleCount(), 0);
  ASSERT_EQ(server.lingeringHandleCount(), 1);

  auto& registry = PendingEndpointRegistry::instance();
  registry.registerSource(
      kConsumerHandle, kPlanNodeId, kExchangeId, localEndpoint(port));
  registry.sealInput(kConsumerHandle, kPlanNodeId, kExchangeId, 1);

  executor::OutputQueue outputQueue(8);
  auto runner = makeConsumer(&outputQueue);
  ASSERT_NO_THROW(runner->start());
  ASSERT_TRUE(runner->waitForCompletion()) << runner->errorMessage();
  ASSERT_TRUE(runner->isFinished());

  facebook::velox::RowVectorPtr output;
  ASSERT_EQ(
      outputQueue.pop(output, std::chrono::seconds(1)),
      executor::PopResult::DATA);
  ASSERT_NE(output, nullptr);
  ASSERT_EQ(output->size(), 3);
  auto ids = output->childAt(0)->asFlatVector<int64_t>();
  ASSERT_NE(ids, nullptr);
  EXPECT_EQ(ids->valueAt(0), 42);
  EXPECT_EQ(ids->valueAt(1), 7);
  EXPECT_EQ(ids->valueAt(2), 99);
  runner->cleanup();
  EXPECT_EQ(
      outputQueue.pop(output, std::chrono::seconds(1)),
      executor::PopResult::CLOSED);

  server.unregisterHandle(kProducerHandle);
  EXPECT_EQ(server.lingeringHandleCount(), 0);
  server.stop();
  registry.discardExecutor(kConsumerHandle);
}

TEST_F(NativeExchangeDataPlaneE2ETest, CompressedBatchTraversesFlightAndVelox) {
  PendingEndpointRegistry::instance().clear();
  auto rootPool = pangrova::test::testRootPool("CompressedExchangeE2E");
  auto leafPool = rootPool->addLeafChild("producer");
  facebook::velox::test::VectorMaker maker(leafPool.get());
  std::vector<int64_t> ids(512, 42);
  auto input = maker.rowVector({"id"}, {maker.flatVector<int64_t>(ids)});
  shuffle::ShuffleBatchSerializer::Config compressedConfig;
  compressedConfig.compression_codec = "lz4";
  compressedConfig.compression_min_bytes = 1;
  shuffle::ShuffleBatchSerializer compressed(compressedConfig);
  shuffle::ShuffleBatchSerializer plain(
      shuffle::ShuffleBatchSerializer::Config{"none", 10'000});
  auto encoded = compressed.serialize(input, leafPool.get());
  EXPECT_LT(encoded->computeChainDataLength(),
            plain.serialize(input, leafPool.get())->computeChainDataLength());

  shuffle::SerializedShuffleQueue producerQueue(8);
  shuffle::ShuffleBatch batch;
  batch.partition_id = 0;
  batch.data = std::move(encoded);
  batch.row_count = input->size();
  ASSERT_TRUE(producerQueue.push(std::move(batch), std::chrono::seconds(1)));
  ASSERT_TRUE(producerQueue.commit());

  shuffle::FlightShuffleServerConfig serverConfig;
  serverConfig.port = 0;
  serverConfig.reuse_port = true;
  shuffle::FlightShuffleServer server(serverConfig);
  const int port = server.start();
  ASSERT_GT(port, 0);
  server.registerHandle(
      kProducerHandle, &producerQueue, "query-native-e2e", 1, 1,
      kPlanEpoch, kAttemptId, kFencingToken);
  server.unregisterHandle(kProducerHandle);

  auto& registry = PendingEndpointRegistry::instance();
  registry.registerSource(
      kConsumerHandle, kPlanNodeId, kExchangeId, localEndpoint(port));
  registry.sealInput(kConsumerHandle, kPlanNodeId, kExchangeId, 1);

  executor::OutputQueue outputQueue(8);
  auto runner = makeConsumer(&outputQueue);
  ASSERT_NO_THROW(runner->start());
  ASSERT_TRUE(runner->waitForCompletion()) << runner->errorMessage();
  facebook::velox::RowVectorPtr output;
  int64_t rows = 0;
  while (outputQueue.pop(output, std::chrono::seconds(1)) ==
         executor::PopResult::DATA) {
    ASSERT_NE(output, nullptr);
    auto values = output->childAt(0)->asFlatVector<int64_t>();
    ASSERT_NE(values, nullptr);
    for (int i = 0; i < output->size(); ++i) {
      EXPECT_EQ(values->valueAt(i), 42);
    }
    rows += output->size();
  }
  EXPECT_EQ(rows, ids.size());
  runner->cleanup();
  server.unregisterHandle(kProducerHandle);
  server.stop();
  registry.discardExecutor(kConsumerHandle);
}

TEST_F(NativeExchangeDataPlaneE2ETest, WaitBudgetDoesNotCancelSparseStream) {
  constexpr int64_t consumerHandle = kConsumerHandle + 1;
  PendingEndpointRegistry::instance().clear();
  auto rootPool = pangrova::test::testRootPool("FlightWaitBudgetE2E");
  auto leafPool = rootPool->addLeafChild("consumer");

  shuffle::SerializedShuffleQueue producerQueue(8);
  shuffle::FlightShuffleServerConfig serverConfig;
  serverConfig.port = 0;
  serverConfig.reuse_port = true;
  shuffle::FlightShuffleServer server(serverConfig);
  const int port = server.start();
  ASSERT_GT(port, 0);
  server.registerHandle(
      kProducerHandle,
      &producerQueue,
      "query-native-e2e",
      1,
      1,
      kPlanEpoch,
      kAttemptId,
      kFencingToken);

  auto queue = std::make_shared<facebook::velox::exec::ExchangeQueue>(1, 0);
  auto source = makeDirectSource(
      consumerHandle, port, queue, leafPool.get());

  ASSERT_TRUE(armRequest(source));
  const auto started = std::chrono::steady_clock::now();
  auto timeoutResponse = source->request(
      1024 * 1024, std::chrono::milliseconds(25)).get();
  const auto waited = std::chrono::steady_clock::now() - started;
  EXPECT_EQ(timeoutResponse.bytes, 0);
  EXPECT_FALSE(timeoutResponse.atEnd);
  EXPECT_LT(waited, std::chrono::seconds(1));

  facebook::velox::test::VectorMaker maker(leafPool.get());
  auto input = maker.rowVector(
      {"id"}, {maker.flatVector<int64_t>({11, 22})});
  shuffle::ShuffleBatchSerializer serializer(
      shuffle::ShuffleBatchSerializer::Config{"none", 10'000});
  shuffle::ShuffleBatch batch;
  batch.partition_id = 0;
  batch.data = serializer.serialize(input, leafPool.get());
  batch.row_count = input->size();

  ASSERT_TRUE(armRequest(source));
  auto resumed = source->request(
      1024 * 1024, std::chrono::seconds(2));
  ASSERT_TRUE(producerQueue.push(std::move(batch), std::chrono::seconds(1)));
  ASSERT_TRUE(producerQueue.commit());
  auto dataResponse = std::move(resumed).get();
  EXPECT_GT(dataResponse.bytes, 0);
  EXPECT_FALSE(dataResponse.atEnd);
  EXPECT_EQ(queue->receivedPages(), 1);

  source->close();
  server.unregisterHandle(kProducerHandle);
  server.stop();
  PendingEndpointRegistry::instance().discardExecutor(consumerHandle);
}

TEST_F(
    NativeExchangeDataPlaneE2ETest,
    FlightDataSizeProbeRetainsOversizedBatchUntilAdmittedRequest) {
  constexpr int64_t consumerHandle = kConsumerHandle + 10;
  constexpr size_t payloadBytes = 1024 * 1024 + 257;
  PendingEndpointRegistry::instance().clear();
  auto rootPool = pangrova::test::testRootPool("FlightDataSizeProbeE2E");
  auto leafPool = rootPool->addLeafChild("consumer");

  shuffle::SerializedShuffleQueue producerQueue(4);
  shuffle::ShuffleBatch batch;
  batch.partition_id = 0;
  batch.data = folly::IOBuf::copyBuffer(std::string(payloadBytes, 'p'));
  batch.row_count = 1;
  ASSERT_TRUE(producerQueue.push(std::move(batch), std::chrono::seconds(1)));
  ASSERT_TRUE(producerQueue.commit());

  shuffle::FlightShuffleServerConfig serverConfig;
  serverConfig.port = 0;
  serverConfig.reuse_port = true;
  shuffle::FlightShuffleServer server(serverConfig);
  const int port = server.start();
  ASSERT_GT(port, 0);
  server.registerHandle(
      kProducerHandle, &producerQueue, "query-native-e2e", 1, 1,
      kPlanEpoch, kAttemptId, kFencingToken);

  auto queue = std::make_shared<facebook::velox::exec::ExchangeQueue>(1, 0);
  auto source = makeDirectSource(
      consumerHandle, port, queue, leafPool.get());
  ASSERT_TRUE(armRequest(source));
  auto probe = source->requestDataSizes(std::chrono::seconds(2)).get();
  EXPECT_EQ(probe.bytes, 0);
  ASSERT_EQ(probe.remainingBytes.size(), 1);
  EXPECT_EQ(probe.remainingBytes.front(), payloadBytes);
  EXPECT_EQ(queue->receivedPages(), 0);

  ASSERT_TRUE(armRequest(source));
  auto admitted = source->request(payloadBytes, std::chrono::seconds(2)).get();
  EXPECT_EQ(admitted.bytes, payloadBytes);
  EXPECT_TRUE(admitted.remainingBytes.empty());
  EXPECT_EQ(queue->receivedPages(), 1);

  source->close();
  server.unregisterHandle(kProducerHandle);
  server.stop();
  PendingEndpointRegistry::instance().discardExecutor(consumerHandle);
}

TEST_F(
    NativeExchangeDataPlaneE2ETest,
    ExternalDataSizeProbeUsesIngressAndRetainsOversizedBatch) {
  constexpr int64_t consumerHandle = kConsumerHandle + 11;
  constexpr char planNode[] = "external-probe";
  constexpr char exchangeId[] = "external-probe-exchange";
  const std::string payload(1024 * 1024 + 73, 'e');
  const auto checksum = batchChecksum(payload);
  auto& registry = PendingEndpointRegistry::instance();
  registry.clear();
  const auto deadline = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count() +
      60'000);
  registry.registerExternalSource(
      consumerHandle, planNode, exchangeId, deadline, 4 * 1024 * 1024);
  registry.sealInput(consumerHandle, planNode, exchangeId, 1);
  auto binding = registry.takeSealedInput(consumerHandle, planNode);
  ASSERT_TRUE(binding.has_value());
  ASSERT_EQ(binding->sources.size(), 1);

  auto rootPool = pangrova::test::testRootPool("ExternalDataSizeProbeE2E");
  auto leafPool = rootPool->addLeafChild("consumer");
  auto queue = std::make_shared<facebook::velox::exec::ExchangeQueue>(1, 0);
  auto source = std::make_shared<PangrovaExchangeSource>(
      binding->sources.front().runtime_source_id, 0, queue, leafPool.get());
  for (uint32_t index = 0; index < 2; ++index) {
    registry.pushExternalBatch(
        consumerHandle,
        planNode,
        exchangeId,
        externalChunk(0, payload, index, checksum));
  }

  ASSERT_TRUE(armRequest(source));
  auto probe = source->requestDataSizes(std::chrono::seconds(2)).get();
  EXPECT_EQ(probe.bytes, 0);
  ASSERT_EQ(probe.remainingBytes.size(), 1);
  EXPECT_EQ(probe.remainingBytes.front(), payload.size());
  EXPECT_EQ(queue->receivedPages(), 0);

  ASSERT_TRUE(armRequest(source));
  auto admitted =
      source->request(payload.size(), std::chrono::seconds(2)).get();
  EXPECT_EQ(admitted.bytes, payload.size());
  EXPECT_EQ(queue->receivedPages(), 1);

  source->close();
  registry.discardExecutor(consumerHandle);
}

TEST_F(
    NativeExchangeDataPlaneE2ETest,
    MultiSourceVeloxClientProbeDoesNotBypassQueueBackpressure) {
  constexpr int64_t secondProducerHandle = kProducerHandle + 100;
  constexpr int64_t consumerHandle = kConsumerHandle + 12;
  constexpr size_t payloadBytes = 1024 * 1024 + 257;
  constexpr int64_t queueBudget = payloadBytes + 4096;
  PendingEndpointRegistry::instance().clear();
  auto rootPool = pangrova::test::testRootPool("MultiSourceProbeE2E");
  auto leafPool = rootPool->addLeafChild("consumer");

  shuffle::SerializedShuffleQueue firstQueue(8);
  shuffle::SerializedShuffleQueue secondQueue(8);
  auto fill = [](shuffle::SerializedShuffleQueue& queue, char value) {
    for (int batchIndex = 0; batchIndex < 3; ++batchIndex) {
      shuffle::ShuffleBatch batch;
      batch.partition_id = 0;
      batch.data = folly::IOBuf::copyBuffer(
          std::string(payloadBytes, value));
      batch.row_count = 1;
      EXPECT_TRUE(queue.push(std::move(batch), std::chrono::seconds(1)));
    }
    EXPECT_TRUE(queue.commit());
  };
  fill(firstQueue, 'a');
  fill(secondQueue, 'b');

  shuffle::FlightShuffleServerConfig serverConfig;
  serverConfig.port = 0;
  serverConfig.reuse_port = true;
  shuffle::FlightShuffleServer server(serverConfig);
  const int port = server.start();
  ASSERT_GT(port, 0);
  server.registerHandle(
      kProducerHandle, &firstQueue, "query-native-e2e", 1, 1,
      kPlanEpoch, kAttemptId, kFencingToken);
  server.registerHandle(
      secondProducerHandle, &secondQueue, "query-native-e2e", 1, 1,
      kPlanEpoch, kAttemptId, kFencingToken);

  auto& registry = PendingEndpointRegistry::instance();
  registry.registerSource(
      consumerHandle,
      kPlanNodeId,
      kExchangeId,
      localEndpoint(port, kProducerHandle, 0));
  registry.registerSource(
      consumerHandle,
      kPlanNodeId,
      kExchangeId,
      localEndpoint(port, secondProducerHandle, 1));
  registry.sealInput(consumerHandle, kPlanNodeId, kExchangeId, 2);
  auto binding = registry.takeSealedInput(consumerHandle, kPlanNodeId);
  ASSERT_TRUE(binding.has_value());
  ASSERT_EQ(binding->sources.size(), 2);

  auto executor = pangrova::jni::globalExecutor();
  ASSERT_NE(executor, nullptr);
  auto client = std::make_shared<facebook::velox::exec::ExchangeClient>(
      "multi-source-probe",
      0,
      queueBudget,
      1,
      0,
      leafPool.get(),
      executor.get(),
      1,
      false,
      true);
  for (const auto& source : binding->sources) {
    client->addRemoteTaskId(source.runtime_source_id);
  }
  client->noMoreRemoteTasks();
  bool atEnd = false;
  facebook::velox::ContinueFuture waitFuture;
  auto pages = client->next(0, queueBudget, &atEnd, &waitFuture);
  EXPECT_TRUE(pages.empty());
  EXPECT_FALSE(atEnd);

  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(3);
  while (client->queue()->receivedPages() == 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_EQ(client->queue()->receivedPages(), 1);
  EXPECT_EQ(client->queue()->totalBytes(), payloadBytes);
  std::this_thread::sleep_for(std::chrono::milliseconds(250));
  EXPECT_EQ(client->queue()->receivedPages(), 1);
  EXPECT_LE(client->queue()->totalBytes(), queueBudget);

  client->close();
  server.unregisterHandle(kProducerHandle);
  server.unregisterHandle(secondProducerHandle);
  server.stop();
  registry.discardExecutor(consumerHandle);
}

TEST_F(NativeExchangeDataPlaneE2ETest, CloseCancelsBlockedRead) {
  constexpr int64_t consumerHandle = kConsumerHandle + 2;
  PendingEndpointRegistry::instance().clear();
  auto rootPool = pangrova::test::testRootPool("FlightCloseCancellationE2E");
  auto leafPool = rootPool->addLeafChild("consumer");

  shuffle::SerializedShuffleQueue producerQueue(8);
  shuffle::FlightShuffleServerConfig serverConfig;
  serverConfig.port = 0;
  serverConfig.reuse_port = true;
  shuffle::FlightShuffleServer server(serverConfig);
  const int port = server.start();
  ASSERT_GT(port, 0);
  server.registerHandle(
      kProducerHandle,
      &producerQueue,
      "query-native-e2e",
      1,
      1,
      kPlanEpoch,
      kAttemptId,
      kFencingToken);

  auto queue = std::make_shared<facebook::velox::exec::ExchangeQueue>(1, 0);
  auto source = makeDirectSource(
      consumerHandle, port, queue, leafPool.get());
  ASSERT_TRUE(armRequest(source));
  auto initialWait = source->request(
      1024 * 1024, std::chrono::milliseconds(25));
  EXPECT_FALSE(std::move(initialWait).get().atEnd);

  ASSERT_TRUE(armRequest(source));
  auto blocked = source->request(
      1024 * 1024, std::chrono::microseconds(0));
  source->close();
  auto closedResponse = std::move(blocked).get(std::chrono::seconds(2));
  EXPECT_EQ(closedResponse.bytes, 0);
  EXPECT_FALSE(closedResponse.atEnd);

  ASSERT_TRUE(producerQueue.abort());
  server.unregisterHandle(kProducerHandle);
  server.stop();
  PendingEndpointRegistry::instance().discardExecutor(consumerHandle);
}

TEST_F(NativeExchangeDataPlaneE2ETest, MutualTlsTraversesNativeConsumer) {
  constexpr int64_t consumerHandle = kConsumerHandle + 3;
  TransportSecurityReset securityReset;
  PendingEndpointRegistry::instance().clear();
  auto rootPool = pangrova::test::testRootPool("FlightMutualTlsConsumerE2E");
  auto leafPool = rootPool->addLeafChild("consumer");

  shuffle::SerializedShuffleQueue producerQueue(8);
  ASSERT_TRUE(producerQueue.commit());
  shuffle::FlightShuffleServerConfig serverConfig;
  serverConfig.port = 0;
  serverConfig.reuse_port = true;
  serverConfig.tls_cert_chain_path =
      tlsResource("localhost-cert.pem").string();
  serverConfig.tls_private_key_path =
      tlsResource("localhost-key.pem").string();
  serverConfig.tls_root_ca_path =
      tlsResource("localhost-cert.pem").string();
  shuffle::FlightShuffleServer server(serverConfig);
  const int port = server.start();
  ASSERT_GT(port, 0);
  server.registerHandle(
      kProducerHandle,
      &producerQueue,
      "query-native-e2e",
      1,
      1,
      kPlanEpoch,
      kAttemptId,
      kFencingToken);

  auto queue = std::make_shared<facebook::velox::exec::ExchangeQueue>(1, 0);
  auto source = makeDirectSource(
      consumerHandle, port, queue, leafPool.get(), true);
  ASSERT_TRUE(armRequest(source));
  auto response = source->request(
      1024 * 1024, std::chrono::seconds(2)).get();
  EXPECT_TRUE(response.atEnd);
  EXPECT_EQ(response.bytes, 0);

  source->close();
  server.unregisterHandle(kProducerHandle);
  server.stop();
  PendingEndpointRegistry::instance().discardExecutor(consumerHandle);
}

}  // namespace
}  // namespace pangrova::velox
