#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <string>

#include <openssl/evp.h>

#include "engine/velox/exchange/PendingEndpointRegistry.h"

namespace pangrova::velox {
namespace {

std::string sha256(const std::string& input) {
  std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
  unsigned int size = 0;
  EXPECT_EQ(EVP_Digest(input.data(), input.size(), digest.data(), &size,
                       EVP_sha256(), nullptr),
            1);
  EXPECT_EQ(size, 32u);
  return {reinterpret_cast<const char*>(digest.data()), size};
}

ExternalShuffleChunk externalChunk(int64_t sequence, std::string data) {
  ExternalShuffleChunk chunk;
  chunk.batch_sequence = sequence;
  chunk.chunk_count = 1;
  chunk.batch_size_bytes = data.size();
  chunk.batch_checksum = sha256(data);
  chunk.data = std::move(data);
  return chunk;
}

PendingEndpoint endpoint(std::string task, int32_t partition,
                         uint32_t attempt = 0, uint64_t fence = 0) {
  PendingEndpoint value;
  value.source_task_id = std::move(task);
  value.source_stage_id = 1;
  value.source_partition_id = partition;
  value.output_partition_id = 0;
  value.plan_epoch = 9;
  value.attempt_id = attempt;
  value.fencing_token = fence == 0 ? 1 : fence;
  value.host = "127.0.0.1";
  value.port = 32000 + partition;
  value.signed_ticket = "opaque-ticket-" + std::to_string(partition);
  value.ticket_expires_at_epoch_ms = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count() +
      600'000);
  return value;
}

class PendingEndpointRegistryTest : public ::testing::Test {
protected:
  void SetUp() override { PendingEndpointRegistry::instance().clear(); }
  void TearDown() override { PendingEndpointRegistry::instance().clear(); }
};

TEST_F(PendingEndpointRegistryTest, SealedBindingIsTaskScopedAndConsumable) {
  auto& registry = PendingEndpointRegistry::instance();
  registry.registerSource(41, "exchange-input-a", "a", endpoint("p0", 0));
  registry.registerSource(41, "exchange-input-a", "a", endpoint("p1", 1));
  registry.sealInput(41, "exchange-input-a", "a", 2);

  auto binding = registry.takeSealedInput(41, "exchange-input-a");
  ASSERT_TRUE(binding.has_value());
  ASSERT_EQ(binding->sources.size(), 2);
  EXPECT_EQ(binding->expected_source_partitions, 2);
  EXPECT_FALSE(registry.takeSealedInput(42, "exchange-input-a").has_value());

  for (const auto& source : binding->sources) {
    auto resolved = registry.takeEndpoint(source.runtime_source_id);
    ASSERT_TRUE(resolved.has_value());
    EXPECT_EQ(resolved->source_task_id, source.endpoint.source_task_id);
  }
}

TEST_F(PendingEndpointRegistryTest, SealRejectsIncompleteInput) {
  auto& registry = PendingEndpointRegistry::instance();
  registry.registerSource(51, "exchange-input-b", "b", endpoint("p0", 0));
  EXPECT_THROW(registry.sealInput(51, "exchange-input-b", "b", 2),
               std::logic_error);
  EXPECT_FALSE(registry.takeSealedInput(51, "exchange-input-b").has_value());
}

TEST_F(PendingEndpointRegistryTest, RejectsStaleAndUnfencedMutation) {
  auto& registry = PendingEndpointRegistry::instance();
  registry.registerSource(61, "exchange-input-c", "c",
                          endpoint("producer", 0, 2, 20));

  EXPECT_THROW(registry.registerSource(61, "exchange-input-c", "c",
                                       endpoint("producer", 0, 1, 19)),
               std::logic_error);

  auto changed = endpoint("producer", 0, 2, 20);
  changed.host = "other-host";
  EXPECT_THROW(
      registry.registerSource(61, "exchange-input-c", "c", std::move(changed)),
      std::logic_error);
}

TEST_F(PendingEndpointRegistryTest, NewerFencedAttemptReplacesOldEndpoint) {
  auto& registry = PendingEndpointRegistry::instance();
  registry.registerSource(71, "exchange-input-d", "d",
                          endpoint("producer", 0, 1, 10));
  auto newer = endpoint("producer", 0, 2, 11);
  newer.signed_ticket = "new-ticket";
  registry.registerSource(71, "exchange-input-d", "d", std::move(newer));
  registry.sealInput(71, "exchange-input-d", "d", 1);

  auto binding = registry.takeSealedInput(71, "exchange-input-d");
  ASSERT_TRUE(binding.has_value());
  ASSERT_EQ(binding->sources.size(), 1);
  auto resolved =
      registry.takeEndpoint(binding->sources.front().runtime_source_id);
  ASSERT_TRUE(resolved.has_value());
  EXPECT_EQ(resolved->attempt_id, 2);
  EXPECT_EQ(resolved->fencing_token, 11);
  EXPECT_EQ(resolved->signed_ticket, "new-ticket");
}

TEST_F(PendingEndpointRegistryTest, DiscardRemovesOnlyTargetExecutor) {
  auto& registry = PendingEndpointRegistry::instance();
  registry.registerSource(81, "exchange-input-e", "e", endpoint("p1", 0));
  registry.registerSource(82, "exchange-input-e", "e", endpoint("p2", 0));
  registry.sealInput(81, "exchange-input-e", "e", 1);
  registry.sealInput(82, "exchange-input-e", "e", 1);

  registry.discardExecutor(81);
  EXPECT_FALSE(registry.takeSealedInput(81, "exchange-input-e").has_value());
  EXPECT_TRUE(registry.takeSealedInput(82, "exchange-input-e").has_value());
}

TEST_F(PendingEndpointRegistryTest, ExternalIngressSurvivesSealAndBindingTake) {
  auto& registry = PendingEndpointRegistry::instance();
  const auto deadline = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count() +
      600'000);
  registry.registerExternalSource(91, "exchange-input-external", "external",
                                  deadline, 64 * 1024 * 1024);
  registry.sealInput(91, "exchange-input-external", "external", 1);

  auto binding = registry.takeSealedInput(91, "exchange-input-external");
  ASSERT_TRUE(binding.has_value());
  ASSERT_EQ(binding->sources.size(), 1);
  auto endpoint =
      registry.takeEndpoint(binding->sources.front().runtime_source_id);
  ASSERT_TRUE(endpoint.has_value());
  ASSERT_NE(endpoint->external_ingress, nullptr);

  ASSERT_NO_THROW(registry.pushExternalBatch(
      91, "exchange-input-external", "external",
      externalChunk(0, "batch-0")));
  auto data = endpoint->external_ingress->take(std::chrono::seconds(1));
  ASSERT_EQ(data.kind, ExternalShuffleIngress::ResultKind::kData);
  EXPECT_EQ(data.data, "batch-0");

  ASSERT_NO_THROW(registry.finishExternalSource(91, "exchange-input-external",
                                                "external", 0, "", {}));
  auto finished = endpoint->external_ingress->take(std::chrono::seconds(1));
  EXPECT_EQ(finished.kind, ExternalShuffleIngress::ResultKind::kFinished);

  registry.discardExecutor(91);
  auto discarded = endpoint->external_ingress->take(std::chrono::seconds(1));
  EXPECT_EQ(discarded.kind, ExternalShuffleIngress::ResultKind::kCancelled);
  EXPECT_THROW(registry.pushExternalBatch(
                   91, "exchange-input-external", "external",
                   externalChunk(1, "late")),
               std::logic_error);
}

TEST_F(PendingEndpointRegistryTest, ClearCancelsTakenExternalIngress) {
  auto& registry = PendingEndpointRegistry::instance();
  const auto deadline = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count() +
      600'000);
  registry.registerExternalSource(92, "exchange-input-clear", "external-clear",
                                  deadline, 64 * 1024 * 1024);
  registry.sealInput(92, "exchange-input-clear", "external-clear", 1);
  auto binding = registry.takeSealedInput(92, "exchange-input-clear");
  ASSERT_TRUE(binding.has_value());
  auto endpoint =
      registry.takeEndpoint(binding->sources.front().runtime_source_id);
  ASSERT_TRUE(endpoint.has_value());
  ASSERT_NE(endpoint->external_ingress, nullptr);

  registry.clear();
  auto result = endpoint->external_ingress->take(std::chrono::seconds(1));
  EXPECT_EQ(result.kind, ExternalShuffleIngress::ResultKind::kCancelled);
}

TEST_F(PendingEndpointRegistryTest, RejectsMixedDirectAndExternalTransport) {
  auto& registry = PendingEndpointRegistry::instance();
  const auto deadline = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count() +
      600'000);
  registry.registerExternalSource(93, "exchange-input-mixed", "mixed",
                                  deadline, 64 * 1024 * 1024);
  EXPECT_THROW(registry.registerSource(93, "exchange-input-mixed", "mixed",
                                       endpoint("producer", 0)),
               std::logic_error);
}

} // namespace
} // namespace pangrova::velox
