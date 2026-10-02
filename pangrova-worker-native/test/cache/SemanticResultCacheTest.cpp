#include <gtest/gtest.h>

#include <chrono>
#include <string>

#include "cache/SemanticResultCache.h"

namespace pangrova::cache {
namespace {

ReuseIdentity identity(std::string tenant = "tenant-a") {
  return ReuseIdentity{
      std::move(tenant), "alice", "policy-v1", "model-a@42#signature",
      "text-generation@1#signature", "ai_complete", "VARCHAR",
      "temperature=0", "orders", "orders-snapshot-7"};
}

ReusePolicy policy(uint64_t max_entries = 1000, uint64_t ttl_ms = 60'000) {
  return ReusePolicy{ttl_ms, max_entries, 64 * 1024 * 1024, 0.98, 256};
}

TEST(SemanticResultCacheTest, NormalizedEquivalentRequestHitsTypedValue) {
  auto& cache = SemanticResultCache::instance();
  cache.invalidateAll();
  const auto configured = policy();
  cache.put(
      identity(), SemanticResultCache::requestKey("Hello   WORLD", 256),
      std::string("governed result"), configured, "write-task");

  auto hit = cache.lookup(
      identity(), SemanticResultCache::requestKey("hello world", 256),
      configured, "read-task");

  ASSERT_TRUE(hit.has_value());
  EXPECT_EQ(std::get<std::string>(*hit), "governed result");
  EXPECT_EQ(cache.taskUsage("read-task").hits, 1);
}

TEST(SemanticResultCacheTest, HardIsolationDimensionsNeverShareCandidates) {
  auto& cache = SemanticResultCache::instance();
  cache.invalidateAll();
  const auto configured = policy();
  const auto key = SemanticResultCache::requestKey("summarize this", 256);
  cache.put(identity(), key, std::string("secret"), configured, "writer");

  auto tenant = identity("tenant-b");
  auto principal = identity();
  principal.principal = "bob";
  auto model = identity();
  model.model_identity = "model-a@43#signature";
  auto security = identity();
  security.policy_context_hash = "policy-v2";
  auto parameters = identity();
  parameters.parameter_fingerprint = "temperature=1";
  auto source = identity();
  source.dependency_scope_hash = "customers";

  EXPECT_FALSE(cache.lookup(tenant, key, configured, "tenant").has_value());
  EXPECT_FALSE(cache.lookup(principal, key, configured, "principal").has_value());
  EXPECT_FALSE(cache.lookup(model, key, configured, "model").has_value());
  EXPECT_FALSE(cache.lookup(security, key, configured, "policy").has_value());
  EXPECT_FALSE(cache.lookup(parameters, key, configured, "params").has_value());
  EXPECT_FALSE(cache.lookup(source, key, configured, "source").has_value());
}

TEST(SemanticResultCacheTest, SnapshotChangeInvalidatesPriorGeneration) {
  auto& cache = SemanticResultCache::instance();
  cache.invalidateAll();
  const auto configured = policy();
  const auto key = SemanticResultCache::requestKey("status", 256);
  cache.put(identity(), key, std::string("old"), configured, "writer");

  auto newer = identity();
  newer.dependency_version_hash = "orders-snapshot-8";
  EXPECT_FALSE(cache.lookup(newer, key, configured, "reader").has_value());
  EXPECT_GT(cache.taskUsage("reader").evictions, 0);
  EXPECT_FALSE(cache.lookup(identity(), key, configured, "stale").has_value());
}

TEST(SemanticResultCacheTest, TtlAndEntryQuotaBoundResidentValues) {
  auto& cache = SemanticResultCache::instance();
  cache.invalidateAll();
  auto short_lived = policy(1, 1);
  auto first = identity();
  first.parameter_fingerprint = "first";
  const auto key = SemanticResultCache::requestKey("first", 256);
  cache.put(first, key, std::string("one"), short_lived, "writer-1");
  const auto deadline = std::chrono::steady_clock::now() +
      std::chrono::milliseconds(2);
  while (std::chrono::steady_clock::now() < deadline) {}
  EXPECT_FALSE(cache.lookup(first, key, short_lived, "expired").has_value());

  auto bounded = policy(1);
  cache.put(first, key, std::string("one"), bounded, "writer-2");
  cache.put(
      first, SemanticResultCache::requestKey("second", 256),
      std::string("two"), bounded, "writer-2");
  EXPECT_FALSE(cache.lookup(first, key, bounded, "evicted").has_value());
  EXPECT_TRUE(cache.lookup(
      first, SemanticResultCache::requestKey("second", 256), bounded,
      "retained").has_value());
}

TEST(SemanticResultCacheTest, PartitionQuotaNeverEvictsAnotherPartition) {
  auto& cache = SemanticResultCache::instance();
  cache.invalidateAll();
  const auto bounded = policy(1);
  auto first_partition = identity();
  first_partition.parameter_fingerprint = "temperature=0";
  auto second_partition = identity();
  second_partition.parameter_fingerprint = "temperature=1";
  const auto first_key = SemanticResultCache::requestKey("first", 256);
  const auto replacement_key = SemanticResultCache::requestKey("replacement", 256);
  const auto independent_key = SemanticResultCache::requestKey("independent", 256);

  ASSERT_TRUE(cache.put(
      first_partition, first_key, std::string("first"), bounded, "writer"));
  ASSERT_TRUE(cache.put(
      second_partition, independent_key, std::string("independent"),
      bounded, "writer"));
  ASSERT_TRUE(cache.put(
      first_partition, replacement_key, std::string("replacement"),
      bounded, "writer"));

  EXPECT_FALSE(cache.lookup(
      first_partition, first_key, bounded, "reader").has_value());
  EXPECT_TRUE(cache.lookup(
      first_partition, replacement_key, bounded, "reader").has_value());
  EXPECT_TRUE(cache.lookup(
      second_partition, independent_key, bounded, "reader").has_value());
}

TEST(SemanticResultCacheTest, QueryQuotaNeverEvictsAnotherTenant) {
  auto& cache = SemanticResultCache::instance();
  cache.invalidateAll();
  const auto bounded = policy(1);
  const auto first_key = SemanticResultCache::requestKey("tenant a", 256);
  const auto second_key = SemanticResultCache::requestKey("tenant b", 256);

  ASSERT_TRUE(cache.put(
      identity("tenant-a"), first_key, std::string("a"), bounded,
      "writer"));
  ASSERT_TRUE(cache.put(
      identity("tenant-b"), second_key, std::string("b"), bounded,
      "writer"));

  EXPECT_TRUE(cache.lookup(
      identity("tenant-a"), first_key, bounded, "reader").has_value());
  EXPECT_TRUE(cache.lookup(
      identity("tenant-b"), second_key, bounded, "reader").has_value());
}

TEST(SemanticResultCacheTest, OversizedValueIsRejectedAndAccounted) {
  auto& cache = SemanticResultCache::instance();
  cache.invalidateAll();
  auto tiny = policy();
  tiny.max_bytes = 64;
  const auto key = SemanticResultCache::requestKey("oversized", 256);

  EXPECT_FALSE(cache.put(
      identity(), key, std::string(1024, 'x'), tiny, "writer"));
  EXPECT_EQ(cache.taskUsage("writer").admission_rejections, 1);
  EXPECT_FALSE(cache.lookup(identity(), key, tiny, "reader").has_value());
}

TEST(SemanticResultCacheTest, HnswCandidateIndexPreservesExactScopeAndHit) {
  auto& cache = SemanticResultCache::instance();
  cache.invalidateAll();
  const auto configured = policy(10'000);
  for (int index = 0; index < 300; ++index) {
    auto item = identity();
    const std::string prompt = "document number " + std::to_string(index);
    cache.put(
        item, SemanticResultCache::requestKey(prompt, 256), prompt,
        configured, "bulk-writer");
  }
  auto target = identity();
  auto hit = cache.lookup(
      target, SemanticResultCache::requestKey("document number 271", 256),
      configured, "hnsw-reader");
  ASSERT_TRUE(hit.has_value());
  EXPECT_EQ(std::get<std::string>(*hit), "document number 271");
}

}  // namespace
}  // namespace pangrova::cache
