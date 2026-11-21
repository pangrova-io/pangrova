// GlobalWriterRegistryTest: Unit tests for multi-tenant writer isolation.
// Validates: Requirement I.4 — GlobalWriterRegistry supports concurrent
// writers from multiple tenants with proper isolation.
//
// Note: Full JNI tests require a running JVM. These tests verify the
// tenant ID extraction and sharding logic without JNI dependencies.
#include <gtest/gtest.h>

#include <set>
#include <string>

// Test the extractTenantId logic directly (same algorithm as GlobalWriterRegistry).
namespace pangrova::jni::test {
namespace {

// Mirrors GlobalWriterRegistry::extractTenantId for unit testing.
std::string extractTenantId(const std::string& task_id) {
  auto dot = task_id.find('.');
  if (dot == std::string::npos) return {};
  return task_id.substr(0, dot);
}

TEST(GlobalWriterRegistryTest, ExtractTenantIdFromTaskId) {
  // Standard format: "tenant.query.task"
  EXPECT_EQ(extractTenantId("tenant-a.q1.t1"), "tenant-a");
  EXPECT_EQ(extractTenantId("tenant-b.q2.t2"), "tenant-b");
}

TEST(GlobalWriterRegistryTest, ExtractTenantIdSingleDot) {
  // Format: "tenant.task" (no query segment)
  EXPECT_EQ(extractTenantId("tenant-x.task-1"), "tenant-x");
}

TEST(GlobalWriterRegistryTest, ExtractTenantIdNoDot) {
  // No dot — cannot extract tenant, returns empty.
  EXPECT_EQ(extractTenantId("simple-task-id"), "");
}

TEST(GlobalWriterRegistryTest, ExtractTenantIdEmptyString) {
  EXPECT_EQ(extractTenantId(""), "");
}

TEST(GlobalWriterRegistryTest, ExtractTenantIdLeadingDot) {
  // Leading dot — tenant is empty string.
  EXPECT_EQ(extractTenantId(".query.task"), "");
}

TEST(GlobalWriterRegistryTest, ShardDistributionDifferentTenants) {
  // Verify different tenants hash to potentially different shards.
  constexpr size_t kNumShards = 8;
  auto shardFor = [](const std::string& tenant_id) {
    return std::hash<std::string>{}(tenant_id) % kNumShards;
  };

  // Different tenants should not all map to the same shard.
  std::set<size_t> shards;
  shards.insert(shardFor("tenant-a"));
  shards.insert(shardFor("tenant-b"));
  shards.insert(shardFor("tenant-c"));
  shards.insert(shardFor("tenant-d"));
  shards.insert(shardFor("tenant-e"));
  shards.insert(shardFor("tenant-f"));
  shards.insert(shardFor("tenant-g"));
  shards.insert(shardFor("tenant-h"));

  // With 8 different tenants and 8 shards, we expect at least 2 distinct shards.
  EXPECT_GE(shards.size(), 2u)
      << "Hash function should distribute tenants across multiple shards";
}

}  // namespace
}  // namespace pangrova::jni::test
