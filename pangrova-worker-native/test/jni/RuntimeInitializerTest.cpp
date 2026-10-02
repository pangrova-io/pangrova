// Tests for RuntimeInitializer — native runtime lifecycle management.
//
// Validates config parsing, runtime initialization (VeloxMemoryGovernor,
// AsyncDataCache, UDF registry), idempotency, and shutdown cleanup.
// All tests run without a real JVM — they exercise the C++ runtime only.

#include <gtest/gtest.h>

#include <filesystem>

#include <vector>

#include "jni/RuntimeInitializer.h"
#include "pangrova/protocol/v2/runtime.pb.h"

namespace pangrova::jni::test {

namespace {

void enableTestArtifactCache(NativeRuntimeConfig& config) {
  config.cache_enabled = true;
  config.artifact_cache_path =
      (std::filesystem::temp_directory_path() /
       "pangrova-runtime-artifact-cache-test").string();
  config.artifact_cache_bytes = 64 * 1024 * 1024;
  config.artifact_index_memory_bytes = 16 * 1024 * 1024;
}

}  // namespace
namespace {

class RuntimeInitializerTest : public ::testing::Test {
 protected:
  void TearDown() override {
    shutdownVeloxRuntime();
  }
};

// --- Config parsing ---

TEST_F(RuntimeInitializerTest, EmptyConfigReturnsDefaults) {
  auto config = parseNativeConfig({});
  EXPECT_EQ(config.max_memory_bytes, 8L * 1024 * 1024 * 1024);
  EXPECT_EQ(config.io_thread_count, 4);
  EXPECT_FALSE(config.cache_enabled);
}

TEST_F(RuntimeInitializerTest, GarbageConfigIsRejected) {
  std::vector<uint8_t> garbage = {0xFF, 0xFE, 0xFD};
  EXPECT_THROW(parseNativeConfig(garbage), std::invalid_argument);
}

TEST_F(RuntimeInitializerTest, ParsesCanonicalV2RuntimeConfig) {
  pangrova::protocol::v2::RuntimeInitConfig proto;
  proto.set_max_memory_bytes(1024);
  proto.set_io_thread_count(7);
  proto.set_spill_enabled(true);
  proto.set_spill_directory("/tmp/pangrova-spill-test");
  proto.set_shuffle_retention_bytes(16 * 1024 * 1024);
  proto.set_shuffle_transit_bytes(16 * 1024 * 1024);
  proto.set_shuffle_spill_bytes(1024 * 1024 * 1024);
  proto.set_shuffle_spill_files(256);
  proto.set_shuffle_spill_directory("/var/lib/pangrova/shuffle");
  const auto bytes = proto.SerializeAsString();

  auto config = parseNativeConfig(
      std::vector<uint8_t>(bytes.begin(), bytes.end()));

  EXPECT_EQ(config.max_memory_bytes, 1024);
  EXPECT_EQ(config.io_thread_count, 7);
  EXPECT_TRUE(config.spill_enabled);
  EXPECT_EQ(config.spill_directory, "/tmp/pangrova-spill-test");
  EXPECT_EQ(config.shuffle_retention_bytes, 16u * 1024 * 1024);
  EXPECT_EQ(config.shuffle_transit_bytes, 16u * 1024 * 1024);
  EXPECT_EQ(config.shuffle_spill_bytes, 1024u * 1024 * 1024);
  EXPECT_EQ(config.shuffle_spill_files, 256u);
  EXPECT_EQ(config.shuffle_spill_directory, "/var/lib/pangrova/shuffle");
}

TEST_F(RuntimeInitializerTest, PartialTransportTlsConfigIsRejected) {
  pangrova::protocol::v2::RuntimeInitConfig proto;
  auto* security = proto.mutable_transport_security();
  security->set_tls_enabled(true);
  security->set_certificate_chain_path("cert.pem");
  const auto bytes = proto.SerializeAsString();

  EXPECT_THROW(
      parseNativeConfig(std::vector<uint8_t>(bytes.begin(), bytes.end())),
      std::invalid_argument);
}

TEST_F(RuntimeInitializerTest, CompleteTransportTlsConfigIsPreserved) {
  pangrova::protocol::v2::RuntimeInitConfig proto;
  auto* security = proto.mutable_transport_security();
  security->set_tls_enabled(true);
  security->set_certificate_chain_path("cert.pem");
  security->set_private_key_path("key.pem");
  security->set_root_ca_path("ca.pem");
  security->set_tls_identity("worker-1.internal");
  const auto bytes = proto.SerializeAsString();

  auto config = parseNativeConfig(
      std::vector<uint8_t>(bytes.begin(), bytes.end()));

  EXPECT_TRUE(config.transport_tls_enabled);
  EXPECT_EQ(config.transport_tls_certificate_chain_path, "cert.pem");
  EXPECT_EQ(config.transport_tls_private_key_path, "key.pem");
  EXPECT_EQ(config.transport_tls_root_ca_path, "ca.pem");
  EXPECT_EQ(config.transport_tls_identity, "worker-1.internal");
}

// --- Runtime initialization ---

TEST_F(RuntimeInitializerTest, InitCreatesMemoryBridge) {
  NativeRuntimeConfig config;
  initializeVeloxRuntime(config);

  EXPECT_NE(globalVeloxMemoryGovernor(), nullptr);
}

TEST_F(RuntimeInitializerTest, CacheDisabledByDefault) {
  NativeRuntimeConfig config;
  initializeVeloxRuntime(config);

  // AsyncDataCache is null when cache is disabled.
  EXPECT_EQ(globalAsyncDataCache(), nullptr);
}

TEST_F(RuntimeInitializerTest, CacheEnabledCreatesAsyncDataCache) {
  NativeRuntimeConfig config;
  enableTestArtifactCache(config);
  initializeVeloxRuntime(config);

  EXPECT_NE(globalAsyncDataCache(), nullptr);
}

// --- Shutdown ---

TEST_F(RuntimeInitializerTest, ShutdownCleansAllState) {
  NativeRuntimeConfig config;
  enableTestArtifactCache(config);
  initializeVeloxRuntime(config);

  EXPECT_NE(globalVeloxMemoryGovernor(), nullptr);
  EXPECT_NE(globalAsyncDataCache(), nullptr);

  shutdownVeloxRuntime();

  EXPECT_EQ(globalVeloxMemoryGovernor(), nullptr);
  EXPECT_EQ(globalAsyncDataCache(), nullptr);
}

TEST_F(RuntimeInitializerTest, ReinitAfterShutdownWorks) {
  NativeRuntimeConfig config;
  initializeVeloxRuntime(config);
  shutdownVeloxRuntime();

  NativeRuntimeConfig config2;
  config2.max_memory_bytes = 1L * 1024 * 1024 * 1024;
  initializeVeloxRuntime(config2);

  EXPECT_NE(globalVeloxMemoryGovernor(), nullptr);
}

}  // namespace
}  // namespace pangrova::jni::test
