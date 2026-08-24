// Tests for RuntimeInitializer — native runtime lifecycle management.
//
// Validates config parsing, runtime initialization (VeloxMemoryGovernor,
// AsyncDataCache, UDF registry), idempotency, and shutdown cleanup.
// All tests run without a real JVM — they exercise the C++ runtime only.

#include <gtest/gtest.h>

#include <vector>

#include "jni/RuntimeInitializer.h"

namespace pangrova::jni::test {
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

TEST_F(RuntimeInitializerTest, GarbageConfigReturnsDefaults) {
  std::vector<uint8_t> garbage = {0xFF, 0xFE, 0xFD};
  auto config = parseNativeConfig(garbage);
  EXPECT_EQ(config.max_memory_bytes, 8L * 1024 * 1024 * 1024);
  EXPECT_EQ(config.io_thread_count, 4);
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
  config.cache_enabled = true;
  initializeVeloxRuntime(config);

  EXPECT_NE(globalAsyncDataCache(), nullptr);
}

// --- Shutdown ---

TEST_F(RuntimeInitializerTest, ShutdownCleansAllState) {
  NativeRuntimeConfig config;
  config.cache_enabled = true;
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
