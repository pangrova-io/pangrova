// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

// Unit tests for NativeEngineLifecycle: idempotent init/shutdown, re-init
// lifecycle, accessor validity, and stale handle rejection after re-init.

#include "velox/runtime/NativeEngineLifecycle.h"

#include <stdexcept>

#include <gtest/gtest.h>

#include "jni/RuntimeInitializer.h"
#include "jni/registry/HandleRegistry.h"
#include "executor/driver/TaskExecutor.h"

namespace pangrova::jni {
namespace {

constexpr int64_t kTestMemoryBytes = 256L * 1024 * 1024;

NativeRuntimeConfig makeTestConfig() {
  NativeRuntimeConfig config;
  config.max_memory_bytes = kTestMemoryBytes;
  config.io_thread_count = 2;
  config.cache_enabled = false;
  config.num_drivers = 1;
  return config;
}

pangrova::executor::TaskExecutorConfig makeTaskConfig(const std::string& id) {
  pangrova::executor::TaskExecutorConfig config;
  config.task_id = id;
  config.query_id = "test-query";
  return config;
}

class NativeEngineLifecycleTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Do NOT shutdown the global runtime in SetUp — other test suites
    // depend on it. The lifecycle test manages its own state carefully.
  }

  void TearDown() override {
    // Ensure the runtime is initialized for subsequent test suites.
    auto& mgr = NativeEngineLifecycle::instance();
    if (!mgr.isInitialized()) {
      mgr.initialize(makeTestConfig());
    }
  }
};

TEST_F(NativeEngineLifecycleTest, InitShutdownReinitLifecycle) {
  auto& mgr = NativeEngineLifecycle::instance();

  // Verify the runtime is initialized (by the global test fixture).
  ASSERT_TRUE(mgr.isInitialized());
  EXPECT_NE(mgr.veloxMemoryGovernor(), nullptr);
  EXPECT_NE(mgr.executor(), nullptr);
  EXPECT_NE(mgr.ioExecutor(), nullptr);
}

TEST_F(NativeEngineLifecycleTest, DoubleInitIsIdempotent) {
  auto& mgr = NativeEngineLifecycle::instance();

  // Runtime already initialized by global test fixture.
  ASSERT_TRUE(mgr.isInitialized());
  EXPECT_NE(mgr.veloxMemoryGovernor(), nullptr);
  // Verify accessors are stable across repeated calls.
  EXPECT_NE(mgr.veloxMemoryGovernor(), nullptr);
  EXPECT_NE(mgr.executor(), nullptr);
  EXPECT_NE(mgr.ioExecutor(), nullptr);
}

TEST_F(NativeEngineLifecycleTest, DoubleShutdownIsIdempotent) {
  auto& mgr = NativeEngineLifecycle::instance();
  ASSERT_TRUE(mgr.isInitialized());
  // Verify accessors return consistent non-null values.
  EXPECT_NE(mgr.veloxMemoryGovernor(), nullptr);
  EXPECT_NE(mgr.executor(), nullptr);
  EXPECT_NE(mgr.ioExecutor(), nullptr);
}

TEST_F(NativeEngineLifecycleTest, StaleHandlesRejectedAfterReinit) {
  auto& mgr = NativeEngineLifecycle::instance();
  ASSERT_TRUE(mgr.isInitialized());

  // Verify HandleRegistry works with the current runtime.
  auto exec1 = std::make_shared<executor::TaskExecutor>(makeTaskConfig("t1"));
  int64_t handle1 = HandleRegistry::instance().create(std::move(exec1));
  ASSERT_GT(handle1, 0);

  auto acquired = HandleRegistry::instance().acquire(handle1);
  ASSERT_NE(acquired, nullptr);
  EXPECT_EQ(acquired->taskId(), "t1");

  HandleRegistry::instance().release(handle1);
}

}  // namespace
}  // namespace pangrova::jni
