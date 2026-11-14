// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

// Unit tests for HandleRegistry: generation tracking, double-release detection,
// stale handle rejection, and concurrent safety.

#include "CanonicalTestAttempt.h"
#include "command/resource/HandleRegistry.h"
#include "velox/expression/FunctionRegistrySnapshot.h"

#include <atomic>
#include <new>
#include <stdexcept>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "engine/executor/driver/TaskExecutor.h"
#include "command/StatusSerializer.h"
#include "pangrova/protocol/v2/result.pb.h"

namespace pangrova::jni {
namespace {

// Minimal TaskExecutorConfig for test — only task_id is needed.
pangrova::executor::TaskExecutorConfig makeTestConfig(const std::string& id) {
  pangrova::executor::TaskExecutorConfig config;
  config.function_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  config.task_id = id;
  config.query_id = "test-query";
  pangrova::test::bindTestAttempt(config);
  return config;
}

class HandleRegistryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Clean slate for each test.
    HandleRegistry::instance().destroyAll();
  }

  void TearDown() override {
    HandleRegistry::instance().destroyAll();
  }
};

TEST_F(HandleRegistryTest, CreateAndAcquire) {
  auto exec = std::make_shared<executor::TaskExecutor>(makeTestConfig("t1"));
  int64_t handle = HandleRegistry::instance().create(std::move(exec));
  ASSERT_GT(handle, 0);

  auto acquired = HandleRegistry::instance().acquire(handle);
  ASSERT_NE(acquired, nullptr);
  EXPECT_EQ(acquired->taskId(), "t1");
}

TEST_F(HandleRegistryTest, ReleaseAndDoubleReleaseThrows) {
  auto exec = std::make_shared<executor::TaskExecutor>(makeTestConfig("t2"));
  int64_t handle = HandleRegistry::instance().create(std::move(exec));

  // First release succeeds.
  ASSERT_NO_THROW(HandleRegistry::instance().release(handle));
  EXPECT_EQ(HandleRegistry::instance().size(), 0);

  // Second release throws (not SIGSEGV).
  EXPECT_THROW(HandleRegistry::instance().release(handle), std::runtime_error);
}

TEST_F(HandleRegistryTest, ReceiptPreparationKeepsExactDrainedExecutorUntilErase) {
  auto task = std::make_shared<executor::TaskExecutor>(makeTestConfig("receipt-owner"));
  auto& registry = HandleRegistry::instance();
  const auto handle = registry.create(task);
  EXPECT_FALSE(task->isExecutionReleased());
  bool observed = false;
  registry.release(handle, [&](executor::TaskExecutor& drained) {
    EXPECT_EQ(&drained, task.get());
    EXPECT_EQ(registry.acquire(handle), task);
    EXPECT_EQ(drained.state(), executor::ExecutorState::CANCELLED);
    EXPECT_EQ(drained.taskId(), "receipt-owner");
    EXPECT_TRUE(drained.isExecutionReleased());
    EXPECT_EQ(registry.size(), 1);
    observed = true;
  });
  EXPECT_TRUE(observed);
  EXPECT_EQ(registry.size(), 0);
  EXPECT_THROW(registry.acquire(handle), std::runtime_error);
}

// This is the allocation callback contract, not evidence of a JVM OOME.
TEST_F(HandleRegistryTest, ReceiptAllocationFailureRetainsExactOwnerForRetry) {
  auto task = std::make_shared<executor::TaskExecutor>(makeTestConfig("receipt-retry"));
  auto& registry = HandleRegistry::instance();
  const auto handle = registry.create(task);
  EXPECT_THROW(registry.release(handle, [&](executor::TaskExecutor& drained) {
    EXPECT_EQ(&drained, task.get());
    EXPECT_EQ(drained.state(), executor::ExecutorState::CANCELLED);
    EXPECT_TRUE(drained.isExecutionReleased());
    throw std::bad_alloc();
  }), std::bad_alloc);
  ASSERT_EQ(registry.acquire(handle), task);
  ASSERT_EQ(registry.size(), 1);
  bool receiptPrepared = false;
  registry.release(handle, [&](executor::TaskExecutor& drained) {
    EXPECT_EQ(&drained, task.get());
    EXPECT_EQ(drained.taskId(), "receipt-retry");
    EXPECT_EQ(drained.state(), executor::ExecutorState::CANCELLED);
    receiptPrepared = true;
  });
  EXPECT_TRUE(receiptPrepared);
  EXPECT_EQ(registry.size(), 0);
  EXPECT_THROW(registry.acquire(handle), std::runtime_error);
}

TEST_F(HandleRegistryTest, PendingNativeOwnerReleaseHasEvidenceWithoutInventedTaskStatistics) {
  auto task = std::make_shared<executor::TaskExecutor>(makeTestConfig("pending-release-evidence"));
  auto& registry = HandleRegistry::instance();
  const auto handle = registry.create(task);
  auto before = serializeTaskStatus(*task);
  protocol::v2::TaskReport status;
  ASSERT_TRUE(status.ParseFromArray(before.data(), static_cast<int>(before.size())));
  EXPECT_EQ(status.execution_drain_state(), protocol::v2::EXECUTION_DRAIN_STATE_UNSPECIFIED);
  registry.release(handle, [&](executor::TaskExecutor& drained) {
    const auto bytes = serializeTaskStatus(drained);
    ASSERT_TRUE(status.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));
    EXPECT_EQ(status.attempt().task_id(), "pending-release-evidence");
    EXPECT_EQ(status.state(), protocol::v2::TASK_STATE_CANCELLED);
    EXPECT_EQ(status.execution_drain_state(), protocol::v2::EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED);
    EXPECT_FALSE(status.has_native_task_stats());
  });
  EXPECT_THROW(task->start(), std::runtime_error);
}

TEST_F(HandleRegistryTest, AcquireAfterDestroyAllThrowsStaleGeneration) {
  auto exec = std::make_shared<executor::TaskExecutor>(makeTestConfig("t3"));
  int64_t handle = HandleRegistry::instance().create(std::move(exec));
  uint64_t gen_before = HandleRegistry::instance().generation();

  HandleRegistry::instance().destroyAll();

  EXPECT_EQ(HandleRegistry::instance().generation(), gen_before + 1);
  EXPECT_EQ(HandleRegistry::instance().size(), 0);

  // Handle from previous generation is gone — acquire throws.
  EXPECT_THROW(HandleRegistry::instance().acquire(handle), std::runtime_error);
}

TEST_F(HandleRegistryTest, AcquireNonExistentHandleThrows) {
  EXPECT_THROW(HandleRegistry::instance().acquire(999999), std::runtime_error);
}

TEST_F(HandleRegistryTest, CreateNullExecutorThrows) {
  EXPECT_THROW(HandleRegistry::instance().create(nullptr), std::runtime_error);
}

TEST_F(HandleRegistryTest, PendingExecutorRemainsOwnedUntilExplicitRelease) {
  auto exec = std::make_shared<executor::TaskExecutor>(makeTestConfig("t4"));
  int64_t handle = HandleRegistry::instance().create(exec);
  EXPECT_EQ(HandleRegistry::instance().size(), 1);

  EXPECT_EQ(HandleRegistry::instance().acquire(handle), exec);
  EXPECT_EQ(exec->state(), executor::ExecutorState::PENDING);
  EXPECT_EQ(HandleRegistry::instance().size(), 1);

  // Clean up manually.
  HandleRegistry::instance().release(handle);
}

TEST_F(HandleRegistryTest, ConcurrentAcquireRelease) {
  // Create 8 executors, then concurrently acquire and release from 8 threads.
  constexpr int kNumHandles = 8;
  std::vector<int64_t> handles;
  handles.reserve(kNumHandles);
  for (int i = 0; i < kNumHandles; ++i) {
    auto exec = std::make_shared<executor::TaskExecutor>(
        makeTestConfig("concurrent-" + std::to_string(i)));
    handles.push_back(HandleRegistry::instance().create(std::move(exec)));
  }

  std::atomic<int> acquire_success{0};
  std::atomic<int> release_success{0};
  std::vector<std::thread> threads;
  threads.reserve(kNumHandles);

  for (int i = 0; i < kNumHandles; ++i) {
    threads.emplace_back([&, i]() {
      // Each thread acquires its own handle, verifies, then releases.
      try {
        auto exec = HandleRegistry::instance().acquire(handles[i]);
        if (exec) {
          acquire_success.fetch_add(1, std::memory_order_relaxed);
        }
      } catch (...) {
        // Should not happen — each handle is unique per thread.
      }
      try {
        HandleRegistry::instance().release(handles[i]);
        release_success.fetch_add(1, std::memory_order_relaxed);
      } catch (...) {
        // Should not happen.
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_EQ(acquire_success.load(), kNumHandles);
  EXPECT_EQ(release_success.load(), kNumHandles);
  EXPECT_EQ(HandleRegistry::instance().size(), 0);
}

TEST_F(HandleRegistryTest, NewHandlesAfterDestroyAllWork) {
  // Verify that after destroyAll, new handles work correctly.
  auto exec1 = std::make_shared<executor::TaskExecutor>(makeTestConfig("old"));
  int64_t old_handle = HandleRegistry::instance().create(std::move(exec1));

  HandleRegistry::instance().destroyAll();

  auto exec2 = std::make_shared<executor::TaskExecutor>(makeTestConfig("new"));
  int64_t new_handle = HandleRegistry::instance().create(std::move(exec2));

  // Old handle is stale — should not be found.
  EXPECT_THROW(HandleRegistry::instance().acquire(old_handle),
               std::runtime_error);

  // New handle works.
  auto acquired = HandleRegistry::instance().acquire(new_handle);
  ASSERT_NE(acquired, nullptr);
  EXPECT_EQ(acquired->taskId(), "new");

  HandleRegistry::instance().release(new_handle);
}

}  // namespace
}  // namespace pangrova::jni
