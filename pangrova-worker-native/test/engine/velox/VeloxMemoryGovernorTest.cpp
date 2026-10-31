// VeloxMemoryGovernorTest: Unit tests for VeloxMemoryGovernor — Velox
// SharedArbitrator integration, task pool creation, and memory reporting.
// Validates: Requirements R3, R21, G.2
//
// Tests run against real Velox MemoryManager with SharedArbitrator (null
// listener = test mode, bypasses JNI PangrovaArbitrator).
// No mocks — all memory operations are real Velox allocations.

#include <gtest/gtest.h>
#include <folly/ScopeGuard.h>

#include <array>
#include <barrier>
#include <condition_variable>
#include <exception>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "engine/executor/driver/PlanExecutionDriver.h"
#include "bridge/RuntimeInitializer.h"
#include "velox/common/base/Exceptions.h"
#include "engine/velox/memory/VeloxMemoryGovernor.h"
#include "pangrova/protocol/v2/runtime.pb.h"

#include "TestMemoryHelper.h"

namespace pangrova::velox::test {
namespace {

class VeloxMemoryGovernorTest : public ::testing::Test {
 protected:
  static constexpr int64_t kDefaultMemory = 256L * 1024 * 1024;  // 256MB

  static void SetUpTestSuite() {
    // These unit tests install a standalone process-wide Velox MemoryManager.
    // Stop the lifecycle-owned manager first so its governor cannot retain a
    // pointer that testingSetInstance() invalidates.
    pangrova::jni::shutdownVeloxRuntime();
  }

  static void TearDownTestSuite() {
    // Restore the lifecycle-owned manager for every suite that follows.  A
    // bare testingSetInstance() here would leave globalVeloxMemoryGovernor()
    // pointing at the manager deleted by the next standalone construction.
    pangrova::test::testMemoryManager();
  }

  // Create a VeloxMemoryGovernor in test mode (null listener).
  std::unique_ptr<VeloxMemoryGovernor> createGovernor(
      int64_t memory = kDefaultMemory,
      const VeloxMemoryGovernorConfig& config = {}) {
    return std::make_unique<VeloxMemoryGovernor>(
        memory, /*listener=*/nullptr, config);
  }
};

// --- Construction ---

TEST_F(VeloxMemoryGovernorTest, ConstructWithDefaults) {
  auto governor = createGovernor();
  EXPECT_EQ(governor->totalMemory(), kDefaultMemory);
  EXPECT_GE(governor->usedMemory(), 0);
  EXPECT_NE(governor->veloxMemoryManager(), nullptr);
}

TEST_F(VeloxMemoryGovernorTest, ConstructWithConfig) {
  VeloxMemoryGovernorConfig config;
  config.arbitrator_capacity = kDefaultMemory;
  config.reserved_capacity = 16L * 1024 * 1024;
  config.initial_query_capacity = 32L * 1024 * 1024;
  config.enable_spill = true;
  config.spill_directory = "/tmp/pangrova-test-spill";
  config.max_spill_bytes = 1L * 1024 * 1024 * 1024;

  auto governor = createGovernor(kDefaultMemory, config);
  EXPECT_EQ(governor->totalMemory(), kDefaultMemory);
  EXPECT_TRUE(governor->spillEnabled());
  EXPECT_EQ(governor->spillDirectory(), "/tmp/pangrova-test-spill");
  EXPECT_EQ(governor->maxSpillBytes(), 1L * 1024 * 1024 * 1024);
}

TEST_F(VeloxMemoryGovernorTest, ConstructWithZeroMemoryThrows) {
  EXPECT_THROW(
      createGovernor(0),
      std::invalid_argument);
}

TEST_F(VeloxMemoryGovernorTest, ConstructWithNegativeMemoryThrows) {
  EXPECT_THROW(
      createGovernor(-1),
      std::invalid_argument);
}

// --- Task pool creation ---

TEST_F(VeloxMemoryGovernorTest, CreateTaskPoolReturnsValidPool) {
  auto governor = createGovernor();
  auto pool = governor->createTaskPool("query-1", "task-1", 64L * 1024 * 1024);
  ASSERT_NE(pool, nullptr);
  EXPECT_GE(pool->capacity(), 0);
}

TEST_F(VeloxMemoryGovernorTest, MultipleTaskPoolsCoexist) {
  auto governor = createGovernor();
  auto pool1 = governor->createTaskPool("q1", "t1", 64L * 1024 * 1024);
  auto pool2 = governor->createTaskPool("q2", "t2", 64L * 1024 * 1024);
  ASSERT_NE(pool1, nullptr);
  ASSERT_NE(pool2, nullptr);
  EXPECT_NE(pool1.get(), pool2.get());
}

// --- Memory reporting ---

TEST_F(VeloxMemoryGovernorTest, UsedMemoryReflectsAllocations) {
  auto governor = createGovernor();
  int64_t before = governor->usedMemory();

  auto rootPool = governor->createTaskPool("q1", "t1", 64L * 1024 * 1024);
  // Velox only allows allocation on leaf pools, not root pools.
  auto leafPool = rootPool->addLeafChild("leaf");
  void* buf = leafPool->allocate(1024);
  int64_t after = governor->usedMemory();

  EXPECT_GE(after, before);

  leafPool->free(buf, 1024);
}

// --- Static availability ---

TEST_F(VeloxMemoryGovernorTest, IsAvailableReturnsTrue) {
  EXPECT_TRUE(VeloxMemoryGovernor::isAvailable());
}

// --- Spill config defaults ---

TEST_F(VeloxMemoryGovernorTest, SpillDisabledByDefault) {
  auto governor = createGovernor();
  EXPECT_FALSE(governor->spillEnabled());
  EXPECT_TRUE(governor->spillDirectory().empty());
  EXPECT_EQ(governor->maxSpillBytes(), 0);
}

TEST_F(VeloxMemoryGovernorTest, WorkerLocalSpillPolicyIntersectsNodeAndTaskLimits) {
  VeloxMemoryGovernorConfig config;
  config.enable_spill = true;
  config.spill_directory = "/worker-local/spill";
  config.max_spill_bytes = 512L * 1024 * 1024;
  auto governor = createGovernor(kDefaultMemory, config);

  const auto policy = pangrova::executor::resolveWorkerLocalSpillPolicy(
      256L * 1024 * 1024, governor.get());
  EXPECT_TRUE(policy.enabled);
  EXPECT_EQ(policy.directory, "/worker-local/spill");
  EXPECT_EQ(policy.max_bytes, 256L * 1024 * 1024);

  const auto node_capped = pangrova::executor::resolveWorkerLocalSpillPolicy(
      1024L * 1024 * 1024, governor.get());
  EXPECT_EQ(node_capped.max_bytes, 512L * 1024 * 1024);
}

TEST_F(VeloxMemoryGovernorTest, WorkerLocalSpillPolicyRequiresBothPolicies) {
  {
    auto disabled_governor = createGovernor();
    EXPECT_FALSE(pangrova::executor::resolveWorkerLocalSpillPolicy(
        1024, disabled_governor.get()).enabled);
  }

  VeloxMemoryGovernorConfig config;
  config.enable_spill = true;
  config.spill_directory = "/worker-local/spill";
  auto enabled_governor = createGovernor(kDefaultMemory, config);
  EXPECT_FALSE(pangrova::executor::resolveWorkerLocalSpillPolicy(
      0, enabled_governor.get()).enabled);
  EXPECT_FALSE(pangrova::executor::resolveWorkerLocalSpillPolicy(
      1024, nullptr).enabled);
}

// --- signalMemoryPressure with null cache ---

TEST_F(VeloxMemoryGovernorTest, SignalMemoryPressureNullCacheNoOp) {
  auto governor = createGovernor();
  governor->signalMemoryPressure(nullptr, 90.0);
}

// --- Memory snapshot ---

TEST_F(VeloxMemoryGovernorTest, GetMemorySnapshotReturnsValidData) {
  auto governor = createGovernor();
  auto snapshot = governor->getMemorySnapshot();
  EXPECT_GE(snapshot.current_used_bytes, 0);
  EXPECT_EQ(snapshot.arbitrator_capacity_bytes, kDefaultMemory);
  EXPECT_GE(snapshot.arbitrator_free_bytes, 0);
}

// --- G.2: MemorySnapshot spill_bytes, cache_used_bytes, active_pool_count ---

TEST_F(VeloxMemoryGovernorTest, ReportSpillBytesAccumulatesInSnapshot) {
  auto governor = createGovernor();
  auto pool = governor->createTaskPool("q1", "t1", 64L * 1024 * 1024);
  std::weak_ptr<facebook::velox::memory::MemoryPool> rootOwner = pool;

  governor->reportSpillBytes(1024);

  auto snapshot = governor->getMemorySnapshot();
  EXPECT_EQ(snapshot.spill_bytes, 1024);
  EXPECT_EQ(snapshot.active_pool_count, 1);
  EXPECT_FALSE(rootOwner.expired());
  pool.reset();
  EXPECT_TRUE(rootOwner.expired());
  EXPECT_EQ(governor->getMemorySnapshot().active_pool_count, 0);
  EXPECT_EQ(governor->getMemorySnapshot().spill_bytes, 1024);
}

TEST_F(VeloxMemoryGovernorTest, ReportSpillBytesAccumulatesMultipleTasks) {
  auto governor = createGovernor();
  auto pool1 = governor->createTaskPool("q1", "t1", 32L * 1024 * 1024);
  auto pool2 = governor->createTaskPool("q2", "t2", 32L * 1024 * 1024);

  // Simulate two tasks completing with spill.
  governor->reportSpillBytes(500);
  governor->reportSpillBytes(300);

  auto snapshot = governor->getMemorySnapshot();
  EXPECT_EQ(snapshot.spill_bytes, 800);
  EXPECT_EQ(snapshot.active_pool_count, 2);
  pool1.reset();
  EXPECT_EQ(governor->getMemorySnapshot().active_pool_count, 1);
  pool2.reset();
  EXPECT_EQ(governor->getMemorySnapshot().active_pool_count, 0);
  EXPECT_EQ(governor->getMemorySnapshot().spill_bytes, 800);
}

TEST_F(VeloxMemoryGovernorTest, ActivePoolCountTracksCreateAndRelease) {
  auto governor = createGovernor();

  auto snapshot_before = governor->getMemorySnapshot();
  EXPECT_EQ(snapshot_before.active_pool_count, 0);

  auto pool1 = governor->createTaskPool("q1", "t1", 32L * 1024 * 1024);
  auto pool2 = governor->createTaskPool("q2", "t2", 32L * 1024 * 1024);

  auto snapshot_during = governor->getMemorySnapshot();
  EXPECT_EQ(snapshot_during.active_pool_count, 2);

  governor->reportSpillBytes(0);
  EXPECT_EQ(governor->getMemorySnapshot().active_pool_count, 2);
  pool1.reset();
  auto snapshot_after_one = governor->getMemorySnapshot();
  EXPECT_EQ(snapshot_after_one.active_pool_count, 1);

  governor->reportSpillBytes(0);
  EXPECT_EQ(governor->getMemorySnapshot().active_pool_count, 1);
  pool2.reset();
  auto snapshot_after_two = governor->getMemorySnapshot();
  EXPECT_EQ(snapshot_after_two.active_pool_count, 0);
}

TEST_F(VeloxMemoryGovernorTest, CacheUsedBytesZeroWhenNoCacheSet) {
  auto governor = createGovernor();
  // No cache set — cache_used_bytes should remain 0.
  auto snapshot = governor->getMemorySnapshot();
  EXPECT_EQ(snapshot.cache_used_bytes, 0);
}

TEST_F(VeloxMemoryGovernorTest, SpillBytesZeroWithNoSpill) {
  auto governor = createGovernor();
  auto pool = governor->createTaskPool("q1", "t1", 64L * 1024 * 1024);

  // Task completes with zero spill.
  governor->reportSpillBytes(0);

  auto snapshot = governor->getMemorySnapshot();
  EXPECT_EQ(snapshot.spill_bytes, 0);
}

// --- G.2: End-to-end proto serialization of MemorySnapshot after task with spill ---

TEST_F(VeloxMemoryGovernorTest, MemorySnapshotProtoFieldsNonZeroAfterTaskWithSpill) {
  auto governor = createGovernor();
  const auto bytesBaseline = governor->usedMemory();
  const auto managerBaseline = governor->veloxMemoryManager()->numPools();

  // Producer completion reports spill while the actual allocation is still owned.
  auto pool = governor->createTaskPool("q-spill", "t-spill", 64L * 1024 * 1024);
  std::weak_ptr<facebook::velox::memory::MemoryPool> rootOwner = pool;
  auto leafPool = pool->addLeafChild("leaf-spill");
  void* buf = leafPool->allocate(4096);

  // Simulate task completion with spill (as PlanExecutionDriver does).
  int64_t spillBytes = 8192;
  governor->reportSpillBytes(spillBytes);

  // Collect snapshot and serialize to proto (same path as JniBridge).
  auto snapshot = governor->getMemorySnapshot();
  pangrova::protocol::v2::MemorySnapshot proto;
  proto.set_current_used_bytes(snapshot.current_used_bytes);
  proto.set_peak_used_bytes(snapshot.peak_used_bytes);
  proto.set_spill_bytes(snapshot.spill_bytes);
  proto.set_cache_used_bytes(snapshot.cache_used_bytes);
  proto.set_arbitrator_free_bytes(snapshot.arbitrator_free_bytes);
  proto.set_arbitrator_capacity_bytes(snapshot.arbitrator_capacity_bytes);
  proto.set_active_pool_count(snapshot.active_pool_count);

  // Verify proto fields are populated with actual values.
  EXPECT_GT(proto.current_used_bytes(), 0)
      << "current_used_bytes must reflect active allocation";
  EXPECT_GT(proto.peak_used_bytes(), 0)
      << "peak_used_bytes must track high watermark";
  EXPECT_EQ(proto.spill_bytes(), spillBytes)
      << "spill_bytes must reflect reported spill from task";
  EXPECT_GT(proto.arbitrator_capacity_bytes(), 0)
      << "arbitrator_capacity_bytes must be configured capacity";
  EXPECT_EQ(proto.active_pool_count(), 1);

  // Verify round-trip: serialize → deserialize → fields preserved.
  std::vector<uint8_t> wire(proto.ByteSizeLong());
  proto.SerializeToArray(wire.data(), static_cast<int>(wire.size()));
  ASSERT_FALSE(wire.empty());

  pangrova::protocol::v2::MemorySnapshot deserialized;
  ASSERT_TRUE(deserialized.ParseFromArray(wire.data(), static_cast<int>(wire.size())));
  EXPECT_EQ(deserialized.spill_bytes(), spillBytes);
  EXPECT_EQ(deserialized.current_used_bytes(), proto.current_used_bytes());
  EXPECT_EQ(deserialized.peak_used_bytes(), proto.peak_used_bytes());
  EXPECT_EQ(deserialized.arbitrator_capacity_bytes(), proto.arbitrator_capacity_bytes());
  EXPECT_EQ(deserialized.active_pool_count(), 1);

  leafPool->free(buf, 4096);
  pool.reset();
  EXPECT_FALSE(rootOwner.expired());
  EXPECT_EQ(governor->getMemorySnapshot().active_pool_count, 1);
  leafPool.reset();
  EXPECT_TRUE(rootOwner.expired());
  const auto released = governor->getMemorySnapshot();
  EXPECT_EQ(released.active_pool_count, 0);
  EXPECT_EQ(released.current_used_bytes, bytesBaseline);
  EXPECT_EQ(released.spill_bytes, spillBytes);
  EXPECT_EQ(released.peak_used_bytes, proto.peak_used_bytes());
  EXPECT_EQ(governor->veloxMemoryManager()->numPools(), managerBaseline);
}

TEST_F(VeloxMemoryGovernorTest, ActualRootDestructionWithoutSpillClearsActiveCount) {
  auto governor = createGovernor();
  const auto managerBaseline = governor->veloxMemoryManager()->numPools();
  auto root = governor->createTaskPool("ownership", "root", 1024 * 1024);
  std::weak_ptr<facebook::velox::memory::MemoryPool> rootOwner = root;
  EXPECT_EQ(governor->getMemorySnapshot().active_pool_count, 1);

  root.reset();

  EXPECT_TRUE(rootOwner.expired());
  EXPECT_EQ(governor->veloxMemoryManager()->numPools(), managerBaseline);
  EXPECT_EQ(governor->getMemorySnapshot().active_pool_count, 0);
  EXPECT_EQ(governor->getMemorySnapshot().spill_bytes, 0);
}

TEST_F(VeloxMemoryGovernorTest, ActualLeafRetainsRootUntilItsLastOwnerExits) {
  auto governor = createGovernor();
  const auto managerBaseline = governor->veloxMemoryManager()->numPools();
  const auto bytesBaseline = governor->usedMemory();
  auto root = governor->createTaskPool("ownership", "leaf", 1024 * 1024);
  auto leaf = root->addLeafChild("retained-output");
  std::weak_ptr<facebook::velox::memory::MemoryPool> rootOwner = root;
  void* buffer = leaf->allocate(4096);
  root.reset();

  EXPECT_FALSE(rootOwner.expired());
  EXPECT_EQ(governor->getMemorySnapshot().active_pool_count, 1);
  EXPECT_GT(governor->usedMemory(), bytesBaseline);
  leaf->free(buffer, 4096);
  EXPECT_EQ(governor->getMemorySnapshot().active_pool_count, 1);

  leaf.reset();

  EXPECT_TRUE(rootOwner.expired());
  EXPECT_EQ(governor->getMemorySnapshot().active_pool_count, 0);
  EXPECT_EQ(governor->usedMemory(), bytesBaseline);
  EXPECT_EQ(governor->veloxMemoryManager()->numPools(), managerBaseline);
}

TEST_F(VeloxMemoryGovernorTest, SpillReportsWithoutAnyPoolCannotCreateNegativeOwnership) {
  auto governor = createGovernor();
  int64_t expectedSpillBytes = 0;
  for (const auto bytes : std::array<int64_t, 5>{0, -1, 1024, 1024, 0}) {
    governor->reportSpillBytes(bytes);
    if (bytes > 0) expectedSpillBytes += bytes;
    EXPECT_EQ(governor->getMemorySnapshot().active_pool_count, 0);
    EXPECT_EQ(governor->getMemorySnapshot().spill_bytes, expectedSpillBytes);
  }
}

TEST_F(VeloxMemoryGovernorTest, RepeatedSpillReportsCannotReleaseActualLiveRoot) {
  auto governor = createGovernor();
  auto root = governor->createTaskPool("ownership", "spill", 1024 * 1024);
  std::weak_ptr<facebook::velox::memory::MemoryPool> rootOwner = root;
  int64_t expectedSpillBytes = 0;
  for (const auto bytes : std::array<int64_t, 5>{0, -1, 1024, 1024, 0}) {
    governor->reportSpillBytes(bytes);
    if (bytes > 0) expectedSpillBytes += bytes;
    EXPECT_FALSE(rootOwner.expired());
    EXPECT_EQ(governor->getMemorySnapshot().active_pool_count, 1);
    EXPECT_EQ(governor->getMemorySnapshot().spill_bytes, expectedSpillBytes);
  }
  root.reset();
  EXPECT_TRUE(rootOwner.expired());
  EXPECT_EQ(governor->getMemorySnapshot().active_pool_count, 0);
}

TEST_F(VeloxMemoryGovernorTest, ActualSdkRootConstructionFailureDoesNotRegisterOwnership) {
  auto governor = createGovernor();
  const auto managerBaseline = governor->veloxMemoryManager()->numPools();

  EXPECT_THROW(
      governor->createTaskPool("ownership", "zero-cap", 0),
      facebook::velox::VeloxRuntimeError);

  EXPECT_EQ(governor->getMemorySnapshot().active_pool_count, 0);
  EXPECT_EQ(governor->veloxMemoryManager()->numPools(), managerBaseline);
}

TEST_F(VeloxMemoryGovernorTest, ActualDuplicateRootFailureDoesNotAddOwnership) {
  auto governor = createGovernor();
  const auto managerBaseline = governor->veloxMemoryManager()->numPools();
  auto root = governor->createTaskPool("ownership", "duplicate", 1024 * 1024);

  EXPECT_THROW(
      governor->createTaskPool("ownership", "duplicate", 1024 * 1024),
      facebook::velox::VeloxRuntimeError);

  EXPECT_EQ(governor->getMemorySnapshot().active_pool_count, 1);
  EXPECT_EQ(governor->veloxMemoryManager()->numPools(), managerBaseline + 1);
  root.reset();
  EXPECT_EQ(governor->getMemorySnapshot().active_pool_count, 0);
  EXPECT_EQ(governor->veloxMemoryManager()->numPools(), managerBaseline);
}

TEST_F(VeloxMemoryGovernorTest, ActualSdkChildFailureUnwindsAlreadyCreatedRootOwnership) {
  auto governor = createGovernor();
  const auto managerBaseline = governor->veloxMemoryManager()->numPools();
  std::weak_ptr<facebook::velox::memory::MemoryPool> rootOwner;
  const auto createDuplicateChild = [&] {
    auto root = governor->createTaskPool("ownership", "child-failure", 1024 * 1024);
    rootOwner = root;
    auto leaf = root->addLeafChild("duplicate");
    (void)root->addLeafChild("duplicate");
  };

  EXPECT_THROW(createDuplicateChild(), facebook::velox::VeloxRuntimeError);

  EXPECT_TRUE(rootOwner.expired());
  EXPECT_EQ(governor->getMemorySnapshot().active_pool_count, 0);
  EXPECT_EQ(governor->veloxMemoryManager()->numPools(), managerBaseline);
}

TEST_F(VeloxMemoryGovernorTest, RepeatedActualCreateAndDropDoesNotAccumulateActiveRoots) {
  auto governor = createGovernor();
  const auto managerBaseline = governor->veloxMemoryManager()->numPools();
  for (int round = 0; round < 64; ++round) {
    auto root = governor->createTaskPool("ownership", "reused-task", 1024 * 1024);
    auto leaf = root->addLeafChild("retained-output");
    std::weak_ptr<facebook::velox::memory::MemoryPool> rootOwner = root;
    root.reset();
    EXPECT_EQ(governor->getMemorySnapshot().active_pool_count, 1);
    EXPECT_FALSE(rootOwner.expired());
    leaf.reset();
    EXPECT_TRUE(rootOwner.expired());
    EXPECT_EQ(governor->getMemorySnapshot().active_pool_count, 0);
    EXPECT_EQ(governor->veloxMemoryManager()->numPools(), managerBaseline);
  }
}

TEST_F(VeloxMemoryGovernorTest, ConcurrentSnapshotsObserveActualCreateAndDropOwnership) {
  constexpr int kWorkers = 8;
  constexpr int kRounds = 32;
  auto governor = createGovernor();
  const auto managerBaseline = governor->veloxMemoryManager()->numPools();
  std::barrier phase(kWorkers + 1);
  std::array<std::exception_ptr, kWorkers> failures;
  std::mutex startupMutex;
  std::condition_variable startupReady;
  bool started = false;
  bool aborted = false;
  std::vector<std::thread> workers;
  workers.reserve(kWorkers);
  auto workerCleanup = folly::makeGuard([&] {
    {
      std::lock_guard lock(startupMutex);
      aborted = true;
    }
    startupReady.notify_all();
    for (auto& worker : workers) {
      if (worker.joinable()) worker.join();
    }
  });
  for (int worker = 0; worker < kWorkers; ++worker) {
    workers.emplace_back([&, worker] {
      {
        std::unique_lock lock(startupMutex);
        startupReady.wait(lock, [&] { return started || aborted; });
        if (aborted) return;
      }
      for (int round = 0; round < kRounds; ++round) {
        std::shared_ptr<facebook::velox::memory::MemoryPool> root;
        std::shared_ptr<facebook::velox::memory::MemoryPool> leaf;
        try {
          root = governor->createTaskPool(
              "concurrent", std::to_string(worker), 1024 * 1024);
          leaf = root->addLeafChild("retained-output");
          root.reset();
        } catch (...) {
          failures[worker] = std::current_exception();
        }
        phase.arrive_and_wait();
        phase.arrive_and_wait();
        leaf.reset();
        root.reset();
        phase.arrive_and_wait();
        phase.arrive_and_wait();
      }
    });
  }
  {
    std::lock_guard lock(startupMutex);
    started = true;
  }
  startupReady.notify_all();
  for (int round = 0; round < kRounds; ++round) {
    for (int sample = 0; sample < 8; ++sample) {
      const auto count = governor->getMemorySnapshot().active_pool_count;
      EXPECT_GE(count, 0);
      EXPECT_LE(count, kWorkers);
    }
    phase.arrive_and_wait();
    EXPECT_EQ(governor->getMemorySnapshot().active_pool_count, kWorkers);
    phase.arrive_and_wait();
    for (int sample = 0; sample < 8; ++sample) {
      const auto count = governor->getMemorySnapshot().active_pool_count;
      EXPECT_GE(count, 0);
      EXPECT_LE(count, kWorkers);
    }
    phase.arrive_and_wait();
    EXPECT_EQ(governor->getMemorySnapshot().active_pool_count, 0);
    EXPECT_EQ(governor->veloxMemoryManager()->numPools(), managerBaseline);
    phase.arrive_and_wait();
  }
  for (auto& worker : workers) worker.join();
  for (const auto& failure : failures) {
    if (failure) std::rethrow_exception(failure);
  }
}

}  // namespace
}  // namespace pangrova::velox::test
