#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <thread>

#include "engine/executor/driver/TaskExecutor.h"
#include "command/resource/HandleRegistry.h"
#include "engine/shuffle/FlightShuffleServer.h"
#include "velox/expression/FunctionRegistrySnapshot.h"

namespace pangrova::jni {
namespace {
using namespace std::chrono_literals;

executor::TaskExecutorConfig producerConfig(const std::string& task_id) {
  executor::TaskExecutorConfig config;
  config.function_registry = facebook::velox::exec::FunctionRegistrySnapshot::capture();
  config.task_id = task_id;
  config.query_id = "shuffle-control";
  config.stage_id = 1;
  config.num_shuffle_partitions = 1;
  config.plan_epoch = 2;
  config.fencing_token = 3;
  config.deadline_epoch_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count() + 60'000;
  return config;
}

bool awaitSeal(executor::TaskExecutor& task) {
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!task.isShuffleStartSealed() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  return task.isShuffleStartSealed();
}

class TaskShuffleControlTest : public ::testing::Test {
 protected:
  void SetUp() override { HandleRegistry::instance().destroyAll(); }
  void TearDown() override { HandleRegistry::instance().destroyAll(); }
};

TEST_F(TaskShuffleControlTest, ReleaseSealsAdmittedStartBeforeActualOwnerDrainAndReceipt) {
  auto task = std::make_shared<executor::TaskExecutor>(producerConfig("release-race"));
  const auto handle = HandleRegistry::instance().create(task);
  shuffle::FlightShuffleServer server(shuffle::FlightShuffleServerConfig{});
  auto lease = task->acquireShuffleStart();
  const auto owner = server.registerHandle(handle, task->serializedShuffleQueue(),
      task->queryId(), task->stageId(), 1, task->planEpoch(), task->attemptId(), task->fencingToken());
  task->installShuffleRegistration(lease, owner);
  std::atomic<bool> receipt_observed{false};
  auto release = std::async(std::launch::async, [&] {
    HandleRegistry::instance().release(handle, [&](executor::TaskExecutor& actual) {
      EXPECT_TRUE(actual.isExecutionReleased());
      EXPECT_EQ(owner->state(), shuffle::ShuffleProducerRegistration::State::CLOSED);
      EXPECT_TRUE(actual.serializedShuffleQueue()->isAborted());
      receipt_observed.store(true);
    });
  });
  EXPECT_TRUE(awaitSeal(*task));
  EXPECT_THROW(task->acquireShuffleStart(), std::logic_error);
  EXPECT_EQ(release.wait_for(0ms), std::future_status::timeout);
  EXPECT_FALSE(receipt_observed.load());
  EXPECT_EQ(HandleRegistry::instance().acquire(handle), task);
  EXPECT_EQ(server.bufferUsage().handles, 1u);
  lease.reset();
  ASSERT_EQ(release.wait_for(5s), std::future_status::ready);
  EXPECT_NO_THROW(release.get());
  EXPECT_TRUE(receipt_observed.load());
  EXPECT_EQ(server.bufferUsage().handles, 0u);
  EXPECT_THROW(HandleRegistry::instance().acquire(handle), std::runtime_error);
}

TEST_F(TaskShuffleControlTest, ConcurrentCancelAndReleaseShareWatchdogJoinAndDoNotReadmit) {
  auto task = std::make_shared<executor::TaskExecutor>(producerConfig("cancel-release"));
  const auto handle = HandleRegistry::instance().create(task);
  auto lease = task->acquireShuffleStart();
  auto cancelling = std::async(std::launch::async, [&] { task->cancel(); });
  auto releasing = std::async(std::launch::async, [&] { task->release(); });
  EXPECT_TRUE(awaitSeal(*task));
  EXPECT_EQ(cancelling.wait_for(0ms), std::future_status::timeout);
  EXPECT_EQ(releasing.wait_for(0ms), std::future_status::timeout);
  lease.reset();
  ASSERT_EQ(cancelling.wait_for(5s), std::future_status::ready);
  ASSERT_EQ(releasing.wait_for(5s), std::future_status::ready);
  EXPECT_NO_THROW(cancelling.get());
  EXPECT_NO_THROW(releasing.get());
  EXPECT_TRUE(task->isExecutionReleased());
  EXPECT_TRUE(task->serializedShuffleQueue()->isAborted());
  EXPECT_THROW(task->acquireShuffleStart(), std::logic_error);
  EXPECT_NO_THROW(HandleRegistry::instance().release(handle));
}

TEST_F(TaskShuffleControlTest, CommittedProducerWinsCancellationAndLingerRemainsReadable) {
  auto task = std::make_shared<executor::TaskExecutor>(producerConfig("committed-winner"));
  const auto handle = HandleRegistry::instance().create(task);
  shuffle::FlightShuffleServer server(shuffle::FlightShuffleServerConfig{});
  auto lease = task->acquireShuffleStart();
  const auto owner = server.registerHandle(handle, task->serializedShuffleQueue(),
      task->queryId(), task->stageId(), 1, task->planEpoch(), task->attemptId(), task->fencingToken());
  task->installShuffleRegistration(lease, owner);
  ASSERT_TRUE(task->serializedShuffleQueue()->commit());
  lease.reset();
  task->cancel();
  EXPECT_TRUE(task->serializedShuffleQueue()->isCommitted());
  EXPECT_FALSE(task->serializedShuffleQueue()->isAborted());
  EXPECT_NE(task->state(), executor::ExecutorState::CANCELLED);
  EXPECT_THROW(task->acquireShuffleStart(), std::logic_error);
  owner->close();
  EXPECT_EQ(owner->state(), shuffle::ShuffleProducerRegistration::State::LINGERING);
  shuffle::ShuffleBatch batch;
  EXPECT_FALSE(server.popForHandle(handle, 0, batch, 1s));
  EXPECT_EQ(server.evictFullyConsumedLingering(), 1u);
  EXPECT_NO_THROW(HandleRegistry::instance().release(handle));
  EXPECT_TRUE(task->serializedShuffleQueue()->isCommitted());
}

TEST_F(TaskShuffleControlTest, DeadlineWatchdogSealAndConcurrentReleaseDoNotMutuallyJoin) {
  auto config = producerConfig("deadline-release");
  config.deadline_epoch_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count() + 1000;
  auto task = std::make_shared<executor::TaskExecutor>(config);
  const auto handle = HandleRegistry::instance().create(task);
  auto lease = task->acquireShuffleStart();
  // Only the actual deadline watchdog can seal this held lease at this point.
  EXPECT_TRUE(awaitSeal(*task));
  EXPECT_THROW(task->acquireShuffleStart(), std::logic_error);
  auto release = std::async(std::launch::async, [&] { HandleRegistry::instance().release(handle); });
  EXPECT_EQ(release.wait_for(0ms), std::future_status::timeout);
  EXPECT_EQ(HandleRegistry::instance().acquire(handle), task);
  lease.reset();
  ASSERT_EQ(release.wait_for(5s), std::future_status::ready);
  EXPECT_NO_THROW(release.get());
  EXPECT_EQ(task->state(), executor::ExecutorState::FAILED);
  EXPECT_NE(task->errorMessage().find("Task deadline exceeded"), std::string::npos);
  EXPECT_TRUE(task->isExecutionReleased());
  EXPECT_TRUE(task->serializedShuffleQueue()->isAborted());
}

}  // namespace
}  // namespace pangrova::jni
