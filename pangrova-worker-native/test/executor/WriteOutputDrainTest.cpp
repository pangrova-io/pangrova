#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <thread>

#include "executor/driver/WriteOutputDrain.h"

namespace pangrova::executor::test {
namespace {

using namespace std::chrono_literals;

TEST(WriteOutputDrainTest, OneFreedQueueSlotWakesOnlyOneDeferredProducer) {
  OutputQueue queue(1);
  ASSERT_TRUE(queue.push(nullptr, 10ms));
  int first = 0;
  int second = 0;
  queue.registerSpaceCallback([&] {
    ++first;
    EXPECT_TRUE(queue.tryPush(nullptr));
  });
  queue.registerSpaceCallback([&] {
    ++second;
    EXPECT_TRUE(queue.tryPush(nullptr));
  });

  facebook::velox::RowVectorPtr batch;
  EXPECT_EQ(queue.pop(batch, 10ms), PopResult::DATA);
  EXPECT_EQ(first, 1);
  EXPECT_EQ(second, 0);
  EXPECT_EQ(queue.pop(batch, 10ms), PopResult::DATA);
  EXPECT_EQ(second, 1);
  EXPECT_EQ(queue.pop(batch, 10ms), PopResult::DATA);
}

TEST(WriteOutputDrainTest, DeferredPushDoesNotLoseWakeupOrDuplicatePromise) {
  OutputQueue queue(1);
  ASSERT_TRUE(queue.push(nullptr, 10ms));
  int retry = 0;
  EXPECT_EQ(queue.tryPushOrDefer(nullptr, [&] { ++retry; }),
            DeferredPushResult::DEFERRED);
  facebook::velox::RowVectorPtr batch;
  EXPECT_EQ(queue.pop(batch, 10ms), PopResult::DATA);
  EXPECT_EQ(retry, 1);
  EXPECT_EQ(queue.tryPushOrDefer(nullptr, [] {}),
            DeferredPushResult::PUSHED);
  queue.close();
  EXPECT_EQ(queue.tryPushOrDefer(nullptr, [] {}),
            DeferredPushResult::CLOSED);
}

TEST(WriteOutputDrainTest, TimeoutDoesNotFinishBeforeDelayedProducerCloses) {
  OutputQueue queue(2);
  auto outcome = std::make_shared<std::atomic<WriteProducerOutcome>>(
      WriteProducerOutcome::RUNNING);
  std::atomic<int> consumed{0};
  std::atomic<int> finalized{0};

  auto drain = std::async(std::launch::async, [&] {
    return drainWriteOutput(
        &queue, outcome, 10ms,
        [&](const facebook::velox::RowVectorPtr&) { ++consumed; },
        [&] { ++finalized; });
  });

  // Force several poll timeouts before the producer emits its batch.
  std::this_thread::sleep_for(150ms);
  ASSERT_EQ(drain.wait_for(0ms), std::future_status::timeout);
  ASSERT_TRUE(queue.push(nullptr, 10ms));
  ASSERT_TRUE(markWriteProducerSucceeded(outcome));
  queue.close();

  EXPECT_TRUE(drain.get());
  EXPECT_EQ(consumed.load(), 1);
  EXPECT_EQ(finalized.load(), 1);
}

TEST(WriteOutputDrainTest, FailedProducerNeverFinalizesStagedOutput) {
  OutputQueue queue(2);
  auto outcome = std::make_shared<std::atomic<WriteProducerOutcome>>(
      WriteProducerOutcome::RUNNING);
  int consumed = 0;
  int finalized = 0;
  ASSERT_TRUE(queue.push(nullptr, 10ms));
  markWriteProducerFailed(outcome);
  queue.close();

  EXPECT_FALSE(drainWriteOutput(
      &queue, outcome, 10ms,
      [&](const facebook::velox::RowVectorPtr&) { ++consumed; },
      [&] { ++finalized; }));
  EXPECT_EQ(consumed, 1);
  EXPECT_EQ(finalized, 0);
}

TEST(WriteOutputDrainTest, CancellationCloseIsNotSuccessfulEndOfStream) {
  OutputQueue queue(1);
  auto outcome = std::make_shared<std::atomic<WriteProducerOutcome>>(
      WriteProducerOutcome::CANCELLED);
  int finalized = 0;
  queue.close();

  EXPECT_FALSE(drainWriteOutput(
      &queue, outcome, 10ms,
      [](const facebook::velox::RowVectorPtr&) {},
      [&] { ++finalized; }));
  EXPECT_EQ(finalized, 0);
}

TEST(WriteOutputDrainTest, CancelWinsRaceWithUnclaimedSuccess) {
  OutputQueue queue(1);
  auto outcome = std::make_shared<std::atomic<WriteProducerOutcome>>(
      WriteProducerOutcome::RUNNING);
  int finalized = 0;
  ASSERT_TRUE(markWriteProducerSucceeded(outcome));
  ASSERT_TRUE(markWriteProducerCancelled(outcome));
  queue.close();

  EXPECT_FALSE(drainWriteOutput(
      &queue, outcome, 10ms,
      [](const facebook::velox::RowVectorPtr&) {},
      [&] { ++finalized; }));
  EXPECT_EQ(finalized, 0);
  EXPECT_EQ(outcome->load(), WriteProducerOutcome::CANCELLED);
}

TEST(WriteOutputDrainTest, FinalizeClaimIsNotOverwrittenByLateCancel) {
  OutputQueue queue(1);
  auto outcome = std::make_shared<std::atomic<WriteProducerOutcome>>(
      WriteProducerOutcome::RUNNING);
  std::promise<void> finalize_claimed;
  std::promise<void> allow_finalize;
  auto allow = allow_finalize.get_future();
  ASSERT_TRUE(markWriteProducerSucceeded(outcome));
  queue.close();

  auto drain = std::async(std::launch::async, [&] {
    return drainWriteOutput(
        &queue, outcome, 10ms,
        [](const facebook::velox::RowVectorPtr&) {}, [&] {
          finalize_claimed.set_value();
          allow.wait();
        });
  });
  finalize_claimed.get_future().wait();
  EXPECT_FALSE(markWriteProducerCancelled(outcome));
  allow_finalize.set_value();
  EXPECT_TRUE(drain.get());
  EXPECT_EQ(outcome->load(), WriteProducerOutcome::FINISHED);
}

}  // namespace
}  // namespace pangrova::executor::test
