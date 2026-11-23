#include <gtest/gtest.h>

#include <atomic>
#include <future>

#include "engine/result/ResultOwnershipState.h"

namespace pangrova::result::test {
namespace {
class ResultOwnershipStateTest : public ::testing::Test {
 protected:
  std::shared_ptr<ResultOwnershipState> state_ = std::make_shared<ResultOwnershipState>();
};

TEST_F(ResultOwnershipStateTest, DeliversOnceOnlyAfterProducerQueueAndAllBorrowsExit) {
  auto owner = std::make_shared<ResultOwnershipState>();
  owner->acquireReader();
  owner->acquireBatch();
  std::atomic<int> calls{0};
  owner->watchDrain([&] {
    EXPECT_TRUE(owner->evidence().drained());
    ++calls;
  });
  owner->stopProducer();
  owner->revokeAndClearQueue();
  EXPECT_EQ(calls, 0);
  auto reader = std::async(std::launch::async, [&] { owner->releaseReader(); });
  auto batch = std::async(std::launch::async, [&] { owner->releaseBatch(); });
  reader.get();
  batch.get();
  EXPECT_EQ(calls, 1);
  owner->stopProducer();
  owner->revokeAndClearQueue();
  EXPECT_EQ(calls, 1);
}

TEST_F(ResultOwnershipStateTest, RegistrationReplaysCompletedDrainOnce) {
  ResultOwnershipState owner;
  owner.revokeAndClearQueue();
  owner.stopProducer();
  int calls = 0;
  owner.watchDrain([&] { ++calls; });
  EXPECT_EQ(calls, 1);
  EXPECT_THROW(owner.watchDrain([&] { ++calls; }), std::logic_error);
  EXPECT_EQ(calls, 1);
}

TEST_F(ResultOwnershipStateTest, NoReadersDoesNotProveUnclaimedProducerOrQueueDrained) {
  EXPECT_FALSE(state_->evidence().drained());
  state_->stopProducer();
  EXPECT_FALSE(state_->evidence().drained());
  state_->revokeAndClearQueue();
  EXPECT_TRUE(state_->evidence().drained());
  EXPECT_THROW(state_->acquireReader(), std::runtime_error);
}

TEST_F(ResultOwnershipStateTest, ReaderLifetimeRemainsChargedAfterProducerEofAndRevocation) {
  state_->acquireReader();
  state_->stopProducer();
  state_->revokeAndClearQueue();
  EXPECT_EQ(state_->evidence().active_readers, 1);
  EXPECT_FALSE(state_->evidence().drained());
  state_->releaseReader();
  EXPECT_TRUE(state_->evidence().drained());
}

TEST_F(ResultOwnershipStateTest, PreparationAndRevocationCannotReportDrainEarly) {
  ASSERT_TRUE(state_->tryAcquireBatch());
  state_->stopProducer();
  state_->revokeAndClearQueue();
  EXPECT_FALSE(state_->tryAcquireBatch());
  EXPECT_FALSE(state_->evidence().drained());
  state_->releaseBatch();
  EXPECT_TRUE(state_->evidence().drained());
}
}  // namespace
}  // namespace pangrova::result::test
