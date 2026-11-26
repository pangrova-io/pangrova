#include <gtest/gtest.h>

#include <arrow/array/builder_primitive.h>
#include <arrow/buffer.h>
#include <chrono>

#include "TestMemoryHelper.h"
#include "engine/memory/ColumnarMemoryPool.h"
#include "engine/result/ResultOwnershipState.h"
#include "engine/result/ResultPayloadQueue.h"

namespace pangrova::result::test {
namespace {

class ResultPayloadQueueTest : public ::testing::Test {
 protected:
  ResultPayloadQueue::Frame frame(int64_t value, const std::shared_ptr<void>& source = {}) {
    arrow::Int64Builder builder(pool_.get());
    EXPECT_TRUE(builder.Append(value).ok());
    auto array = builder.Finish().ValueOrDie();
    auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("id", arrow::int64())}), 1, {array});
    auto options = arrow::ipc::IpcWriteOptions::Defaults();
    options.memory_pool = pool_.get();
    arrow::ipc::IpcPayload payload;
    EXPECT_TRUE(arrow::ipc::GetRecordBatchPayload(*batch, options, &payload).ok());
    const auto bytes = arrow::ipc::GetPayloadSize(payload, options);
    return ResultPayloadQueue::Frame(std::move(payload), bytes, pool_, state_, source);
  }

  arrow::Result<ResultPayloadQueue::Frame> next(const std::shared_ptr<ResultPayloadQueue>& queue) {
    return queue->next(std::chrono::steady_clock::now() + std::chrono::seconds(1), [] { return false; });
  }

  void expectValue(const ResultPayloadQueue::Frame& input, int64_t value) {
    auto payload = input.flightPayload();
    ASSERT_NE(payload.ipc_message.metadata, nullptr);
    const auto& body = payload.ipc_message.body_buffers.back();
    ASSERT_GE(body->size(), static_cast<int64_t>(sizeof(value)));
    EXPECT_EQ(*reinterpret_cast<const int64_t*>(body->data()), value);
  }

  std::shared_ptr<facebook::velox::memory::MemoryPool> leaf_ =
      pangrova::test::testLeafPool("result-wire-credit");
  std::shared_ptr<memory::ColumnarMemoryPool> pool_ =
      std::make_shared<memory::ColumnarMemoryPool>(leaf_);
  std::shared_ptr<ResultOwnershipState> state_ = std::make_shared<ResultOwnershipState>();
};

TEST_F(ResultPayloadQueueTest, PayloadBodySliceRetainsCreditAllocatorAndPhysicalOwner) {
  auto queue = std::make_shared<ResultPayloadQueue>(1);
  auto source = std::make_shared<int>(7);
  std::weak_ptr<int> sourceWitness = source;
  auto original = frame(42, source);
  ASSERT_TRUE(queue->tryPush(original).ValueOrDie());
  source.reset();
  auto popped = next(queue).ValueOrDie();
  auto payload = popped.flightPayload();
  auto body = arrow::SliceBuffer(payload.ipc_message.body_buffers.back(), 0);
  popped = {};
  payload = {};
  ASSERT_GT(queue->retainedBytes(), 1);
  EXPECT_FALSE(sourceWitness.expired());
  ASSERT_GT(pool_->bytes_allocated(), 0);
  queue->abort(arrow::Status::Cancelled("revoked"));
  state_->stopProducer();
  state_->revokeAndClearQueue();
  EXPECT_FALSE(state_->evidence().drained());
  EXPECT_EQ(*reinterpret_cast<const int64_t*>(body->data()), 42);
  body.reset();
  EXPECT_TRUE(state_->evidence().drained());
  EXPECT_TRUE(sourceWitness.expired());
  EXPECT_EQ(queue->retainedBytes(), 0);
  EXPECT_EQ(pool_->bytes_allocated(), 0);
  EXPECT_EQ(leaf_->usedBytes(), 0);
}

TEST_F(ResultPayloadQueueTest, ExclusiveCreditPromotesEveryFrameInOrderBeforeDriverResumes) {
  auto queue = std::make_shared<ResultPayloadQueue>(1);
  auto first = frame(41);
  ASSERT_TRUE(queue->tryPush(first).ValueOrDie());
  auto transport = next(queue).ValueOrDie();
  expectValue(transport, 41);
  auto second = frame(42);
  auto third = frame(43);
  EXPECT_FALSE(queue->tryPush(second).ValueOrDie());
  std::vector<ResultPayloadQueue::Frame> remaining;
  remaining.push_back(std::move(second));
  remaining.push_back(std::move(third));
  int wakeups = 0;
  ASSERT_TRUE(queue->pushOrDefer(std::move(remaining), [&](const auto& status) {
    EXPECT_TRUE(status.ok());
    ++wakeups;
  }).ok());
  EXPECT_EQ(wakeups, 0);
  EXPECT_TRUE(queue->seal().IsInvalid());
  transport = {};
  EXPECT_EQ(wakeups, 0);
  transport = next(queue).ValueOrDie();
  expectValue(transport, 42);
  transport = {};
  EXPECT_EQ(wakeups, 1);
  ASSERT_TRUE(queue->seal().ok());
  transport = next(queue).ValueOrDie();
  expectValue(transport, 43);
  EXPECT_FALSE(next(queue).ValueOrDie());
  // EOF does not return credits still held by the transport.
  EXPECT_GT(queue->retainedBytes(), 0);
  transport = {};
  EXPECT_EQ(queue->retainedBytes(), 0);
}

TEST_F(ResultPayloadQueueTest, AbortDiscardsWaitersAndRetainsTheOriginalFailure) {
  auto queue = std::make_shared<ResultPayloadQueue>(1);
  auto first = frame(7);
  ASSERT_TRUE(queue->tryPush(first).ValueOrDie());
  std::vector<ResultPayloadQueue::Frame> waiting;
  waiting.push_back(frame(8));
  waiting.push_back(frame(9));
  int wakeups = 0;
  const auto cause = arrow::Status::ExecutionError("producer fault");
  ASSERT_TRUE(queue->pushOrDefer(std::move(waiting), [&](const auto& status) {
    EXPECT_EQ(status.ToString(), cause.ToString());
    ++wakeups;
  }).ok());
  queue->abort(cause);
  queue->abort(arrow::Status::Cancelled("subsequent cancellation"));
  EXPECT_EQ(wakeups, 1);
  EXPECT_EQ(next(queue).status().ToString(), cause.ToString());
  EXPECT_EQ(queue->retainedBytes(), 0);
  EXPECT_EQ(state_->evidence().retained_batch_owners, 0u);
}

TEST_F(ResultPayloadQueueTest, ZeroColumnMessageRetainsMetadataAfterQueueAndReaderExit) {
  auto queue = std::make_shared<ResultPayloadQueue>(1024);
  auto batch = arrow::RecordBatch::Make(arrow::schema({}), 7, arrow::ArrayVector{});
  const auto options = arrow::ipc::IpcWriteOptions::Defaults();
  arrow::ipc::IpcPayload message;
  ASSERT_TRUE(arrow::ipc::GetRecordBatchPayload(*batch, options, &message).ok());
  const auto bytes = arrow::ipc::GetPayloadSize(message, options);
  auto input = ResultPayloadQueue::Frame(std::move(message), bytes, pool_, state_);
  ASSERT_TRUE(queue->tryPush(input).ValueOrDie());
  auto metadata = next(queue).ValueOrDie().flightPayload().ipc_message.metadata;
  ASSERT_NE(metadata, nullptr);
  ASSERT_TRUE(queue->seal().ok());
  EXPECT_FALSE(next(queue).ValueOrDie());
  state_->stopProducer();
  state_->revokeAndClearQueue();
  EXPECT_FALSE(state_->evidence().drained());
  metadata.reset();
  EXPECT_TRUE(state_->evidence().drained());
  EXPECT_EQ(queue->retainedBytes(), 0);
}

TEST_F(ResultPayloadQueueTest, ElapsedWaitAndCancellationNeverReturnEof) {
  auto queue = std::make_shared<ResultPayloadQueue>(1024);
  auto result = queue->next(std::chrono::steady_clock::now(), [] { return false; });
  EXPECT_TRUE(result.status().IsCancelled());
  auto pending = next(queue);
  EXPECT_TRUE(pending.status().IsCancelled());
  result = queue->next(std::chrono::steady_clock::now() + std::chrono::seconds(1),
      [] { return true; });
  EXPECT_TRUE(result.status().IsCancelled());
  ASSERT_TRUE(queue->seal().ok());
  EXPECT_FALSE(next(queue).ValueOrDie());
}

}  // namespace
}  // namespace pangrova::result::test
