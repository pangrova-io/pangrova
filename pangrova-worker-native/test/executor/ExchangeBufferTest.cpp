// Tests for ExchangeBuffer and TaskExecutor exchange integration.
// Validates per-source buffering, multi-source isolation, source
// completion, no-more-sources signaling, and thread-safety.
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "velox/common/memory/Memory.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/type/Type.h"

#include "executor/queue/ExchangeBuffer.h"
#include "executor/driver/TaskExecutor.h"

namespace pangrova::executor::test {
namespace {

using std::chrono::milliseconds;

// Shared memory pool for test RowVector construction.
static std::shared_ptr<facebook::velox::memory::MemoryPool> testPool() {
  static auto pool =
      facebook::velox::memory::MemoryManager::getInstance()->addLeafPool(
          "exchange_buffer_test");
  return pool;
}

// --- Helper: create a simple int64 RowVector with given value ---

facebook::velox::RowVectorPtr makeRowVector(int64_t value) {
  auto pool = testPool();
  auto rowType = facebook::velox::ROW({"v"}, {facebook::velox::BIGINT()});
  auto vector = facebook::velox::BaseVector::create(
      facebook::velox::BIGINT(), 1, pool.get());
  vector->asFlatVector<int64_t>()->set(0, value);
  return std::make_shared<facebook::velox::RowVector>(
      pool.get(), rowType, nullptr, 1,
      std::vector<facebook::velox::VectorPtr>{vector});
}

int64_t readValue(const facebook::velox::RowVectorPtr& row) {
  return row->childAt(0)->asFlatVector<int64_t>()->valueAt(0);
}

// ===== ExchangeBuffer unit tests =====

TEST(ExchangeBufferTest, AddAndPollSingleSource) {
  ExchangeBuffer buf(16);
  ASSERT_TRUE(buf.addBatch("src-0", makeRowVector(42)));
  EXPECT_EQ(buf.totalBuffered(), 1u);
  auto batch = buf.pollBatch(milliseconds(100));
  ASSERT_NE(batch, nullptr);
  EXPECT_EQ(readValue(batch), 42);
  EXPECT_EQ(buf.totalBuffered(), 0u);
}

TEST(ExchangeBufferTest, MultipleSources) {
  ExchangeBuffer buf(16);
  buf.addBatch("src-a", makeRowVector(1));
  buf.addBatch("src-b", makeRowVector(2));
  buf.addBatch("src-a", makeRowVector(3));

  EXPECT_EQ(buf.sourceCount(), 2u);
  EXPECT_EQ(buf.totalBuffered(), 3u);

  std::vector<int64_t> values;
  for (int i = 0; i < 3; ++i) {
    auto b = buf.pollBatch(milliseconds(100));
    ASSERT_NE(b, nullptr);
    values.push_back(readValue(b));
  }
  std::sort(values.begin(), values.end());
  EXPECT_EQ(values, (std::vector<int64_t>{1, 2, 3}));
}

TEST(ExchangeBufferTest, SourceCompletionRejectsNewBatches) {
  ExchangeBuffer buf(16);
  buf.addBatch("src-0", makeRowVector(1));
  buf.markSourceComplete("src-0");
  EXPECT_FALSE(buf.addBatch("src-0", makeRowVector(2)));
  EXPECT_EQ(buf.totalBuffered(), 1u);
}

TEST(ExchangeBufferTest, IsFinishedWhenAllDrained) {
  ExchangeBuffer buf(16);
  buf.addBatch("src-0", makeRowVector(1));
  buf.markSourceComplete("src-0");
  buf.markNoMoreSources();

  EXPECT_FALSE(buf.isFinished());  // Still has buffered data.
  buf.pollBatch(milliseconds(100));
  EXPECT_TRUE(buf.isFinished());
}

TEST(ExchangeBufferTest, PollReturnsNullOnTimeout) {
  ExchangeBuffer buf(16);
  auto batch = buf.pollBatch(milliseconds(10));
  EXPECT_EQ(batch, nullptr);
}

TEST(ExchangeBufferTest, BackpressureWhenFull) {
  ExchangeBuffer buf(2);
  EXPECT_TRUE(buf.addBatch("s", makeRowVector(1)));
  EXPECT_TRUE(buf.addBatch("s", makeRowVector(2)));
  EXPECT_FALSE(buf.addBatch("s", makeRowVector(3)));  // Full.
}

TEST(ExchangeBufferTest, CloseUnblocksWaiters) {
  ExchangeBuffer buf(16);
  std::thread t([&] {
    buf.pollBatch(milliseconds(5000));
  });
  std::this_thread::sleep_for(milliseconds(20));
  buf.close();
  t.join();
}

TEST(ExchangeBufferTest, AddAfterCloseReturnsFalse) {
  ExchangeBuffer buf(16);
  buf.close();
  EXPECT_FALSE(buf.addBatch("src-0", makeRowVector(1)));
}

// ===== TaskExecutor exchange integration tests =====

TaskExecutorConfig makeExchangeConfig() {
  TaskExecutorConfig config;
  config.task_id = "exchange-test";
  config.query_id = "q-1";
  config.queue_poll_timeout = milliseconds(100);
  return config;
}

TEST(TaskExecutorExchangeTest, AddExchangeBatchBuffers) {
  auto config = makeExchangeConfig();
  TaskExecutor exec(config);

  exec.addExchangeBatch("src-0", makeRowVector(10));
  exec.addExchangeBatch("src-0", makeRowVector(20));

  auto* buf = exec.exchangeBuffer();
  ASSERT_NE(buf, nullptr);
  EXPECT_EQ(buf->totalBuffered(), 2u);
}

TEST(TaskExecutorExchangeTest, MultipleSourcesIsolated) {
  auto config = makeExchangeConfig();
  TaskExecutor exec(config);

  exec.addExchangeBatch("src-a", makeRowVector(1));
  exec.addExchangeBatch("src-b", makeRowVector(2));

  auto* buf = exec.exchangeBuffer();
  EXPECT_EQ(buf->sourceCount(), 2u);
  EXPECT_EQ(buf->totalBuffered(), 2u);
}

TEST(TaskExecutorExchangeTest, NoMoreExchangeDataMarksComplete) {
  auto config = makeExchangeConfig();
  TaskExecutor exec(config);

  exec.addExchangeBatch("src-0", makeRowVector(1));
  exec.noMoreExchangeData("src-0");

  exec.addExchangeBatch("src-0", makeRowVector(2));
  EXPECT_EQ(exec.exchangeBuffer()->totalBuffered(), 1u);
}

TEST(TaskExecutorExchangeTest, NoMoreExchangeSourcesSignals) {
  auto config = makeExchangeConfig();
  TaskExecutor exec(config);

  exec.addExchangeBatch("src-0", makeRowVector(1));
  exec.noMoreExchangeData("src-0");
  exec.noMoreExchangeSources();

  auto* buf = exec.exchangeBuffer();
  EXPECT_FALSE(buf->isFinished());
  buf->pollBatch(milliseconds(50));
  EXPECT_TRUE(buf->isFinished());
}

TEST(TaskExecutorExchangeTest, SilentDropAfterTerminal) {
  auto config = makeExchangeConfig();
  TaskExecutor exec(config);
  exec.addExchangeBatch("src-0", makeRowVector(1));

  exec.cancel();
  exec.addExchangeBatch("src-0", makeRowVector(2));
  // Should not crash — silently dropped.
}

// ===== Thread-safety: concurrent add and poll =====

TEST(TaskExecutorExchangeTest, ConcurrentAddAndPoll) {
  ExchangeBuffer buf(256);
  constexpr int kBatchesPerSource = 100;
  constexpr int kNumSources = 4;

  std::vector<std::thread> producers;
  for (int s = 0; s < kNumSources; ++s) {
    producers.emplace_back([&buf, s] {
      std::string id = "src-" + std::to_string(s);
      for (int i = 0; i < kBatchesPerSource; ++i) {
        while (!buf.addBatch(id, makeRowVector(s * 1000 + i))) {
          std::this_thread::sleep_for(milliseconds(1));
        }
      }
      buf.markSourceComplete(id);
    });
  }

  int consumed = 0;
  std::thread consumer([&] {
    while (consumed < kBatchesPerSource * kNumSources) {
      auto b = buf.pollBatch(milliseconds(50));
      if (b) ++consumed;
    }
  });

  for (auto& t : producers) t.join();
  buf.markNoMoreSources();
  consumer.join();

  EXPECT_EQ(consumed, kBatchesPerSource * kNumSources);
  EXPECT_TRUE(buf.isFinished());
}

}  // namespace
}  // namespace pangrova::executor::test
