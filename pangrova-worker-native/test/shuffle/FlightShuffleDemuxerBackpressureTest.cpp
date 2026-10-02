// Retention is byte-bounded. Full resident credit spills rather than parking
// the source drain behind a partition that may have no consumer.

#include <gtest/gtest.h>

#include <chrono>
#include <future>

#include <folly/io/IOBuf.h>

#include "shuffle/FlightShuffleServer.h"
#include "shuffle/SerializedShuffleQueue.h"

namespace pangrova::shuffle {
namespace {

ShuffleBatch makeBatch(int32_t partition_id, int64_t tag) {
  ShuffleBatch b;
  b.partition_id = partition_id;
  b.row_count = tag;  // distinct tag so FIFO order/identity is checkable
  b.data = folly::IOBuf::copyBuffer(&tag, sizeof(tag));
  return b;
}

bool popWithin(PartitionDemuxer& d, int32_t pid, ShuffleBatch& out,
               std::chrono::milliseconds budget) {
  auto deadline = std::chrono::steady_clock::now() + budget;
  while (std::chrono::steady_clock::now() < deadline) {
    if (d.pop(pid, out, std::chrono::milliseconds(50))) {
      return true;
    }
    if (d.isPartitionDrained(pid)) {
      return false;
    }
  }
  return false;
}

TEST(FlightShuffleDemuxerBackpressureTest, BoundedResidentBytesPreserveFifoWithSpill) {
  constexpr int32_t kPartition = 0;
  constexpr int kStaged = 5;
  SerializedShuffleQueue queue(/*capacity=*/16);

  auto sample = makeBatch(kPartition, 0);
  ShuffleBufferLimits limits;
  limits.memory_bytes = sample.data->capacity() + sizeof(folly::IOBuf);
  auto store = std::make_shared<ShuffleBufferStore>(limits);
  for (int i = 1; i <= kStaged; ++i) {
    ASSERT_TRUE(queue.push(makeBatch(kPartition, i), std::chrono::seconds(1)));
  }
  ASSERT_TRUE(queue.commit());

  PartitionDemuxer demuxer(&queue, store);
  demuxer.start();

  ASSERT_TRUE(demuxer.quiesce(std::chrono::seconds(2)));
  EXPECT_EQ(demuxer.bufferedBatchCount(kPartition), kStaged);
  EXPECT_LE(store->usage().memory_bytes, limits.memory_bytes);
  EXPECT_EQ(store->usage().spill_files, kStaged - 1);

  for (int expected = 1; expected <= kStaged; ++expected) {
    ShuffleBatch out;
    ASSERT_TRUE(popWithin(demuxer, kPartition, out, std::chrono::seconds(2)));
    EXPECT_EQ(out.row_count, expected);
    EXPECT_LE(store->usage().memory_bytes, limits.memory_bytes);
  }

  // Fully drained.
  ShuffleBatch none;
  EXPECT_FALSE(popWithin(demuxer, kPartition, none, std::chrono::seconds(1)));
  EXPECT_TRUE(demuxer.isPartitionDrained(kPartition));
  EXPECT_EQ(store->usage().spill_files, 0u);
  EXPECT_EQ(store->usage().memory_bytes, 0u);
  demuxer.stop();
}

TEST(FlightShuffleDemuxerBackpressureTest, DefaultBudgetBuffersSmallInput) {
  constexpr int32_t kPartition = 3;
  constexpr int kStaged = 5;
  SerializedShuffleQueue queue(/*capacity=*/16);

  for (int i = 1; i <= kStaged; ++i) {
    ASSERT_TRUE(queue.push(makeBatch(kPartition, i), std::chrono::seconds(1)));
  }
  ASSERT_TRUE(queue.commit());

  PartitionDemuxer demuxer(&queue);
  demuxer.start();

  ASSERT_TRUE(demuxer.quiesce(std::chrono::seconds(2)));
  EXPECT_EQ(demuxer.bufferedBatchCount(kPartition),
            static_cast<size_t>(kStaged));

  for (int expected = 1; expected <= kStaged; ++expected) {
    ShuffleBatch out;
    ASSERT_TRUE(popWithin(demuxer, kPartition, out, std::chrono::seconds(2)));
    EXPECT_EQ(out.row_count, expected);
  }
  EXPECT_TRUE(demuxer.isPartitionDrained(kPartition));
  demuxer.stop();
}

TEST(FlightShuffleDemuxerBackpressureTest, FullRetentionDrainsWithIndependentTransitCredit) {
  using namespace std::chrono_literals;
  SerializedShuffleQueue queue(4);
  auto sample = makeBatch(0, 1);
  ShuffleBufferLimits limits;
  limits.memory_bytes = sample.data->capacity() + sizeof(folly::IOBuf);
  limits.transit_bytes = 3 * sizeof(int64_t) + 64 * 1024;
  auto store = std::make_shared<ShuffleBufferStore>(limits);
  ASSERT_TRUE(queue.push(std::move(sample), 1s));
  ASSERT_TRUE(queue.push(makeBatch(0, 2), 1s));
  ASSERT_TRUE(queue.commit());
  PartitionDemuxer demuxer(&queue, store);
  demuxer.start();
  ASSERT_TRUE(demuxer.quiesce(2s));
  ASSERT_GT(store->usage().memory_bytes, 0u);
  ASSERT_GT(store->usage().spill_bytes, 0u);
  ShuffleBatch first;
  ASSERT_TRUE(demuxer.pop(0, first, 1s));
  EXPECT_EQ(first.row_count, 1);
  EXPECT_EQ(store->usage().transit_bytes, limits.transit_bytes);
  auto second = std::async(std::launch::async, [&] {
    ShuffleBatch result;
    const bool popped = demuxer.pop(0, result, 1s);
    return popped ? result.row_count : -1;
  });
  EXPECT_EQ(second.wait_for(50ms), std::future_status::timeout);
  first = ShuffleBatch{};
  ASSERT_EQ(second.wait_for(1s), std::future_status::ready);
  EXPECT_EQ(second.get(), 2);
  EXPECT_EQ(store->usage().transit_bytes, 0u);
  demuxer.stop();
}

TEST(FlightShuffleDemuxerBackpressureTest, CancelledReaderReturnsUnreplayedEntry) {
  using namespace std::chrono_literals;
  SerializedShuffleQueue queue(4);
  ShuffleBufferLimits limits;
  limits.transit_bytes = 3 * sizeof(int64_t) + 64 * 1024;
  auto store = std::make_shared<ShuffleBufferStore>(limits);
  ASSERT_TRUE(queue.push(makeBatch(0, 1), 1s));
  ASSERT_TRUE(queue.push(makeBatch(0, 2), 1s));
  ASSERT_TRUE(queue.commit());
  PartitionDemuxer demuxer(&queue, store);
  demuxer.start();
  ASSERT_TRUE(demuxer.quiesce(2s));
  ShuffleBatch first;
  ASSERT_TRUE(demuxer.pop(0, first, 1s));
  std::atomic<bool> cancelled{false};
  auto abandoned = std::async(std::launch::async, [&] {
    ShuffleBatch ignored;
    return demuxer.pop(0, ignored, 1s, [&] { return cancelled.load(); });
  });
  EXPECT_EQ(abandoned.wait_for(50ms), std::future_status::timeout);
  cancelled.store(true);
  ASSERT_EQ(abandoned.wait_for(1s), std::future_status::ready);
  EXPECT_FALSE(abandoned.get());
  EXPECT_TRUE(demuxer.status().ok());
  EXPECT_EQ(demuxer.bufferedBatchCount(0), 1u);
  first = ShuffleBatch{};
  ShuffleBatch resumed;
  ASSERT_TRUE(demuxer.pop(0, resumed, 1s));
  EXPECT_EQ(resumed.row_count, 2);
  demuxer.stop();
}

}  // namespace
}  // namespace pangrova::shuffle
