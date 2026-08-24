// Regression test for F-CHAIN-SHUFFLE-04 / F-NATIVE-SHUF-02 (tasks #101/#103):
// PartitionDemuxer::drainLoop fanned a BOUNDED SerializedShuffleQueue into an
// UNBOUNDED per-partition deque — backpressure was severed at the demux point,
// so a skewed partition could buffer without limit (OOM) and the upstream Velox
// producer never yielded.
//
// The fix gives each partition deque a configurable cap
// (setMaxBufferedBatchesPerPartition). When the cap is set, drainLoop blocks
// (credit-based backpressure) once a partition reaches the cap instead of
// pushing unconditionally; a consumer pop() frees a slot and wakes the drain
// thread. This test asserts the deque never exceeds the cap while many batches
// are staged, and that data still flows out fully in FIFO order. It is a
// deterministic non-gRPC test (no Arrow Flight) to avoid the two-Arrow ODR
// landmine in the standalone Flight test binary.

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

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

// Polls until the predicate holds or the deadline passes; returns the predicate
// result at exit.
template <typename Pred>
bool waitFor(Pred pred, std::chrono::milliseconds budget) {
  auto deadline = std::chrono::steady_clock::now() + budget;
  while (std::chrono::steady_clock::now() < deadline) {
    if (pred()) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  return pred();
}

TEST(FlightShuffleDemuxerBackpressureTest, BoundedDequeNeverExceedsCap) {
  constexpr int32_t kPartition = 0;
  constexpr size_t kCap = 2;
  constexpr int kStaged = 5;
  SerializedShuffleQueue queue(/*capacity=*/16);

  // Stage more batches than the cap, then close the source. With backpressure
  // the drain can fill only up to kCap until a consumer makes room.
  for (int i = 1; i <= kStaged; ++i) {
    ASSERT_TRUE(queue.push(makeBatch(kPartition, i), std::chrono::seconds(1)));
  }
  queue.close();

  PartitionDemuxer demuxer(&queue);
  demuxer.setMaxBufferedBatchesPerPartition(kCap);
  demuxer.start();

  // Drain fills to exactly the cap, then parks.
  ASSERT_TRUE(waitFor(
      [&] { return demuxer.bufferedBatchCount(kPartition) >= kCap; },
      std::chrono::seconds(2)));

  // Backpressure holds: depth pinned at the cap, never exceeds it, even though
  // kStaged batches are staged and the source is already closed.
  for (int i = 0; i < 30; ++i) {
    EXPECT_LE(demuxer.bufferedBatchCount(kPartition), kCap);
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  EXPECT_EQ(demuxer.bufferedBatchCount(kPartition), kCap)
      << "drain should hold exactly at the cap under backpressure";

  // Data still flows out fully, in FIFO order, with depth bounded throughout.
  for (int expected = 1; expected <= kStaged; ++expected) {
    ShuffleBatch out;
    ASSERT_TRUE(popWithin(demuxer, kPartition, out, std::chrono::seconds(2)));
    EXPECT_EQ(out.row_count, expected);
    EXPECT_LE(demuxer.bufferedBatchCount(kPartition), kCap);
  }

  // Fully drained.
  ShuffleBatch none;
  EXPECT_FALSE(popWithin(demuxer, kPartition, none, std::chrono::seconds(1)));
  EXPECT_TRUE(demuxer.isPartitionDrained(kPartition));
  demuxer.stop();
}

TEST(FlightShuffleDemuxerBackpressureTest, UnboundedDefaultBuffersEverything) {
  constexpr int32_t kPartition = 3;
  constexpr int kStaged = 5;
  SerializedShuffleQueue queue(/*capacity=*/16);

  for (int i = 1; i <= kStaged; ++i) {
    ASSERT_TRUE(queue.push(makeBatch(kPartition, i), std::chrono::seconds(1)));
  }
  queue.close();

  PartitionDemuxer demuxer(&queue);
  // No cap set (default 0 == unbounded): legacy behavior, drain buffers all.
  demuxer.start();

  ASSERT_TRUE(waitFor(
      [&] {
        return demuxer.bufferedBatchCount(kPartition) ==
               static_cast<size_t>(kStaged);
      },
      std::chrono::seconds(2)));
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

}  // namespace
}  // namespace pangrova::shuffle
