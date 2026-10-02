// Regression test for F-CHAIN-SHUFFLE-09 (#53): partition-id boundary check.
//
// Before the fix, getOrCreatePartition(partition_id) silently created a buffer
// for ANY partition_id — an out-of-range partition from a buggy producer would
// write data that no consumer ever reads. Invalid producer partitions must
// fail the shuffle, not skip data and report successful completion.
//
// This is a deterministic non-gRPC test (no Arrow Flight) to avoid the
// two-Arrow ODR landmine in the standalone Flight test binary.

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
  b.row_count = tag;
  b.data = folly::IOBuf::copyBuffer(&tag, sizeof(tag));
  return b;
}

// Valid partition_id within bounds works normally.
TEST(FlightShuffleDemuxerPartitionBoundTest, InBoundsPartitionWorks) {
  constexpr int32_t kNumPartitions = 4;
  SerializedShuffleQueue queue(/*capacity=*/16);

  // Push batches to partitions 0, 1, 2, 3 (all in bounds).
  for (int32_t p = 0; p < kNumPartitions; ++p) {
    ASSERT_TRUE(queue.push(makeBatch(p, p + 100), std::chrono::seconds(1)));
  }
  ASSERT_TRUE(queue.commit());

  PartitionDemuxer demuxer(&queue);
  demuxer.setMaxPartitions(kNumPartitions);
  demuxer.start();

  // All 4 partitions should be readable.
  for (int32_t p = 0; p < kNumPartitions; ++p) {
    ShuffleBatch out;
    // Wait for drain loop to dispatch.
    bool got = false;
    for (int attempt = 0; attempt < 20 && !got; ++attempt) {
      got = demuxer.pop(p, out, std::chrono::milliseconds(50));
    }
    ASSERT_TRUE(got) << "partition=" << p;
    EXPECT_EQ(out.row_count, p + 100);
  }

  demuxer.stop();
}

// pop() on out-of-range partition_id returns false (no crash, hard rejection).
TEST(FlightShuffleDemuxerPartitionBoundTest, PopOutOfRangeReturnsFalse) {
  constexpr int32_t kNumPartitions = 4;
  SerializedShuffleQueue queue(/*capacity=*/16);
  ASSERT_TRUE(queue.commit());  // No data, just test validation.

  PartitionDemuxer demuxer(&queue);
  demuxer.setMaxPartitions(kNumPartitions);
  demuxer.start();

  ShuffleBatch out;
  // partition_id == kNumPartitions is out of [0, kNumPartitions).
  EXPECT_FALSE(demuxer.pop(kNumPartitions, out, std::chrono::milliseconds(10)));
  // Negative partition_id.
  EXPECT_FALSE(demuxer.pop(-1, out, std::chrono::milliseconds(10)));
  // Way out of range.
  EXPECT_FALSE(demuxer.pop(999, out, std::chrono::milliseconds(10)));

  demuxer.stop();
}

// A corrupt partition identifier invalidates the whole shuffle.
TEST(FlightShuffleDemuxerPartitionBoundTest,
  DrainLoopFailsOutOfRangePartition) {
  constexpr int32_t kNumPartitions = 4;
  SerializedShuffleQueue queue(/*capacity=*/16);

  // Push a valid batch followed by an out-of-range batch.
  ASSERT_TRUE(queue.push(makeBatch(5, 999), std::chrono::seconds(1)));
  ASSERT_TRUE(queue.push(makeBatch(2, 200), std::chrono::seconds(1)));
  ASSERT_TRUE(queue.commit());

  PartitionDemuxer demuxer(&queue);
  demuxer.setMaxPartitions(kNumPartitions);
  demuxer.start();

  EXPECT_FALSE(demuxer.quiesce(std::chrono::seconds(2)));
  EXPECT_TRUE(demuxer.status().IsInvalid());
  EXPECT_FALSE(demuxer.isComplete());
  ShuffleBatch out;
  EXPECT_FALSE(demuxer.pop(2, out, std::chrono::milliseconds(50)));
  EXPECT_FALSE(demuxer.pop(5, out, std::chrono::milliseconds(50)));

  demuxer.stop();
}

TEST(FlightShuffleDemuxerPartitionBoundTest,
     DemuxFailurePreventsLateProducerCommit) {
  SerializedShuffleQueue queue(/*capacity=*/2);
  PartitionDemuxer demuxer(&queue);
  demuxer.setMaxPartitions(1);
  demuxer.start();
  ASSERT_TRUE(queue.push(makeBatch(1, 9), std::chrono::seconds(1)));

  for (int attempt = 0; attempt < 100 && demuxer.status().ok(); ++attempt) {
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  ASSERT_TRUE(demuxer.status().IsInvalid());
  demuxer.stop();
  EXPECT_TRUE(queue.isAborted());
  EXPECT_FALSE(queue.commit());
  EXPECT_EQ(queue.usedBytes(), 0u);
}

TEST(FlightShuffleDemuxerPartitionBoundTest, DefaultPartitionRangeIsFinite) {
  SerializedShuffleQueue queue(/*capacity=*/16);
  ASSERT_TRUE(queue.push(makeBatch(100, 42), std::chrono::seconds(1)));
  ASSERT_TRUE(queue.commit());

  PartitionDemuxer demuxer(&queue);
  EXPECT_TRUE(demuxer.validPartition(100));
  EXPECT_FALSE(demuxer.validPartition(4096));
  EXPECT_THROW(demuxer.setMaxPartitions(0), std::invalid_argument);
  demuxer.start();

  ShuffleBatch out;
  bool got = false;
  for (int attempt = 0; attempt < 20 && !got; ++attempt) {
    got = demuxer.pop(100, out, std::chrono::milliseconds(50));
  }
  ASSERT_TRUE(got);
  EXPECT_EQ(out.row_count, 42);

  demuxer.stop();
}

// registerHandle with num_partitions propagates to demuxer.
TEST(FlightShuffleDemuxerPartitionBoundTest,
     RegisterHandleSetsMaxPartitions) {
  SerializedShuffleQueue queue(/*capacity=*/16);
  ASSERT_TRUE(queue.commit());

  FlightShuffleServerConfig config;
  config.port = 0;
  FlightShuffleServer server(config);
  // Don't start the Flight server (avoid gRPC/two-Arrow ODR). Just test
  // the handle registration path and the demuxer's partition bound.
  server.registerHandle(42, &queue, "q1", 0, /*num_partitions=*/8,
                        1, 0, 1);

  // Verify via popForHandle: valid partition returns false (empty, not crash).
  ShuffleBatch out;
  EXPECT_FALSE(
      server.popForHandle(42, 0, out, std::chrono::milliseconds(10)));
  // Out-of-range partition on read side also returns false (not crash).
  EXPECT_FALSE(
      server.popForHandle(42, 8, out, std::chrono::milliseconds(10)));
  EXPECT_FALSE(
      server.popForHandle(42, -1, out, std::chrono::milliseconds(10)));

  server.unregisterHandle(42);
}

}  // namespace
}  // namespace pangrova::shuffle
