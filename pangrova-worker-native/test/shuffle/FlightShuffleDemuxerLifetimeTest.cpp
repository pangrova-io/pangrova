// Regression test for F-CHAIN-SHUFFLE-02 (task #99): in-flight DoGet readers
// held a RAW PartitionDemuxer* while unregisterHandle() destroyed the demuxer
// (it was owned by a unique_ptr in the routing map), producing a dangling
// pointer / use-after-free whenever a handle was unregistered while a stream
// was still being read.
//
// The fix gives the demuxer SHARED ownership: the routing map holds a
// shared_ptr and every in-flight reader copies one, so the object outlives a
// concurrent unregister and is freed only by its last holder. This test
// exercises that lifetime contract directly at the PartitionDemuxer level
// (no gRPC / Arrow Flight, so it is deterministic and free of the two-Arrow
// ODR landmine that afflicts the standalone Flight test binary): a reader's
// shared_ptr must keep the demuxer alive and serving its already-buffered
// batches after the routing owner has stopped it and dropped its reference.

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <thread>

#include <folly/io/IOBuf.h>

#include "shuffle/FlightShuffleServer.h"
#include "shuffle/SerializedShuffleQueue.h"

namespace pangrova::shuffle {
namespace {

ShuffleBatch makeBatch(int32_t partition_id, int64_t tag) {
  ShuffleBatch b;
  b.partition_id = partition_id;
  b.row_count = tag;  // distinct tag so ordering/identity is checkable
  b.data = folly::IOBuf::copyBuffer(&tag, sizeof(tag));
  return b;
}

// Drives a demuxer pop with a generous deadline so the background drain thread
// has time to move a source batch into the per-partition buffer.
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

TEST(FlightShuffleDemuxerLifetimeTest, ReaderSharedPtrOutlivesUnregister) {
  constexpr int32_t kPartition = 0;
  SerializedShuffleQueue queue(/*capacity=*/16);

  // Stage three batches and close the source: the drain thread will move all
  // three into the partition buffer and then mark the source closed.
  ASSERT_TRUE(queue.push(makeBatch(kPartition, 100), std::chrono::seconds(1)));
  ASSERT_TRUE(queue.push(makeBatch(kPartition, 200), std::chrono::seconds(1)));
  ASSERT_TRUE(queue.push(makeBatch(kPartition, 300), std::chrono::seconds(1)));
  queue.close();

  auto demuxer = std::make_shared<PartitionDemuxer>(&queue);
  demuxer->start();

  // A reader (simulating an in-flight DoGet stream) takes its own shared_ptr.
  std::shared_ptr<PartitionDemuxer> reader_ref = demuxer;

  // Consume the first batch through the routing owner to prove the flow works.
  ShuffleBatch first;
  ASSERT_TRUE(popWithin(*demuxer, kPartition, first, std::chrono::seconds(2)));
  EXPECT_EQ(first.row_count, 100);

  // Simulate unregisterHandle(): stop the demuxer and drop the routing owner's
  // reference. With the old unique_ptr ownership this freed the object while
  // reader_ref's raw pointer still pointed at it (UAF). Now reader_ref keeps it
  // alive.
  demuxer->stop();
  demuxer.reset();

  ASSERT_NE(reader_ref, nullptr);
  EXPECT_EQ(reader_ref.use_count(), 1)
      << "reader holds the sole surviving reference after unregister";

  // The reader must still drain the batches buffered before unregister — no
  // crash, no data loss, FIFO order preserved.
  ShuffleBatch second;
  ASSERT_TRUE(popWithin(*reader_ref, kPartition, second, std::chrono::seconds(2)));
  EXPECT_EQ(second.row_count, 200);

  ShuffleBatch third;
  ASSERT_TRUE(popWithin(*reader_ref, kPartition, third, std::chrono::seconds(2)));
  EXPECT_EQ(third.row_count, 300);

  // Drained: further pops report end-of-stream, and the partition is drained.
  ShuffleBatch none;
  EXPECT_FALSE(popWithin(*reader_ref, kPartition, none, std::chrono::seconds(1)));
  EXPECT_TRUE(reader_ref->isPartitionDrained(kPartition));

  // Releasing the last reference frees the demuxer here, with no in-flight
  // reader — clean teardown.
  reader_ref.reset();
}

}  // namespace
}  // namespace pangrova::shuffle
