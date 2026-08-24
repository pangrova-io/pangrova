// Regression test for F-CHAIN-SHUFFLE-03 (#100): an end-to-end completeness
// contract so a TRUNCATED / aborted shuffle read is never silently treated as a
// clean EOF (which would drop committed rows or livelock the consumer).
//
// The demuxer now distinguishes two terminal states:
//   * clean completion  — the producer closed the source queue (PopResult::
//     CLOSED). isComplete()==true; after full drain the partition is NOT
//     truncated; committedBatchCount == number of dispatched batches.
//   * forced teardown    — stop() was called before the producer finished.
//     isComplete()==false; a partition that drains to empty reports
//     isPartitionTruncated()==true so the reader surfaces an error.
//
// Deterministic, non-gRPC (no Arrow Flight) to avoid the two-Arrow ODR landmine
// in the standalone Flight test binary — it exercises the PartitionDemuxer
// classification directly, which is the source of truth the Flight reader
// consults.

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

template <typename Pred>
bool waitFor(Pred pred, std::chrono::milliseconds budget) {
  auto deadline = std::chrono::steady_clock::now() + budget;
  while (std::chrono::steady_clock::now() < deadline) {
    if (pred()) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  return pred();
}

// Clean producer close: the partition drains to a real EOF, never reported as
// truncated, and committedBatchCount equals the number of dispatched batches.
TEST(FlightShuffleDemuxerCompletenessTest, CleanCloseIsCompleteNotTruncated) {
  constexpr int32_t kPartition = 0;
  constexpr int kStaged = 4;
  SerializedShuffleQueue queue(/*capacity=*/16);

  for (int i = 1; i <= kStaged; ++i) {
    ASSERT_TRUE(queue.push(makeBatch(kPartition, i), std::chrono::seconds(1)));
  }
  queue.close();  // clean producer completion

  PartitionDemuxer demuxer(&queue);
  demuxer.start();

  // Drain dispatches all staged batches and observes the clean close.
  ASSERT_TRUE(waitFor(
      [&] {
        return demuxer.isComplete() &&
               demuxer.committedBatchCount(kPartition) ==
                   static_cast<int64_t>(kStaged);
      },
      std::chrono::seconds(2)));

  // Consume every batch in FIFO order.
  for (int expected = 1; expected <= kStaged; ++expected) {
    ShuffleBatch out;
    ASSERT_TRUE(demuxer.pop(kPartition, out, std::chrono::seconds(1)));
    EXPECT_EQ(out.row_count, expected);
  }

  // Drained AND complete => clean EOF, NOT a truncation.
  EXPECT_TRUE(demuxer.isPartitionDrained(kPartition));
  EXPECT_TRUE(demuxer.isComplete());
  EXPECT_FALSE(demuxer.isPartitionTruncated(kPartition));
  EXPECT_EQ(demuxer.committedBatchCount(kPartition),
            static_cast<int64_t>(kStaged));

  demuxer.stop();
}

// Forced teardown before the producer signals completion: a drained partition is
// reported as truncated so the Flight reader raises an error instead of a clean
// EOF. The source queue is intentionally NEVER closed by the producer.
TEST(FlightShuffleDemuxerCompletenessTest, ForcedStopMarksTruncated) {
  constexpr int32_t kPartition = 7;
  SerializedShuffleQueue queue(/*capacity=*/16);

  // Stage and dispatch one batch, but the producer never closes the queue.
  ASSERT_TRUE(queue.push(makeBatch(kPartition, 1), std::chrono::seconds(1)));

  PartitionDemuxer demuxer(&queue);
  demuxer.start();

  // Wait until the staged batch is dispatched into the partition buffer.
  ASSERT_TRUE(waitFor(
      [&] { return demuxer.bufferedBatchCount(kPartition) == 1; },
      std::chrono::seconds(2)));

  // Consume it, then force teardown WITHOUT a producer close.
  ShuffleBatch out;
  ASSERT_TRUE(demuxer.pop(kPartition, out, std::chrono::seconds(1)));
  EXPECT_EQ(out.row_count, 1);

  demuxer.stop();  // forced teardown: sets source_closed_ but NOT source_complete_

  // Drained but never completed => truncation, not a clean EOF.
  EXPECT_TRUE(demuxer.isPartitionDrained(kPartition));
  EXPECT_FALSE(demuxer.isComplete());
  EXPECT_TRUE(demuxer.isPartitionTruncated(kPartition));
}

// An unknown partition under a forced (incomplete) teardown is treated as
// truncated — a reader that opened a stream for a never-populated partition must
// not see a clean EOF when the producer never signaled completion.
TEST(FlightShuffleDemuxerCompletenessTest, UnknownPartitionTruncatedOnForcedStop) {
  SerializedShuffleQueue queue(/*capacity=*/4);
  PartitionDemuxer demuxer(&queue);
  demuxer.start();
  demuxer.stop();  // forced teardown, producer never closed

  EXPECT_FALSE(demuxer.isComplete());
  EXPECT_TRUE(demuxer.isPartitionTruncated(/*partition_id=*/42));
}

}  // namespace
}  // namespace pangrova::shuffle
