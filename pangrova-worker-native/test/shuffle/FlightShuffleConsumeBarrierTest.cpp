// Regression test for F-CHAIN-SHUFFLE-01 (#98): the shuffle server/demuxer
// lifecycle was bound to PRODUCER completion, not downstream CONSUMPTION. When a
// producer task finished, unregisterHandle() immediately stopped+destroyed the
// demuxer — so a downstream consumer that had not yet read (or not yet even
// opened) its stream lost the committed data, and an un-closed source could be
// destroyed out from under a still-running drain thread (UAF).
//
// The fix adds a consume-then-release barrier:
//   * unregisterHandle() first quiesce()s — a bounded wait for a clean producer
//     close so every committed batch lands in the per-partition buffers and the
//     drain thread exits (after which it never touches source_).
//   * a producer-finished-but-unconsumed demuxer is moved to a lingering set
//     (still routable) instead of being destroyed; it is reaped only once fully
//     consumed.
//   * an un-clean (cancel/abort) demuxer is force-stopped so the source queue is
//     safe to destroy, and the truncated read surfaces (see #100).
//
// Deterministic, non-gRPC (no Arrow Flight DoGet) to avoid the two-Arrow ODR
// landmine in the standalone Flight test binary: it exercises the demuxer
// quiesce primitives directly and the server's lingering/reap bookkeeping via
// popForHandle() (which mirrors what a DoGet reader does internally).

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

// Demuxer-level: after a clean producer close, quiesce() reports completion and
// ALL committed data remains readable by a consumer that reads AFTER quiesce —
// i.e. producer completion does not destroy unconsumed committed data.
TEST(FlightShuffleConsumeBarrierTest, QuiesceCleanCloseKeepsDataFullyReadable) {
  constexpr int32_t kPartition = 0;
  constexpr int kStaged = 3;
  SerializedShuffleQueue queue(/*capacity=*/16);
  for (int i = 1; i <= kStaged; ++i) {
    ASSERT_TRUE(queue.push(makeBatch(kPartition, i), std::chrono::seconds(1)));
  }
  queue.close();  // clean producer completion

  PartitionDemuxer demuxer(&queue);
  demuxer.start();

  // Quiesce returns true on a clean close; drain thread has exited.
  ASSERT_TRUE(demuxer.quiesce(std::chrono::seconds(2)));
  EXPECT_TRUE(demuxer.isComplete());
  // Producer done, but nothing consumed yet -> NOT fully consumed (the barrier
  // must keep this data, not drop it).
  EXPECT_FALSE(demuxer.isFullyConsumed());

  // A late consumer reads every committed batch in FIFO order.
  for (int expected = 1; expected <= kStaged; ++expected) {
    ShuffleBatch out;
    ASSERT_TRUE(demuxer.pop(kPartition, out, std::chrono::seconds(1)));
    EXPECT_EQ(out.row_count, expected);
  }
  // Now fully consumed -> safe to release.
  EXPECT_TRUE(demuxer.isFullyConsumed());
  demuxer.stop();
}

// Demuxer-level: a producer that never cleanly closes (cancel/abort) makes
// quiesce() fall back to a forced stop within the timeout, so the source queue
// is safe to destroy and the read is surfaced as truncated (not a clean EOF).
TEST(FlightShuffleConsumeBarrierTest, QuiesceForcedStopOnUncleanProducer) {
  constexpr int32_t kPartition = 5;
  SerializedShuffleQueue queue(/*capacity=*/16);
  ASSERT_TRUE(queue.push(makeBatch(kPartition, 1), std::chrono::seconds(1)));
  // Intentionally DO NOT close the queue (simulates cancel/abort).

  PartitionDemuxer demuxer(&queue);
  demuxer.start();

  // Bounded quiesce cannot observe a clean close -> forces teardown, returns
  // false. After this the drain thread has exited (source_ no longer touched).
  EXPECT_FALSE(demuxer.quiesce(std::chrono::milliseconds(200)));
  EXPECT_FALSE(demuxer.isComplete());

  // The one buffered batch is still drainable; once empty the partition reports
  // truncation rather than a clean EOF.
  ShuffleBatch out;
  if (demuxer.pop(kPartition, out, std::chrono::milliseconds(200))) {
    EXPECT_EQ(out.row_count, 1);
  }
  EXPECT_TRUE(demuxer.isPartitionDrained(kPartition));
  EXPECT_TRUE(demuxer.isPartitionTruncated(kPartition));
}

// Server-level: unregistering a producer-finished handle whose data has NOT been
// consumed must LINGER (retain + stay routable) rather than destroy the data; a
// later consumer drains it via the same handle, after which a sweep reaps it.
TEST(FlightShuffleConsumeBarrierTest, ServerUnregisterLingersThenReapsAfterConsume) {
  constexpr int64_t kHandle = 4242;
  constexpr int32_t kPartition = 0;
  constexpr int kStaged = 2;

  // The server only manages the routing map here — we never call start(), so no
  // gRPC server is bound (avoids the DoGet ODR landmine).
  FlightShuffleServerConfig config;
  FlightShuffleServer server(config);

  SerializedShuffleQueue queue(/*capacity=*/16);
  for (int i = 1; i <= kStaged; ++i) {
    ASSERT_TRUE(queue.push(makeBatch(kPartition, i), std::chrono::seconds(1)));
  }
  queue.close();  // clean producer completion

  server.registerHandle(kHandle, &queue);
  EXPECT_EQ(server.handleCount(), 1u);
  EXPECT_EQ(server.lingeringHandleCount(), 0u);

  // Producer task released before any consumer read: unregister must quiesce
  // (clean close) and LINGER the unconsumed committed data instead of dropping
  // it.
  server.unregisterHandle(kHandle);
  EXPECT_EQ(server.handleCount(), 0u);
  EXPECT_EQ(server.lingeringHandleCount(), 1u)
      << "producer-done but unconsumed data must linger, not be destroyed";

  // A late consumer still routes to the handle and reads every committed batch.
  for (int expected = 1; expected <= kStaged; ++expected) {
    ShuffleBatch out;
    ASSERT_TRUE(
        server.popForHandle(kHandle, kPartition, out, std::chrono::seconds(1)));
    EXPECT_EQ(out.row_count, expected);
  }

  // Once fully consumed, a sweep (any (un)register call) reaps the lingering
  // demuxer. unregisterHandle on an already-lingering handle triggers the sweep.
  server.unregisterHandle(kHandle);
  EXPECT_TRUE(waitFor([&] { return server.lingeringHandleCount() == 0u; },
                      std::chrono::seconds(1)))
      << "fully consumed lingering demuxer must be reaped";
}

// Server-level: an un-clean producer (queue never closed) must NOT linger — the
// source queue is about to be destroyed, so unregister force-stops and drops it.
TEST(FlightShuffleConsumeBarrierTest, ServerUnregisterDropsUncleanProducer) {
  constexpr int64_t kHandle = 7;
  constexpr int32_t kPartition = 0;

  FlightShuffleServerConfig config;
  FlightShuffleServer server(config);

  SerializedShuffleQueue queue(/*capacity=*/16);
  ASSERT_TRUE(queue.push(makeBatch(kPartition, 1), std::chrono::seconds(1)));
  // Never close the queue (cancel/abort).

  server.registerHandle(kHandle, &queue);
  EXPECT_EQ(server.handleCount(), 1u);

  // unregister cannot observe a clean close -> force-stops, drops (no linger),
  // so the source queue below can be safely destroyed without a UAF.
  server.unregisterHandle(kHandle);
  EXPECT_EQ(server.handleCount(), 0u);
  EXPECT_EQ(server.lingeringHandleCount(), 0u)
      << "un-clean producer must not linger (source queue about to be freed)";
}

}  // namespace
}  // namespace pangrova::shuffle
