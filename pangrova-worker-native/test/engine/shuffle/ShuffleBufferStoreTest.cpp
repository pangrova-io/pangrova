#include "engine/shuffle/ShuffleResourceTestSupport.h"

#include <fcntl.h>
#include <future>
#include <stdexcept>
#include <unistd.h>
#include <vector>

namespace pangrova::shuffle {
namespace {

using test::taggedBatch;

TEST(ShuffleBufferStoreTest, LocalProducerSpillsWithAvailableMemoryAndReleasesItsFileAfterRead) {
  ShuffleBufferLimits limits;
  auto store = std::make_shared<ShuffleBufferStore>(limits);
  ShuffleBufferStore::HandleScope local(store, true);
  ShuffleBufferStore::HandleScope direct(store);
  auto disk = store->store(taggedBatch(0, 1), local);
  ASSERT_TRUE(disk.ok()) << disk.status().ToString();
  EXPECT_EQ(store->usage().memory_bytes, 0u);
  EXPECT_EQ(store->usage().spill_files, 1u);
  EXPECT_GT(store->usage().spill_bytes, 0u);
  auto memory = store->store(taggedBatch(1, 2), direct);
  ASSERT_TRUE(memory.ok());
  EXPECT_GT(store->usage().memory_bytes, 0u);
  EXPECT_TRUE(store->replay(*disk.ValueOrDie()).ok());
  disk.ValueOrDie().reset();
  EXPECT_EQ(store->usage().spill_files, 0u);
  EXPECT_EQ(store->usage().spill_bytes, 0u);
  memory.ValueOrDie().reset();
  EXPECT_EQ(store->usage().memory_bytes, 0u);
}

TEST(ShuffleBufferStoreTest, ChainedPayloadReplaysExactlyAtSizeLimit) {
  test::SpillDirectory directory;
  ShuffleBufferLimits limits;
  limits.memory_bytes = 0;
  limits.max_payload_bytes = 64;
  limits.spill_directory = directory.path();
  auto store = std::make_shared<ShuffleBufferStore>(limits);
  ShuffleBufferStore::HandleScope scope(store);
  auto batch = taggedBatch(2, 4, 32);
  batch.data->appendChain(folly::IOBuf::copyBuffer(std::string(32, 'z')));
  auto entry = store->store(std::move(batch), scope);
  ASSERT_TRUE(entry.ok()) << entry.status().ToString();
  EXPECT_EQ(store->usage().spill_files, 1u);
  EXPECT_TRUE(std::filesystem::is_empty(directory.path()));
  auto replayed = store->replay(**entry);
  ASSERT_TRUE(replayed.ok());
  EXPECT_EQ(replayed->partition_id, 2);
  EXPECT_EQ(replayed->row_count, 4);
  EXPECT_EQ(replayed->data->to<std::string>(), std::string(32, 'e') + std::string(32, 'z'));
  entry.ValueOrDie().reset();
  EXPECT_EQ(store->usage().spill_bytes, 0u);
}

TEST(ShuffleBufferStoreTest, LargeBackingAllocationIsNotChargedAsSmallPayload) {
  ShuffleBufferLimits limits;
  limits.max_payload_bytes = 64;
  limits.memory_bytes = 1024;
  auto store = std::make_shared<ShuffleBufferStore>(limits);
  ShuffleBufferStore::HandleScope scope(store);
  auto data = folly::IOBuf::create(1024 * 1024);
  data->writableData()[0] = 'x';
  data->append(1);
  auto entry = store->store(ShuffleBatch{0, std::move(data), 1}, scope);
  ASSERT_TRUE(entry.ok());
  EXPECT_EQ(store->usage().memory_bytes, 0u);
  EXPECT_EQ(store->usage().spill_files, 1u);
  auto replayed = store->replay(**entry);
  ASSERT_TRUE(replayed.ok());
  EXPECT_EQ(replayed->data->to<std::string>(), "x");
}

TEST(ShuffleBufferStoreTest, RecordAndFileLimitsAreFiniteAndReusable) {
  ShuffleBufferLimits limits;
  limits.memory_bytes = 0;
  limits.buffered_batches = 1;
  limits.spill_files = 1;
  limits.max_payload_bytes = 64;
  limits.spill_segment_bytes = 4096;
  // Fail-fast mode: this asserts the cap is enforced and credit is reusable,
  // not that an exhausted budget waits for a reader.
  limits.retention_wait_ms = 0;
  auto store = std::make_shared<ShuffleBufferStore>(limits);
  ShuffleBufferStore::HandleScope scope(store);
  auto entry = store->store(taggedBatch(0, 1), scope);
  ASSERT_TRUE(entry.ok());
  EXPECT_TRUE(store->store(taggedBatch(1, 2), scope).status().IsCapacityError());
  entry.ValueOrDie().reset();
  EXPECT_TRUE(store->store(taggedBatch(1, 2), scope).ok());
  EXPECT_EQ(store->usage().buffered_batches, 0u);
  limits.buffered_batches = 2;
  store = std::make_shared<ShuffleBufferStore>(limits);
  scope = ShuffleBufferStore::HandleScope(store);
  entry = store->store(taggedBatch(0, 1), scope);
  ASSERT_TRUE(entry.ok());
  auto denied = store->store(taggedBatch(1, 2), scope);
  EXPECT_TRUE(denied.status().IsCapacityError());
  EXPECT_NE(denied.status().ToString().find("spill file budget exhausted"),
            std::string::npos);
}

TEST(ShuffleBufferStoreTest, ManySmallBatchesShareOneFileAndReleaseIndependentCredits) {
  test::SpillDirectory directory;
  ShuffleBufferLimits limits;
  limits.memory_bytes = 0;
  limits.spill_files = 1;
  limits.spill_directory = directory.path();
  auto store = std::make_shared<ShuffleBufferStore>(limits);
  ShuffleBufferStore::HandleScope scope(store);
  std::vector<std::unique_ptr<ShuffleBufferStore::Entry>> entries;
  for (int i = 0; i < 1000; ++i) {
    auto stored = store->store(taggedBatch(0, i), scope);
    ASSERT_TRUE(stored.ok()) << stored.status().ToString();
    entries.push_back(std::move(*stored));
  }
  EXPECT_EQ(store->usage().spill_files, 1u);
  EXPECT_EQ(store->usage().buffered_batches, 1000u);
  EXPECT_EQ(store->usage().spill_bytes, 1000u * 4096);
  EXPECT_TRUE(std::filesystem::is_empty(directory.path()));
  for (int i : {0, 500, 999}) {
    auto replayed = store->replay(*entries[i]);
    ASSERT_TRUE(replayed.ok()) << replayed.status().ToString();
    EXPECT_EQ(replayed->row_count, i);
  }
  entries.erase(entries.begin(), entries.begin() + 500);
  EXPECT_EQ(store->usage().buffered_batches, 500u);
  EXPECT_EQ(store->usage().spill_files, 1u);
  EXPECT_EQ(store->usage().spill_bytes, 1000u * 4096);
  entries.clear();
  EXPECT_EQ(store->usage().spill_bytes, 0u);
  EXPECT_EQ(store->usage().spill_files, 0u);
}

TEST(ShuffleBufferStoreTest, SegmentRolloverKeepsFileCapAndReusesReleasedSlot) {
  ShuffleBufferLimits limits;
  limits.memory_bytes = 0;
  limits.max_payload_bytes = 64;
  limits.spill_segment_bytes = 4096;
  limits.spill_files = 2;
  // Fail-fast mode: asserts the descriptor cap and slot reuse, not waiting.
  limits.retention_wait_ms = 0;
  auto store = std::make_shared<ShuffleBufferStore>(limits);
  ShuffleBufferStore::HandleScope scope(store);
  auto first = store->store(taggedBatch(0, 1), scope);
  auto second = store->store(taggedBatch(0, 2), scope);
  ASSERT_TRUE(first.ok() && second.ok());
  EXPECT_EQ(store->usage().spill_files, 2u);
  EXPECT_TRUE(store->store(taggedBatch(0, 3), scope).status().IsCapacityError());
  first.ValueOrDie().reset();
  EXPECT_EQ(store->usage().spill_files, 1u);
  auto third = store->store(taggedBatch(0, 3), scope);
  ASSERT_TRUE(third.ok()) << third.status().ToString();
  EXPECT_EQ(store->usage().spill_files, 2u);
  auto replayed = store->replay(**second);
  ASSERT_TRUE(replayed.ok());
  EXPECT_EQ(replayed->row_count, 2);
  replayed = store->replay(**third);
  ASSERT_TRUE(replayed.ok());
  EXPECT_EQ(replayed->row_count, 3);
  second.ValueOrDie().reset();
  third.ValueOrDie().reset();
  EXPECT_EQ(store->usage().spill_files, 0u);
}

TEST(ShuffleBufferStoreTest, ConcurrentWritersUseDisjointOffsetsInSharedSegment) {
  ShuffleBufferLimits limits;
  limits.memory_bytes = 0;
  limits.spill_files = 1;
  auto store = std::make_shared<ShuffleBufferStore>(limits);
  ShuffleBufferStore::HandleScope scope(store);
  using Entries = std::vector<std::unique_ptr<ShuffleBufferStore::Entry>>;
  std::vector<std::future<Entries>> writers;
  for (int writer = 0; writer < 4; ++writer) {
    writers.push_back(std::async(std::launch::async, [store, writer, &scope] {
      Entries entries;
      for (int index = 0; index < 64; ++index) {
        auto stored = store->store(taggedBatch(writer, writer * 64 + index), scope);
        if (!stored.ok()) throw std::runtime_error(stored.status().ToString());
        entries.push_back(std::move(*stored));
      }
      return entries;
    }));
  }
  std::vector<Entries> retained;
  for (auto& writer : writers) retained.push_back(writer.get());
  EXPECT_EQ(store->usage().spill_files, 1u);
  for (int thread = 0; thread < 4; ++thread) {
    for (int index = 0; index < 64; ++index) {
      auto replayed = store->replay(*retained[thread][index]);
      ASSERT_TRUE(replayed.ok()) << replayed.status().ToString();
      EXPECT_EQ(replayed->partition_id, thread);
      EXPECT_EQ(replayed->row_count, thread * 64 + index);
    }
  }
  retained.clear();
  EXPECT_EQ(store->usage().spill_files, 0u);
  EXPECT_EQ(store->usage().spill_bytes, 0u);
}

TEST(ShuffleBufferStoreTest, NullEmptyNegativeAndOversizeNeverAcquireCredit) {
  ShuffleBufferLimits limits;
  limits.max_payload_bytes = 64;
  auto store = std::make_shared<ShuffleBufferStore>(limits);
  ShuffleBufferStore::HandleScope scope(store);
  EXPECT_TRUE(store->store(ShuffleBatch{0, nullptr, 1}, scope).status().IsInvalid());
  EXPECT_TRUE(store->store(taggedBatch(0, 1, 0), scope).status().IsInvalid());
  EXPECT_TRUE(store->store(taggedBatch(0, -1), scope).status().IsInvalid());
  EXPECT_TRUE(store->store(taggedBatch(0, 1, 65), scope).status().IsCapacityError());
  EXPECT_EQ(store->usage().buffered_batches, 0u);
  EXPECT_EQ(store->usage().memory_bytes, 0u);
}

TEST(ShuffleBufferStoreTest, TruncatedSpillFailsInsteadOfReturningPartialData) {
  ShuffleBufferLimits limits;
  limits.memory_bytes = 0;
  auto store = std::make_shared<ShuffleBufferStore>(limits);
  ShuffleBufferStore::HandleScope scope(store);
  auto entry = store->store(taggedBatch(0, 1), scope);
  ASSERT_TRUE(entry.ok());
  ASSERT_EQ(::ftruncate((*entry)->segment->file.fd(), 1), 0);
  EXPECT_TRUE(store->replay(**entry).status().IsIOError());
  entry.ValueOrDie().reset();
  EXPECT_EQ(store->usage().spill_bytes, 0u);
}

TEST(ShuffleBufferStoreTest, ReadErrorRetainsCreditUntilFileIsClosed) {
  test::SpillDirectory directory;
  ShuffleBufferLimits limits;
  limits.memory_bytes = 0;
  auto store = std::make_shared<ShuffleBufferStore>(limits);
  ShuffleBufferStore::HandleScope scope(store);
  auto entry = store->store(taggedBatch(0, 1), scope);
  ASSERT_TRUE(entry.ok());
  // Reading a directory descriptor produces a real deterministic OS read
  // error without mocks, private access, or external services.
  folly::File unreadable(::open(directory.path().c_str(), O_RDONLY), true);
  ASSERT_GE(unreadable.fd(), 0);
  (*entry)->segment->file = std::move(unreadable);
  EXPECT_TRUE(store->replay(**entry).status().IsIOError());
  EXPECT_EQ(store->usage().spill_files, 1u);
  entry.ValueOrDie().reset();
  EXPECT_EQ(store->usage().spill_files, 0u);
}

TEST(ShuffleBufferStoreTest, CancellationRejectsStorageAndReplayAndReleasesOnScopeExit) {
  ShuffleBufferLimits limits;
  limits.memory_bytes = 0;
  auto store = std::make_shared<ShuffleBufferStore>(limits);
  ShuffleBufferStore::HandleScope scope(store);
  CancellationSource cancellation;
  auto entry = store->store(taggedBatch(0, 1), scope, cancellation.token());
  ASSERT_TRUE(entry.ok());
  cancellation.requestStop();
  EXPECT_TRUE(store->replay(**entry, cancellation.token()).status().IsCancelled());
  EXPECT_TRUE(store->store(taggedBatch(1, 2), scope, cancellation.token()).status().IsCancelled());
  entry.ValueOrDie().reset();
  EXPECT_EQ(store->usage().spill_bytes, 0u);
}

TEST(ShuffleBufferStoreTest, StreamCreditIsReleasedWithItsOwner) {
  ShuffleBufferLimits limits;
  limits.streams = 1;
  auto store = std::make_shared<ShuffleBufferStore>(limits);
  ShuffleBufferStore::HandleScope scope(store);
  auto reservation = store->acquireStream();
  ASSERT_TRUE(reservation.ok());
  EXPECT_TRUE(store->acquireStream().status().IsCapacityError());
  reservation.ValueOrDie().reset();
  EXPECT_TRUE(store->acquireStream().ok());
  EXPECT_EQ(store->usage().streams, 0u);
}

TEST(ShuffleBufferStoreTest, TransitCreditIsByteBoundedAndReusable) {
  using namespace std::chrono_literals;
  ShuffleBufferLimits limits;
  constexpr uint64_t kPayload = 1024;
  constexpr uint64_t kCharge = 3 * kPayload + 64 * 1024;
  limits.transit_bytes = kCharge;
  auto store = std::make_shared<ShuffleBufferStore>(limits);
  ShuffleBufferStore::HandleScope scope(store);
  auto first = store->acquireTransit(kPayload);
  ASSERT_TRUE(first.ok()) << first.status().ToString();
  EXPECT_EQ(store->usage().transit_bytes, kCharge);
  auto second = std::async(std::launch::async, [&] {
    return store->acquireTransit(kPayload);
  });
  EXPECT_EQ(second.wait_for(50ms), std::future_status::timeout);
  first.ValueOrDie().reset();
  ASSERT_EQ(second.wait_for(1s), std::future_status::ready);
  auto acquired = second.get();
  ASSERT_TRUE(acquired.ok()) << acquired.status().ToString();
  acquired.ValueOrDie().reset();
  EXPECT_EQ(store->usage().transit_bytes, 0u);
  EXPECT_TRUE(store->acquireTransit(kPayload + 1).status().IsCapacityError());
}

TEST(ShuffleBufferStoreTest, TransitWaitObservesReaderCancellation) {
  using namespace std::chrono_literals;
  ShuffleBufferLimits limits;
  limits.transit_bytes = 3 * 1024 + 64 * 1024;
  auto store = std::make_shared<ShuffleBufferStore>(limits);
  ShuffleBufferStore::HandleScope scope(store);
  auto held = store->acquireTransit(1024);
  ASSERT_TRUE(held.ok());
  std::atomic<bool> cancelled{false};
  auto waiting = std::async(std::launch::async, [&] {
    return store->acquireTransit(1024, {}, [&] { return cancelled.load(); });
  });
  EXPECT_EQ(waiting.wait_for(50ms), std::future_status::timeout);
  cancelled.store(true);
  ASSERT_EQ(waiting.wait_for(1s), std::future_status::ready);
  EXPECT_TRUE(waiting.get().status().IsCancelled());
  held.ValueOrDie().reset();
  EXPECT_EQ(store->usage().transit_bytes, 0u);
}

TEST(ShuffleBufferStoreTest, InvalidLimitsCannotDisableResourceBounds) {
  ShuffleBufferLimits limits;
  limits.spill_bytes = 0;
  EXPECT_THROW(ShuffleBufferStore{limits}, std::invalid_argument);
  limits.spill_bytes = 4096;
  limits.transit_bytes = 0;
  EXPECT_THROW(ShuffleBufferStore{limits}, std::invalid_argument);
  limits.transit_bytes = 64 * 1024 * 1024;
  limits.max_payload_bytes = uint64_t{1} << 32;
  EXPECT_THROW(ShuffleBufferStore{limits}, std::invalid_argument);
}

// A store-wide segment mixes entries from unrelated handles, so draining one
// handle could never release physical blocks while another handle still held
// entries in the same file. The slowest handle would pin everyone's disk credit.
TEST(ShuffleBufferStoreTest, DrainingOneHandleReleasesItsOwnSpillCredit) {
  test::SpillDirectory directory;
  ShuffleBufferLimits limits;
  limits.memory_bytes = 0;  // force every batch to spill
  limits.spill_directory = directory.path();
  auto store = std::make_shared<ShuffleBufferStore>(limits);
  ShuffleBufferStore::HandleScope fast(store);
  ShuffleBufferStore::HandleScope slow(store);

  auto fastEntry = store->store(taggedBatch(0, 1), fast);
  ASSERT_TRUE(fastEntry.ok()) << fastEntry.status().ToString();
  auto slowEntry = store->store(taggedBatch(1, 2), slow);
  ASSERT_TRUE(slowEntry.ok()) << slowEntry.status().ToString();

  // Separate segments, so each handle's reader can release its own file.
  EXPECT_EQ(store->usage().spill_files, 2u);
  const uint64_t bothCharged = store->usage().spill_bytes;
  ASSERT_GT(bothCharged, 0u);

  fastEntry.ValueOrDie().reset();
  const uint64_t afterFastDrained = store->usage().spill_bytes;
  EXPECT_LT(afterFastDrained, bothCharged);
  EXPECT_EQ(store->usage().spill_files, 1u);

  // The slow handle still holds its own credit; nothing was under-reported.
  EXPECT_GT(afterFastDrained, 0u);
  slowEntry.ValueOrDie().reset();
  EXPECT_EQ(store->usage().spill_bytes, 0u);
  EXPECT_EQ(store->usage().spill_files, 0u);
}

// The point of retention backpressure: a slow reader must stall its producer
// rather than fail the query. Releasing credit must wake the waiting producer.
TEST(ShuffleBufferStoreTest, ExhaustedRetentionWaitsForReaderInsteadOfFailing) {
  ShuffleBufferLimits limits;
  limits.memory_bytes = 0;
  limits.buffered_batches = 1;
  limits.max_payload_bytes = 64;
  limits.retention_wait_ms = 5000;
  auto store = std::make_shared<ShuffleBufferStore>(limits);
  ShuffleBufferStore::HandleScope scope(store);
  auto held = store->store(taggedBatch(0, 1), scope);
  ASSERT_TRUE(held.ok()) << held.status().ToString();

  auto waiting = std::async(std::launch::async, [&] {
    return store->store(taggedBatch(0, 2), scope);
  });
  // Producer must still be blocked, not failed, while the reader holds credit.
  ASSERT_EQ(waiting.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);
  held.ValueOrDie().reset();

  ASSERT_EQ(waiting.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  auto resumed = waiting.get();
  EXPECT_TRUE(resumed.ok()) << resumed.status().ToString();
}

// A stuck reader must still fail eventually, and the error must name the
// exhausted budget so the cause is diagnosable.
TEST(ShuffleBufferStoreTest, RetentionWaitTimesOutAndNamesExhaustedBudget) {
  ShuffleBufferLimits limits;
  limits.memory_bytes = 0;
  limits.buffered_batches = 1;
  limits.max_payload_bytes = 64;
  limits.retention_wait_ms = 40;
  auto store = std::make_shared<ShuffleBufferStore>(limits);
  ShuffleBufferStore::HandleScope scope(store);
  auto held = store->store(taggedBatch(0, 1), scope);
  ASSERT_TRUE(held.ok());

  auto denied = store->store(taggedBatch(0, 2), scope);
  ASSERT_TRUE(denied.status().IsCapacityError()) << denied.status().ToString();
  EXPECT_NE(denied.status().ToString().find("shuffle buffered record budget exhausted"),
            std::string::npos);
  // The failed attempt must not leak credit.
  EXPECT_EQ(store->usage().buffered_batches, 1u);
}

// Cancellation must break the wait promptly; stop() joins the drain thread and
// would otherwise hang for the whole retention timeout.
TEST(ShuffleBufferStoreTest, RetentionWaitObservesCancellation) {
  ShuffleBufferLimits limits;
  limits.memory_bytes = 0;
  limits.buffered_batches = 1;
  limits.max_payload_bytes = 64;
  limits.retention_wait_ms = 30'000;
  auto store = std::make_shared<ShuffleBufferStore>(limits);
  ShuffleBufferStore::HandleScope scope(store);
  auto held = store->store(taggedBatch(0, 1), scope);
  ASSERT_TRUE(held.ok());

  CancellationSource source;
  auto waiting = std::async(std::launch::async, [&] {
    return store->store(taggedBatch(0, 2), scope, source.token());
  });
  ASSERT_EQ(waiting.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);
  source.requestStop();
  ASSERT_EQ(waiting.wait_for(std::chrono::seconds(2)), std::future_status::ready);
  EXPECT_TRUE(waiting.get().status().IsCancelled());
  EXPECT_EQ(store->usage().buffered_batches, 1u);
}

}  // namespace
}  // namespace pangrova::shuffle
