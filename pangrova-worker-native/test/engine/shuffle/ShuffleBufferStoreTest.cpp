#include "engine/shuffle/ShuffleResourceTestSupport.h"

#include <fcntl.h>
#include <future>
#include <stdexcept>
#include <unistd.h>
#include <vector>

namespace pangrova::shuffle {
namespace {

using test::taggedBatch;

TEST(ShuffleBufferStoreTest, ChainedPayloadReplaysExactlyAtSizeLimit) {
  test::SpillDirectory directory;
  ShuffleBufferLimits limits;
  limits.memory_bytes = 0;
  limits.max_payload_bytes = 64;
  limits.spill_directory = directory.path();
  auto store = std::make_shared<ShuffleBufferStore>(limits);
  auto batch = taggedBatch(2, 4, 32);
  batch.data->appendChain(folly::IOBuf::copyBuffer(std::string(32, 'z')));
  auto entry = store->store(std::move(batch));
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
  auto data = folly::IOBuf::create(1024 * 1024);
  data->writableData()[0] = 'x';
  data->append(1);
  auto entry = store->store(ShuffleBatch{0, std::move(data), 1});
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
  auto store = std::make_shared<ShuffleBufferStore>(limits);
  auto entry = store->store(taggedBatch(0, 1));
  ASSERT_TRUE(entry.ok());
  EXPECT_TRUE(store->store(taggedBatch(1, 2)).status().IsCapacityError());
  entry.ValueOrDie().reset();
  EXPECT_TRUE(store->store(taggedBatch(1, 2)).ok());
  EXPECT_EQ(store->usage().buffered_batches, 0u);
  limits.buffered_batches = 2;
  store = std::make_shared<ShuffleBufferStore>(limits);
  entry = store->store(taggedBatch(0, 1));
  ASSERT_TRUE(entry.ok());
  auto denied = store->store(taggedBatch(1, 2));
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
  std::vector<std::unique_ptr<ShuffleBufferStore::Entry>> entries;
  for (int i = 0; i < 1000; ++i) {
    auto stored = store->store(taggedBatch(0, i));
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
  auto store = std::make_shared<ShuffleBufferStore>(limits);
  auto first = store->store(taggedBatch(0, 1));
  auto second = store->store(taggedBatch(0, 2));
  ASSERT_TRUE(first.ok() && second.ok());
  EXPECT_EQ(store->usage().spill_files, 2u);
  EXPECT_TRUE(store->store(taggedBatch(0, 3)).status().IsCapacityError());
  first.ValueOrDie().reset();
  EXPECT_EQ(store->usage().spill_files, 1u);
  auto third = store->store(taggedBatch(0, 3));
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
  using Entries = std::vector<std::unique_ptr<ShuffleBufferStore::Entry>>;
  std::vector<std::future<Entries>> writers;
  for (int writer = 0; writer < 4; ++writer) {
    writers.push_back(std::async(std::launch::async, [store, writer] {
      Entries entries;
      for (int index = 0; index < 64; ++index) {
        auto stored = store->store(taggedBatch(writer, writer * 64 + index));
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
  EXPECT_TRUE(store->store(ShuffleBatch{0, nullptr, 1}).status().IsInvalid());
  EXPECT_TRUE(store->store(taggedBatch(0, 1, 0)).status().IsInvalid());
  EXPECT_TRUE(store->store(taggedBatch(0, -1)).status().IsInvalid());
  EXPECT_TRUE(store->store(taggedBatch(0, 1, 65)).status().IsCapacityError());
  EXPECT_EQ(store->usage().buffered_batches, 0u);
  EXPECT_EQ(store->usage().memory_bytes, 0u);
}

TEST(ShuffleBufferStoreTest, TruncatedSpillFailsInsteadOfReturningPartialData) {
  ShuffleBufferLimits limits;
  limits.memory_bytes = 0;
  auto store = std::make_shared<ShuffleBufferStore>(limits);
  auto entry = store->store(taggedBatch(0, 1));
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
  auto entry = store->store(taggedBatch(0, 1));
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
  CancellationSource cancellation;
  auto entry = store->store(taggedBatch(0, 1), cancellation.token());
  ASSERT_TRUE(entry.ok());
  cancellation.requestStop();
  EXPECT_TRUE(store->replay(**entry, cancellation.token()).status().IsCancelled());
  EXPECT_TRUE(store->store(taggedBatch(1, 2), cancellation.token()).status().IsCancelled());
  entry.ValueOrDie().reset();
  EXPECT_EQ(store->usage().spill_bytes, 0u);
}

TEST(ShuffleBufferStoreTest, StreamCreditIsReleasedWithItsOwner) {
  ShuffleBufferLimits limits;
  limits.streams = 1;
  auto store = std::make_shared<ShuffleBufferStore>(limits);
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

}  // namespace
}  // namespace pangrova::shuffle
