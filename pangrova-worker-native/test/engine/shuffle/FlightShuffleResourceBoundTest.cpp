#include "engine/shuffle/ShuffleResourceTestSupport.h"

#include <future>

namespace pangrova::shuffle {
namespace {

using namespace std::chrono_literals;
using test::taggedBatch;

TEST(SerializedShuffleQueueBudgetTest, AdmissionIsByteBoundedBeforeCopy) {
  constexpr size_t payload_bytes = 200;
  const size_t grant = payload_bytes + sizeof(folly::IOBuf) + 128;
  SerializedShuffleQueue queue(/*capacity=*/4, grant);
  ASSERT_TRUE(queue.push(taggedBatch(0, 1, payload_bytes), 1s));
  EXPECT_EQ(queue.usedBytes(), grant);
  EXPECT_FALSE(queue.tryPush(taggedBatch(0, 2, payload_bytes)));
  EXPECT_EQ(queue.usedBytes(), grant);
  ShuffleBatch first;
  ASSERT_EQ(queue.pop(first, 1s), executor::PopResult::DATA);
  EXPECT_EQ(queue.usedBytes(), 0u);
  ASSERT_TRUE(queue.tryPush(taggedBatch(0, 3, payload_bytes)));
  queue.abort();
  queue.drain();
  EXPECT_EQ(queue.usedBytes(), 0u);
}

TEST(SerializedShuffleQueueBudgetTest, AbortWakesByteBlockedProducer) {
  constexpr size_t payload_bytes = 200;
  const size_t grant = payload_bytes + sizeof(folly::IOBuf) + 128;
  SerializedShuffleQueue queue(/*capacity=*/4, grant);
  ASSERT_TRUE(queue.push(taggedBatch(0, 1, payload_bytes), 1s));
  auto blocked = std::async(std::launch::async, [&] {
    return queue.push(taggedBatch(0, 2, payload_bytes), 5s);
  });
  queue.abort();
  ASSERT_EQ(blocked.wait_for(1s), std::future_status::ready);
  EXPECT_FALSE(blocked.get());
  queue.drain();
  EXPECT_EQ(queue.usedBytes(), 0u);
}

TEST(SerializedShuffleQueueBudgetTest, OversizedPageIsRejectedWithoutByteCharge) {
  SerializedShuffleQueue queue(/*capacity=*/4, 16 * 1024 * 1024);
  EXPECT_THROW(queue.push(
      taggedBatch(0, 1, kCanonicalShuffleBatchBytes + 1), 1s),
      std::length_error);
  EXPECT_EQ(queue.usedBytes(), 0u);
}

TEST(SerializedShuffleQueueBudgetTest, ChainedFourMiBPageUsesOneReservedCopy) {
  constexpr size_t half = kCanonicalShuffleBatchBytes / 2;
  std::string left(half, 'l');
  std::string right(half, 'r');
  auto data = folly::IOBuf::copyBuffer(left);
  data->appendChain(folly::IOBuf::create(0));
  data->appendChain(folly::IOBuf::copyBuffer(right));
  const size_t grant = kCanonicalShuffleBatchBytes + sizeof(folly::IOBuf) + 128;
  SerializedShuffleQueue queue(/*capacity=*/2, grant);
  ASSERT_TRUE(queue.push(ShuffleBatch{0, std::move(data), 1}, 1s));
  EXPECT_EQ(queue.usedBytes(), grant);
  ShuffleBatch received;
  ASSERT_EQ(queue.pop(received, 1s), executor::PopResult::DATA);
  EXPECT_EQ(received.data->computeChainDataLength(), kCanonicalShuffleBatchBytes);
  EXPECT_EQ(received.data->data()[0], 'l');
  EXPECT_EQ(received.data->data()[half], 'r');
  EXPECT_EQ(queue.usedBytes(), 0u);
}

FlightShuffleServerConfig spillingConfig(const test::SpillDirectory& directory) {
  FlightShuffleServerConfig config;
  config.buffer_limits.memory_bytes = 0;
  config.buffer_limits.spill_directory = directory.path();
  config.max_batch_bytes = 64;
  return config;
}

TEST(FlightShuffleResourceBoundTest, MissingConsumersDoNotBlockOtherPartitionsOrCompletion) {
  test::SpillDirectory directory;
  auto queue = std::make_unique<SerializedShuffleQueue>(2);
  FlightShuffleServer server(spillingConfig(directory));
  test::registerQueue(server, *queue);
  auto producer = std::async(std::launch::async, [&] {
    for (int i = 0; i < 16; ++i) {
      if (!queue->push(taggedBatch(0, i), 2s)) return false;
    }
    if (!queue->push(taggedBatch(2, 99), 2s) ||
        !queue->push(taggedBatch(1, 42), 2s)) return false;
    if (!queue->commit()) return false;
    return true;
  });
  // Neither partition 0 nor 2 has a consumer. A per-partition batch cap
  // would block before this marker and prevent the producer from completing.
  ShuffleBatch marker;
  ASSERT_TRUE(server.popForHandle(1, 1, marker, 3s));
  EXPECT_EQ(marker.row_count, 42);
  ASSERT_EQ(producer.wait_for(3s), std::future_status::ready);
  ASSERT_TRUE(producer.get());
  server.unregisterHandle(1);
  ASSERT_EQ(server.lingeringHandleCount(), 1u);
  queue.reset();  // Lingering data must not retain/dereference the producer.
  EXPECT_EQ(server.bufferUsage().memory_bytes, 0u);
  // The consumed marker shares a segment with the unread partitions, so its
  // physical disk blocks remain charged until the segment closes.
  EXPECT_EQ(server.bufferUsage().spill_bytes, 18u * 4096);
  EXPECT_TRUE(std::filesystem::is_empty(directory.path()));
  for (int i = 0; i < 16; ++i) {
    ShuffleBatch batch;
    ASSERT_TRUE(server.popForHandle(1, 0, batch, 1s));
    EXPECT_EQ(batch.row_count, i);
    EXPECT_EQ(batch.data->to<std::string>(), std::string(32, 'a' + i % 26));
  }
  server.evictFullyConsumedLingering(std::chrono::steady_clock::now() + 1h);
  EXPECT_EQ(server.bufferUsage().spill_bytes, 0u);
  EXPECT_EQ(server.bufferUsage().buffered_batches, 0u);
}

TEST(FlightShuffleResourceBoundTest, ProductionRegistrationUsesSharedMemoryAndSpillCredits) {
  test::SpillDirectory directory;
  SerializedShuffleQueue first(4), second(4);
  auto config = spillingConfig(directory);
  config.max_batch_bytes = 4096;
  auto resident = taggedBatch(0, 1, 2048);
  config.buffer_limits.memory_bytes = resident.data->capacity() + sizeof(folly::IOBuf);
  FlightShuffleServer server(config);
  ASSERT_TRUE(first.push(std::move(resident), 1s));
  ASSERT_TRUE(first.push(taggedBatch(0, 2, 2048), 1s));
  ASSERT_TRUE(first.commit());
  test::registerQueue(server, first);
  server.unregisterHandle(1);
  // A completed task can release its queue grant only after the demuxer has
  // moved every batch into its independent, linger-safe server reservation.
  EXPECT_EQ(first.usedBytes(), 0u);
  ASSERT_GT(server.bufferUsage().memory_bytes, 0u);
  ASSERT_GT(server.bufferUsage().spill_bytes, 0u);
  ASSERT_TRUE(second.push(taggedBatch(1, 3, 2048), 1s));
  ASSERT_TRUE(second.commit());
  test::registerQueue(server, second, 2);
  server.unregisterHandle(2);
  EXPECT_LE(server.bufferUsage().memory_bytes, config.buffer_limits.memory_bytes);
  EXPECT_EQ(server.bufferUsage().buffered_batches, 3u);
  for (int i = 1; i <= 2; ++i) {
    ShuffleBatch batch;
    ASSERT_TRUE(server.popForHandle(1, 0, batch, 1s));
    EXPECT_EQ(batch.row_count, i);
  }
  ShuffleBatch batch;
  ASSERT_TRUE(server.popForHandle(2, 1, batch, 1s));
  EXPECT_EQ(batch.row_count, 3);
  EXPECT_EQ(server.bufferUsage().memory_bytes, 0u);
  EXPECT_EQ(server.bufferUsage().spill_bytes, 0u);
}

TEST(FlightShuffleResourceBoundTest, ExhaustedDiskFailsFlightAndPreservesOtherHandle) {
  test::SpillDirectory directory;
  SerializedShuffleQueue first(2), second(4);
  auto config = spillingConfig(directory);
  config.buffer_limits.spill_bytes = 4096;
  FlightShuffleServer server(config);
  ASSERT_TRUE(first.push(taggedBatch(0, 7), 1s));
  ASSERT_TRUE(first.commit());
  test::registerQueue(server, first);
  server.unregisterHandle(1);
  test::registerQueue(server, second, 2);
  test::CallContext context;
  auto stream = test::openStream(server, context, 1, 2);
  ASSERT_TRUE(stream.ok());
  ASSERT_TRUE(second.push(taggedBatch(0, 8), 1s));
  auto result = (*stream)->Next();
  EXPECT_TRUE(result.status().IsCapacityError()) << result.status().ToString();
  EXPECT_NE(result.status().ToString().find("spill byte budget exhausted"),
            std::string::npos);
  server.unregisterHandle(2);  // Also joins cancellation/producer wakeup.
  EXPECT_TRUE(second.isClosed());
  EXPECT_FALSE(second.push(taggedBatch(1, 9), 0ms));
  ShuffleBatch preserved;
  ASSERT_TRUE(server.popForHandle(1, 0, preserved, 1s));
  EXPECT_EQ(preserved.row_count, 7);
  EXPECT_EQ(server.bufferUsage().spill_bytes, 0u);
}

TEST(FlightShuffleResourceBoundTest, OversizedChainedPayloadFailsFlightInsteadOfEof) {
  test::SpillDirectory directory;
  SerializedShuffleQueue queue(2);
  FlightShuffleServer server(spillingConfig(directory));
  test::registerQueue(server, queue);
  test::CallContext context;
  auto stream = test::openStream(server, context);
  ASSERT_TRUE(stream.ok());
  auto batch = taggedBatch(0, 1, 33);
  batch.data->appendChain(folly::IOBuf::copyBuffer(std::string(32, 'b')));
  ASSERT_TRUE(queue.push(std::move(batch), 1s));
  EXPECT_TRUE((*stream)->Next().status().IsCapacityError());
  server.unregisterHandle(1);
  EXPECT_TRUE(queue.isClosed());
  EXPECT_EQ(server.bufferUsage().buffered_batches, 0u);
}

TEST(FlightShuffleResourceBoundTest, SpillCreationErrorIsReturnedThroughFlight) {
  test::SpillDirectory directory;
  SerializedShuffleQueue queue(2);
  auto config = spillingConfig(directory);
  config.buffer_limits.spill_directory += "/missing-directory";
  FlightShuffleServer server(config);
  test::registerQueue(server, queue);
  test::CallContext context;
  auto stream = test::openStream(server, context);
  ASSERT_TRUE(stream.ok());
  ASSERT_TRUE(queue.push(taggedBatch(0, 1), 1s));
  EXPECT_TRUE((*stream)->Next().status().IsIOError());
  server.unregisterHandle(1);
  EXPECT_EQ(server.bufferUsage().spill_bytes, 0u);
  EXPECT_EQ(server.bufferUsage().spill_files, 0u);
  EXPECT_TRUE(std::filesystem::is_empty(directory.path()));
}

TEST(FlightShuffleResourceBoundTest, StopWakesWaitingFlightReaderWithoutCleanEof) {
  SerializedShuffleQueue queue(2);
  FlightShuffleServer server(FlightShuffleServerConfig{});
  test::registerQueue(server, queue);
  test::CallContext context;
  auto stream = test::openStream(server, context);
  ASSERT_TRUE(stream.ok());
  std::promise<void> entered;
  auto read = std::async(std::launch::async, [&] {
    entered.set_value();
    return (*stream)->Next().status();
  });
  entered.get_future().wait();
  server.stop();
  ASSERT_EQ(read.wait_for(2s), std::future_status::ready);
  EXPECT_TRUE(read.get().IsCancelled());
  EXPECT_TRUE(queue.isClosed());
  stream.ValueOrDie().reset();
  EXPECT_EQ(server.bufferUsage().handles, 0u);
}

TEST(FlightShuffleResourceBoundTest, FinishedConsumerDoesNotCancelOtherPartitions) {
  test::SpillDirectory directory;
  SerializedShuffleQueue queue(4);
  FlightShuffleServer server(spillingConfig(directory));
  test::registerQueue(server, queue);
  test::CallContext context;
  auto abandoned = test::openStream(server, context);
  ASSERT_TRUE(abandoned.ok());
  ASSERT_TRUE((*abandoned)->Close().ok());
  abandoned.ValueOrDie().reset();
  ASSERT_TRUE(queue.push(taggedBatch(0, 1), 1s));
  ASSERT_TRUE(queue.push(taggedBatch(1, 2), 1s));
  ASSERT_TRUE(queue.commit());
  server.unregisterHandle(1);
  ShuffleBatch batch;
  ASSERT_TRUE(server.popForHandle(1, 1, batch, 1s));
  EXPECT_EQ(batch.row_count, 2);
  ASSERT_TRUE(server.popForHandle(1, 0, batch, 1s));
  EXPECT_EQ(batch.row_count, 1);
}

TEST(FlightShuffleResourceBoundTest, RpcCancellationTerminatesAnIdleRead) {
  SerializedShuffleQueue queue(2);
  FlightShuffleServer server(FlightShuffleServerConfig{});
  test::registerQueue(server, queue);
  test::CallContext context;
  auto stream = test::openStream(server, context);
  ASSERT_TRUE(stream.ok());
  std::promise<void> entered;
  auto read = std::async(std::launch::async, [&] {
    entered.set_value();
    return (*stream)->Next().status();
  });
  entered.get_future().wait();
  context.cancelled.store(true);
  ASSERT_EQ(read.wait_for(2s), std::future_status::ready);
  EXPECT_TRUE(read.get().IsCancelled());
  server.unregisterHandle(1);
}

TEST(FlightShuffleResourceBoundTest, IpcPayloadRetainsTransitCreditUntilSendCompletes) {
  SerializedShuffleQueue queue(2);
  auto config = FlightShuffleServerConfig{};
  config.max_batch_bytes = 32;
  config.buffer_limits.transit_bytes = 3 * 32 + 64 * 1024;
  FlightShuffleServer server(config);
  ASSERT_TRUE(queue.push(taggedBatch(0, 1), 1s));
  ASSERT_TRUE(queue.commit());
  test::registerQueue(server, queue);
  test::CallContext context;
  auto stream = test::openStream(server, context);
  ASSERT_TRUE(stream.ok()) << stream.status().ToString();
  {
    auto payload = (*stream)->Next();
    ASSERT_TRUE(payload.ok()) << payload.status().ToString();
    EXPECT_EQ(server.bufferUsage().transit_bytes, config.buffer_limits.transit_bytes);
    auto eof = (*stream)->Next();
    ASSERT_TRUE(eof.ok()) << eof.status().ToString();
    // The reader has advanced, but the preceding Flight payload still owns
    // its IPC data buffer and therefore the outstanding response credit.
    EXPECT_EQ(server.bufferUsage().transit_bytes, config.buffer_limits.transit_bytes);
  }
  EXPECT_EQ(server.bufferUsage().transit_bytes, 0u);
  stream.ValueOrDie().reset();
  server.unregisterHandle(1);
}

TEST(FlightShuffleResourceBoundTest, ExpiryReleasesSpillEvenWithLiveReader) {
  test::SpillDirectory directory;
  auto queue = std::make_unique<SerializedShuffleQueue>(2);
  FlightShuffleServer server(spillingConfig(directory));
  ASSERT_TRUE(queue->push(taggedBatch(0, 1), 1s));
  ASSERT_TRUE(queue->commit());
  test::registerQueue(server, *queue);
  test::CallContext context;
  auto stream = test::openStream(server, context);
  ASSERT_TRUE(stream.ok());
  server.unregisterHandle(1);
  queue.reset();
  ASSERT_EQ(server.bufferUsage().spill_files, 1u);
  EXPECT_EQ(server.evictFullyConsumedLingering(std::chrono::steady_clock::now() + 1h), 1u);
  EXPECT_EQ(server.bufferUsage().spill_bytes, 0u);
  EXPECT_TRUE((*stream)->Next().status().IsCancelled());
}

TEST(FlightShuffleResourceBoundTest, RejectsUnboundedAndOutOfRangeConfiguration) {
  auto config = FlightShuffleServerConfig{};
  config.max_batch_bytes = 0;
  EXPECT_THROW(FlightShuffleServer{config}, std::invalid_argument);
  config.max_batch_bytes = 64;
  config.buffer_limits.transit_bytes = 3 * 64 + 64 * 1024 - 1;
  EXPECT_THROW(FlightShuffleServer{config}, std::invalid_argument);
  config.buffer_limits.transit_bytes = 64 * 1024 * 1024;
  config.lingering_ttl_ms = 0;
  EXPECT_THROW(FlightShuffleServer{config}, std::invalid_argument);
  config.lingering_ttl_ms = 1000;
  config.buffer_limits.partitions_per_handle = 3;
  config.buffer_limits.handles = 1;
  SerializedShuffleQueue first(1), second(1);
  FlightShuffleServer server(config);
  test::registerQueue(server, first);
  EXPECT_THROW(test::registerQueue(server, second, 2), std::runtime_error);
  test::CallContext context;
  EXPECT_TRUE(test::openStream(server, context, 3).status().IsInvalid());
  server.unregisterHandle(1);
  EXPECT_THROW(server.registerHandle(2, &second, "q", 0, 4, 1, 0, 1),
               std::invalid_argument);
}

}  // namespace
}  // namespace pangrova::shuffle
