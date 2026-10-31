#include <gtest/gtest.h>

#include <barrier>
#include <future>
#include <vector>

#include <arrow/array/array_binary.h>
#include <arrow/flight/client.h>
#include <arrow/table.h>

#include "engine/shuffle/ShuffleResourceTestSupport.h"
#include "engine/shuffle/TicketSigner.h"

namespace pangrova::shuffle {
namespace {
using namespace std::chrono_literals;

TEST(ShuffleProducerRegistrationTest, ExactReplayAtCapacityDoesNotAdmitAnotherOwner) {
  FlightShuffleServerConfig config;
  config.buffer_limits.handles = 1;
  SerializedShuffleQueue queue(8), other(1);
  FlightShuffleServer server(config);
  const auto owner = server.registerHandle(1, &queue, "q", 0, 3, 1, 0, 1);
  for (int retry = 0; retry < 20; ++retry) {
    EXPECT_EQ(server.registerHandle(1, &queue, "q", 0, 3, 1, 0, 1), owner);
    EXPECT_EQ(server.bufferUsage().handles, 1u);
    EXPECT_EQ(server.handleCount(), 1u);
  }
  EXPECT_THROW(server.registerHandle(2, &other, "q", 0, 3, 1, 0, 1), std::runtime_error);
  server.unregisterHandle(1);
  EXPECT_EQ(server.bufferUsage().handles, 0u);
}

TEST(ShuffleProducerRegistrationTest, EveryIdentityConflictLeavesOriginalProducerIntact) {
  SerializedShuffleQueue queue(8), other(1);
  FlightShuffleServerConfig config;
  config.reuse_port = false;
  config.hmac_current_key = "registration-test-key-012345678901";
  FlightShuffleServer server(config);
  ASSERT_GT(server.start(), 0);
  const auto owner = server.registerHandle(1, &queue, "q", 0, 3, 1, 0, 1);
  EXPECT_THROW(server.registerHandle(1, &other, "q", 0, 3, 1, 0, 1), std::logic_error);
  EXPECT_THROW(server.registerHandle(1, &queue, "other", 0, 3, 1, 0, 1), std::logic_error);
  EXPECT_THROW(server.registerHandle(1, &queue, "q", 1, 3, 1, 0, 1), std::logic_error);
  EXPECT_THROW(server.registerHandle(1, &queue, "q", 0, 2, 1, 0, 1), std::logic_error);
  EXPECT_THROW(server.registerHandle(1, &queue, "q", 0, 3, 2, 0, 1), std::logic_error);
  EXPECT_THROW(server.registerHandle(1, &queue, "q", 0, 3, 1, 1, 1), std::logic_error);
  EXPECT_THROW(server.registerHandle(1, &queue, "q", 0, 3, 1, 0, 2), std::logic_error);
  EXPECT_EQ(server.bufferUsage().handles, 1u);
  ASSERT_TRUE(queue.push(test::taggedBatch(0, 17), 1s));
  ASSERT_TRUE(queue.commit());
  owner->close();
  EXPECT_EQ(owner->state(), ShuffleProducerRegistration::State::LINGERING);
  auto location = arrow::flight::Location::ForGrpcTcp("127.0.0.1", server.boundPort());
  ASSERT_TRUE(location.ok()) << location.status().ToString();
  auto client = arrow::flight::FlightClient::Connect(*location);
  ASSERT_TRUE(client.ok()) << client.status().ToString();
  TicketSigner signer(server.signingKey());
  for (int partition = 0; partition < 3; ++partition) {
    arrow::flight::Ticket ticket{signer.sign(
        "1\nq\n0\n" + std::to_string(partition) + "\n1\n0\n1")};
    auto stream = (*client)->DoGet(ticket);
    ASSERT_TRUE(stream.ok()) << stream.status().ToString();
    auto table = (*stream)->ToTable();
    ASSERT_TRUE(table.ok()) << table.status().ToString();
    EXPECT_EQ((*table)->num_rows(), partition == 0 ? 1 : 0);
    if (partition == 0) {
      ASSERT_EQ((*table)->column(0)->num_chunks(), 1);
      const auto data = std::static_pointer_cast<arrow::BinaryArray>(
          (*table)->column(0)->chunk(0));
      EXPECT_EQ(data->GetString(0), std::string(32, 'a' + 17 % 26));
    }
  }
  EXPECT_EQ(server.evictFullyConsumedLingering(), 1u);
  EXPECT_EQ(server.bufferUsage().handles, 0u);
}

TEST(ShuffleProducerRegistrationTest, ConcurrentExactReplayUsesOneRealDemuxer) {
  FlightShuffleServerConfig config;
  config.buffer_limits.handles = 1;
  SerializedShuffleQueue queue(8);
  FlightShuffleServer server(config);
  constexpr int kCallers = 8;
  std::barrier start(kCallers);
  std::vector<std::future<std::shared_ptr<ShuffleProducerRegistration>>> callers;
  for (int caller = 0; caller < kCallers; ++caller) {
    callers.push_back(std::async(std::launch::async, [&] {
      start.arrive_and_wait();
      return server.registerHandle(1, &queue, "q", 0, 3, 1, 0, 1);
    }));
  }
  const auto owner = callers.front().get();
  for (int caller = 1; caller < kCallers; ++caller) EXPECT_EQ(callers[caller].get(), owner);
  EXPECT_EQ(server.bufferUsage().handles, 1u);
  ASSERT_TRUE(queue.push(test::taggedBatch(0, 11), 1s));
  ASSERT_TRUE(queue.push(test::taggedBatch(0, 23), 1s));
  ASSERT_TRUE(queue.commit());
  owner->close();
  EXPECT_THROW(server.registerHandle(1, &queue, "q", 0, 3, 1, 0, 1), std::logic_error);
  ShuffleBatch first, second;
  ASSERT_TRUE(server.popForHandle(1, 0, first, 1s));
  ASSERT_TRUE(server.popForHandle(1, 0, second, 1s));
  EXPECT_EQ(first.row_count, 11);
  EXPECT_EQ(second.row_count, 23);
  ShuffleBatch eof;
  EXPECT_FALSE(server.popForHandle(1, 0, eof, 1s));
  EXPECT_FALSE(server.popForHandle(1, 1, eof, 1s));
  EXPECT_FALSE(server.popForHandle(1, 2, eof, 1s));
  owner->close(true);
}

TEST(ShuffleProducerRegistrationTest, StopWaitsForActualServiceLeaseAndSealsNewRegistration) {
  FlightShuffleServer server(FlightShuffleServerConfig{});
  ASSERT_GT(server.start(), 0);
  auto lease = server.acquireControl();
  auto stopping = std::async(std::launch::async, [&] { server.stop(); });
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  // A real acquire observes the actual seal, not an artificial task barrier.
  bool sealed = false;
  while (!sealed && std::chrono::steady_clock::now() < deadline) {
    try { auto probe = server.acquireControl(); }
    catch (const std::logic_error&) { sealed = true; }
    std::this_thread::yield();
  }
  EXPECT_TRUE(sealed);
  EXPECT_EQ(stopping.wait_for(0ms), std::future_status::timeout);
  lease.reset();
  ASSERT_EQ(stopping.wait_for(5s), std::future_status::ready);
  EXPECT_NO_THROW(stopping.get());
  EXPECT_FALSE(server.isRunning());
  SerializedShuffleQueue queue(1);
  EXPECT_THROW(server.registerHandle(1, &queue, "q", 0, 1, 1, 0, 1), std::logic_error);
  EXPECT_NO_THROW(server.stop());
}

}  // namespace
}  // namespace pangrova::shuffle
