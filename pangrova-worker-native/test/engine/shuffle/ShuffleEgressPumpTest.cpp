#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include <folly/io/IOBuf.h>

#include "engine/shuffle/ShuffleEgressPump.h"

namespace pangrova::shuffle {
namespace {

ShuffleBatch batch(int32_t partition_id, std::string data) {
  ShuffleBatch result;
  result.partition_id = partition_id;
  result.data = folly::IOBuf::copyBuffer(data);
  result.row_count = 1;
  return result;
}

TEST(ShuffleEgressPumpTest, DrainsInterleavedPartitionsInQueueOrder) {
  SerializedShuffleQueue queue(4);
  ASSERT_TRUE(queue.push(batch(1, "first"), std::chrono::milliseconds(0)));
  ASSERT_TRUE(queue.push(batch(0, "second"), std::chrono::milliseconds(0)));
  queue.close();

  std::vector<std::pair<int32_t, std::string>> received;
  drainSerializedShuffle(
      &queue, std::chrono::milliseconds(1),
      [&received](int32_t partition_id, int64_t row_count,
                  const uint8_t* data, int64_t size) {
        EXPECT_EQ(row_count, 1);
        received.emplace_back(
            partition_id,
            std::string(reinterpret_cast<const char*>(data),
                        static_cast<size_t>(size)));
      });

  EXPECT_EQ(received, (std::vector<std::pair<int32_t, std::string>>{
      {1, "first"}, {0, "second"}}));
}

TEST(ShuffleEgressPumpTest, PropagatesConsumerFailureWithoutDroppingIt) {
  SerializedShuffleQueue queue(1);
  ASSERT_TRUE(queue.push(batch(0, "payload"), std::chrono::milliseconds(0)));
  queue.close();

  EXPECT_THROW(
      drainSerializedShuffle(
          &queue, std::chrono::milliseconds(1),
          [](int32_t, int64_t, const uint8_t*, int64_t) {
            throw std::runtime_error("remote write rejected");
          }),
      std::runtime_error);
}

TEST(ShuffleEgressPumpTest, QueueRejectsZeroByteNativeBatchBeforeEgress) {
  SerializedShuffleQueue queue(1);
  EXPECT_THROW(
      queue.push(batch(0, ""), std::chrono::milliseconds(0)),
      std::length_error);
  EXPECT_EQ(queue.usedBytes(), 0u);
}

TEST(ShuffleEgressPumpTest, RejectsMissingAuthoritativeRowCount) {
  SerializedShuffleQueue queue(1);
  auto invalid = batch(0, "payload");
  invalid.row_count = 0;
  ASSERT_TRUE(queue.push(std::move(invalid), std::chrono::milliseconds(0)));
  queue.close();

  EXPECT_THROW(
      drainSerializedShuffle(
          &queue, std::chrono::milliseconds(1),
          [](int32_t, int64_t, const uint8_t*, int64_t) {}),
      std::runtime_error);
}

}  // namespace
}  // namespace pangrova::shuffle
