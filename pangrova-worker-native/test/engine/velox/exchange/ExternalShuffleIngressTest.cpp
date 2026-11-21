#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <future>
#include <string>
#include <utility>

#include <openssl/evp.h>

#include "engine/velox/exchange/ExternalShuffleIngress.h"

namespace pangrova::velox {
namespace {

uint64_t deadlineAfter(std::chrono::milliseconds duration) {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch() + duration)
          .count());
}

std::string sha256(const std::string& input) {
  std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
  unsigned int size = 0;
  EXPECT_EQ(EVP_Digest(input.data(), input.size(), digest.data(), &size,
                       EVP_sha256(), nullptr),
            1);
  EXPECT_EQ(size, 32u);
  return {reinterpret_cast<const char*>(digest.data()), size};
}

ExternalShuffleChunk chunk(
    int64_t sequence,
    const std::string& batch,
    uint32_t index,
    const std::string& checksum) {
  ExternalShuffleChunk result;
  result.batch_sequence = sequence;
  result.chunk_index = index;
  result.chunk_count = static_cast<uint32_t>(
      1 + (batch.size() - 1) / kExternalShuffleChunkBytes);
  result.batch_size_bytes = batch.size();
  const auto offset = static_cast<size_t>(index) * kExternalShuffleChunkBytes;
  const auto size =
      std::min(kExternalShuffleChunkBytes, batch.size() - offset);
  result.data.assign(batch.data() + offset, size);
  result.batch_checksum = checksum;
  return result;
}

TEST(ExternalShuffleIngressTest, ReassemblesCanonicalChunks) {
  const std::string batch(kExternalShuffleChunkBytes + 17, 'a');
  const auto checksum = sha256(batch);
  ExternalShuffleIngress ingress(
      4 * kExternalShuffleChunkBytes, 8 * kExternalShuffleChunkBytes,
      deadlineAfter(std::chrono::minutes(1)));

  ingress.push(chunk(0, batch, 0, checksum));
  ingress.push(chunk(0, batch, 1, checksum));
  ingress.finish(0);

  auto data = ingress.take(std::chrono::seconds(1));
  ASSERT_EQ(data.kind, ExternalShuffleIngress::ResultKind::kData);
  EXPECT_EQ(data.data, batch);
  EXPECT_EQ(ingress.take(std::chrono::seconds(1)).kind,
            ExternalShuffleIngress::ResultKind::kFinished);
}

TEST(ExternalShuffleIngressTest, RejectsNonCanonicalLayoutBeforeAssembly) {
  const std::string batch(kExternalShuffleChunkBytes + 1, 'b');
  const auto checksum = sha256(batch);
  ExternalShuffleIngress ingress(
      4 * kExternalShuffleChunkBytes, 8 * kExternalShuffleChunkBytes,
      deadlineAfter(std::chrono::minutes(1)));

  auto invalid_count = chunk(0, batch, 0, checksum);
  invalid_count.chunk_count = 3;
  EXPECT_THROW(ingress.push(std::move(invalid_count)), std::invalid_argument);

  auto invalid_size = chunk(0, batch, 0, checksum);
  invalid_size.data.pop_back();
  EXPECT_THROW(ingress.push(std::move(invalid_size)), std::invalid_argument);
}

TEST(ExternalShuffleIngressTest, CorruptAssemblyReleasesReservationForRetry) {
  const std::string batch(kExternalShuffleChunkBytes + 7, 'c');
  const auto checksum = sha256(batch);
  ExternalShuffleIngress ingress(
      2 * kExternalShuffleChunkBytes, 4 * kExternalShuffleChunkBytes,
      deadlineAfter(std::chrono::minutes(1)));

  ingress.push(chunk(0, batch, 0, checksum));
  auto corrupt = chunk(0, batch, 1, checksum);
  corrupt.batch_checksum = std::string(32, 'x');
  EXPECT_THROW(ingress.push(std::move(corrupt)), std::logic_error);

  EXPECT_NO_THROW(ingress.push(chunk(0, batch, 0, checksum)));
  EXPECT_NO_THROW(ingress.push(chunk(0, batch, 1, checksum)));
  EXPECT_EQ(ingress.take(std::chrono::seconds(1)).kind,
            ExternalShuffleIngress::ResultKind::kData);
}

TEST(ExternalShuffleIngressTest, MidBatchTerminalReleasesReservationForRetry) {
  const std::string batch(kExternalShuffleChunkBytes + 7, 't');
  const auto checksum = sha256(batch);
  ExternalShuffleIngress ingress(
      2 * kExternalShuffleChunkBytes, 4 * kExternalShuffleChunkBytes,
      deadlineAfter(std::chrono::minutes(1)));

  ingress.push(chunk(0, batch, 0, checksum));
  EXPECT_THROW(ingress.finish(0), std::logic_error);
  EXPECT_NO_THROW(ingress.push(chunk(0, batch, 0, checksum)));
  EXPECT_NO_THROW(ingress.push(chunk(0, batch, 1, checksum)));
  EXPECT_EQ(ingress.take(std::chrono::seconds(1)).kind,
            ExternalShuffleIngress::ResultKind::kData);
}

TEST(ExternalShuffleIngressTest, AppliesBackpressureUntilBufferedBatchIsTaken) {
  const std::string first(kExternalShuffleChunkBytes, 'd');
  const std::string second(kExternalShuffleChunkBytes, 'e');
  ExternalShuffleIngress ingress(
      kExternalShuffleChunkBytes, 2 * kExternalShuffleChunkBytes,
      deadlineAfter(std::chrono::minutes(1)));
  ingress.push(chunk(0, first, 0, sha256(first)));

  auto blocked = std::async(std::launch::async, [&] {
    ingress.push(chunk(1, second, 0, sha256(second)));
  });
  EXPECT_EQ(blocked.wait_for(std::chrono::milliseconds(20)),
            std::future_status::timeout);
  EXPECT_EQ(ingress.take(std::chrono::seconds(1)).kind,
            ExternalShuffleIngress::ResultKind::kData);
  EXPECT_EQ(blocked.wait_for(std::chrono::seconds(1)),
            std::future_status::ready);
  EXPECT_NO_THROW(blocked.get());
  EXPECT_EQ(ingress.take(std::chrono::seconds(1)).kind,
            ExternalShuffleIngress::ResultKind::kData);
}

TEST(ExternalShuffleIngressTest, CancelAndDeadlineReleaseIncompleteBatch) {
  const std::string batch(kExternalShuffleChunkBytes + 1, 'f');
  const auto checksum = sha256(batch);
  ExternalShuffleIngress cancelled(
      2 * kExternalShuffleChunkBytes, 4 * kExternalShuffleChunkBytes,
      deadlineAfter(std::chrono::minutes(1)));
  cancelled.push(chunk(0, batch, 0, checksum));
  cancelled.cancel();
  EXPECT_EQ(cancelled.take(std::chrono::seconds(1)).kind,
            ExternalShuffleIngress::ResultKind::kCancelled);

  ExternalShuffleIngress expired(
      2 * kExternalShuffleChunkBytes, 4 * kExternalShuffleChunkBytes,
      deadlineAfter(std::chrono::milliseconds(20)));
  expired.push(chunk(0, batch, 0, checksum));
  EXPECT_EQ(expired.take(std::chrono::seconds(1)).kind,
            ExternalShuffleIngress::ResultKind::kFailed);
}

}  // namespace
}  // namespace pangrova::velox
