// ShuffleBatchSerializer: PrestoVectorSerde-based shuffle serialization.
// See ShuffleBatchSerializer.h for class documentation.

#include "shuffle/ShuffleBatchSerializer.h"

#include <algorithm>
#include <folly/io/IOBuf.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/ByteStream.h"

namespace pangrova::shuffle {

namespace {

constexpr size_t kSerializedPageOverheadBytes = 64 * 1024;

// Map compression codec string to CompressionKind.
facebook::velox::common::CompressionKind resolveCompressionKind(
    const std::string& codec) {
  if (codec == "lz4") {
    return facebook::velox::common::CompressionKind::
        CompressionKind_LZ4;
  }
  if (codec == "zstd") {
    return facebook::velox::common::CompressionKind::
        CompressionKind_ZSTD;
  }
  if (codec == "none") {
    return facebook::velox::common::CompressionKind::
        CompressionKind_NONE;
  }
  VELOX_USER_FAIL(
      "Unsupported shuffle compression codec: {}. "
      "Supported: lz4, zstd, none",
      codec);
}

}  // namespace

std::vector<size_t> planShuffleBatchRowCounts(
    const std::vector<uint64_t>& estimated_row_bytes,
    size_t max_batch_bytes) {
  VELOX_USER_CHECK_GT(
      max_batch_bytes, kSerializedPageOverheadBytes,
      "Shuffle batch limit must exceed serialization overhead");
  std::vector<size_t> result;
  size_t current_rows = 0;
  uint64_t current_bytes = kSerializedPageOverheadBytes;
  for (const auto row_bytes : estimated_row_bytes) {
    VELOX_USER_CHECK_LE(
        row_bytes,
        max_batch_bytes - kSerializedPageOverheadBytes,
        "SHUFFLE_ROW_EXCEEDS_BATCH_LIMIT: estimated row bytes {} exceed "
        "the {} byte batch payload budget",
        row_bytes, max_batch_bytes - kSerializedPageOverheadBytes);
    if (current_rows > 0 && row_bytes > max_batch_bytes - current_bytes) {
      result.push_back(current_rows);
      current_rows = 0;
      current_bytes = kSerializedPageOverheadBytes;
    }
    ++current_rows;
    current_bytes += row_bytes;
  }
  if (current_rows > 0) {
    result.push_back(current_rows);
  }
  return result;
}

ShuffleBatchSerializer::ShuffleBatchSerializer(const Config& config)
    : config_(config),
      compression_kind_(resolveCompressionKind(config.compression_codec)) {
  VELOX_USER_CHECK_GT(
      config_.max_serialized_batch_bytes, kSerializedPageOverheadBytes,
      "Shuffle batch limit must exceed serialization overhead");
  VELOX_USER_CHECK_LE(
      config_.max_serialized_batch_bytes, kCanonicalShuffleBatchBytes,
      "Shuffle serializer limit exceeds canonical Flight page size");
}

std::unique_ptr<folly::IOBuf> ShuffleBatchSerializer::serialize(
    const facebook::velox::RowVectorPtr& vector,
    facebook::velox::memory::MemoryPool* pool) {
  VELOX_CHECK_NOT_NULL(vector, "Input vector must not be null");
  VELOX_CHECK_NOT_NULL(pool, "Memory pool must not be null");

  return serializeRange(vector, 0, vector->size(), pool);
}

std::unique_ptr<folly::IOBuf> ShuffleBatchSerializer::serializeRange(
    const facebook::velox::RowVectorPtr& vector,
    facebook::velox::vector_size_t begin,
    facebook::velox::vector_size_t size,
    facebook::velox::memory::MemoryPool* pool) {

  // PERF-T1: Use thread_local cached serializer to eliminate cross-thread
  // lock contention. Each Velox Driver thread owns its own serializer
  // instance, so no synchronization is needed.
  static thread_local ThreadLocalSerializer tls_serializer;

  // L4-1B: Determine effective compression. Skip compression for small
  // vectors where CPU cost exceeds network savings (tiny buffers typically
  // expand under compression due to framing overhead).
  auto effective_compression = compression_kind_;
  auto estimated_bytes = vector->estimateFlatSize();
  if (config_.compression_min_bytes > 0 &&
      estimated_bytes < static_cast<uint64_t>(config_.compression_min_bytes)) {
    effective_compression = facebook::velox::common::CompressionKind::
        CompressionKind_NONE;
  }

  // L2-8: Reuse the cached BatchSerializer instance when the pool and
  // compression match. createBatchSerializer() allocates internal scratch
  // buffers; reusing avoids that overhead on every serialize() call.
  if (!tls_serializer.serializer || tls_serializer.pool != pool ||
      tls_serializer.compression != effective_compression) {
    facebook::velox::serializer::presto::PrestoVectorSerde::PrestoOptions
        options;
    options.compressionKind = effective_compression;
    options.useLosslessTimestamp = true;
    tls_serializer.serializer = serde_.createBatchSerializer(pool, &options);
    tls_serializer.pool = pool;
    tls_serializer.compression = effective_compression;
  }

  facebook::velox::IndexRange range{
      begin, size};
  std::vector<facebook::velox::IndexRange> ranges = {range};

  auto outputStream =
      std::make_unique<facebook::velox::IOBufOutputStream>(*pool);

  tls_serializer.serializer->serialize(
      vector,
      folly::Range<const facebook::velox::IndexRange*>(
          ranges.data(), ranges.size()),
      outputStream.get());

  return outputStream->getIOBuf();
}

std::vector<uint64_t> ShuffleBatchSerializer::estimateRowBytes(
    const facebook::velox::RowVectorPtr& vector) {
  std::vector<facebook::velox::IndexRange> ranges;
  ranges.reserve(vector->size());
  std::vector<facebook::velox::vector_size_t> estimates(vector->size(), 0);
  std::vector<facebook::velox::vector_size_t*> estimate_pointers;
  estimate_pointers.reserve(vector->size());
  for (facebook::velox::vector_size_t row = 0; row < vector->size(); ++row) {
    ranges.push_back({row, 1});
    estimate_pointers.push_back(&estimates[row]);
  }
  facebook::velox::Scratch scratch;
  serde_.estimateSerializedSize(
      vector.get(),
      folly::Range<const facebook::velox::IndexRange*>(
          ranges.data(), ranges.size()),
      estimate_pointers.data(), scratch);
  std::vector<uint64_t> result;
  result.reserve(estimates.size());
  for (const auto estimate : estimates) {
    VELOX_USER_CHECK_GE(
        estimate, 0, "Shuffle serializer produced a negative row estimate");
    result.push_back(static_cast<uint64_t>(estimate));
  }
  return result;
}

std::vector<SerializedShuffleSlice> ShuffleBatchSerializer::serializeBatches(
    const facebook::velox::RowVectorPtr& vector,
    facebook::velox::memory::MemoryPool* pool) {
  VELOX_CHECK_NOT_NULL(vector, "Input vector must not be null");
  VELOX_CHECK_NOT_NULL(pool, "Memory pool must not be null");
  if (vector->size() == 0) {
    return {};
  }
  const auto row_counts = planShuffleBatchRowCounts(
      estimateRowBytes(vector), config_.max_serialized_batch_bytes);
  std::vector<SerializedShuffleSlice> result;
  result.reserve(row_counts.size());
  facebook::velox::vector_size_t begin = 0;
  for (const auto row_count : row_counts) {
    const auto size =
        static_cast<facebook::velox::vector_size_t>(row_count);
    serializeBoundedRange(vector, begin, size, pool, result);
    begin += size;
  }
  return result;
}

void ShuffleBatchSerializer::serializeBoundedRange(
    const facebook::velox::RowVectorPtr& vector,
    facebook::velox::vector_size_t begin,
    facebook::velox::vector_size_t size,
    facebook::velox::memory::MemoryPool* pool,
    std::vector<SerializedShuffleSlice>& output) {
  auto data = serializeRange(vector, begin, size, pool);
  const auto serialized_bytes = data->computeChainDataLength();
  if (serialized_bytes <= config_.max_serialized_batch_bytes) {
    output.push_back({std::move(data), size});
    return;
  }
  VELOX_USER_CHECK_GT(
      size, 1,
      "SHUFFLE_ROW_EXCEEDS_BATCH_LIMIT: serialized row bytes {} exceed the "
      "{} byte batch limit",
      serialized_bytes, config_.max_serialized_batch_bytes);
  data.reset();
  const auto first_size = size / 2;
  serializeBoundedRange(vector, begin, first_size, pool, output);
  serializeBoundedRange(
      vector, begin + first_size, size - first_size, pool, output);
}

facebook::velox::RowVectorPtr ShuffleBatchSerializer::deserialize(
    folly::IOBuf&& data,
    const facebook::velox::RowTypePtr& type,
    facebook::velox::memory::MemoryPool* pool) {
  VELOX_CHECK_NOT_NULL(pool, "Memory pool must not be null");

  auto byteStream = std::make_unique<facebook::velox::BufferInputStream>(
      facebook::velox::byteRangesFromIOBuf(&data));

  facebook::velox::serializer::presto::PrestoVectorSerde::PrestoOptions
      options;
  options.compressionKind = resolveCompressionKind(config_.compression_codec);
  options.useLosslessTimestamp = true;

  facebook::velox::RowVectorPtr result;
  serde_.deserialize(byteStream.get(), pool, type, &result, &options);

  return result;
}

}  // namespace pangrova::shuffle
