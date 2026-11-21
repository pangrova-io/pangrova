// ShuffleBatchSerializer: standard Arrow stream shuffle serialization.
// See ShuffleBatchSerializer.h for class documentation.

#include "engine/shuffle/ShuffleBatchSerializer.h"
#include "engine/velox/serde/CanonicalResultNullability.h"

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
  VELOX_USER_CHECK_GT(config_.batch_size, 0, "Shuffle batch row limit must be positive");
  VELOX_USER_CHECK_GE(config_.compression_min_bytes, 0, "Shuffle compression threshold must be non-negative");
  VELOX_USER_CHECK_GT(
      config_.max_serialized_batch_bytes, kSerializedPageOverheadBytes,
      "Shuffle batch limit must exceed serialization overhead");
  VELOX_USER_CHECK_LE(
      config_.max_serialized_batch_bytes, kCanonicalShuffleBatchBytes,
      "Shuffle serializer limit exceeds canonical Flight page size");
  VELOX_USER_CHECK_NOT_NULL(config_.canonical_schema,
      "Shuffle serialization requires its admitted canonical schema");
  velox::admittedCanonicalResultType(*config_.canonical_schema);
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

  // Serializer ownership is scoped to this call; no driver-local cache retains
  // a raw pool pointer after its task has terminated.
  auto effective_compression = compression_kind_;
  const auto selected = std::dynamic_pointer_cast<facebook::velox::RowVector>(vector->slice(begin, size));
  const auto estimated_bytes = selected->estimateFlatSize();
  if (config_.compression_min_bytes > 0 &&
      estimated_bytes < static_cast<uint64_t>(config_.compression_min_bytes)) {
    effective_compression = facebook::velox::common::CompressionKind::
        CompressionKind_NONE;
  }

  pangrova::velox::ArrowBatchVectorSerde::StreamOptions options;
  options.compressionKind = effective_compression;
  options.maximum_bytes = config_.max_serialized_batch_bytes;
  options.canonical_schema = config_.canonical_schema;
  auto serializer = serde_.createBatchSerializer(pool, &options);

  facebook::velox::IndexRange range{
      begin, size};
  std::vector<facebook::velox::IndexRange> ranges = {range};

  auto outputStream =
      std::make_unique<facebook::velox::IOBufOutputStream>(*pool);

  serializer->serialize(
      vector,
      folly::Range<const facebook::velox::IndexRange*>(
          ranges.data(), ranges.size()),
      outputStream.get());

  return outputStream->getIOBuf();
}

std::vector<uint64_t> ShuffleBatchSerializer::estimateRowBytes(
    const facebook::velox::RowVectorPtr& vector) {
  const auto average = vector->estimateFlatSize() / std::max(1, vector->size());
  // A planning hint only: compressed output size is checked by the bounded
  // Arrow sink, not inferred from a row's uncompressed representation.
  const auto budget = config_.max_serialized_batch_bytes - kSerializedPageOverheadBytes;
  return std::vector<uint64_t>(vector->size(), std::min<uint64_t>(average, budget));
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
    for (facebook::velox::vector_size_t offset = 0; offset < size;) {
      const auto count = std::min(config_.batch_size, size - offset);
      serializeBoundedRange(vector, begin + offset, count, pool, result);
      offset += count;
    }
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
  try {
    auto data = serializeRange(vector, begin, size, pool);
    output.push_back({std::move(data), size});
    return;
  } catch (const pangrova::velox::ArrowBatchWireLimitException&) {
    VELOX_USER_CHECK_GT(size, 1,
        "SHUFFLE_ROW_EXCEEDS_BATCH_LIMIT: complete Arrow row exceeds the {} byte batch limit",
        config_.max_serialized_batch_bytes);
  }
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

  velox::ArrowBatchVectorSerde::StreamOptions options;
  options.maximum_bytes = static_cast<int64_t>(config_.max_serialized_batch_bytes);
  options.canonical_schema = config_.canonical_schema;
  options.compressionKind = resolveCompressionKind(config_.compression_codec);

  facebook::velox::RowVectorPtr result;
  serde_.deserialize(byteStream.get(), pool, type, &result, &options);

  return result;
}

}  // namespace pangrova::shuffle
