// ShuffleBatchSerializer: PrestoVectorSerde-based shuffle serialization.
// See ShuffleBatchSerializer.h for class documentation.

#include "shuffle/ShuffleBatchSerializer.h"

#include <folly/io/IOBuf.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/ByteStream.h"

namespace pangrova::shuffle {

namespace {

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

ShuffleBatchSerializer::ShuffleBatchSerializer(const Config& config)
    : config_(config),
      compression_kind_(resolveCompressionKind(config.compression_codec)) {}

std::unique_ptr<folly::IOBuf> ShuffleBatchSerializer::serialize(
    const facebook::velox::RowVectorPtr& vector,
    facebook::velox::memory::MemoryPool* pool) {
  VELOX_CHECK_NOT_NULL(vector, "Input vector must not be null");
  VELOX_CHECK_NOT_NULL(pool, "Memory pool must not be null");

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

  auto numRows = vector->size();
  facebook::velox::IndexRange range{
      0, static_cast<facebook::velox::vector_size_t>(numRows)};
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
