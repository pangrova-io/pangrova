// ShuffleBatchSerializer: Serializes complete standard Arrow streams for shuffle.
//
// Placement: shuffle/ — Native shuffle serialization components.
#pragma once

#include <memory>
#include <string>
#include <vector>

#include <folly/io/IOBuf.h>

#include "shuffle/ShuffleTransferLimits.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/common/compression/Compression.h"
#include "velox/serde/ArrowBatchVectorSerde.h"
#include "velox/vector/ComplexVector.h"

namespace pangrova::shuffle {

inline constexpr size_t kMaxCanonicalShuffleBatchBytes =
    kCanonicalShuffleBatchBytes;

struct SerializedShuffleSlice {
  std::unique_ptr<folly::IOBuf> data;
  int64_t row_count = 0;
};

std::vector<size_t> planShuffleBatchRowCounts(
    const std::vector<uint64_t>& estimated_row_bytes,
    size_t max_batch_bytes);

// Shares the sole VectorSerde implementation with result and Exchange.
//
// Memory allocation uses the caller-provided MemoryPool, which
// participates in SharedArbitrator governance via PangrovaArbitrator.
class ShuffleBatchSerializer {
 public:
  struct Config {
    std::string compression_codec = "none";  // lz4, zstd, none
    int32_t batch_size = 10000;
    // Minimum estimated selected-vector size (bytes) to apply compression.
    // Partitions smaller than this skip compression to avoid CPU waste
    // on tiny buffers that compress poorly (overhead > savings).
    int64_t compression_min_bytes = 4096;  // 4KB
    size_t max_serialized_batch_bytes = kMaxCanonicalShuffleBatchBytes;
    std::shared_ptr<const substrait::NamedStruct> canonical_schema;
  };

  explicit ShuffleBatchSerializer(const Config& config);

  // Non-copyable, non-movable.
  ShuffleBatchSerializer(const ShuffleBatchSerializer&) = delete;
  ShuffleBatchSerializer& operator=(const ShuffleBatchSerializer&) = delete;

  // Serialize a RowVector to a complete standard Arrow stream.
  // Returns compressed bytes ready for cross-process transfer.
  std::unique_ptr<folly::IOBuf> serialize(
      const facebook::velox::RowVectorPtr& vector,
      facebook::velox::memory::MemoryPool* pool);

  // Serializes into complete pages bounded by max_serialized_batch_bytes.
  // A single row that cannot fit fails with SHUFFLE_ROW_EXCEEDS_BATCH_LIMIT.
  std::vector<SerializedShuffleSlice> serializeBatches(
      const facebook::velox::RowVectorPtr& vector,
      facebook::velox::memory::MemoryPool* pool);

  // Deserialize Arrow bytes back to RowVector. The shared serde performs a
  // governed contiguous-input copy before importing Arrow buffer ownership.
  facebook::velox::RowVectorPtr deserialize(
      folly::IOBuf&& data,
      const facebook::velox::RowTypePtr& type,
      facebook::velox::memory::MemoryPool* pool);

 private:
  std::unique_ptr<folly::IOBuf> serializeRange(
      const facebook::velox::RowVectorPtr& vector,
      facebook::velox::vector_size_t begin,
      facebook::velox::vector_size_t size,
      facebook::velox::memory::MemoryPool* pool);

  void serializeBoundedRange(
      const facebook::velox::RowVectorPtr& vector,
      facebook::velox::vector_size_t begin,
      facebook::velox::vector_size_t size,
      facebook::velox::memory::MemoryPool* pool,
      std::vector<SerializedShuffleSlice>& output);

  std::vector<uint64_t> estimateRowBytes(
      const facebook::velox::RowVectorPtr& vector);

  const Config config_;
  const facebook::velox::common::CompressionKind compression_kind_;
  pangrova::velox::ArrowBatchVectorSerde serde_;
};

}  // namespace pangrova::shuffle
