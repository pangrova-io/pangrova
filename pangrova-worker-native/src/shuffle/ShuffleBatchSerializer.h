// ShuffleBatchSerializer: Serializes columnar batches to PrestoVectorSerde
// format for cross-process shuffle transfer. Supports LZ4/ZSTD/None compression.
//
// Placement: shuffle/ — Native shuffle serialization components.
#pragma once

#include <memory>
#include <string>

#include <folly/io/IOBuf.h>

#include "velox/common/memory/MemoryPool.h"
#include "velox/common/compression/Compression.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/ComplexVector.h"

namespace pangrova::shuffle {

// Serializes columnar batches to PrestoVectorSerde format bytes.
// Uses the registered PrestoVectorSerde for serialization,
// bypassing intermediate conversion layers.
//
// Memory allocation uses the caller-provided MemoryPool, which
// participates in SharedArbitrator governance via PangrovaArbitrator.
class ShuffleBatchSerializer {
 public:
  struct Config {
    std::string compression_codec = "none";  // lz4, zstd, none
    int32_t batch_size = 10000;
    // L4-1B: Minimum serialized size (bytes) to apply compression.
    // Partitions smaller than this skip compression to avoid CPU waste
    // on tiny buffers that compress poorly (overhead > savings).
    int64_t compression_min_bytes = 4096;  // 4KB
  };

  explicit ShuffleBatchSerializer(const Config& config);

  // Non-copyable, non-movable.
  ShuffleBatchSerializer(const ShuffleBatchSerializer&) = delete;
  ShuffleBatchSerializer& operator=(const ShuffleBatchSerializer&) = delete;

  // Serialize a RowVector to PrestoVectorSerde format bytes.
  // Returns compressed bytes ready for cross-process transfer.
  std::unique_ptr<folly::IOBuf> serialize(
      const facebook::velox::RowVectorPtr& vector,
      facebook::velox::memory::MemoryPool* pool);

  // Deserialize PrestoVectorSerde bytes back to RowVector.
  // Takes ownership of the IOBuf to avoid a copy.
  facebook::velox::RowVectorPtr deserialize(
      folly::IOBuf&& data,
      const facebook::velox::RowTypePtr& type,
      facebook::velox::memory::MemoryPool* pool);

 private:
  const Config config_;
  const facebook::velox::common::CompressionKind compression_kind_;
  facebook::velox::serializer::presto::PrestoVectorSerde serde_;

  // L2-8 / PERF-T1: Per-thread cached BatchSerializer via thread_local.
  // PrestoVectorSerde::BatchSerializer is stateless between calls — it only
  // holds the pool and options, so it is safe to reuse across multiple
  // serialize() invocations with the same pool.
  // Previous design used a single mutex to protect a shared cached_serializer_,
  // causing all Velox Driver threads to serialize sequentially.
  // Now each thread owns its own serializer instance via thread_local,
  // eliminating cross-thread lock contention entirely.
  struct ThreadLocalSerializer {
    std::unique_ptr<facebook::velox::BatchVectorSerializer> serializer;
    facebook::velox::memory::MemoryPool* pool = nullptr;
    facebook::velox::common::CompressionKind compression =
        facebook::velox::common::CompressionKind::CompressionKind_NONE;
  };
};

}  // namespace pangrova::shuffle
