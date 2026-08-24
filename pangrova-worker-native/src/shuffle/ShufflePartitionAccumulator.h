// ShufflePartitionAccumulator: Coalesces small per-partition row sets across
// multiple consumer callback invocations before serializing. Reduces
// PrestoVectorSerde serialization overhead when partition count is high
// (200+) and each callback batch yields only a few rows per partition.
//
// Thread-safe: consumer callbacks may be invoked from multiple Velox Driver
// threads concurrently. All public methods are guarded by mutex_.
//
// Placement: shuffle/ — Native shuffle serialization components.
#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include "shuffle/SerializedShuffleQueue.h"
#include "shuffle/ShuffleBatchSerializer.h"
#include "velox/vector/ComplexVector.h"

namespace pangrova::shuffle {

// Minimum rows per partition before flushing. Below this threshold,
// rows are accumulated to amortize PrestoVectorSerde fixed overhead.
inline constexpr int32_t kMinPartitionFlushRows = 128;

class ShufflePartitionAccumulator {
 public:
  ShufflePartitionAccumulator(
      int32_t num_partitions,
      ShuffleBatchSerializer* writer,
      SerializedShuffleQueue* queue,
      std::chrono::milliseconds push_timeout);

  // Accumulate rows for a partition. Flushes automatically when the
  // partition's row count exceeds kMinPartitionFlushRows.
  // Returns false if a flush push timed out (caller should cancel task).
  bool accumulate(
      int32_t partition_id,
      facebook::velox::RowVectorPtr source_vector,
      std::vector<facebook::velox::vector_size_t> row_indices);

  // Flush all remaining accumulated rows. Called at task completion.
  // Returns false if any push timed out.
  bool flushAll(facebook::velox::memory::MemoryPool* pool);

 private:
  // Serialize and push a single partition's accumulated rows.
  // Caller must hold mutex_.
  bool flushPartition(int32_t partition_id,
                      facebook::velox::memory::MemoryPool* pool);

  // Per-partition accumulation state.
  struct PartitionBuffer {
    // Source vectors and their corresponding row indices.
    // Multiple vectors may be accumulated when rows come from
    // different consumer callback invocations.
    std::vector<facebook::velox::RowVectorPtr> vectors;
    std::vector<std::vector<facebook::velox::vector_size_t>> indices;
    int64_t total_rows = 0;
  };

  const int32_t num_partitions_;
  ShuffleBatchSerializer* writer_;
  SerializedShuffleQueue* queue_;
  std::chrono::milliseconds push_timeout_;
  // F-NATIVE-SHUF-03: Per-partition locks replace the single global mutex
  // to allow concurrent serialization across different partitions.
  std::vector<std::mutex> partition_mutexes_;
  std::vector<PartitionBuffer> buffers_;
};

}  // namespace pangrova::shuffle
