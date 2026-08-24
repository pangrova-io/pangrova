// ShufflePartitionAccumulator: Coalesces small per-partition row sets.
#include "shuffle/ShufflePartitionAccumulator.h"

#include "velox/vector/BaseVector.h"

namespace pangrova::shuffle {

ShufflePartitionAccumulator::ShufflePartitionAccumulator(
    int32_t num_partitions,
    ShuffleBatchSerializer* writer,
    SerializedShuffleQueue* queue,
    std::chrono::milliseconds push_timeout)
    : num_partitions_(num_partitions),
      writer_(writer),
      queue_(queue),
      push_timeout_(push_timeout),
      partition_mutexes_(num_partitions),
      buffers_(num_partitions) {}

bool ShufflePartitionAccumulator::accumulate(
    int32_t partition_id,
    facebook::velox::RowVectorPtr source_vector,
    std::vector<facebook::velox::vector_size_t> row_indices) {
  if (partition_id < 0 || partition_id >= num_partitions_ ||
      !source_vector || row_indices.empty()) {
    return true;
  }
  // F-NATIVE-SHUF-03: Per-partition lock instead of global mutex
  std::lock_guard<std::mutex> lock(partition_mutexes_[partition_id]);
  auto& buf = buffers_[partition_id];
  buf.total_rows += static_cast<int64_t>(row_indices.size());
  buf.vectors.push_back(std::move(source_vector));
  buf.indices.push_back(std::move(row_indices));

  if (buf.total_rows >= kMinPartitionFlushRows) {
    return flushPartition(partition_id, buf.vectors.back()->pool());
  }
  return true;
}

bool ShufflePartitionAccumulator::flushPartition(
    int32_t partition_id,
    facebook::velox::memory::MemoryPool* pool) {
  auto& buf = buffers_[partition_id];
  if (buf.total_rows == 0) {
    return true;
  }

  // Fast path: single source vector — wrap children individually.
  // BaseVector::wrapInDictionary on a RowVector creates DictionaryVector<ComplexType>
  // which is NOT a RowVector subclass — dynamic_pointer_cast<RowVector> returns nullptr.
  if (buf.vectors.size() == 1) {
    const auto& rows = buf.indices[0];
    auto indices = facebook::velox::allocateIndices(rows.size(), pool);
    auto* raw = indices->asMutable<facebook::velox::vector_size_t>();
    for (size_t i = 0; i < rows.size(); ++i) {
      raw[i] = rows[i];
    }
    auto cnt = static_cast<facebook::velox::vector_size_t>(rows.size());
    std::vector<facebook::velox::VectorPtr> wrappedChildren(
        buf.vectors[0]->childrenSize());
    for (size_t i = 0; i < buf.vectors[0]->childrenSize(); ++i) {
      wrappedChildren[i] = facebook::velox::BaseVector::wrapInDictionary(
          nullptr, indices, cnt, buf.vectors[0]->childAt(i));
    }
    auto subVector = std::make_shared<facebook::velox::RowVector>(
        pool, buf.vectors[0]->type(), nullptr, cnt,
        std::move(wrappedChildren));
    auto serialized = writer_->serialize(subVector, pool);
    ShuffleBatch batch;
    batch.partition_id = partition_id;
    batch.data = std::move(serialized);
    batch.row_count = buf.total_rows;
    buf = PartitionBuffer{};
    return queue_->push(std::move(batch), push_timeout_);
  }

  // Multi-vector path: serialize each source's rows separately and
  // chain the IOBufs. PrestoVectorSerde produces self-contained pages,
  // so chaining is safe — the downstream deserializer reads page by page.
  std::unique_ptr<folly::IOBuf> chain;
  int64_t totalRows = 0;
  for (size_t v = 0; v < buf.vectors.size(); ++v) {
    const auto& rows = buf.indices[v];
    if (rows.empty()) continue;
    auto* vpool = buf.vectors[v]->pool();
    auto indices = facebook::velox::allocateIndices(rows.size(), vpool);
    auto* raw = indices->asMutable<facebook::velox::vector_size_t>();
    for (size_t i = 0; i < rows.size(); ++i) {
      raw[i] = rows[i];
    }
    auto cnt = static_cast<facebook::velox::vector_size_t>(rows.size());
    std::vector<facebook::velox::VectorPtr> wrappedChildren(
        buf.vectors[v]->childrenSize());
    for (size_t ci = 0; ci < buf.vectors[v]->childrenSize(); ++ci) {
      wrappedChildren[ci] = facebook::velox::BaseVector::wrapInDictionary(
          nullptr, indices, cnt, buf.vectors[v]->childAt(ci));
    }
    auto subVector = std::make_shared<facebook::velox::RowVector>(
        vpool, buf.vectors[v]->type(), nullptr, cnt,
        std::move(wrappedChildren));
    auto serialized = writer_->serialize(subVector, vpool);
    totalRows += static_cast<int64_t>(rows.size());
    if (!chain) {
      chain = std::move(serialized);
    } else {
      chain->prependChain(std::move(serialized));
    }
  }

  ShuffleBatch batch;
  batch.partition_id = partition_id;
  batch.data = std::move(chain);
  batch.row_count = totalRows;
  buf = PartitionBuffer{};
  return queue_->push(std::move(batch), push_timeout_);
}

bool ShufflePartitionAccumulator::flushAll(
    facebook::velox::memory::MemoryPool* pool) {
  for (int32_t pid = 0; pid < num_partitions_; ++pid) {
    std::lock_guard<std::mutex> lock(partition_mutexes_[pid]);
    if (buffers_[pid].total_rows > 0) {
      auto* flush_pool = pool;
      // Prefer the pool from the last accumulated vector if available.
      if (!buffers_[pid].vectors.empty() &&
          buffers_[pid].vectors.back()->pool()) {
        flush_pool = buffers_[pid].vectors.back()->pool();
      }
      if (!flushPartition(pid, flush_pool)) {
        return false;
      }
    }
  }
  return true;
}

}  // namespace pangrova::shuffle
