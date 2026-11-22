// ShufflePartitionAccumulator: Coalesces small per-partition row sets.
#include "engine/shuffle/ShufflePartitionAccumulator.h"

#include "velox/vector/BaseVector.h"

namespace pangrova::shuffle {
namespace {

using facebook::velox::BaseVector;
using facebook::velox::RowVector;
using facebook::velox::RowVectorPtr;
using facebook::velox::vector_size_t;

RowVectorPtr selectPartitionRows(const RowVectorPtr& source, const std::vector<vector_size_t>& rows) {
  auto* pool = source->pool();
  auto indices = facebook::velox::allocateIndices(rows.size(), pool);
  std::copy(rows.begin(), rows.end(), indices->asMutable<vector_size_t>());
  auto count = static_cast<vector_size_t>(rows.size());
  std::vector<facebook::velox::VectorPtr> children(source->childrenSize());
  for (size_t column = 0; column < source->childrenSize(); ++column) {
    children[column] = BaseVector::wrapInDictionary(nullptr, indices, count, source->childAt(column));
  }
  return std::make_shared<RowVector>(pool, source->type(), nullptr, count, std::move(children));
}

bool fitsCoalescingBudget(const std::vector<RowVectorPtr>& sources, int64_t rows, size_t limit) {
  if (sources.size() <= 1 || rows > 2 * kMinPartitionFlushRows || limit <= kSerializedPageOverheadBytes) {
    return false;
  }
  auto bytes_remaining = limit - kSerializedPageOverheadBytes;
  for (const auto& source : sources) {
    const auto estimated = source->estimateFlatSize();
    if (estimated > bytes_remaining) return false;
    bytes_remaining -= estimated;
  }
  return true;
}

RowVectorPtr coalescePartitionRows(const std::vector<RowVectorPtr>& sources,
    const std::vector<std::vector<vector_size_t>>& selections, vector_size_t count,
    facebook::velox::memory::MemoryPool* pool) {
  auto result = BaseVector::create<RowVector>(sources.front()->type(), count, pool);
  vector_size_t target = 0;
  std::vector<BaseVector::CopyRange> ranges;
  for (size_t source = 0; source < sources.size(); ++source) {
    VELOX_USER_CHECK(*sources[source]->type() == *result->type(), "Shuffle partition source schema changed");
    ranges.clear();
    for (const auto row : selections[source]) {
      BaseVector::CopyRange next{row, target, 1};
      if (!ranges.empty() && ranges.back().mergeable(next)) {
        ++ranges.back().count;
      } else {
        ranges.push_back(next);
      }
      ++target;
    }
    result->copyRanges(sources[source].get(), folly::Range<const BaseVector::CopyRange*>(ranges.data(), ranges.size()));
  }
  return result;
}

}  // namespace

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

  const auto payload_limit = queue_->maximumPayloadBytes();
  if (fitsCoalescingBudget(buf.vectors, buf.total_rows, payload_limit)) {
    auto combined = coalescePartitionRows(buf.vectors, buf.indices,
        static_cast<vector_size_t>(buf.total_rows), pool);
    auto pages = writer_->serializeBatches(combined, pool, payload_limit);
    combined.reset();
    buf = PartitionBuffer{};
    return pushPages(partition_id, std::move(pages));
  }

  for (size_t v = 0; v < buf.vectors.size(); ++v) {
    auto selected = selectPartitionRows(buf.vectors[v], buf.indices[v]);
    auto pages = writer_->serializeBatches(selected, selected->pool());
    selected.reset();
    if (!pushPages(partition_id, std::move(pages))) {
      buf = PartitionBuffer{};
      return false;
    }
  }
  buf = PartitionBuffer{};
  return true;
}

bool ShufflePartitionAccumulator::pushPages(int32_t partition_id, std::vector<SerializedShuffleSlice> pages) {
  for (auto& page : pages) {
    if (!queue_->push(ShuffleBatch{partition_id, std::move(page.data), page.row_count}, push_timeout_)) {
      return false;
    }
  }
  return true;
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
