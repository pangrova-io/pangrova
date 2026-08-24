// SerializedShuffleQueue: Bounded queue for pre-serialized shuffle batches.
// Replaces ShuffleQueue<RecordBatch> with serialized PrestoVectorSerde bytes,
// eliminating Arrow C Data export overhead on the JNI boundary.
//
// Placement: shuffle/ — Native shuffle serialization components.
#pragma once

#include <cstdint>
#include <memory>

#include <folly/io/IOBuf.h>

#include "executor/queue/OutputQueue.h"

namespace pangrova::shuffle {

// A single serialized shuffle batch ready for cross-process transfer.
// Contains PrestoVectorSerde-compressed bytes for one partition.
struct ShuffleBatch {
  int32_t partition_id;
  std::unique_ptr<folly::IOBuf> data;
  int64_t row_count;
};

// Bounded blocking queue for serialized shuffle batches.
// Reuses BoundedQueue from executor/queue/ for thread-safe push/pop.
using SerializedShuffleQueue = pangrova::executor::BoundedQueue<ShuffleBatch>;

}  // namespace pangrova::shuffle
