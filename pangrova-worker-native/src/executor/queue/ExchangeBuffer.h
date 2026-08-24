#pragma once

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "velox/vector/ComplexVector.h"

namespace pangrova::executor {

// Thread-safe per-source exchange buffer for Velox Exchange operator.
// Java/JNI thread pushes RowVectors via addBatch(); Velox execution thread
// pulls via pollBatch(). Supports source-level completion and global
// "no more sources" signaling.
//
// Stores RowVectorPtr directly — no intermediate arrow::RecordBatch
// conversion. Data stays in Velox-native format from JNI import to
// Velox operator consumption.
class ExchangeBuffer {
 public:
  explicit ExchangeBuffer(size_t per_source_capacity = 64);

  // Push a RowVector for the given source. Returns false if source is
  // already marked complete or buffer is closed.
  bool addBatch(const std::string& source_id,
                facebook::velox::RowVectorPtr row_vector);

  // Pull next RowVector from any source. Returns nullptr when all sources
  // are complete and drained, or on timeout.
  facebook::velox::RowVectorPtr pollBatch(
      std::chrono::milliseconds timeout);

  // Mark a single source as complete — no more batches from it.
  void markSourceComplete(const std::string& source_id);

  // Signal that no additional sources will be added.
  void markNoMoreSources();

  // True when all sources are complete, drained, and no more sources.
  bool isFinished() const;

  // Total buffered batch count across all sources.
  size_t totalBuffered() const;

  // Number of registered sources.
  size_t sourceCount() const;

  // Close buffer — unblock all waiters.
  void close();

 private:
  bool allSourcesDrained() const;

  const size_t per_source_capacity_;

  mutable std::mutex mutex_;
  std::condition_variable not_empty_;

  // Per-source FIFO queues storing Velox RowVectors directly.
  std::unordered_map<std::string,
                     std::queue<facebook::velox::RowVectorPtr>>
      buffers_;

  // P3-BUG-23: Ordered source keys for round-robin polling fairness.
  // unordered_map iteration order is hash-dependent and fixed, which can
  // starve sources whose keys hash to later positions.
  std::vector<std::string> source_order_;
  size_t poll_index_{0};  // Round-robin cursor for pollBatch().

  // Sources that have been marked complete.
  std::unordered_set<std::string> completed_sources_;

  bool no_more_sources_ = false;
  bool closed_ = false;
};

}  // namespace pangrova::executor
