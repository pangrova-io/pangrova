#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <vector>

#include "velox/vector/ComplexVector.h"

namespace pangrova::executor {

// P1-QUEUE-1: Result of a pop operation — distinguishes data, timeout, and closed.
enum class PopResult {
  DATA,     // Item was successfully dequeued.
  TIMEOUT,  // Wait timed out, queue may still have future items.
  CLOSED    // Queue is closed and empty — no more items will arrive.
};

// Bounded blocking queue using std::queue + condition_variable.
// Thread-safe: multiple producers and consumers supported.
// close() signals no more items; subsequent push returns false,
// pop returns CLOSED once the queue is drained.
//
// Supports non-blocking backpressure via registerSpaceCallback():
// when a push fails because the queue is full, the caller can register
// a callback that will be invoked when space becomes available. This
// enables Velox Consumer callbacks to return kWaitForConsumer with a
// ContinueFuture instead of busy-waiting.
template <typename T>
class BoundedQueue {
 public:
  explicit BoundedQueue(size_t capacity) : capacity_(capacity) {}

  // Push item with timeout. Returns false if closed or timed out.
  bool push(T item, std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mutex_);
    bool ok = not_full_.wait_for(lock, timeout, [this] {
      return closed_ || queue_.size() < capacity_;
    });
    if (!ok || closed_) {
      return false;
    }
    queue_.push(std::move(item));
    not_empty_.notify_one();
    return true;
  }

  // Try to push without blocking. Returns false if closed or full.
  bool tryPush(T item) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (closed_ || queue_.size() >= capacity_) {
      return false;
    }
    queue_.push(std::move(item));
    not_empty_.notify_one();
    return true;
  }

  // Pop item with timeout. Returns PopResult to distinguish timeout from closed.
  PopResult pop(T& item, std::chrono::milliseconds timeout) {
    std::vector<std::function<void()>> cbs;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      bool ok = not_empty_.wait_for(lock, timeout, [this] {
        return closed_ || !queue_.empty();
      });
      if (!ok) {
        return PopResult::TIMEOUT;
      }
      if (queue_.empty()) {
        return PopResult::CLOSED;
      }
      item = std::move(queue_.front());
      queue_.pop();
      not_full_.notify_one();
      // Drain space-available callbacks registered by non-blocking producers
      // (Velox Consumer backpressure). Callbacks are one-shot: drained here
      // and invoked outside the lock to avoid deadlock with Velox internals.
      cbs.swap(space_callbacks_);
    }
    for (auto& cb : cbs) {
      cb();
    }
    return PopResult::DATA;
  }

  size_t size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.size();
  }

  // F-NATIVE-EXEC-08: Expose capacity for queue depth metrics.
  size_t capacity() const { return capacity_; }

  // Register a one-shot callback invoked when space becomes available.
  // Used by Velox Consumer callbacks to fulfill a ContinuePromise when
  // the queue transitions from full to not-full, enabling the Driver
  // thread to resume without busy-waiting.
  //
  // If the queue already has space or is closed, the callback is invoked
  // immediately (outside the lock) and not stored.
  void registerSpaceCallback(std::function<void()> cb) {
    bool invoke_now = false;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (closed_ || queue_.size() < capacity_) {
        invoke_now = true;
      } else {
        space_callbacks_.push_back(std::move(cb));
      }
    }
    if (invoke_now) {
      cb();
    }
  }

  // Signal no more items. Wakes all waiters and fires all space callbacks.
  void close() {
    std::vector<std::function<void()>> cbs;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      closed_ = true;
      cbs.swap(space_callbacks_);
      not_empty_.notify_all();
      not_full_.notify_all();
    }
    for (auto& cb : cbs) {
      cb();
    }
  }

  bool isClosed() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return closed_;
  }

  // Discard all queued items. Used during fallback transitions to prevent
  // mixing output formats. Does NOT close the queue.
  void drain() {
    std::lock_guard<std::mutex> lock(mutex_);
    while (!queue_.empty()) {
      queue_.pop();
    }
    not_full_.notify_all();
  }

 private:
  const size_t capacity_;
  std::queue<T> queue_;
  mutable std::mutex mutex_;
  std::condition_variable not_empty_;
  std::condition_variable not_full_;
  bool closed_ = false;
  // One-shot callbacks invoked when space becomes available.
  // @GuardedBy: mutex_
  std::vector<std::function<void()>> space_callbacks_;
};

// Result batch queue — stores RowVectorPtr directly.
// Java pulls via nativeGetNextBatch using C Data Interface
// (ColumnarDataExporter::exportBatchToJava), avoiding RecordBatch intermediary.
using OutputQueue = BoundedQueue<facebook::velox::RowVectorPtr>;

}  // namespace pangrova::executor
