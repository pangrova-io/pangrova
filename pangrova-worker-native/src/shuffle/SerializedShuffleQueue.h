// Independently owned, byte- and record-bounded producer shuffle queue.
#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>

#include <folly/io/IOBuf.h>

#include "executor/queue/OutputQueue.h"
#include "shuffle/ShuffleTransferLimits.h"

namespace pangrova::shuffle {

// A batch may retain a server-wide transit reservation after leaving demux
// retention. The payload owner (and therefore the credit) survives through
// Arrow IPC and Flight's synchronous WriteData call.
struct ShuffleTransitLease {
  virtual ~ShuffleTransitLease() = default;
};

struct ShuffleBatch {
  int32_t partition_id;
  std::unique_ptr<folly::IOBuf> data;
  int64_t row_count;
  std::shared_ptr<ShuffleTransitLease> transit_lease{};
};

// A producer reserves a slot and bytes before copying out of its Velox pool.
// Flight demux retention is separately bounded after a pop transfers ownership.
class SerializedShuffleQueue final {
 public:
  explicit SerializedShuffleQueue(
      size_t capacity, size_t max_bytes = 16 * 1024 * 1024)
      : capacity_(capacity), max_bytes_(max_bytes) {
    if (capacity_ == 0 || max_bytes_ == 0) {
      throw std::invalid_argument("shuffle queue capacity and byte grant must be positive");
    }
  }

  bool push(ShuffleBatch batch, std::chrono::milliseconds timeout) {
    if (!batch.data) {
      throw std::invalid_argument("shuffle batch has no serialized bytes");
    }
    const auto bytes = batch.data->computeChainDataLength();
    constexpr size_t kCopyCapacitySlack = 128;
    if (bytes == 0 || bytes > kCanonicalShuffleBatchBytes ||
        bytes > std::numeric_limits<size_t>::max() -
            sizeof(folly::IOBuf) - kCopyCapacitySlack) {
      throw std::length_error("shuffle batch exceeds canonical byte limit");
    }
    const auto charge = bytes + sizeof(folly::IOBuf) + kCopyCapacitySlack;
    if (charge > max_bytes_) {
      throw std::length_error("shuffle batch exceeds admitted queue byte grant");
    }
    {
      std::unique_lock<std::mutex> lock(mutex_);
      const auto ready = [this, charge] {
        return closed_ || outcome_ != Outcome::OPEN ||
            (queue_.size() + copying_ < capacity_ &&
             charge <= max_bytes_ - used_bytes_);
      };
      if (!not_full_.wait_for(lock, timeout, ready) || closed_ ||
          outcome_ != Outcome::OPEN) {
        return false;
      }
      used_bytes_ += charge;
      ++copying_;
    }

    // The pending reservation is rolled back on copy, allocation or enqueue
    // failure. Commit waits for this guard to settle before publishing EOF.
    struct PendingCopy {
      SerializedShuffleQueue& queue;
      size_t charge;
      bool retained = false;
      ~PendingCopy() {
        std::lock_guard<std::mutex> lock(queue.mutex_);
        --queue.copying_;
        if (!retained) queue.used_bytes_ -= charge;
        queue.not_full_.notify_all();
        queue.settled_.notify_all();
      }
    } pending{*this, charge};
    // Copy the chain directly into one independently owned buffer. coalesce()
    // would first allocate another uncharged full-page temporary.
    auto owned = folly::IOBuf::create(bytes);
    auto* destination = owned->writableData();
    const auto* node = batch.data.get();
    size_t copied = 0;
    do {
      if (node->length() != 0) {
        std::memcpy(destination, node->data(), node->length());
        destination += node->length();
        copied += node->length();
      }
      node = node->next();
    } while (node != batch.data.get());
    if (copied != bytes) {
      throw std::logic_error("shuffle chain length changed during owned copy");
    }
    owned->append(bytes);
    if (owned->capacity() > bytes + kCopyCapacitySlack) {
      throw std::length_error("shuffle copy exceeded reserved byte capacity");
    }
    batch.data = std::move(owned);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (closed_ || outcome_ == Outcome::ABORTED) {
        return false;
      }
      queue_.push_back({std::move(batch), charge});
      pending.retained = true;
      not_empty_.notify_one();
    }
    return true;
  }

  bool tryPush(ShuffleBatch batch) {
    return push(std::move(batch), std::chrono::milliseconds(0));
  }

  executor::PopResult pop(
      ShuffleBatch& batch, std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!not_empty_.wait_for(lock, timeout, [this] {
          return closed_ || !queue_.empty();
        })) {
      return executor::PopResult::TIMEOUT;
    }
    if (queue_.empty()) return executor::PopResult::CLOSED;
    auto entry = std::move(queue_.front());
    queue_.pop_front();
    used_bytes_ -= entry.charge;
    batch = std::move(entry.batch);
    not_full_.notify_all();
    return executor::PopResult::DATA;
  }

  bool commit() {
    std::unique_lock<std::mutex> lock(mutex_);
    if (outcome_ != Outcome::OPEN) return outcome_ == Outcome::COMMITTED;
    outcome_ = Outcome::COMMITTED;
    not_full_.notify_all();
    settled_.wait(lock, [this] { return copying_ == 0; });
    closed_ = true;
    not_empty_.notify_all();
    return true;
  }

  bool abort() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (outcome_ != Outcome::OPEN) return outcome_ == Outcome::ABORTED;
    outcome_ = Outcome::ABORTED;
    closed_ = true;
    not_full_.notify_all();
    not_empty_.notify_all();
    return true;
  }

  void close() {
    std::lock_guard<std::mutex> lock(mutex_);
    closed_ = true;
    not_full_.notify_all();
    not_empty_.notify_all();
  }

  void drain() {
    std::deque<Entry> discarded;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      discarded.swap(queue_);
      for (const auto& entry : discarded) used_bytes_ -= entry.charge;
      not_full_.notify_all();
    }
  }

  bool isClosed() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return closed_;
  }
  bool isAborted() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return outcome_ == Outcome::ABORTED;
  }
  bool isCommitted() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return outcome_ == Outcome::COMMITTED;
  }
  size_t size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.size();
  }
  size_t capacity() const { return capacity_; }
  size_t usedBytes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return used_bytes_;
  }

 private:
  struct Entry {
    ShuffleBatch batch;
    size_t charge;
  };
  enum class Outcome : uint8_t { OPEN, COMMITTED, ABORTED };
  const size_t capacity_;
  const size_t max_bytes_;
  mutable std::mutex mutex_;
  std::condition_variable not_full_;
  std::condition_variable not_empty_;
  std::condition_variable settled_;
  std::deque<Entry> queue_;
  size_t used_bytes_ = 0;
  size_t copying_ = 0;
  bool closed_ = false;
  Outcome outcome_ = Outcome::OPEN;
};

}  // namespace pangrova::shuffle
