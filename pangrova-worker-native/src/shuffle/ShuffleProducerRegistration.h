#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <string>

#include "shuffle/SerializedShuffleQueue.h"

namespace pangrova::shuffle {
class PartitionDemuxer;

struct ShuffleProducerIdentity {
  int64_t handle;
  SerializedShuffleQueue* queue;
  std::string query_id;
  int32_t stage_id;
  int32_t partition_count;
  uint64_t plan_epoch;
  uint32_t attempt_id;
  uint64_t fencing_token;
  bool operator==(const ShuffleProducerIdentity&) const = default;
};

// The same owner is retained by the task, routing map and active callers.
// Queue borrowing ends at graceful close; clean buffered output may linger.
class ShuffleProducerRegistration {
 public:
  enum class State { STARTING, ACTIVE, DRAINING, LINGERING, CLOSE_FAILED, CLOSED };
  struct Snapshot {
    ShuffleProducerIdentity identity;
    std::shared_ptr<PartitionDemuxer> demuxer;
    State state;
  };

  explicit ShuffleProducerRegistration(ShuffleProducerIdentity identity);
  ~ShuffleProducerRegistration();
  const ShuffleProducerIdentity& identity() const { return identity_; }
  void publish(std::shared_ptr<PartitionDemuxer> demuxer);
  void failStart(std::exception_ptr error);
  void awaitActive() const;
  State state() const;
  Snapshot snapshot() const;
  void close(bool force = false);
  bool shouldEvict(std::chrono::steady_clock::time_point now,
                   std::chrono::milliseconds ttl) const;

 private:
  const ShuffleProducerIdentity identity_;
  mutable std::mutex mutex_;
  mutable std::condition_variable cv_;
  State state_ = State::STARTING;
  std::shared_ptr<PartitionDemuxer> demuxer_;
  std::exception_ptr startup_error_;
  std::chrono::steady_clock::time_point entered_linger_;
};
}  // namespace pangrova::shuffle
