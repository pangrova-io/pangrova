#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>

#include "shuffle/ShuffleTransferLimits.h"

namespace pangrova::velox {

inline constexpr size_t kExternalShuffleChunkBytes =
    pangrova::shuffle::kCanonicalShuffleChunkBytes;
inline constexpr size_t kExternalShuffleMaxBatchBytes =
    pangrova::shuffle::kCanonicalShuffleBatchBytes;

struct ExternalShuffleChunk {
  int64_t batch_sequence = -1;
  uint32_t chunk_index = 0;
  uint32_t chunk_count = 0;
  uint64_t batch_size_bytes = 0;
  std::string data;
  std::string batch_checksum;
};

class ExternalShuffleChecksumError : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

class ExternalShuffleIngress {
 public:
  enum class ResultKind { kData, kFinished, kPending, kFailed, kCancelled };

  struct Result {
    ResultKind kind = ResultKind::kPending;
    std::string data;
    std::string error;
  };

  ExternalShuffleIngress(
      size_t normal_buffer_bytes,
      size_t max_batch_bytes,
      uint64_t deadline_epoch_ms);

  void push(ExternalShuffleChunk chunk);
  void finish(int64_t last_sequence);
  // Acceptance records task failure metadata before waking the consumer. The
  // callback must only record memory-owned evidence, never close/cancel ingress.
  void fail(std::string message, const std::function<void()>& on_accepted);
  void cancel();
  Result take(std::chrono::microseconds max_wait);

 private:
  bool deadlineExpired() const;
  void discardAssemblyLocked();

  struct Assembly {
    int64_t batch_sequence = -1;
    uint32_t chunk_count = 0;
    uint32_t next_chunk_index = 0;
    size_t batch_size_bytes = 0;
    std::string batch_checksum;
    std::string data;
  };

  const size_t normal_buffer_bytes_;
  const size_t max_batch_bytes_;
  const uint64_t deadline_epoch_ms_;
  std::mutex mutex_;
  std::condition_variable not_empty_;
  std::condition_variable not_full_;
  std::deque<std::string> batches_;
  size_t buffered_bytes_ = 0;
  int64_t next_sequence_ = 0;
  std::optional<Assembly> assembly_;
  bool finished_ = false;
  bool cancelled_ = false;
  std::string failure_;
};

}  // namespace pangrova::velox
