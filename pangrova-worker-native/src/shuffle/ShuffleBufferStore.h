#pragma once

#include <cstdint>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>

#include <arrow/result.h>
#include <folly/File.h>

#include "shuffle/CancellationToken.h"
#include "shuffle/SerializedShuffleQueue.h"

namespace pangrova::shuffle {

// Worker-server-wide limits, shared by active and lingering handles. Zero
// resident bytes forces spill; every other limit must be strictly positive.
// These govern demux retention, not the producer's separately owned queue.
struct ShuffleBufferLimits {
  uint64_t memory_bytes = 64ULL * 1024 * 1024;
  // Separate from retention, so a full resident buffer cannot prevent a
  // reader from acquiring enough credit to drain it.
  uint64_t transit_bytes = 64ULL * 1024 * 1024;
  uint64_t spill_bytes = 8ULL * 1024 * 1024 * 1024;
  uint64_t buffered_batches = 65536;
  uint64_t spill_files = 256;
  uint64_t max_payload_bytes = 4ULL * 1024 * 1024;
  uint64_t handles = 128;
  uint64_t streams = 128;
  int32_t partitions_per_handle = 4096;
  // Empty selects the system temporary directory. Files are private and
  // unlinked before writing; closing the last descriptor reclaims storage.
  std::string spill_directory;
};

struct ShuffleBufferUsage {
  uint64_t memory_bytes = 0;
  uint64_t transit_bytes = 0;
  uint64_t spill_bytes = 0;
  uint64_t buffered_batches = 0;
  uint64_t spill_files = 0;
  uint64_t handles = 0;
  uint64_t streams = 0;
};

// Thread-safe budget and storage shared by all demuxers on one server. Never
// performs filesystem I/O under its accounting mutex. Lack of disk/descriptor/
// metadata credit is a terminal resource error, never a wait on another partition.
class ShuffleBufferStore : public std::enable_shared_from_this<ShuffleBufferStore> {
 public:
  explicit ShuffleBufferStore(ShuffleBufferLimits limits = {});

  struct Reservation : ShuffleTransitLease {
    explicit Reservation(std::shared_ptr<ShuffleBufferStore> store)
        : owner(std::move(store)) {}
    const std::shared_ptr<ShuffleBufferStore> owner;
    ShuffleBufferUsage charge;
    ~Reservation();
  };

  // Internal move-only record. Reservation outlives the anonymous file so disk
  // credit is returned only after the descriptor (and its storage) is released.
  struct Entry {
    std::shared_ptr<Reservation> reservation;
    ShuffleBatch batch{};
    uint64_t payload_bytes = 0;
    folly::File file;
  };

  // Reserves before retaining a batch. Oversized/empty/null data is an error.
  arrow::Result<std::unique_ptr<Entry>> store(
      ShuffleBatch batch, CancellationToken cancellation = {});
  // Reconstructs exactly one record. Short reads are errors, not partial data.
  static arrow::Result<ShuffleBatch> replay(
      Entry& entry, CancellationToken cancellation = {});
  // Pins a handle's metadata budget through lingering and outstanding readers.
  arrow::Result<std::shared_ptr<Reservation>> acquireHandle();
  // Bounds simultaneous Flight response/replay allocations outside retention.
  arrow::Result<std::shared_ptr<Reservation>> acquireStream();
  // Covers spill replay, Arrow IPC metadata and Flight's in-flight payload.
  // Waits without holding a demux lock; cancellation wakes within 20 ms.
  arrow::Result<std::shared_ptr<Reservation>> acquireTransit(
      uint64_t payload_bytes, CancellationToken cancellation = {},
      const std::function<bool()>& reader_cancelled = {});
  ShuffleBufferUsage usage() const;
  const ShuffleBufferLimits& limits() const { return limits_; }

 private:
  arrow::Result<std::shared_ptr<Reservation>> reserve(
      uint64_t payload_bytes, uint64_t resident_bytes);
  void release(const ShuffleBufferUsage& charge);
  const ShuffleBufferLimits limits_;
  mutable std::mutex mutex_;
  std::condition_variable transit_available_;
  ShuffleBufferUsage usage_;
};

}  // namespace pangrova::shuffle
