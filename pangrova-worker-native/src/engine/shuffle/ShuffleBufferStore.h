#pragma once

#include <cstdint>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>

#include <arrow/result.h>
#include <folly/File.h>

#include "engine/shuffle/CancellationToken.h"
#include "engine/shuffle/SerializedShuffleQueue.h"

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
  // Several small batches share one anonymous spill file. This keeps the
  // descriptor limit independent of the number of buffered batches.
  uint64_t spill_segment_bytes = 16ULL * 1024 * 1024;
  uint64_t max_payload_bytes = 4ULL * 1024 * 1024;
  // Retention credit is refillable: a slow reader should stall its own producer
  // rather than fail the query. Bound the wait so a stuck reader still fails.
  uint64_t retention_wait_ms = 30'000;
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
// performs filesystem I/O under its accounting mutex. Retention credit (resident
// bytes, spill bytes, descriptors, records) is refillable, so an exhausted
// retention budget waits for a reader up to retention_wait_ms instead of failing
// the query. Oversized payloads, accounting overflow and handle/stream admission
// remain terminal. Each demuxer drains on its own thread and the wait releases
// the mutex, so a stalled producer does not block other handles.
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

  struct SpillSegment {
    explicit SpillSegment(std::shared_ptr<Reservation> file_credit)
        : file_credit(std::move(file_credit)) {}
    // Destroy the file before releasing its descriptor credit.
    std::shared_ptr<Reservation> file_credit;
    folly::File file;
    std::once_flag create_once;
    arrow::Status create_status = arrow::Status::OK();
    uint64_t next_offset = 0;  // guarded by the owning store's mutex
  };

  // Internal move-only record. The segment remains open until its final entry
  // is released. The segment owns physical disk and file credits; each entry
  // owns its record credit.
  struct Entry {
    std::shared_ptr<Reservation> reservation;
    ShuffleBatch batch{};
    uint64_t payload_bytes = 0;
    std::shared_ptr<SpillSegment> segment;
    uint64_t spill_offset = 0;
  };

  // Segments are scoped to one handle so that handle's reader can fully drain
  // them. A store-wide segment mixes entries from unrelated handles, so no
  // single reader can ever release its physical blocks — the slowest handle
  // would pin everyone's disk credit. Physical accounting stays on the store
  // because an anonymous file's blocks are only reclaimed when its LAST entry
  // is released; per-entry accounting would under-report real disk usage.
  class HandleScope {
   public:
    explicit HandleScope(std::shared_ptr<ShuffleBufferStore> store, bool disk_only = false)
        : store_(std::move(store)), disk_only_(disk_only) {}
    const std::shared_ptr<ShuffleBufferStore>& store() const { return store_; }

   private:
    friend class ShuffleBufferStore;
    std::shared_ptr<ShuffleBufferStore> store_;
    bool disk_only_;
    // Guarded by store_->mutex_.
    std::weak_ptr<SpillSegment> active_segment_;
  };

  // Reserves before retaining a batch. Oversized/empty/null data is an error.
  arrow::Result<std::unique_ptr<Entry>> store(
      ShuffleBatch batch, HandleScope& scope,
      CancellationToken cancellation = {});
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
  struct StorageReservation {
    std::shared_ptr<Reservation> entry;
    std::shared_ptr<SpillSegment> segment;
    uint64_t spill_offset = 0;
  };
  arrow::Result<StorageReservation> reserve(
      uint64_t payload_bytes, uint64_t resident_bytes, HandleScope& scope,
      const CancellationToken& cancellation);
  void release(const ShuffleBufferUsage& charge);
  const ShuffleBufferLimits limits_;
  mutable std::mutex mutex_;
  std::condition_variable transit_available_;
  std::condition_variable storage_available_;
  ShuffleBufferUsage usage_;
};

}  // namespace pangrova::shuffle
