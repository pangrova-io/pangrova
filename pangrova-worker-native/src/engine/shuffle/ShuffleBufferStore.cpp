#include "engine/shuffle/ShuffleBufferStore.h"

#include <algorithm>
#include <chrono>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <limits>
#include <stdexcept>
#include <unistd.h>

namespace pangrova::shuffle {
namespace {

arrow::Status fileError(const char* operation) {
  return arrow::Status::IOError("shuffle spill ", operation, ": ", std::strerror(errno));
}

arrow::Result<folly::File> createSpillFile(const std::string& directory) {
  const auto root = directory.empty() ? std::filesystem::temp_directory_path()
                                      : std::filesystem::path(directory);
  auto path = (root / "pangrova-shuffle-XXXXXX").string();
  const int descriptor = ::mkstemp(path.data());
  if (descriptor < 0) return fileError("create");
  folly::File file(descriptor, true);
  // Unlink before any payload is written. There is no named data left behind
  // on cancellation, exception, or process exit, and no delete-versus-read race.
  if (::unlink(path.c_str()) != 0) return fileError("unlink");
  return file;
}

arrow::Status writePayload(int descriptor, uint64_t offset, const folly::IOBuf& data,
                           CancellationToken cancellation) {
  const auto* node = &data;
  do {
    size_t written = 0;
    while (written < node->length()) {
      if (cancellation.stopRequested()) return arrow::Status::Cancelled("shuffle spill cancelled");
      const auto count = ::pwrite(descriptor, node->data() + written,
                                  std::min<size_t>(node->length() - written, 65536),
                                  static_cast<off_t>(offset));
      if (count < 0 && errno == EINTR) continue;
      if (count < 0) return fileError("write");
      if (count == 0) return arrow::Status::IOError("shuffle spill write made no progress");
      written += static_cast<size_t>(count);
      offset += static_cast<uint64_t>(count);
    }
    node = node->next();
  } while (node != &data);
  return arrow::Status::OK();
}

arrow::Status ensureSpillFile(ShuffleBufferStore::SpillSegment& segment,
                              const std::string& directory) {
  std::call_once(segment.create_once, [&] {
    auto result = createSpillFile(directory);
    if (!result.ok()) segment.create_status = result.status();
    else segment.file = std::move(*result);
  });
  return segment.create_status;
}

}  // namespace

ShuffleBufferStore::ShuffleBufferStore(ShuffleBufferLimits limits)
    : limits_(std::move(limits)) {
  if (limits_.transit_bytes == 0 || limits_.spill_bytes == 0 || limits_.buffered_batches == 0 ||
      limits_.spill_files == 0 || limits_.handles == 0 || limits_.streams == 0 ||
      limits_.partitions_per_handle <= 0 || limits_.max_payload_bytes == 0 ||
      limits_.max_payload_bytes > static_cast<uint64_t>(std::numeric_limits<int32_t>::max()) ||
      limits_.spill_segment_bytes <
          ((limits_.max_payload_bytes + 4095) / 4096 * 4096) ||
      limits_.spill_segment_bytes > static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
    throw std::invalid_argument("shuffle buffer limits must be positive and payload fit binary offsets");
  }
}

ShuffleBufferStore::Reservation::~Reservation() {
  owner->release(charge);
}

arrow::Result<ShuffleBufferStore::StorageReservation>
ShuffleBufferStore::reserve(
    uint64_t payload_bytes, uint64_t resident_bytes, HandleScope& scope,
    const CancellationToken& cancellation) {
  StorageReservation result;
  result.entry = std::make_shared<Reservation>(shared_from_this());
  // Allocate the possible next segment before taking the accounting lock.
  // A failed allocation must never destroy a Reservation under that lock.
  auto next_file_credit = std::make_shared<Reservation>(shared_from_this());
  auto next_segment = std::make_shared<SpillSegment>(next_file_credit);
  const auto deadline = std::chrono::steady_clock::now() +
      std::chrono::milliseconds(limits_.retention_wait_ms);

  // Declared outside the loop so the previous iteration's segment is destroyed
  // with no lock held: ~Reservation re-enters release() and takes mutex_.
  std::shared_ptr<SpillSegment> retired_segment;
  while (true) {
    retired_segment.reset();
    if (cancellation.stopRequested()) {
      return arrow::Status::Cancelled("shuffle buffering cancelled");
    }
    std::unique_lock<std::mutex> lock(mutex_);
    // Retention credit is refillable, so wait for a reader instead of failing
    // the query. Payload/accounting errors above are terminal and never wait.
    bool retention_exhausted = false;
    const char* blocked = "shuffle buffered record budget exhausted";
    if (usage_.buffered_batches >= limits_.buffered_batches) {
      retention_exhausted = true;
    } else if (!scope.disk_only_ && resident_bytes <= limits_.max_payload_bytes + sizeof(folly::IOBuf) &&
               resident_bytes <= limits_.memory_bytes - usage_.memory_bytes) {
      result.entry->charge.memory_bytes = resident_bytes;
      usage_.memory_bytes += resident_bytes;
    } else {
      // Account allocation granularity as well as logical bytes for tiny spills.
      const uint64_t disk_bytes = (payload_bytes + 4095) / 4096 * 4096;
      if (disk_bytes > limits_.spill_bytes) {
        return arrow::Status::CapacityError("shuffle spill payload exceeds byte grant: limit=",
            limits_.spill_bytes, " requested=", disk_bytes);
      }
      if (disk_bytes > limits_.spill_bytes - usage_.spill_bytes) {
        retention_exhausted = true;
        blocked = "shuffle spill byte budget exhausted";
      } else {
        result.segment = scope.active_segment_.lock();
        if (!result.segment ||
            disk_bytes > limits_.spill_segment_bytes - result.segment->next_offset) {
          if (usage_.spill_files >= limits_.spill_files) {
            retention_exhausted = true;
            blocked = "shuffle spill file budget exhausted";
          } else {
            // The old segment may be losing its final entry concurrently. Keep
            // its last reference alive until after the lock is released.
            retired_segment = std::move(result.segment);
            result.segment = next_segment;
            result.segment->file_credit->charge.spill_files = 1;
            ++usage_.spill_files;
            scope.active_segment_ = result.segment;
          }
        }
        if (!retention_exhausted) {
          result.spill_offset = result.segment->next_offset;
          result.segment->next_offset += disk_bytes;
          // Releasing one entry cannot reclaim its blocks while another entry
          // keeps the same anonymous file alive. Charge physical space to the
          // segment; per-entry accounting would under-report real disk usage.
          result.segment->file_credit->charge.spill_bytes += disk_bytes;
          usage_.spill_bytes += disk_bytes;
        }
      }
    }

    if (!retention_exhausted) {
      result.entry->charge.buffered_batches = 1;
      ++usage_.buffered_batches;
      return result;
    }

    // Reset partial state before waiting so a retry starts clean.
    result.segment.reset();
    result.spill_offset = 0;
    if (std::chrono::steady_clock::now() >= deadline) {
      return arrow::Status::CapacityError(blocked,
          ": waited_ms=", limits_.retention_wait_ms,
          " spill_bytes_used=", usage_.spill_bytes, " spill_limit=", limits_.spill_bytes,
          " batches_used=", usage_.buffered_batches, " batch_limit=", limits_.buffered_batches,
          " files_used=", usage_.spill_files, " file_limit=", limits_.spill_files);
    }
    storage_available_.wait_for(lock, std::chrono::milliseconds(20));
    lock.unlock();
  }
}

arrow::Result<std::unique_ptr<ShuffleBufferStore::Entry>>
ShuffleBufferStore::store(
    ShuffleBatch batch, HandleScope& scope, CancellationToken cancellation) {
  if (cancellation.stopRequested()) return arrow::Status::Cancelled("shuffle buffering cancelled");
  if (!batch.data || batch.row_count < 0) {
    return arrow::Status::Invalid("shuffle batch requires data and nonnegative row count");
  }
  uint64_t payload_bytes = 0;
  uint64_t resident_bytes = 0;
  const auto* node = batch.data.get();
  do {
    if (node->length() > limits_.max_payload_bytes - payload_bytes) {
      return arrow::Status::CapacityError("shuffle payload exceeds byte limit");
    }
    payload_bytes += node->length();
    const auto remaining = std::numeric_limits<uint64_t>::max() - resident_bytes;
    if (remaining < sizeof(folly::IOBuf) ||
      node->capacity() > remaining - sizeof(folly::IOBuf)) {
      return arrow::Status::CapacityError("shuffle allocation accounting overflow");
    }
    resident_bytes += node->capacity() + sizeof(folly::IOBuf);
    node = node->next();
  } while (node != batch.data.get());
  if (payload_bytes == 0) return arrow::Status::Invalid("shuffle payload is empty");
  ARROW_ASSIGN_OR_RAISE(auto storage, reserve(payload_bytes, resident_bytes, scope, cancellation));
  auto entry = std::make_unique<Entry>();
  entry->reservation = std::move(storage.entry);
  entry->payload_bytes = payload_bytes;
  entry->segment = std::move(storage.segment);
  entry->spill_offset = storage.spill_offset;
  if (entry->segment) {
    ARROW_RETURN_NOT_OK(ensureSpillFile(*entry->segment, limits_.spill_directory));
    ARROW_RETURN_NOT_OK(writePayload(entry->segment->file.fd(), entry->spill_offset,
                                     *batch.data, cancellation));
    batch.data.reset();
  }
  entry->batch = std::move(batch);
  return entry;
}

arrow::Result<ShuffleBatch> ShuffleBufferStore::replay(
    Entry& entry, CancellationToken cancellation) {
  if (cancellation.stopRequested()) return arrow::Status::Cancelled("shuffle replay cancelled");
  if (!entry.segment) return std::move(entry.batch);
  auto data = folly::IOBuf::create(entry.payload_bytes);
  size_t offset = 0;
  while (offset < entry.payload_bytes) {
    if (cancellation.stopRequested()) return arrow::Status::Cancelled("shuffle replay cancelled");
    const auto count = ::pread(entry.segment->file.fd(), data->writableData() + offset,
                               std::min<uint64_t>(entry.payload_bytes - offset, 65536),
                               static_cast<off_t>(entry.spill_offset + offset));
    if (count < 0 && errno == EINTR) continue;
    if (count < 0) return fileError("read");
    if (count == 0) return arrow::Status::IOError("shuffle spill unexpectedly truncated");
    offset += static_cast<size_t>(count);
  }
  data->append(entry.payload_bytes);
  return ShuffleBatch{entry.batch.partition_id, std::move(data), entry.batch.row_count};
}

arrow::Result<std::shared_ptr<ShuffleBufferStore::Reservation>>
ShuffleBufferStore::acquireHandle() {
  auto reservation = std::make_shared<Reservation>(shared_from_this());
  std::lock_guard<std::mutex> lock(mutex_);
  if (usage_.handles >= limits_.handles) {
    return arrow::Status::CapacityError("shuffle handle budget exhausted");
  }
  ++usage_.handles;
  reservation->charge.handles = 1;
  return reservation;
}

ShuffleBufferUsage ShuffleBufferStore::usage() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return usage_;
}

arrow::Result<std::shared_ptr<ShuffleBufferStore::Reservation>>
ShuffleBufferStore::acquireStream() {
  auto reservation = std::make_shared<Reservation>(shared_from_this());
  std::lock_guard<std::mutex> lock(mutex_);
  if (usage_.streams >= limits_.streams) {
    return arrow::Status::CapacityError("shuffle Flight stream budget exhausted");
  }
  ++usage_.streams;
  reservation->charge.streams = 1;
  return reservation;
}

arrow::Result<std::shared_ptr<ShuffleBufferStore::Reservation>>
ShuffleBufferStore::acquireTransit(
    uint64_t payload_bytes, CancellationToken cancellation,
    const std::function<bool()>& reader_cancelled) {
  // Flight may hold the replayed bytes, an IPC representation and a gRPC
  // response concurrently. Charge actual bytes, rather than every stream's
  // maximum page size. Include a bounded per-response metadata allowance.
  constexpr uint64_t kFlightMetadataBytes = 64 * 1024;
  if (payload_bytes == 0 || payload_bytes > limits_.max_payload_bytes ||
      payload_bytes > (std::numeric_limits<uint64_t>::max() - kFlightMetadataBytes) / 3) {
    return arrow::Status::CapacityError("shuffle transit payload exceeds byte limit");
  }
  const uint64_t charge = 3 * payload_bytes + kFlightMetadataBytes;
  if (charge > limits_.transit_bytes) {
    return arrow::Status::CapacityError("shuffle transit payload exceeds service byte grant");
  }
  auto reservation = std::make_shared<Reservation>(shared_from_this());
  std::unique_lock<std::mutex> lock(mutex_);
  while (charge > limits_.transit_bytes - usage_.transit_bytes) {
    if (cancellation.stopRequested() || (reader_cancelled && reader_cancelled())) {
      return arrow::Status::Cancelled("shuffle transit wait cancelled");
    }
    transit_available_.wait_for(lock, std::chrono::milliseconds(20));
  }
  if (cancellation.stopRequested() || (reader_cancelled && reader_cancelled())) {
    return arrow::Status::Cancelled("shuffle transit wait cancelled");
  }
  usage_.transit_bytes += charge;
  reservation->charge.transit_bytes = charge;
  return reservation;
}

void ShuffleBufferStore::release(const ShuffleBufferUsage& charge) {
  std::lock_guard<std::mutex> lock(mutex_);
  usage_.memory_bytes -= charge.memory_bytes;
  usage_.transit_bytes -= charge.transit_bytes;
  usage_.spill_bytes -= charge.spill_bytes;
  usage_.buffered_batches -= charge.buffered_batches;
  usage_.spill_files -= charge.spill_files;
  usage_.handles -= charge.handles;
  usage_.streams -= charge.streams;
  if (charge.transit_bytes != 0) transit_available_.notify_all();
  if (charge.memory_bytes != 0 || charge.spill_bytes != 0 ||
      charge.buffered_batches != 0 || charge.spill_files != 0) {
    storage_available_.notify_all();
  }
}

}  // namespace pangrova::shuffle
