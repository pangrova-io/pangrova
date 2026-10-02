#include "shuffle/ShuffleBufferStore.h"

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

arrow::Status writePayload(int descriptor, const folly::IOBuf& data,
                           CancellationToken cancellation) {
  const auto* node = &data;
  do {
    size_t written = 0;
    while (written < node->length()) {
      if (cancellation.stopRequested()) return arrow::Status::Cancelled("shuffle spill cancelled");
      const auto count = ::write(descriptor, node->data() + written,
                                 std::min<size_t>(node->length() - written, 65536));
      if (count < 0 && errno == EINTR) continue;
      if (count < 0) return fileError("write");
      if (count == 0) return arrow::Status::IOError("shuffle spill write made no progress");
      written += static_cast<size_t>(count);
    }
    node = node->next();
  } while (node != &data);
  return arrow::Status::OK();
}

}  // namespace

ShuffleBufferStore::ShuffleBufferStore(ShuffleBufferLimits limits)
    : limits_(std::move(limits)) {
  if (limits_.transit_bytes == 0 || limits_.spill_bytes == 0 || limits_.buffered_batches == 0 ||
      limits_.spill_files == 0 || limits_.handles == 0 || limits_.streams == 0 ||
      limits_.partitions_per_handle <= 0 || limits_.max_payload_bytes == 0 ||
      limits_.max_payload_bytes > static_cast<uint64_t>(std::numeric_limits<int32_t>::max())) {
    throw std::invalid_argument("shuffle buffer limits must be positive and payload fit binary offsets");
  }
}

ShuffleBufferStore::Reservation::~Reservation() {
  owner->release(charge);
}

arrow::Result<std::shared_ptr<ShuffleBufferStore::Reservation>>
ShuffleBufferStore::reserve(uint64_t payload_bytes, uint64_t resident_bytes) {
  auto reservation = std::make_shared<Reservation>(shared_from_this());
  std::lock_guard<std::mutex> lock(mutex_);
  if (usage_.buffered_batches >= limits_.buffered_batches) {
    return arrow::Status::CapacityError("shuffle buffered record budget exhausted");
  }
  if (resident_bytes <= limits_.max_payload_bytes + sizeof(folly::IOBuf) &&
      resident_bytes <= limits_.memory_bytes - usage_.memory_bytes) {
    reservation->charge.memory_bytes = resident_bytes;
    usage_.memory_bytes += resident_bytes;
  } else {
    // Account allocation granularity as well as logical bytes for tiny spills.
    const uint64_t disk_bytes = (payload_bytes + 4095) / 4096 * 4096;
    if (disk_bytes > limits_.spill_bytes - usage_.spill_bytes ||
        usage_.spill_files >= limits_.spill_files) {
      return arrow::Status::CapacityError("shuffle spill byte/file budget exhausted");
    }
    reservation->charge.spill_bytes = disk_bytes;
    reservation->charge.spill_files = 1;
    usage_.spill_bytes += disk_bytes;
    ++usage_.spill_files;
  }
  reservation->charge.buffered_batches = 1;
  ++usage_.buffered_batches;
  return reservation;
}

arrow::Result<std::unique_ptr<ShuffleBufferStore::Entry>>
ShuffleBufferStore::store(ShuffleBatch batch, CancellationToken cancellation) {
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
  ARROW_ASSIGN_OR_RAISE(auto reservation, reserve(payload_bytes, resident_bytes));
  auto entry = std::make_unique<Entry>();
  entry->reservation = std::move(reservation);
  entry->payload_bytes = payload_bytes;
  if (entry->reservation->charge.spill_files != 0) {
    ARROW_ASSIGN_OR_RAISE(entry->file, createSpillFile(limits_.spill_directory));
    ARROW_RETURN_NOT_OK(writePayload(entry->file.fd(), *batch.data, cancellation));
    batch.data.reset();
  }
  entry->batch = std::move(batch);
  return entry;
}

arrow::Result<ShuffleBatch> ShuffleBufferStore::replay(
    Entry& entry, CancellationToken cancellation) {
  if (cancellation.stopRequested()) return arrow::Status::Cancelled("shuffle replay cancelled");
  if (entry.reservation->charge.spill_files == 0) return std::move(entry.batch);
  auto data = folly::IOBuf::create(entry.payload_bytes);
  size_t offset = 0;
  while (offset < entry.payload_bytes) {
    if (cancellation.stopRequested()) return arrow::Status::Cancelled("shuffle replay cancelled");
    const auto count = ::pread(entry.file.fd(), data->writableData() + offset,
                               std::min<uint64_t>(entry.payload_bytes - offset, 65536),
                               static_cast<off_t>(offset));
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
}

}  // namespace pangrova::shuffle
