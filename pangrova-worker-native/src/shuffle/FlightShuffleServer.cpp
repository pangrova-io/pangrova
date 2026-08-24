// FlightShuffleServer: Multiplexed Arrow Flight server that serves shuffle
// data from multiple executor handles on a single port per worker.
//
// Configures SO_REUSEPORT on the server socket and routes DoGet requests
// to the correct PartitionDemuxer based on the executor handle in the ticket.

#include "shuffle/FlightShuffleServer.h"

#include <fstream>
#include <limits>
#include <sstream>

// gRPC headers must precede glog to avoid macro redefinition conflicts
// between abseil-logging (pulled by grpcpp) and Velox's bundled glog.
#include <grpc/impl/channel_arg_names.h>
#include <grpcpp/grpcpp.h>

#include <arrow/buffer.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <glog/logging.h>

#include "observability/PangrovaLog.h"

namespace pangrova::shuffle {

// ---------------------------------------------------------------------------
// detail:: implementations
// ---------------------------------------------------------------------------
namespace detail {

bool ticketOwnershipValid(
    const std::string& ticket_query_id,
    int32_t ticket_stage_id,
    const std::string& registered_query_id,
    int32_t registered_stage_id) {
  // Identity not recorded (legacy path) -> accept regardless.
  if (registered_query_id.empty() && registered_stage_id < 0) {
    return true;
  }
  // Query ID mismatch -> reject.
  if (registered_query_id != ticket_query_id) {
    return false;
  }
  // Stage not recorded -> query_id alone is sufficient.
  if (registered_stage_id < 0) {
    return true;
  }
  // Both recorded -> must match.
  return registered_stage_id == ticket_stage_id;
}

}  // namespace detail

// ── PartitionDemuxer ────────────────────────────────────────────────

PartitionDemuxer::PartitionDemuxer(SerializedShuffleQueue* source)
    : source_(source) {
  CHECK(source_ != nullptr) << "SerializedShuffleQueue must not be null";
}

PartitionDemuxer::~PartitionDemuxer() {
  stop();
}

void PartitionDemuxer::start() {
  CHECK(!running_.load()) << "PartitionDemuxer already running";
  running_.store(true);
  drain_exited_.store(false);
  drain_thread_ = std::thread([this]() { drainLoop(); });
}

void PartitionDemuxer::stop() {
  if (!running_.exchange(false)) {
    return;
  }
  // Wake drain thread from backpressure wait if stuck.
  {
    std::lock_guard<std::mutex> lock(backpressure_mutex_);
    backpressure_cv_.notify_all();
  }
  if (drain_thread_.joinable()) {
    drain_thread_.join();
  }
  // Mark source closed and notify all partition waiters.
  source_closed_.store(true);
  drain_exited_.store(true);
  {
    std::lock_guard<std::mutex> lock(quiesce_mutex_);
    quiesce_cv_.notify_all();
  }
  std::lock_guard<std::mutex> reg_lock(registry_mutex_);
  for (auto& [pid, buf] : partitions_) {
    std::lock_guard<std::mutex> lock(buf->mutex);
    buf->cv.notify_all();
  }
}

PartitionDemuxer::PartitionBuffer& PartitionDemuxer::getOrCreatePartition(
    int32_t partition_id) {
  std::lock_guard<std::mutex> lock(registry_mutex_);
  auto it = partitions_.find(partition_id);
  if (it == partitions_.end()) {
    auto buf = std::make_unique<PartitionBuffer>();
    auto& ref = *buf;
    partitions_[partition_id] = std::move(buf);
    return ref;
  }
  return *it->second;
}

bool PartitionDemuxer::pop(int32_t partition_id, ShuffleBatch& batch,
                           std::chrono::milliseconds timeout) {
  // Partition bound check.
  int32_t max_p = max_partitions_.load();
  if (max_p > 0 && (partition_id < 0 || partition_id >= max_p)) {
    PANGROVA_LOG(ERROR) << "event=partition_pop_out_of_range"
                      << " partition_id=" << partition_id
                      << " max_partitions=" << max_p;
    return false;
  }
  auto& buf = getOrCreatePartition(partition_id);
  std::unique_lock<std::mutex> lock(buf.mutex);
  bool ok = buf.cv.wait_for(lock, timeout, [&] {
    return !buf.batches.empty() || source_closed_.load();
  });
  if (!ok) {
    return false;  // Timeout
  }
  if (buf.batches.empty()) {
    return false;  // Source closed and partition buffer empty
  }
  batch = std::move(buf.batches.front());
  buf.batches.pop_front();
  buf.popped_count++;
  buf.approx_size.fetch_sub(1, std::memory_order_release);
  // Release buf.mutex BEFORE acquiring backpressure_mutex_ to prevent
  // lock-order inversion with drainLoop (F-NATIVE-02).
  lock.unlock();
  // Wake drain thread from backpressure wait (slot freed).
  {
    std::lock_guard<std::mutex> bp_lock(backpressure_mutex_);
    backpressure_cv_.notify_all();
  }
  return true;
}

bool PartitionDemuxer::isPartitionDrained(int32_t partition_id) {
  if (!source_closed_.load()) {
    return false;
  }
  std::lock_guard<std::mutex> reg_lock(registry_mutex_);
  auto it = partitions_.find(partition_id);
  if (it == partitions_.end()) {
    return true;
  }
  std::lock_guard<std::mutex> lock(it->second->mutex);
  return it->second->batches.empty();
}

void PartitionDemuxer::drainLoop() {
  // PERF-T5: Adaptive backoff — busy-poll when data flows, back off when idle.
  // Reduces shuffle end-to-end latency from ~100ms to <1ms in burst scenarios.
  std::chrono::milliseconds timeout(0);
  static constexpr std::chrono::milliseconds kMaxBackoff(10);

  while (running_.load()) {
    ShuffleBatch batch;
    auto result = source_->pop(batch, timeout);

    if (result == pangrova::executor::PopResult::CLOSED) {
      source_closed_.store(true);
      source_complete_.store(true);  // Clean producer close
      // Notify all partition waiters.
      std::lock_guard<std::mutex> reg_lock(registry_mutex_);
      for (auto& [pid, buf] : partitions_) {
        std::lock_guard<std::mutex> lock(buf->mutex);
        buf->cv.notify_all();
      }
      drain_exited_.store(true);
      {
        std::lock_guard<std::mutex> lock(quiesce_mutex_);
        quiesce_cv_.notify_all();
      }
      return;
    }

    if (result == pangrova::executor::PopResult::DATA) {
      // Partition bound check on production side.
      int32_t max_p = max_partitions_.load();
      if (max_p > 0 && (batch.partition_id < 0 || batch.partition_id >= max_p)) {
        PANGROVA_LOG(ERROR) << "event=invalid_partition_id"
                          << " partition_id=" << batch.partition_id
                          << " max_partitions=" << max_p
                          << " action=skip_batch";
        continue;
      }

      auto& buf = getOrCreatePartition(batch.partition_id);

      // Backpressure: wait if partition is at cap.
      size_t cap = max_buffered_per_partition_.load();
      if (cap > 0) {
        std::unique_lock<std::mutex> bp_lock(backpressure_mutex_);
        backpressure_cv_.wait(bp_lock, [&] {
          if (!running_.load()) return true;
          // Read atomic approx_size instead of locking buf.mutex to avoid
          // lock-order inversion with pop() (F-NATIVE-02).
          return buf.approx_size.load(std::memory_order_acquire) < cap;
        });
        if (!running_.load()) break;
      }

      std::lock_guard<std::mutex> lock(buf.mutex);
      buf.batches.push_back(std::move(batch));
      buf.committed_count++;
      buf.approx_size.fetch_add(1, std::memory_order_release);
      buf.cv.notify_all();
      // PERF-T5: Data received — reset to busy-poll for low-latency drain.
      timeout = std::chrono::milliseconds(0);
    }
    // TIMEOUT: adaptive backoff to avoid excessive CPU spin.
    if (result == pangrova::executor::PopResult::TIMEOUT) {
      timeout = std::min(timeout + std::chrono::milliseconds(1), kMaxBackoff);
    }
  }
  // Running set to false externally — mark source closed (forced, not complete).
  source_closed_.store(true);
  drain_exited_.store(true);
  {
    std::lock_guard<std::mutex> lock(quiesce_mutex_);
    quiesce_cv_.notify_all();
  }
  std::lock_guard<std::mutex> reg_lock(registry_mutex_);
  for (auto& [pid, buf] : partitions_) {
    std::lock_guard<std::mutex> lock(buf->mutex);
    buf->cv.notify_all();
  }
}

bool PartitionDemuxer::quiesce(std::chrono::milliseconds timeout) {
  {
    std::unique_lock<std::mutex> lock(quiesce_mutex_);
    bool exited = quiesce_cv_.wait_for(lock, timeout, [this] {
      return drain_exited_.load();
    });
    if (exited) {
      return source_complete_.load();
    }
  }
  // Lock released before stop() to avoid deadlock (stop() also acquires
  // quiesce_mutex_ to notify waiters).
  stop();
  return false;
}

bool PartitionDemuxer::isComplete() const {
  return source_complete_.load();
}

bool PartitionDemuxer::isFullyConsumed() const {
  if (!source_closed_.load()) return false;
  std::lock_guard<std::mutex> reg_lock(
      const_cast<std::mutex&>(registry_mutex_));
  for (auto& [pid, buf] : partitions_) {
    std::lock_guard<std::mutex> lock(buf->mutex);
    if (!buf->batches.empty()) return false;
  }
  return true;
}

bool PartitionDemuxer::isPartitionTruncated(int32_t partition_id) {
  if (source_complete_.load()) {
    return false;  // Clean close means no truncation.
  }
  return isPartitionDrained(partition_id);
}

void PartitionDemuxer::setMaxBufferedBatchesPerPartition(size_t cap) {
  max_buffered_per_partition_.store(cap);
}

size_t PartitionDemuxer::bufferedBatchCount(int32_t partition_id) {
  std::lock_guard<std::mutex> reg_lock(registry_mutex_);
  auto it = partitions_.find(partition_id);
  if (it == partitions_.end()) return 0;
  std::lock_guard<std::mutex> lock(it->second->mutex);
  return it->second->batches.size();
}

int64_t PartitionDemuxer::committedBatchCount(int32_t partition_id) {
  std::lock_guard<std::mutex> reg_lock(registry_mutex_);
  auto it = partitions_.find(partition_id);
  if (it == partitions_.end()) return 0;
  std::lock_guard<std::mutex> lock(it->second->mutex);
  return it->second->committed_count;
}

void PartitionDemuxer::setMaxPartitions(int32_t max_partitions) {
  max_partitions_.store(max_partitions);
}

// ── FlightShuffleServer ─────────────────────────────────────────────

namespace {

// Schema for streaming serialized shuffle data over Flight.
// Single binary column carries the PrestoVectorSerde bytes.
std::shared_ptr<arrow::Schema> shuffleSchema() {
  static auto schema = arrow::schema({
      arrow::field("data", arrow::binary()),
  });
  return schema;
}

// Parse a multiplexed ticket into (handle, query_id, stage_id, partition_id).
// Wire format: "handle\nquery_id\nstage_id\npartition_id"
bool parseTicket(const std::string& ticket,
                 int64_t& handle,
                 std::string& query_id,
                 int32_t& stage_id,
                 int32_t& partition_id) {
  std::istringstream iss(ticket);
  std::string handle_str, stage_str, part_str;
  if (!std::getline(iss, handle_str) ||
      !std::getline(iss, query_id) ||
      !std::getline(iss, stage_str) ||
      !std::getline(iss, part_str)) {
    return false;
  }
  try {
    handle = std::stoll(handle_str);
    stage_id = std::stoi(stage_str);
    partition_id = std::stoi(part_str);
  } catch (...) {
    return false;
  }
  return true;
}

// RecordBatchReader that reads from a specific partition via PartitionDemuxer.
class ShuffleQueueBatchReader : public arrow::RecordBatchReader {
 public:
  ShuffleQueueBatchReader(
      PartitionDemuxer* demuxer,
      int32_t partition_id,
      int64_t max_batch_bytes)
      : demuxer_(demuxer),
        partition_id_(partition_id),
        max_batch_bytes_(max_batch_bytes),
        schema_(shuffleSchema()) {}

  std::shared_ptr<arrow::Schema> schema() const override { return schema_; }

  arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override {
    ShuffleBatch batch;
    while (true) {
      if (demuxer_->pop(partition_id_, batch, std::chrono::milliseconds(500))) {
        break;
      }
      if (demuxer_->isPartitionDrained(partition_id_)) {
        *out = nullptr;
        return arrow::Status::OK();
      }
    }

    arrow::BinaryBuilder builder;
    int64_t total_bytes = 0;

    if (batch.data) {
      const uint8_t* data_ptr;
      size_t data_len;
      std::unique_ptr<folly::IOBuf> coalesced_holder;
      if (batch.data->isChained()) {
        coalesced_holder = batch.data->cloneCoalesced();
        data_ptr = coalesced_holder->data();
        data_len = coalesced_holder->length();
      } else {
        data_ptr = batch.data->data();
        data_len = batch.data->length();
      }
      // Per-entry size guard: reject entries exceeding INT32_MAX to
      // prevent BinaryBuilder overflow (arrow uses int32 offsets).
      if (data_len > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
        return arrow::Status::Invalid(
            "Shuffle entry exceeds max size: " + std::to_string(data_len));
      }
      ARROW_RETURN_NOT_OK(builder.Append(
          data_ptr, static_cast<int32_t>(data_len)));
      total_bytes += static_cast<int64_t>(data_len);
    }

    // Drain more batches for this partition up to max_batch_bytes_.
    while (total_bytes < max_batch_bytes_) {
      ShuffleBatch extra;
      if (!demuxer_->pop(partition_id_, extra, std::chrono::milliseconds(0))) {
        break;
      }
      if (extra.data) {
        const uint8_t* extra_ptr;
        size_t extra_len;
        std::unique_ptr<folly::IOBuf> extra_holder;
        if (extra.data->isChained()) {
          extra_holder = extra.data->cloneCoalesced();
          extra_ptr = extra_holder->data();
          extra_len = extra_holder->length();
        } else {
          extra_ptr = extra.data->data();
          extra_len = extra.data->length();
        }
        // Per-entry size guard: reject entries exceeding INT32_MAX to
        // prevent BinaryBuilder overflow (arrow uses int32 offsets).
        if (extra_len > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
          return arrow::Status::Invalid(
              "Shuffle entry exceeds max size: " + std::to_string(extra_len));
        }
        ARROW_RETURN_NOT_OK(builder.Append(
            extra_ptr, static_cast<int32_t>(extra_len)));
        total_bytes += static_cast<int64_t>(extra_len);
      }
    }

    if (total_bytes == 0) {
      *out = nullptr;
      return arrow::Status::OK();
    }

    std::shared_ptr<arrow::Array> data_array;
    ARROW_RETURN_NOT_OK(builder.Finish(&data_array));

    *out = arrow::RecordBatch::Make(schema_, data_array->length(), {data_array});
    return arrow::Status::OK();
  }

 private:
  PartitionDemuxer* demuxer_;
  int32_t partition_id_;
  int64_t max_batch_bytes_;
  std::shared_ptr<arrow::Schema> schema_;
};

}  // namespace

FlightShuffleServer::FlightShuffleServer(FlightShuffleServerConfig config)
    : config_(std::move(config)) {
  if (!config_.hmac_current_key.empty()) {
    ticket_verifier_ = std::make_unique<TicketVerifier>(
        config_.hmac_current_key, config_.hmac_previous_key);
    PANGROVA_LOG(INFO) << "event=ticket_verifier_enabled"
                      << " has_previous_key=" << !config_.hmac_previous_key.empty();
  }
}

FlightShuffleServer::~FlightShuffleServer() {
  if (running_.load()) {
    stop();
  }
  // Safety net: if Serve() already flipped running_=false (so stop() early-
  // returned or was never called), the evictor thread may still be joinable.
  {
    std::lock_guard<std::mutex> lock(evictor_mutex_);
    evictor_cv_.notify_all();
  }
  if (lingering_evictor_thread_.joinable()) {
    lingering_evictor_thread_.join();
  }
}

// Interval between background sweeps that reap fully-consumed / TTL-expired
// lingering demuxers (F-SHUF-LINGER).
static constexpr std::chrono::seconds kLingeringEvictInterval{10};

int FlightShuffleServer::start() {
  CHECK(!running_.load()) << "FlightShuffleServer already running";

  auto location_result =
      arrow::flight::Location::ForGrpcTcp("0.0.0.0", config_.port);
  CHECK(location_result.ok())
      << "Failed to create Flight location: " << location_result.status().ToString();

  arrow::flight::FlightServerOptions options(*location_result);

  // Configure SO_REUSEPORT for port sharing across threads.
  if (config_.reuse_port) {
    options.builder_hook = [](void* raw_builder) {
      auto* builder = static_cast<grpc::ServerBuilder*>(raw_builder);
      builder->AddChannelArgument(GRPC_ARG_ALLOW_REUSEPORT, 1);
    };
  }

  // Configure TLS if certificate paths are provided.
  if (!config_.tls_cert_chain_path.empty()) {
    std::ifstream cert_file(config_.tls_cert_chain_path);
    std::ifstream key_file(config_.tls_private_key_path);
    if (!cert_file.good() || !key_file.good()) {
      PANGROVA_LOG(ERROR) << "event=flight_shuffle_server_tls_error"
                        << " cert=" << config_.tls_cert_chain_path
                        << " key=" << config_.tls_private_key_path;
    } else {
      std::string cert_pem((std::istreambuf_iterator<char>(cert_file)),
                            std::istreambuf_iterator<char>());
      std::string key_pem((std::istreambuf_iterator<char>(key_file)),
                           std::istreambuf_iterator<char>());
      arrow::flight::CertKeyPair cert_key{cert_pem, key_pem};
      options.tls_certificates.push_back(cert_key);

      if (!config_.tls_root_ca_path.empty()) {
        std::ifstream ca_file(config_.tls_root_ca_path);
        if (ca_file.good()) {
          std::string ca_pem((std::istreambuf_iterator<char>(ca_file)),
                              std::istreambuf_iterator<char>());
          options.root_certificates = ca_pem;
          options.verify_client = true;
        }
      }
      PANGROVA_LOG(INFO) << "event=flight_shuffle_server_tls_enabled"
                        << " port=" << config_.port;
    }
  } else {
    PANGROVA_LOG(WARNING) << "event=flight_shuffle_server_no_tls"
                        << " port=" << config_.port
                        << " detail=binding_without_tls_or_auth";
  }

  auto init_status = Init(options);
  CHECK(init_status.ok())
      << "Failed to init Flight server: " << init_status.ToString();

  bound_port_ = port();
  running_.store(true);

  server_thread_ = std::thread([this]() {
    auto status = Serve();
    if (!status.ok()) {
      PANGROVA_LOG(ERROR) << "event=flight_serve_error error=" << status.ToString();
    }
    running_.store(false);
  });

  // F-SHUF-LINGER: background evictor reaps lingering demuxers on a timer so a
  // client that disconnects mid-DoGet cannot leak them unboundedly. Eviction
  // previously fired only on registerHandle(), so an idle server never reaped.
  lingering_evictor_thread_ = std::thread([this]() {
    std::unique_lock<std::mutex> lock(evictor_mutex_);
    while (running_.load()) {
      evictor_cv_.wait_for(lock, kLingeringEvictInterval,
                           [this]() { return !running_.load(); });
      if (!running_.load()) {
        break;
      }
      // evictFullyConsumedLingering() takes lingering_mutex_ internally; drop
      // evictor_mutex_ across the call to keep lock scopes minimal.
      lock.unlock();
      size_t evicted = evictFullyConsumedLingering();
      if (evicted > 0) {
        PANGROVA_LOG(INFO) << "event=lingering_evictor_reaped count=" << evicted;
      }
      lock.lock();
    }
  });

  PANGROVA_LOG(INFO) << "event=flight_shuffle_server_started port=" << bound_port_
                    << " reuse_port=" << config_.reuse_port;
  return bound_port_;
}

void FlightShuffleServer::stop() {
  if (!running_.exchange(false)) {
    return;
  }

  // F-SHUF-LINGER: wake and join the background evictor before tearing down
  // demuxers so it never touches lingering_demuxers_ concurrently with cleanup.
  {
    std::lock_guard<std::mutex> lock(evictor_mutex_);
    evictor_cv_.notify_all();
  }
  if (lingering_evictor_thread_.joinable()) {
    lingering_evictor_thread_.join();
  }

  // Stop all registered demuxers.
  {
    std::lock_guard<std::mutex> lock(handles_mutex_);
    for (auto& [handle, demuxer] : handle_demuxers_) {
      demuxer->stop();
    }
    handle_demuxers_.clear();
  }

  // Clean up lingering demuxers to prevent resource leaks on shutdown.
  {
    std::lock_guard<std::mutex> lock(lingering_mutex_);
    for (auto& [handle, entry] : lingering_demuxers_) {
      entry.demuxer->stop();
    }
    size_t count = lingering_demuxers_.size();
    lingering_demuxers_.clear();
    if (count > 0) {
      PANGROVA_LOG(INFO) << "event=lingering_demuxers_cleaned count=" << count;
    }
  }

  auto status = Shutdown();
  if (!status.ok()) {
    PANGROVA_LOG(ERROR) << "event=flight_shutdown_error error=" << status.ToString();
  }

  if (server_thread_.joinable()) {
    server_thread_.join();
  }

  PANGROVA_LOG(INFO) << "event=flight_shuffle_server_stopped port=" << bound_port_;
}

size_t FlightShuffleServer::evictFullyConsumedLingering() {
  auto now = std::chrono::steady_clock::now();
  std::lock_guard<std::mutex> lock(lingering_mutex_);
  size_t evicted = 0;
  for (auto it = lingering_demuxers_.begin(); it != lingering_demuxers_.end();) {
    bool fully_consumed = it->second.demuxer->isFullyConsumed();
    bool ttl_expired = config_.lingering_ttl_ms > 0 &&
        std::chrono::duration_cast<std::chrono::milliseconds>(
            now - it->second.entered_at).count() >= config_.lingering_ttl_ms;
    if (fully_consumed || ttl_expired) {
      if (ttl_expired && !fully_consumed) {
        it->second.demuxer->stop();
        PANGROVA_LOG(WARNING) << "event=lingering_demuxer_ttl_evicted handle=" << it->first;
      } else {
        PANGROVA_LOG(INFO) << "event=lingering_demuxer_evicted handle=" << it->first;
      }
      it = lingering_demuxers_.erase(it);
      ++evicted;
    } else {
      ++it;
    }
  }
  return evicted;
}

void FlightShuffleServer::registerHandle(int64_t handle,
                                         SerializedShuffleQueue* queue) {
  CHECK(queue != nullptr) << "queue must not be null for handle=" << handle;
  evictFullyConsumedLingering();
  auto demuxer = std::make_unique<PartitionDemuxer>(queue);
  demuxer->start();

  std::lock_guard<std::mutex> lock(handles_mutex_);
  handle_demuxers_[handle] = std::move(demuxer);
  PANGROVA_LOG(INFO) << "event=shuffle_handle_registered handle=" << handle
                    << " total_handles=" << handle_demuxers_.size();
}

void FlightShuffleServer::registerHandle(int64_t handle,
                                         SerializedShuffleQueue* queue,
                                         const std::string& query_id,
                                         int32_t /*stage_id*/,
                                         int32_t num_partitions) {
  CHECK(queue != nullptr) << "queue must not be null for handle=" << handle;
  evictFullyConsumedLingering();
  auto demuxer = std::make_unique<PartitionDemuxer>(queue);
  if (num_partitions > 0) {
    demuxer->setMaxPartitions(num_partitions);
  }
  demuxer->start();

  std::lock_guard<std::mutex> lock(handles_mutex_);
  handle_demuxers_[handle] = std::move(demuxer);
  if (!query_id.empty()) {
    handle_query_ids_[handle] = query_id;
  }
  PANGROVA_LOG(INFO) << "event=shuffle_handle_registered handle=" << handle
                    << " num_partitions=" << num_partitions
                    << " total_handles=" << handle_demuxers_.size();
}

void FlightShuffleServer::unregisterHandle(int64_t handle) {
  std::unique_ptr<PartitionDemuxer> demuxer;
  {
    std::lock_guard<std::mutex> lock(handles_mutex_);
    auto it = handle_demuxers_.find(handle);
    if (it == handle_demuxers_.end()) {
      // Check lingering set — reap if fully consumed.
      std::lock_guard<std::mutex> ling_lock(lingering_mutex_);
      auto lit = lingering_demuxers_.find(handle);
      if (lit != lingering_demuxers_.end() && lit->second.demuxer->isFullyConsumed()) {
        lingering_demuxers_.erase(lit);
      }
      return;
    }
    demuxer = std::move(it->second);
    handle_demuxers_.erase(it);
    handle_query_ids_.erase(handle);
  }
  if (!demuxer) return;

  // Quiesce outside the lock: bounded wait for clean producer close.
  bool clean = demuxer->quiesce(std::chrono::milliseconds(500));
  if (clean && !demuxer->isFullyConsumed()) {
    // Producer done cleanly but data unconsumed — linger for late consumers.
    std::lock_guard<std::mutex> ling_lock(lingering_mutex_);
    lingering_demuxers_[handle] = LingeringEntry{
        std::move(demuxer), std::chrono::steady_clock::now()};
    PANGROVA_LOG(INFO) << "event=shuffle_handle_lingering handle=" << handle;
  } else {
    // Un-clean or already consumed — drop.
    if (demuxer) demuxer->stop();
    PANGROVA_LOG(INFO) << "event=shuffle_handle_unregistered handle=" << handle;
  }
}

size_t FlightShuffleServer::handleCount() const {
  std::lock_guard<std::mutex> lock(handles_mutex_);
  return handle_demuxers_.size();
}

size_t FlightShuffleServer::lingeringHandleCount() const {
  std::lock_guard<std::mutex> lock(lingering_mutex_);
  return lingering_demuxers_.size();
}

bool FlightShuffleServer::popForHandle(int64_t handle, int32_t partition_id,
                                       ShuffleBatch& batch,
                                       std::chrono::milliseconds timeout) {
  // Check active handles first.
  {
    std::lock_guard<std::mutex> lock(handles_mutex_);
    auto it = handle_demuxers_.find(handle);
    if (it != handle_demuxers_.end()) {
      return it->second->pop(partition_id, batch, timeout);
    }
  }
  // Check lingering handles.
  {
    std::lock_guard<std::mutex> lock(lingering_mutex_);
    auto it = lingering_demuxers_.find(handle);
    if (it != lingering_demuxers_.end()) {
      return it->second.demuxer->pop(partition_id, batch, timeout);
    }
  }
  return false;
}

arrow::Status FlightShuffleServer::DoGet(
    const arrow::flight::ServerCallContext& context,
    const arrow::flight::Ticket& request,
    std::unique_ptr<arrow::flight::FlightDataStream>* stream) {
  // Verify ticket HMAC signature and expiry before processing.
  if (ticket_verifier_) {
    auto verify_result = ticket_verifier_->verify(request.ticket);
    if (!verify_result.ok()) {
      PANGROVA_LOG(WARNING) << "event=ticket_verification_failed"
                           << " peer=" << context.peer();
      return arrow::flight::MakeFlightError(
          arrow::flight::FlightStatusCode::Unauthenticated,
          "Ticket verification failed");
    }
  }

  int64_t handle = 0;
  std::string query_id;
  int32_t stage_id = 0;
  int32_t partition_id = 0;
  if (!parseTicket(request.ticket, handle, query_id, stage_id, partition_id)) {
    return arrow::Status::Invalid(
        "Invalid ticket format. Expected: handle\\nquery_id\\nstage_id\\npartition_id");
  }

  PartitionDemuxer* demuxer = nullptr;
  {
    std::lock_guard<std::mutex> lock(handles_mutex_);
    auto it = handle_demuxers_.find(handle);
    if (it == handle_demuxers_.end()) {
      return arrow::Status::KeyError(
          "Unknown executor handle in ticket: " + std::to_string(handle));
    }
    demuxer = it->second.get();
    // Verify ticket ownership: the query_id in the ticket must match
    // the query_id associated with this handle to prevent cross-query
    // data leaks via forged or stale tickets.
    auto qit = handle_query_ids_.find(handle);
    if (qit != handle_query_ids_.end() && qit->second != query_id) {
      PANGROVA_LOG(WARNING) << "event=ticket_ownership_rejected"
                          << " handle=" << handle
                          << " ticket_query_id=" << query_id
                          << " peer=" << context.peer();
      return arrow::flight::MakeFlightError(
          arrow::flight::FlightStatusCode::Unauthorized,
          "Ticket query_id does not match handle owner");
    }
  }

  PANGROVA_LOG_DEBUG << "event=flight_doget handle=" << handle
                  << " query_id=" << query_id
                  << " stage_id=" << stage_id
                  << " partition_id=" << partition_id;

  auto reader = std::make_shared<ShuffleQueueBatchReader>(
      demuxer, partition_id, config_.max_batch_bytes);

  *stream = std::make_unique<arrow::flight::RecordBatchStream>(reader);

  return arrow::Status::OK();
}

}  // namespace pangrova::shuffle
