// FlightShuffleServer: Multiplexed Arrow Flight server that serves shuffle
// data from multiple executor handles on a single port per worker.
//
// Configures SO_REUSEPORT on the server socket and routes DoGet requests
// to the correct PartitionDemuxer based on the executor handle in the ticket.

#include "shuffle/FlightShuffleServer.h"

#include <algorithm>
#include <fstream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <vector>

// gRPC headers must precede glog to avoid macro redefinition conflicts
// between abseil-logging (pulled by grpcpp) and Velox's bundled glog.
#include <grpc/impl/channel_arg_names.h>
#include <grpcpp/grpcpp.h>

#include <glog/logging.h>

#include "observability/PangrovaLog.h"
#include "shuffle/ShuffleFlightBatchReader.h"

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
  return !registered_query_id.empty() && registered_stage_id >= 0 &&
      registered_query_id == ticket_query_id &&
      registered_stage_id == ticket_stage_id;
}

}  // namespace detail

// ── PartitionDemuxer ────────────────────────────────────────────────

PartitionDemuxer::PartitionDemuxer(
    SerializedShuffleQueue* source, std::shared_ptr<ShuffleBufferStore> store)
    : source_(source), store_(std::move(store)),
      max_partitions_(store_ ? store_->limits().partitions_per_handle : 0),
      completed_partitions_(max_partitions_, false) {
  if (!source_ || !store_) throw std::invalid_argument("shuffle requires source and store");
  auto reservation = store_->acquireHandle();
  if (!reservation.ok()) throw std::runtime_error(reservation.status().ToString());
  handle_reservation_ = std::move(*reservation);
}

PartitionDemuxer::~PartitionDemuxer() {
  stop();
}

void PartitionDemuxer::start() {
  if (running_.load() || drain_thread_.joinable()) {
    throw std::logic_error("PartitionDemuxer already started");
  }
  running_.store(true);
  drain_thread_ = std::thread([this]() { drainLoop(); });
}

void PartitionDemuxer::stop() {
  {
    std::unique_lock<std::mutex> lock(stop_mutex_);
    if (stopping_) stop_cv_.wait(lock, [this] { return stopped_; });
    if (stopped_) return;
    stopping_ = true;
  }
  if (!isFullyConsumed()) fail(arrow::Status::Cancelled("shuffle stopped before consumption"));
  running_.store(false);
  if (drain_thread_.joinable()) drain_thread_.join();
  finishDrain(false);
  // Release anonymous files outside all locks, including on TTL expiry with
  // still-live Flight readers. An in-progress replay owns its separate entry.
  std::unordered_map<int32_t, PartitionBuffer> discarded;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    discarded.swap(partitions_);
  }
  discarded.clear();
  {
    std::lock_guard<std::mutex> lock(stop_mutex_);
    stopped_ = true;
    stop_cv_.notify_all();
  }
}

arrow::Status PartitionDemuxer::status() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return status_;
}

bool PartitionDemuxer::validPartition(int32_t partition_id) const {
  return partition_id >= 0 && partition_id < max_partitions_;
}

void PartitionDemuxer::fail(arrow::Status error) {
  std::unordered_map<int32_t, PartitionBuffer> discarded;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (status_.ok()) status_ = std::move(error);
    running_.store(false);
    discarded.swap(partitions_);
    cv_.notify_all();
  }
  cancellation_.requestStop();
}

void PartitionDemuxer::finishDrain(bool complete) {
  std::lock_guard<std::mutex> lock(mutex_);
  source_complete_ = source_complete_ || (complete && status_.ok());
  source_closed_ = true;
  drain_exited_ = true;
  cv_.notify_all();
}

bool PartitionDemuxer::pop(int32_t partition_id, ShuffleBatch& batch,
                           std::chrono::milliseconds timeout,
                           const std::function<bool()>& reader_cancelled) {
  if (!validPartition(partition_id)) return false;
  std::unique_ptr<ShuffleBufferStore::Entry> entry;
  {
    std::unique_lock<std::mutex> lock(mutex_);
    auto ready = [&] {
      const auto it = partitions_.find(partition_id);
      return !status_.ok() || (it == partitions_.end() ? source_closed_ :
          !it->second.reading && (!it->second.batches.empty() || source_closed_));
    };
    if (!cv_.wait_for(lock, timeout, ready) || !status_.ok()) return false;
    auto it = partitions_.find(partition_id);
    if (it == partitions_.end() || it->second.batches.empty()) return false;
    entry = std::move(it->second.batches.front());
    it->second.batches.pop_front();
    it->second.reading = true;
  }
  try {
    // Acquire transit credit before replaying spill or releasing retention.
    // The two independent pools ensure a full resident store can drain.
    auto transit = store_->acquireTransit(
        entry->payload_bytes, cancellation_.token(), reader_cancelled);
    if (!transit.ok()) {
      if (transit.status().IsCancelled() && reader_cancelled &&
          reader_cancelled() && !cancellation_.token().stopRequested()) {
        // This Flight reader alone went away. Preserve the unread record for
        // another authorized reader; retention credit stays with the entry.
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = partitions_.find(partition_id);
        if (it != partitions_.end() && status_.ok()) {
          it->second.batches.push_front(std::move(entry));
          it->second.reading = false;
          cv_.notify_all();
          return false;
        }
      }
      fail(transit.status());
    } else {
      auto result = ShuffleBufferStore::replay(*entry, cancellation_.token());
      if (!result.ok()) fail(result.status());
      else {
        batch = std::move(*result);
        batch.transit_lease = std::move(*transit);
      }
    }
  } catch (const std::exception& error) {
    fail(arrow::Status::IOError("shuffle replay failed: ", error.what()));
  }
  entry.reset();
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = partitions_.find(partition_id);
    if (it != partitions_.end()) it->second.reading = false;
    cv_.notify_all();
    if (status_.ok()) return true;
  }
  batch.data.reset();
  batch.transit_lease.reset();
  return false;
}

bool PartitionDemuxer::isPartitionDrained(int32_t partition_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto it = partitions_.find(partition_id);
  return source_closed_ && (it == partitions_.end() ||
      (it->second.batches.empty() && !it->second.reading));
}

void PartitionDemuxer::drainLoop() {
  bool complete = false;
  try {
    while (running_.load()) {
      ShuffleBatch batch;
      const auto result = source_->pop(batch, std::chrono::milliseconds(10));
      if (result == pangrova::executor::PopResult::CLOSED) {
        if (source_->isAborted()) {
          fail(arrow::Status::IOError("shuffle producer aborted before commit"));
        } else if (!source_->isCommitted()) {
          fail(arrow::Status::IOError("shuffle producer closed without commit"));
        } else {
          complete = true;
        }
        break;
      }
      if (result == pangrova::executor::PopResult::TIMEOUT) continue;
      if (!running_.load()) break;
      if (!validPartition(batch.partition_id)) {
        fail(arrow::Status::Invalid("shuffle producer partition out of range"));
        break;
      }
      const auto partition_id = batch.partition_id;
      auto stored = store_->store(std::move(batch), cancellation_.token());
      if (!stored.ok()) {
        fail(stored.status());
        break;
      }
      auto entry = std::move(*stored);
      std::lock_guard<std::mutex> lock(mutex_);
      if (!running_.load()) break;
      auto& partition = partitions_[partition_id];
      partition.batches.push_back(std::move(entry));
      ++partition.committed_count;
      cv_.notify_all();
    }
  } catch (const std::exception& error) {
    fail(arrow::Status::IOError("shuffle buffering failed: ", error.what()));
  }
  if (!complete || !status().ok()) {
    // Only the drain thread touches the borrowed source during cancellation.
    // Abort wins against a late producer commit and wakes blocked producers;
    // a failed demux must never be published as a successful Flight EOF.
    try {
      source_->abort();
      source_->drain();
    } catch (const std::exception& error) {
      fail(arrow::Status::IOError("shuffle source cancellation failed: ", error.what()));
    }
  }
  finishDrain(complete);
}

bool PartitionDemuxer::quiesce(std::chrono::milliseconds timeout) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (drain_exited_) return source_complete_ && status_.ok();
  }
  if (!source_->isClosed()) {
    stop();
    return false;
  }
  {
    std::unique_lock<std::mutex> lock(mutex_);
    if (cv_.wait_for(lock, timeout, [this] { return drain_exited_; })) {
      return source_complete_ && status_.ok();
    }
  }
  stop();
  return false;
}

bool PartitionDemuxer::isComplete() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return source_complete_ && status_.ok();
}

bool PartitionDemuxer::isFullyConsumed() const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!source_closed_) return false;
  for (const auto& [pid, buf] : partitions_) {
    if (!buf.batches.empty() || buf.reading) return false;
  }
  return true;
}

bool PartitionDemuxer::markPartitionReadComplete(int32_t partition_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!validPartition(partition_id) || !source_complete_ || !status_.ok()) {
    return false;
  }
  const auto it = partitions_.find(partition_id);
  if (it != partitions_.end() &&
      (!it->second.batches.empty() || it->second.reading)) {
    return false;
  }
  completed_partitions_[partition_id] = true;
  return true;
}

bool PartitionDemuxer::allPartitionsReadComplete() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return std::all_of(
      completed_partitions_.begin(), completed_partitions_.end(),
      [](bool completed) { return completed; });
}

bool PartitionDemuxer::isPartitionTruncated(int32_t partition_id) {
  return !isComplete() && isPartitionDrained(partition_id);
}

size_t PartitionDemuxer::bufferedBatchCount(int32_t partition_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = partitions_.find(partition_id);
  if (it == partitions_.end()) return 0;
  return it->second.batches.size();
}

int64_t PartitionDemuxer::committedBatchCount(int32_t partition_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = partitions_.find(partition_id);
  if (it == partitions_.end()) return 0;
  return it->second.committed_count;
}

void PartitionDemuxer::setMaxPartitions(int32_t max_partitions) {
  if (running_.load() || drain_thread_.joinable() || max_partitions <= 0 ||
      max_partitions > store_->limits().partitions_per_handle) {
    throw std::invalid_argument("invalid shuffle partition limit or already started");
  }
  max_partitions_ = max_partitions;
  completed_partitions_.assign(max_partitions, false);
}

// ── FlightShuffleServer ─────────────────────────────────────────────

namespace {

// Parse a multiplexed ticket into its producer-owned routing and attempt
// identity. Wire format:
// handle\nquery_id\nstage_id\noutput_partition_id\nplan_epoch\nattempt_id\nfencing_token
bool parseTicket(const std::string& ticket,
                 int64_t& handle,
                 std::string& query_id,
                 int32_t& stage_id,
                 int32_t& partition_id,
                 uint64_t& plan_epoch,
                 uint32_t& attempt_id,
                 uint64_t& fencing_token) {
  std::istringstream iss(ticket);
  std::string handle_str, stage_str, part_str, epoch_str, attempt_str,
      fence_str, trailing;
  if (!std::getline(iss, handle_str) ||
      !std::getline(iss, query_id) ||
      !std::getline(iss, stage_str) ||
      !std::getline(iss, part_str) ||
      !std::getline(iss, epoch_str) ||
      !std::getline(iss, attempt_str) ||
      !std::getline(iss, fence_str) ||
      std::getline(iss, trailing)) {
    return false;
  }
  try {
    handle = std::stoll(handle_str);
    stage_id = std::stoi(stage_str);
    partition_id = std::stoi(part_str);
    plan_epoch = std::stoull(epoch_str);
    const auto parsed_attempt = std::stoull(attempt_str);
    if (parsed_attempt > std::numeric_limits<uint32_t>::max()) {
      return false;
    }
    attempt_id = static_cast<uint32_t>(parsed_attempt);
    fencing_token = std::stoull(fence_str);
  } catch (...) {
    return false;
  }
  return handle > 0 && !query_id.empty() && stage_id >= 0 &&
      partition_id >= 0 && plan_epoch > 0 && fencing_token > 0;
}

}  // namespace

FlightShuffleServer::FlightShuffleServer(FlightShuffleServerConfig config)
    : config_(std::move(config)) {
  if (config_.max_batch_bytes <= 0 ||
      config_.max_batch_bytes > static_cast<int64_t>(kCanonicalShuffleBatchBytes) ||
      config_.lingering_ttl_ms <= 0) {
    throw std::invalid_argument("shuffle response size and linger TTL must be positive");
  }
  config_.buffer_limits.max_payload_bytes = static_cast<uint64_t>(config_.max_batch_bytes);
  constexpr uint64_t kFlightMetadataBytes = 64 * 1024;
  if (config_.buffer_limits.transit_bytes <
      3 * config_.buffer_limits.max_payload_bytes + kFlightMetadataBytes) {
    throw std::invalid_argument(
        "shuffle transit grant cannot serve a canonical maximum-size batch");
  }
  buffer_store_ = std::make_shared<ShuffleBufferStore>(config_.buffer_limits);
  if (!config_.hmac_current_key.empty()) {
    ticket_verifier_ = std::make_shared<TicketVerifier>(
        config_.hmac_current_key, config_.hmac_previous_key);
    PANGROVA_LOG(INFO) << "event=ticket_verifier_enabled"
                      << " has_previous_key=" << !config_.hmac_previous_key.empty();
  }
}

void FlightShuffleServer::setHmacKeys(std::string current_key,
                                      std::string previous_key) {
  if (current_key.size() < 32) {
    throw std::invalid_argument(
        "shuffle HMAC key must contain at least 32 bytes");
  }
  if (!previous_key.empty() && previous_key.size() < 32) {
    throw std::invalid_argument(
        "previous shuffle HMAC key must contain at least 32 bytes");
  }
  auto replacement = std::make_shared<TicketVerifier>(
      current_key, previous_key);
  const bool has_previous_key = !previous_key.empty();
  {
    std::lock_guard<std::mutex> lock(ticket_verifier_mutex_);
    ticket_verifier_ = std::move(replacement);
  }
  PANGROVA_LOG(INFO) << "event=flight_shuffle_hmac_keys_updated"
                    << " has_previous_key=" << has_previous_key;
}

FlightShuffleServer::~FlightShuffleServer() {
  stop();
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
  if (running_.load()) {
    throw std::logic_error("FlightShuffleServer already running");
  }

  const bool any_tls_path = !config_.tls_cert_chain_path.empty() ||
      !config_.tls_private_key_path.empty() ||
      !config_.tls_root_ca_path.empty();
  auto location_result = any_tls_path
      ? arrow::flight::Location::ForGrpcTls("0.0.0.0", config_.port)
      : arrow::flight::Location::ForGrpcTcp("0.0.0.0", config_.port);
  if (!location_result.ok()) {
    throw std::runtime_error(
        "Failed to create Flight location: " +
        location_result.status().ToString());
  }

  arrow::flight::FlightServerOptions options(*location_result);

  // Configure SO_REUSEPORT for port sharing across threads.
  if (config_.reuse_port) {
    options.builder_hook = [](void* raw_builder) {
      auto* builder = static_cast<grpc::ServerBuilder*>(raw_builder);
      builder->AddChannelArgument(GRPC_ARG_ALLOW_REUSEPORT, 1);
    };
  }

  // Configure TLS if certificate paths are provided.
  if (any_tls_path) {
    if (config_.tls_cert_chain_path.empty() ||
        config_.tls_private_key_path.empty() ||
        config_.tls_root_ca_path.empty()) {
      throw std::invalid_argument(
          "Flight shuffle mutual TLS requires certificate chain, private key and root CA");
    }
    std::ifstream cert_file(config_.tls_cert_chain_path);
    std::ifstream key_file(config_.tls_private_key_path);
    if (!cert_file.good() || !key_file.good()) {
      throw std::runtime_error(
          "Failed to read Flight shuffle TLS certificate or private key");
    }
    std::string cert_pem((std::istreambuf_iterator<char>(cert_file)),
                          std::istreambuf_iterator<char>());
    std::string key_pem((std::istreambuf_iterator<char>(key_file)),
                         std::istreambuf_iterator<char>());
    arrow::flight::CertKeyPair cert_key{cert_pem, key_pem};
    options.tls_certificates.push_back(cert_key);

    std::ifstream ca_file(config_.tls_root_ca_path);
    if (!ca_file.good()) {
      throw std::runtime_error(
          "Failed to read Flight shuffle TLS root CA");
    }
    std::string ca_pem((std::istreambuf_iterator<char>(ca_file)),
                        std::istreambuf_iterator<char>());
    options.root_certificates = ca_pem;
    options.verify_client = true;
    PANGROVA_LOG(INFO) << "event=flight_shuffle_server_tls_enabled"
                      << " port=" << config_.port;
  } else {
    PANGROVA_LOG(WARNING) << "event=flight_shuffle_server_no_tls"
                        << " port=" << config_.port
                        << " detail=binding_without_tls_or_auth";
  }

  auto init_status = Init(options);
  if (!init_status.ok()) {
    throw std::runtime_error(
        "Failed to init Flight server: " + init_status.ToString());
  }

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
  running_.store(false);

  // F-SHUF-LINGER: wake and join the background evictor before tearing down
  // demuxers so it never touches lingering_demuxers_ concurrently with cleanup.
  {
    std::lock_guard<std::mutex> lock(evictor_mutex_);
    evictor_cv_.notify_all();
  }
  if (lingering_evictor_thread_.joinable()) {
    lingering_evictor_thread_.join();
  }

  // Drop routing under the maps' locks, then stop demuxers outside the locks.
  // stop() joins the drain thread and must never serialize unrelated routing.
  std::vector<std::shared_ptr<PartitionDemuxer>> demuxers_to_stop;
  {
    std::lock_guard<std::mutex> lock(handles_mutex_);
    for (auto& [handle, route] : handle_routes_) {
      demuxers_to_stop.push_back(route.demuxer);
    }
    handle_routes_.clear();
  }

  // Clean up lingering demuxers to prevent resource leaks on shutdown.
  {
    std::lock_guard<std::mutex> lock(lingering_mutex_);
    for (auto& [handle, entry] : lingering_demuxers_) {
      demuxers_to_stop.push_back(entry.route.demuxer);
    }
    size_t count = lingering_demuxers_.size();
    lingering_demuxers_.clear();
    if (count > 0) {
      PANGROVA_LOG(INFO) << "event=lingering_demuxers_cleaned count=" << count;
    }
  }
  for (const auto& demuxer : demuxers_to_stop) {
    demuxer->stop();
  }

  arrow::Status shutdown_status = arrow::Status::OK();
  if (bound_port_ > 0) {
    shutdown_status = Shutdown();
    bound_port_ = 0;
  }

  if (server_thread_.joinable()) {
    server_thread_.join();
  }

  if (!shutdown_status.ok()) {
    throw std::runtime_error(
        "Native Flight shuffle shutdown failed: " + shutdown_status.ToString());
  }

  PANGROVA_LOG(INFO) << "event=flight_shuffle_server_stopped port=" << bound_port_;
}

size_t FlightShuffleServer::evictFullyConsumedLingering(
    std::chrono::steady_clock::time_point now) {
  std::vector<std::shared_ptr<PartitionDemuxer>> evicted_demuxers;
  size_t evicted = 0;
  {
    std::lock_guard<std::mutex> lock(lingering_mutex_);
    for (auto it = lingering_demuxers_.begin();
         it != lingering_demuxers_.end();) {
      bool fully_consumed = it->second.route.demuxer->isFullyConsumed() &&
          it->second.route.demuxer->allPartitionsReadComplete();
      bool ttl_expired = config_.lingering_ttl_ms > 0 &&
          std::chrono::duration_cast<std::chrono::milliseconds>(
              now - it->second.entered_at).count() >=
              config_.lingering_ttl_ms;
      if (fully_consumed || ttl_expired) {
        if (ttl_expired && !fully_consumed) {
          PANGROVA_LOG(WARNING)
              << "event=lingering_demuxer_ttl_evicted handle=" << it->first;
        } else {
          PANGROVA_LOG(INFO)
              << "event=lingering_demuxer_evicted handle=" << it->first;
        }
        evicted_demuxers.push_back(it->second.route.demuxer);
        it = lingering_demuxers_.erase(it);
        ++evicted;
      } else {
        ++it;
      }
    }
  }
  for (const auto& demuxer : evicted_demuxers) {
    demuxer->stop();
  }
  return evicted;
}

void FlightShuffleServer::registerHandle(int64_t handle,
                                         SerializedShuffleQueue* queue,
                                         const std::string& query_id,
                                         int32_t stage_id,
                                         int32_t num_partitions,
                                         uint64_t plan_epoch,
                                         uint32_t attempt_id,
                                         uint64_t fencing_token) {
  if (handle <= 0 || queue == nullptr || query_id.empty() || stage_id < 0 ||
      num_partitions <= 0 || plan_epoch == 0 || fencing_token == 0) {
    throw std::invalid_argument(
        "shuffle registration requires exact fenced producer identity");
  }
  evictFullyConsumedLingering();
  auto demuxer = std::make_shared<PartitionDemuxer>(queue, buffer_store_);
  demuxer->setMaxPartitions(num_partitions);

  {
    std::scoped_lock lock(handles_mutex_, lingering_mutex_);
    if (handle_routes_.contains(handle) ||
        lingering_demuxers_.contains(handle)) {
      throw std::logic_error(
          "shuffle handle is already registered: " +
          std::to_string(handle));
    }
    handle_routes_.emplace(
        handle, HandleRoute{
            demuxer, query_id, stage_id, plan_epoch, attempt_id,
            fencing_token});
  }
  try {
    demuxer->start();
  } catch (...) {
    std::lock_guard<std::mutex> lock(handles_mutex_);
    handle_routes_.erase(handle);
    throw;
  }
  PANGROVA_LOG(INFO) << "event=shuffle_handle_registered handle=" << handle
                    << " query_id=" << query_id
                    << " stage_id=" << stage_id
                    << " plan_epoch=" << plan_epoch
                    << " attempt_id=" << attempt_id
                    << " fencing_token=" << fencing_token
                    << " num_partitions=" << num_partitions
                    << " total_handles=" << handleCount();
}

void FlightShuffleServer::unregisterHandle(int64_t handle) {
  HandleRoute route;
  bool already_lingering = false;
  {
    std::lock_guard<std::mutex> lock(handles_mutex_);
    auto it = handle_routes_.find(handle);
    if (it == handle_routes_.end()) {
      // Check lingering set — reap if fully consumed.
      std::lock_guard<std::mutex> ling_lock(lingering_mutex_);
      auto lit = lingering_demuxers_.find(handle);
      if (lit != lingering_demuxers_.end() &&
          lit->second.route.demuxer->isFullyConsumed() &&
          lit->second.route.demuxer->allPartitionsReadComplete()) {
        route = std::move(lit->second.route);
        lingering_demuxers_.erase(lit);
      }
      already_lingering = true;
    } else {
      route = std::move(it->second);
      handle_routes_.erase(it);
    }
  }
  if (!route.demuxer) return;
  if (already_lingering) {
    route.demuxer->stop();
    return;
  }

  // Quiesce outside the lock: bounded wait for clean producer close.
  bool clean = route.demuxer->quiesce(std::chrono::milliseconds(500));
  if (clean && !(route.demuxer->isFullyConsumed() &&
                 route.demuxer->allPartitionsReadComplete())) {
    // A clean producer must remain routable until every partition, including
    // zero-row partitions, has delivered EOF to its downstream reader.
    std::lock_guard<std::mutex> ling_lock(lingering_mutex_);
    lingering_demuxers_[handle] = LingeringEntry{
        std::move(route), std::chrono::steady_clock::now()};
    PANGROVA_LOG(INFO) << "event=shuffle_handle_lingering handle=" << handle;
  } else {
    // Un-clean or already consumed — drop.
    route.demuxer->stop();
    PANGROVA_LOG(INFO) << "event=shuffle_handle_unregistered handle=" << handle;
  }
}

size_t FlightShuffleServer::handleCount() const {
  std::lock_guard<std::mutex> lock(handles_mutex_);
  return handle_routes_.size();
}

size_t FlightShuffleServer::lingeringHandleCount() const {
  std::lock_guard<std::mutex> lock(lingering_mutex_);
  return lingering_demuxers_.size();
}

bool FlightShuffleServer::popForHandle(int64_t handle, int32_t partition_id,
                                       ShuffleBatch& batch,
                                       std::chrono::milliseconds timeout) {
  std::shared_ptr<PartitionDemuxer> demuxer;
  // Check active handles first.
  {
    std::lock_guard<std::mutex> lock(handles_mutex_);
    auto it = handle_routes_.find(handle);
    if (it != handle_routes_.end()) {
      demuxer = it->second.demuxer;
    }
  }
  // Check lingering handles.
  if (!demuxer) {
    std::lock_guard<std::mutex> lock(lingering_mutex_);
    auto it = lingering_demuxers_.find(handle);
    if (it != lingering_demuxers_.end()) {
      demuxer = it->second.route.demuxer;
    }
  }
  if (!demuxer) return false;
  if (demuxer->pop(partition_id, batch, timeout)) return true;
  if (demuxer->isComplete() && demuxer->isPartitionDrained(partition_id)) {
    demuxer->markPartitionReadComplete(partition_id);
  }
  return false;
}

arrow::Status FlightShuffleServer::DoGet(
    const arrow::flight::ServerCallContext& context,
    const arrow::flight::Ticket& request,
    std::unique_ptr<arrow::flight::FlightDataStream>* stream) {
  std::string routing_ticket = request.ticket;
  // Verify ticket HMAC signature and expiry before processing. Parse routing
  // fields from the verified payload, never from the signed envelope.
  std::shared_ptr<TicketVerifier> ticket_verifier;
  {
    std::lock_guard<std::mutex> lock(ticket_verifier_mutex_);
    ticket_verifier = ticket_verifier_;
  }
  if (ticket_verifier) {
    auto verify_result = ticket_verifier->verify(request.ticket);
    if (!verify_result.ok()) {
      PANGROVA_LOG(WARNING) << "event=ticket_verification_failed"
                           << " peer=" << context.peer();
      return arrow::flight::MakeFlightError(
          arrow::flight::FlightStatusCode::Unauthenticated,
          "Ticket verification failed");
    }
    routing_ticket = std::move(verify_result->payload);
  }

  int64_t handle = 0;
  std::string query_id;
  int32_t stage_id = 0;
  int32_t partition_id = 0;
  uint64_t plan_epoch = 0;
  uint32_t attempt_id = 0;
  uint64_t fencing_token = 0;
  if (!parseTicket(
          routing_ticket, handle, query_id, stage_id, partition_id,
          plan_epoch, attempt_id, fencing_token)) {
    return arrow::Status::Invalid(
        "Invalid fenced shuffle ticket format");
  }

  HandleRoute route;
  {
    std::lock_guard<std::mutex> lock(handles_mutex_);
    auto it = handle_routes_.find(handle);
    if (it != handle_routes_.end()) {
      route = it->second;
    }
  }
  if (!route.demuxer) {
    std::lock_guard<std::mutex> lock(lingering_mutex_);
    auto it = lingering_demuxers_.find(handle);
    if (it != lingering_demuxers_.end()) {
      route = it->second.route;
    }
  }
  if (!route.demuxer) {
    return arrow::Status::KeyError(
        "Unknown executor handle in ticket: " + std::to_string(handle));
  }
  if (!detail::ticketOwnershipValid(
          query_id, stage_id, route.query_id, route.stage_id)) {
    PANGROVA_LOG(WARNING) << "event=ticket_ownership_rejected"
                        << " handle=" << handle
                        << " ticket_query_id=" << query_id
                        << " ticket_stage_id=" << stage_id
                        << " registered_query_id=" << route.query_id
                        << " registered_stage_id=" << route.stage_id
                        << " peer=" << context.peer();
    return arrow::flight::MakeFlightError(
        arrow::flight::FlightStatusCode::Unauthorized,
        "Ticket query/stage identity does not match handle owner");
  }
  if (route.plan_epoch != plan_epoch || route.attempt_id != attempt_id ||
      route.fencing_token != fencing_token) {
    PANGROVA_LOG(WARNING) << "event=ticket_attempt_rejected"
                        << " handle=" << handle
                        << " ticket_plan_epoch=" << plan_epoch
                        << " ticket_attempt_id=" << attempt_id
                        << " ticket_fencing_token=" << fencing_token
                        << " peer=" << context.peer();
    return arrow::flight::MakeFlightError(
        arrow::flight::FlightStatusCode::Unauthorized,
        "Ticket attempt identity does not match handle owner");
  }
  if (!route.demuxer->validPartition(partition_id)) {
    return arrow::Status::Invalid("shuffle ticket partition out of range");
  }
  ARROW_RETURN_NOT_OK(route.demuxer->status());

  PANGROVA_LOG_DEBUG << "event=flight_doget handle=" << handle
                  << " query_id=" << query_id
                  << " stage_id=" << stage_id
                  << " partition_id=" << partition_id;

  ARROW_ASSIGN_OR_RAISE(auto reservation, buffer_store_->acquireStream());
  auto reader = std::make_shared<detail::ShuffleFlightBatchReader>(
      std::move(route.demuxer), partition_id, context, std::move(reservation));

  *stream = std::make_unique<arrow::flight::RecordBatchStream>(reader);

  return arrow::Status::OK();
}

}  // namespace pangrova::shuffle
