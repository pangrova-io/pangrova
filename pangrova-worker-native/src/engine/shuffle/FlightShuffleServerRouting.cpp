#include "engine/shuffle/FlightShuffleServer.h"

#include <stdexcept>
#include <vector>

#include "engine/observability/PangrovaLog.h"

namespace pangrova::shuffle {

size_t FlightShuffleServer::evictFullyConsumedLingering(
    std::chrono::steady_clock::time_point now) {
  std::vector<std::shared_ptr<ShuffleProducerRegistration>> candidates;
  {
    std::lock_guard<std::mutex> lock(handles_mutex_);
    for (const auto& [handle, owner] : handle_routes_) candidates.push_back(owner);
  }
  size_t evicted = 0;
  for (const auto& owner : candidates) {
    if (!owner->shouldEvict(now, std::chrono::milliseconds(config_.lingering_ttl_ms))) continue;
    owner->close(true);
    {
      std::lock_guard<std::mutex> lock(handles_mutex_);
      const auto it = handle_routes_.find(owner->identity().handle);
      if (it != handle_routes_.end() && it->second == owner) {
        handle_routes_.erase(it);
        ++evicted;
      }
    }
  }
  return evicted;
}

std::shared_ptr<ShuffleProducerRegistration> FlightShuffleServer::registerHandle(
    int64_t handle, SerializedShuffleQueue* queue, const std::string& query_id,
    int32_t stage_id, int32_t num_partitions, uint64_t plan_epoch,
    uint32_t attempt_id, uint64_t fencing_token) {
  auto lease = acquireControl();
  if (num_partitions <= 0 || num_partitions > buffer_store_->limits().partitions_per_handle) {
    throw std::invalid_argument("invalid shuffle partition limit");
  }
  ShuffleProducerIdentity identity{
      handle, queue, query_id, stage_id, num_partitions, plan_epoch, attempt_id, fencing_token};
  // Validate before consulting an existing identity or reserving any credit.
  if (handle <= 0 || !queue || query_id.empty() || stage_id < 0 ||
      plan_epoch == 0 || fencing_token == 0) {
    throw std::invalid_argument("shuffle registration requires exact fenced producer identity");
  }
  evictFullyConsumedLingering();
  std::shared_ptr<ShuffleProducerRegistration> owner;
  bool created = false;
  {
    std::lock_guard<std::mutex> lock(handles_mutex_);
    const auto it = handle_routes_.find(handle);
    if (it != handle_routes_.end()) {
      owner = it->second;
      if (owner->identity() != identity) {
        throw std::logic_error("shuffle registration conflicts with the exact producer identity");
      }
    } else {
      if (handle_routes_.size() >= buffer_store_->limits().handles) {
        throw std::runtime_error("shuffle handle budget exhausted");
      }
      owner = std::make_shared<ShuffleProducerRegistration>(std::move(identity));
      handle_routes_.emplace(handle, owner);
      created = true;
    }
  }
  if (!created) {
    owner->awaitActive();
    return owner;
  }
  try {
    auto demuxer = std::make_shared<PartitionDemuxer>(queue, buffer_store_);
    demuxer->setMaxPartitions(num_partitions);
    demuxer->start();
    owner->publish(std::move(demuxer));
  } catch (...) {
    owner->failStart(std::current_exception());
    std::lock_guard<std::mutex> lock(handles_mutex_);
    const auto it = handle_routes_.find(handle);
    if (it != handle_routes_.end() && it->second == owner) handle_routes_.erase(it);
    throw;
  }
  PANGROVA_LOG(INFO) << "event=shuffle_handle_registered handle=" << handle
                    << " query_id=" << query_id << " stage_id=" << stage_id
                    << " plan_epoch=" << plan_epoch << " attempt_id=" << attempt_id
                    << " fencing_token=" << fencing_token << " num_partitions=" << num_partitions;
  return owner;
}

void FlightShuffleServer::unregisterHandle(int64_t handle) {
  std::shared_ptr<ShuffleProducerRegistration> owner;
  {
    std::lock_guard<std::mutex> lock(handles_mutex_);
    const auto it = handle_routes_.find(handle);
    if (it != handle_routes_.end()) owner = it->second;
  }
  if (!owner) return;
  owner->close();
  evictFullyConsumedLingering();
}

size_t FlightShuffleServer::handleCount() const {
  std::lock_guard<std::mutex> lock(handles_mutex_);
  size_t count = 0;
  for (const auto& [handle, owner] : handle_routes_) {
    const auto state = owner->state();
    if (state != ShuffleProducerRegistration::State::CLOSED &&
        state != ShuffleProducerRegistration::State::LINGERING) ++count;
  }
  return count;
}

size_t FlightShuffleServer::lingeringHandleCount() const {
  std::lock_guard<std::mutex> lock(handles_mutex_);
  size_t count = 0;
  for (const auto& [handle, owner] : handle_routes_) {
    if (owner->state() == ShuffleProducerRegistration::State::LINGERING) ++count;
  }
  return count;
}

bool FlightShuffleServer::popForHandle(int64_t handle, int32_t partition_id,
    ShuffleBatch& batch, std::chrono::milliseconds timeout) {
  std::shared_ptr<ShuffleProducerRegistration> owner;
  {
    std::lock_guard<std::mutex> lock(handles_mutex_);
    const auto it = handle_routes_.find(handle);
    if (it != handle_routes_.end()) owner = it->second;
  }
  if (!owner) return false;
  auto demuxer = owner->snapshot().demuxer;
  if (!demuxer) return false;
  if (demuxer->pop(partition_id, batch, timeout)) return true;
  if (demuxer->isComplete() && demuxer->isPartitionDrained(partition_id)) {
    demuxer->markPartitionReadComplete(partition_id);
  }
  return false;
}

}  // namespace pangrova::shuffle
