// PendingEndpointRegistry: Sharded implementation for reduced lock contention.
#include "velox/exchange/PendingEndpointRegistry.h"

#include <functional>

namespace pangrova::velox {

PendingEndpointRegistry& PendingEndpointRegistry::instance() {
  static PendingEndpointRegistry registry;
  return registry;
}

PendingEndpointRegistry::Shard& PendingEndpointRegistry::shardFor(
    const std::string& source_id) {
  auto idx = std::hash<std::string>{}(source_id) % kNumShards;
  return shards_[idx];
}

void PendingEndpointRegistry::put(
    const std::string& source_id, PendingEndpoint endpoint) {
  auto& shard = shardFor(source_id);
  std::lock_guard<std::mutex> lock(shard.mutex);
  shard.store[source_id] = std::move(endpoint);
}

std::optional<PendingEndpoint> PendingEndpointRegistry::take(
    const std::string& source_id) {
  auto& shard = shardFor(source_id);
  std::lock_guard<std::mutex> lock(shard.mutex);
  auto it = shard.store.find(source_id);
  if (it == shard.store.end()) {
    return std::nullopt;
  }
  auto endpoint = std::move(it->second);
  shard.store.erase(it);
  return endpoint;
}

void PendingEndpointRegistry::clear() {
  for (auto& shard : shards_) {
    std::lock_guard<std::mutex> lock(shard.mutex);
    shard.store.clear();
  }
}

}  // namespace pangrova::velox
