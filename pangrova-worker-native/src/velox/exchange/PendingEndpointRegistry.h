// PendingEndpointRegistry: Process-global registry for Flight endpoints
// that have been announced via CONNECT_EXCHANGE_SOURCE but not yet consumed
// by an PangrovaExchangeSource instance.
//
// Flow: Java sends CONNECT_EXCHANGE_SOURCE → dispatcher stores endpoint here
// → PangrovaExchangeSource checks on construction and activates Flight mode.
//
// Sharded by hash(source_id) for reduced lock contention.
#pragma once

#include <array>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

namespace pangrova::velox {

struct PendingEndpoint {
  std::string host;
  int port = 0;
  int64_t executor_handle = 0;
  std::string query_id;
  int stage_id = 0;
  int partition_id = 0;
};

class PendingEndpointRegistry {
 public:
  static PendingEndpointRegistry& instance();

  // Store a pending endpoint for a source. Called from command dispatcher.
  void put(const std::string& source_id, PendingEndpoint endpoint);

  // Take and remove a pending endpoint for a source. Returns nullopt if
  // no endpoint is registered. Called from PangrovaExchangeSource constructor.
  std::optional<PendingEndpoint> take(const std::string& source_id);

  // Remove all endpoints (cleanup on shutdown).
  void clear();

 private:
  PendingEndpointRegistry() = default;

  static constexpr size_t kNumShards = 16;

  struct Shard {
    mutable std::mutex mutex;
    std::unordered_map<std::string, PendingEndpoint> store;
  };

  Shard& shardFor(const std::string& source_id);

  std::array<Shard, kNumShards> shards_;
};

}  // namespace pangrova::velox
