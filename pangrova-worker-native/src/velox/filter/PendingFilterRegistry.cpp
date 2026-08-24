// PendingFilterRegistry: Sharded implementation for reduced lock contention.
// Each shard owns an independent mutex + map, keyed by hash(task_id) % N.
// Concurrent push/drain on different tasks never contend.
#include "velox/filter/PendingFilterRegistry.h"

#include <functional>

namespace pangrova::velox {

PendingFilterRegistry& PendingFilterRegistry::instance() {
  static PendingFilterRegistry registry;
  return registry;
}

PendingFilterRegistry::Shard& PendingFilterRegistry::shardFor(
    const std::string& task_id) {
  auto idx = std::hash<std::string>{}(task_id) % kNumShards;
  return shards_[idx];
}

const PendingFilterRegistry::Shard& PendingFilterRegistry::shardFor(
    const std::string& task_id) const {
  auto idx = std::hash<std::string>{}(task_id) % kNumShards;
  return shards_[idx];
}

void PendingFilterRegistry::push(
    const std::string& task_id,
    facebook::velox::column_index_t column_index,
    std::shared_ptr<facebook::velox::common::Filter> filter) {
  auto& shard = shardFor(task_id);
  std::lock_guard<std::mutex> lock(shard.mutex);
  shard.store[task_id].push_back({column_index, std::move(filter)});
}

std::vector<PendingFilterEntry> PendingFilterRegistry::drain(
    const std::string& task_id) {
  auto& shard = shardFor(task_id);
  std::lock_guard<std::mutex> lock(shard.mutex);
  auto it = shard.store.find(task_id);
  if (it == shard.store.end()) {
    return {};
  }
  std::vector<PendingFilterEntry> result;
  result.swap(it->second);
  return result;
}

void PendingFilterRegistry::remove(const std::string& task_id) {
  auto& shard = shardFor(task_id);
  std::lock_guard<std::mutex> lock(shard.mutex);
  shard.store.erase(task_id);
}

size_t PendingFilterRegistry::taskCount() const {
  size_t total = 0;
  for (const auto& shard : shards_) {
    std::lock_guard<std::mutex> lock(shard.mutex);
    total += shard.store.size();
  }
  return total;
}

}  // namespace pangrova::velox
