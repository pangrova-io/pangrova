// PendingFilterRegistry: Sharded implementation for reduced lock contention.
// Each shard owns an independent mutex + map, keyed by hash(task_id) % N.
#include "engine/velox/filter/PendingFilterRegistry.h"

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
    const std::string& plan_node_id,
    facebook::velox::column_index_t column_index,
    std::shared_ptr<facebook::velox::common::Filter> filter) {
  auto& shard = shardFor(task_id);
  std::lock_guard<std::mutex> lock(shard.mutex);
  shard.store[task_id][plan_node_id].push_back(
      {column_index, std::move(filter)});
  publication_.fetch_add(1, std::memory_order_release);
}

std::vector<PendingFilterEntry> PendingFilterRegistry::readAfter(
    const std::string& task_id,
    const std::string& plan_node_id,
    PendingFilterCursor& cursor) const {
  const auto published = publication_.load(std::memory_order_acquire);
  if (cursor.observed_publication == published) return {};
  const auto& shard = shardFor(task_id);
  std::lock_guard<std::mutex> lock(shard.mutex);
  cursor.observed_publication = published;
  auto task_it = shard.store.find(task_id);
  if (task_it == shard.store.end()) {
    return {};
  }
  auto node_it = task_it->second.find(plan_node_id);
  if (node_it == task_it->second.end() || cursor.next_filter >= node_it->second.size()) {
    return {};
  }
  const auto& filters = node_it->second;
  std::vector<PendingFilterEntry> result(filters.begin() + cursor.next_filter, filters.end());
  cursor.next_filter = filters.size();
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
