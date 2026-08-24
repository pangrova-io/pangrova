// PendingFilterRegistry: Thread-safe registry for externally injected
// dynamic filters. Bridges the gap between VeloxTaskRunner (which receives
// filters from Java via JNI) and PangrovaVeloxDataSource (which applies them).
//
// Lifecycle: VeloxTaskRunner registers filters via push(); DataSource
// drains them via drain() on each next() call. Registry entries are
// cleaned up when the task completes via remove().
//
// Placement: velox/filter/ — Dynamic filter infrastructure.
#pragma once

#include <array>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "velox/type/Filter.h"

namespace pangrova::velox {

// A single pending dynamic filter entry.
struct PendingFilterEntry {
  facebook::velox::column_index_t column_index;
  std::shared_ptr<facebook::velox::common::Filter> filter;
};

// Process-global registry for pending dynamic filters.
// Thread-safe: sharded by task_id hash to reduce lock contention under
// high concurrency. Each shard has its own mutex, so concurrent push/drain
// on different tasks never contend.
// @ThreadSafe
class PendingFilterRegistry {
 public:
  static PendingFilterRegistry& instance();

  // Push a filter for a given task. Called by VeloxTaskRunner.
  void push(const std::string& task_id,
            facebook::velox::column_index_t column_index,
            std::shared_ptr<facebook::velox::common::Filter> filter);

  // Drain all pending filters for a task. Called by PangrovaVeloxDataSource.
  // Returns and removes all accumulated filters for the task.
  std::vector<PendingFilterEntry> drain(const std::string& task_id);

  // Remove all entries for a task. Called on task completion/cancellation.
  void remove(const std::string& task_id);

  // Number of tasks with pending filters. For monitoring/diagnostics only.
  size_t taskCount() const;

 private:
  PendingFilterRegistry() = default;

  static constexpr size_t kNumShards = 16;

  struct Shard {
    mutable std::mutex mutex;
    std::unordered_map<std::string, std::vector<PendingFilterEntry>> store;
  };

  Shard& shardFor(const std::string& task_id);
  const Shard& shardFor(const std::string& task_id) const;

  std::array<Shard, kNumShards> shards_;
};

}  // namespace pangrova::velox
