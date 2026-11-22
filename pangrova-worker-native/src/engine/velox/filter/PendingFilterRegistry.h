// PendingFilterRegistry: Thread-safe registry for externally injected
// dynamic filters. Bridges the gap between VeloxTaskRunner (which receives
// filters from Java via JNI) and PangrovaVeloxDataSource (which applies them).
//
// Lifecycle: VeloxTaskRunner registers filters via push(); the matching
// DataSource reads new entries with a private cursor. Registry entries are
// cleaned up when the task completes via remove().
//
// Placement: velox/filter/ — Dynamic filter infrastructure.
#pragma once

#include <array>
#include <atomic>
#include <cstdint>
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

// Each DataSource consumes the same task-scoped publication independently.
struct PendingFilterCursor {
  uint64_t observed_publication = 0;
  size_t next_filter = 0;
};

// Process-global registry for pending dynamic filters.
// Thread-safe: sharded by task_id hash to reduce lock contention under
// high concurrency. Readers bypass the shards when no publication changed.
// @ThreadSafe
class PendingFilterRegistry {
 public:
  static PendingFilterRegistry& instance();

  // Push a filter for one scan node in a task. Called by VeloxTaskRunner.
  void push(const std::string& task_id,
            const std::string& plan_node_id,
            facebook::velox::column_index_t column_index,
            std::shared_ptr<facebook::velox::common::Filter> filter);

  // Reads this source's unseen filters without consuming a sibling driver's copy.
  std::vector<PendingFilterEntry> readAfter(
      const std::string& task_id, const std::string& plan_node_id,
      PendingFilterCursor& cursor) const;

  // Remove all entries for a task. Called on task completion/cancellation.
  void remove(const std::string& task_id);

  // Number of tasks with pending filters. For monitoring/diagnostics only.
  size_t taskCount() const;

 private:
  PendingFilterRegistry() = default;

  static constexpr size_t kNumShards = 16;

  struct Shard {
    mutable std::mutex mutex;
    using PlanNodeStore =
        std::unordered_map<std::string, std::vector<PendingFilterEntry>>;
    std::unordered_map<std::string, PlanNodeStore> store;
  };

  Shard& shardFor(const std::string& task_id);
  const Shard& shardFor(const std::string& task_id) const;

  std::array<Shard, kNumShards> shards_;
  std::atomic<uint64_t> publication_{0};
};

}  // namespace pangrova::velox
