// DiskAnnIndexCache: Global LRU cache for loaded VamanaIndex instances.
//
// Eliminates repeated loadFromDisk() calls for the same index path.
// Uses shared_ptr to ensure safe concurrent access: an index remains
// alive as long as any search is using it, even if evicted from cache.
//
// Thread-safe: all operations are protected by a mutex.
// Placement: vector/diskann/ — Native disk-resident vector index.
#pragma once

#include <cstddef>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "vector/diskann/VamanaGraph.h"

namespace pangrova::vector::diskann {

class DiskAnnIndexCache {
 public:
  static DiskAnnIndexCache& instance();

  // Returns cached index or loads from disk. Returns nullptr on load failure.
  std::shared_ptr<VamanaIndex> getOrLoad(
      const std::string& path, int64_t dimension);

  // Explicitly evict an entry (e.g., when index is rebuilt).
  void evict(const std::string& path);

  // Configure max cache entries (default: 16).
  void setMaxEntries(size_t maxEntries);

  // Current number of cached entries (for testing/monitoring).
  size_t size() const;

 private:
  DiskAnnIndexCache() = default;
  void evictLRU();

  mutable std::mutex mu_;
  std::unordered_map<std::string, std::shared_ptr<VamanaIndex>> cache_;
  std::list<std::string> lruList_;  // front = most recently used
  // PERF-T2: O(1) LRU index — maps path to its iterator in lruList_.
  // Replaces O(N) std::list::remove(path) with O(1) erase-by-iterator.
  std::unordered_map<std::string, std::list<std::string>::iterator> lruIndex_;
  size_t maxEntries_ = 16;
};

}  // namespace pangrova::vector::diskann
