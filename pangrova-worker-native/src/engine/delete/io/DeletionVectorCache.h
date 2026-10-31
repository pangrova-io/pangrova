// DeletionVectorCache: Bounded LRU cache for Roaring Bitmap deletion
// vectors. Avoids redundant I/O when multiple splits reference the same
// DV file (common in Paimon MOW and Iceberg v3 tables).
//
// Thread-safe: all public methods are protected by a mutex.
// Lifetime: one cache per TaskExecutor or per-process singleton.
//
// Placement: delete/io/ — file reading and caching utilities.
// Validates: Requirements R21.2, R21.5
#pragma once

#include <cstddef>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include <roaring.hh>

namespace pangrova::delete_filter::io {

// Bounded LRU cache for deletion vector Roaring Bitmaps.
// Evicts least-recently-used entries when capacity is exceeded.
//
// @ThreadSafe — all public methods are synchronized via mutex_.
class DeletionVectorCache {
 public:
  // Default max entries. Configurable at construction.
  static constexpr size_t kDefaultMaxEntries = 1024;

  explicit DeletionVectorCache(size_t max_entries = kDefaultMaxEntries);

  // Get a cached DV bitmap by file path. Returns nullptr on miss.
  // Promotes the entry to most-recently-used on hit.
  std::shared_ptr<roaring::Roaring> get(const std::string& dv_path);

  // Insert a DV bitmap into the cache. If the cache is full, evicts
  // the least-recently-used entry. If the key already exists, updates
  // the value and promotes to most-recently-used.
  void put(const std::string& dv_path,
           std::shared_ptr<roaring::Roaring> bitmap);

  // Get or load: returns cached bitmap if present, otherwise calls
  // the provided loader function, caches the result, and returns it.
  // The loader is called outside the lock to avoid holding the mutex
  // during I/O.
  std::shared_ptr<roaring::Roaring> getOrLoad(
      const std::string& dv_path,
      std::function<roaring::Roaring()> loader);

  // Number of cached entries.
  size_t size() const;

  // Clear all cached entries.
  void clear();

  // Maximum capacity.
  size_t maxEntries() const { return max_entries_; }

 private:
  // Evict the least-recently-used entry. Caller must hold mutex_.
  void evictLru();

  size_t max_entries_;

  // LRU list: front = most recently used, back = least recently used.
  using LruList = std::list<std::pair<std::string, std::shared_ptr<roaring::Roaring>>>;
  LruList lru_list_;

  // Map from dv_path to iterator in lru_list_ for O(1) lookup.
  std::unordered_map<std::string, LruList::iterator> cache_map_;

  mutable std::mutex mutex_;
};

}  // namespace pangrova::delete_filter::io
