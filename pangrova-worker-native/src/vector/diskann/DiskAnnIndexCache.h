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
#include <cstdint>
#include <memory>
#include <string>

#include "vector/diskann/VamanaGraph.h"

namespace pangrova::vector::diskann {

class DiskAnnIndexCache {
 public:
  static DiskAnnIndexCache& instance();

  // Returns cached index or loads from disk. Returns nullptr on load failure.
  std::shared_ptr<VamanaIndex> getOrLoad(
      const std::string& path, int64_t dimension);

  // Configures a hard process-resident byte quota. A zero quota disables the
  // cache and therefore the DiskANN retrieval capability.
  void configure(uint64_t capacityBytes);
  void shutdown();
  bool enabled() const;
  uint64_t capacityBytes() const;
  uint64_t usedBytes() const;
  uint64_t reservedBytes() const;

  // Explicitly evict an entry.
  void evict(const std::string& path);

  // Current number of cached entries (for testing/monitoring).
  size_t size() const;

 private:
  DiskAnnIndexCache() = default;
};

}  // namespace pangrova::vector::diskann
