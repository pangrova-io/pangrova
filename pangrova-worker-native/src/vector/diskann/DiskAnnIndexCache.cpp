// DiskAnnIndexCache: Global LRU cache implementation.

#include "vector/diskann/DiskAnnIndexCache.h"

namespace pangrova::vector::diskann {

DiskAnnIndexCache& DiskAnnIndexCache::instance() {
  static DiskAnnIndexCache cache;
  return cache;
}

std::shared_ptr<VamanaIndex> DiskAnnIndexCache::getOrLoad(
    const std::string& path, int64_t dimension) {
  // Fast path: cache hit under the lock, promoted to most-recently-used.
  {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = cache_.find(path);
    if (it != cache_.end()) {
      // PERF-T2: O(1) move-to-front via splice (no linear scan).
      auto lruIt = lruIndex_.find(path);
      if (lruIt != lruIndex_.end()) {
        lruList_.splice(lruList_.begin(), lruList_, lruIt->second);
      }
      return it->second;
    }
  }

  // Load from disk OUTSIDE the lock: disk I/O must never block other cache
  // users (no lock-during-I/O). Concurrent callers may load the same index
  // redundantly, but only one wins insertion; the losers' shared_ptr is
  // released by RAII.
  VamanaConfig config;
  config.dimension = dimension;
  auto index = std::make_shared<VamanaIndex>(config, path);
  if (!index->loadFromDisk(path)) {
    return nullptr;
  }

  // Publish under the lock, re-checking in case another thread won the race.
  std::lock_guard<std::mutex> lock(mu_);
  auto it = cache_.find(path);
  if (it != cache_.end()) {
    auto lruIt = lruIndex_.find(path);
    if (lruIt != lruIndex_.end()) {
      lruList_.splice(lruList_.begin(), lruList_, lruIt->second);
    }
    return it->second;
  }
  while (cache_.size() >= maxEntries_ && !lruList_.empty()) {
    evictLRU();
  }
  cache_[path] = index;
  lruList_.push_front(path);
  lruIndex_[path] = lruList_.begin();
  return index;
}

void DiskAnnIndexCache::evict(const std::string& path) {
  std::lock_guard<std::mutex> lock(mu_);
  cache_.erase(path);
  // PERF-T2: O(1) erase by iterator instead of O(N) list::remove.
  auto lruIt = lruIndex_.find(path);
  if (lruIt != lruIndex_.end()) {
    lruList_.erase(lruIt->second);
    lruIndex_.erase(lruIt);
  }
}

void DiskAnnIndexCache::setMaxEntries(size_t maxEntries) {
  std::lock_guard<std::mutex> lock(mu_);
  maxEntries_ = maxEntries;
  while (cache_.size() > maxEntries_ && !lruList_.empty()) {
    evictLRU();
  }
}

size_t DiskAnnIndexCache::size() const {
  std::lock_guard<std::mutex> lock(mu_);
  return cache_.size();
}

void DiskAnnIndexCache::evictLRU() {
  // Evict least recently used (back of list).
  if (lruList_.empty()) return;
  std::string victim = lruList_.back();
  lruList_.pop_back();
  lruIndex_.erase(victim);
  cache_.erase(victim);
}

}  // namespace pangrova::vector::diskann
