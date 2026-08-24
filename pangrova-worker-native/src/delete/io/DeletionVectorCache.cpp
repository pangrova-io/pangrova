// DeletionVectorCache implementation.
// Validates: Requirements R21.2, R21.5
#include "delete/io/DeletionVectorCache.h"

namespace pangrova::delete_filter::io {

DeletionVectorCache::DeletionVectorCache(size_t max_entries)
    : max_entries_(max_entries > 0 ? max_entries : kDefaultMaxEntries) {}

std::shared_ptr<roaring::Roaring> DeletionVectorCache::get(
    const std::string& dv_path) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = cache_map_.find(dv_path);
  if (it == cache_map_.end()) {
    return nullptr;
  }
  // Promote to front (most recently used).
  lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
  return it->second->second;
}

void DeletionVectorCache::put(
    const std::string& dv_path,
    std::shared_ptr<roaring::Roaring> bitmap) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = cache_map_.find(dv_path);
  if (it != cache_map_.end()) {
    // Update existing entry and promote to front.
    it->second->second = std::move(bitmap);
    lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
    return;
  }
  // Evict if at capacity.
  if (cache_map_.size() >= max_entries_) {
    evictLru();
  }
  // Insert at front.
  lru_list_.emplace_front(dv_path, std::move(bitmap));
  cache_map_[dv_path] = lru_list_.begin();
}

std::shared_ptr<roaring::Roaring> DeletionVectorCache::getOrLoad(
    const std::string& dv_path,
    std::function<roaring::Roaring()> loader) {
  // Phase 1: Check cache under lock.
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = cache_map_.find(dv_path);
    if (it != cache_map_.end()) {
      // Promote to front (most recently used) and return.
      lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
      return it->second->second;
    }
  }
  // Phase 2: Load outside the lock to avoid holding mutex during I/O.
  auto bitmap = std::make_shared<roaring::Roaring>(loader());
  // Phase 3: Double-check under lock before inserting (another thread
  // may have loaded the same key concurrently).
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = cache_map_.find(dv_path);
    if (it != cache_map_.end()) {
      // Another thread won the race — promote and return its result.
      lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
      return it->second->second;
    }
    // Insert our result.
    if (cache_map_.size() >= max_entries_) {
      evictLru();
    }
    lru_list_.emplace_front(dv_path, bitmap);
    cache_map_[dv_path] = lru_list_.begin();
  }
  return bitmap;
}

size_t DeletionVectorCache::size() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return cache_map_.size();
}

void DeletionVectorCache::clear() {
  std::lock_guard<std::mutex> lock(mutex_);
  cache_map_.clear();
  lru_list_.clear();
}

void DeletionVectorCache::evictLru() {
  // Caller must hold mutex_.
  if (lru_list_.empty()) {
    return;
  }
  auto& back = lru_list_.back();
  cache_map_.erase(back.first);
  lru_list_.pop_back();
}

}  // namespace pangrova::delete_filter::io
