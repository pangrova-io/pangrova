// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include "capabilities/artifact/ParsedArtifactCache.h"

#include <stdexcept>

namespace pangrova::artifact {

ParsedArtifactCache& ParsedArtifactCache::instance() {
  static ParsedArtifactCache cache;
  return cache;
}

void ParsedArtifactCache::configure(uint64_t capacity_bytes) {
  if (capacity_bytes == 0) {
    throw std::invalid_argument(
        "Parsed artifact cache requires a positive byte capacity");
  }
  std::lock_guard<std::mutex> lock(mu_);
  if (!loads_.empty()) {
    throw std::logic_error("Cannot configure parsed artifact cache during a load");
  }
  cache_.clear();
  lru_.clear();
  lru_index_.clear();
  used_bytes_ = 0;
  reserved_bytes_ = 0;
  capacity_bytes_ = capacity_bytes;
}

void ParsedArtifactCache::shutdown() {
  std::lock_guard<std::mutex> lock(mu_);
  if (!loads_.empty()) {
    throw std::logic_error("Cannot stop parsed artifact cache during a load");
  }
  cache_.clear();
  lru_.clear();
  lru_index_.clear();
  used_bytes_ = 0;
  reserved_bytes_ = 0;
  capacity_bytes_ = 0;
}

void ParsedArtifactCache::evict(const std::string& key) {
  std::lock_guard<std::mutex> lock(mu_);
  auto entry = cache_.find(key);
  if (entry != cache_.end()) {
    used_bytes_ -= entry->second.bytes;
    cache_.erase(entry);
  }
  auto position = lru_index_.find(key);
  if (position != lru_index_.end()) {
    lru_.erase(position->second);
    lru_index_.erase(position);
  }
}

bool ParsedArtifactCache::enabled() const {
  std::lock_guard<std::mutex> lock(mu_);
  return capacity_bytes_ > 0;
}

uint64_t ParsedArtifactCache::capacityBytes() const {
  std::lock_guard<std::mutex> lock(mu_);
  return capacity_bytes_;
}

uint64_t ParsedArtifactCache::usedBytes() const {
  std::lock_guard<std::mutex> lock(mu_);
  return used_bytes_;
}

uint64_t ParsedArtifactCache::reservedBytes() const {
  std::lock_guard<std::mutex> lock(mu_);
  return reserved_bytes_;
}

size_t ParsedArtifactCache::size() const {
  std::lock_guard<std::mutex> lock(mu_);
  return cache_.size();
}

bool ParsedArtifactCache::fits(uint64_t bytes) const {
  return used_bytes_ <= capacity_bytes_ - bytes &&
      reserved_bytes_ <= capacity_bytes_ - bytes - used_bytes_;
}

bool ParsedArtifactCache::evictLRU() {
  for (auto iterator = lru_.rbegin(); iterator != lru_.rend(); ++iterator) {
    auto entry = cache_.find(*iterator);
    if (entry == cache_.end() || entry->second.value.use_count() != 1) {
      continue;
    }
    used_bytes_ -= entry->second.bytes;
    lru_index_.erase(entry->first);
    lru_.erase(std::next(iterator).base());
    cache_.erase(entry);
    return true;
  }
  return false;
}

void ParsedArtifactCache::touch(const std::string& key) {
  auto position = lru_index_.find(key);
  if (position != lru_index_.end()) {
    lru_.splice(lru_.begin(), lru_, position->second);
  }
}

void ParsedArtifactCache::completeFailedLoad(
    const std::string& key,
    const std::shared_ptr<LoadState>& load) {
  load->complete = true;
  loads_.erase(key);
  load->ready.notify_all();
}

void ParsedArtifactCache::finishFailedLoad(
    const std::string& key,
    const std::shared_ptr<LoadState>& load,
    uint64_t reservation) {
  std::lock_guard<std::mutex> lock(mu_);
  if (reservation > reserved_bytes_) {
    throw std::logic_error("Parsed artifact reservation accounting underflow");
  }
  reserved_bytes_ -= reservation;
  completeFailedLoad(key, load);
}

}  // namespace pangrova::artifact
