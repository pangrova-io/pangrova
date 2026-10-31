// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <typeindex>
#include <unordered_map>

namespace pangrova::artifact {

/** Process-wide weighted LRU for all parsed immutable artifact structures. */
class ParsedArtifactCache final {
 public:
  template <typename T>
  struct LoadedArtifact final {
    std::shared_ptr<T> value;
    uint64_t resident_bytes{0};
  };

  static ParsedArtifactCache& instance();

  template <typename T>
  std::shared_ptr<T> getOrLoad(
      const std::string& key,
      const std::function<uint64_t()>& declared_bytes,
      const std::function<LoadedArtifact<T>(uint64_t)>& loader) {
    std::shared_ptr<LoadState> load;
    {
      std::unique_lock<std::mutex> lock(mu_);
      if (capacity_bytes_ == 0 || key.empty()) {
        return nullptr;
      }
      auto cached = cache_.find(key);
      if (cached != cache_.end()) {
        touch(cached->first);
        return cached->second.type == std::type_index(typeid(T))
            ? std::static_pointer_cast<T>(cached->second.value)
            : nullptr;
      }
      auto loading = loads_.find(key);
      if (loading != loads_.end()) {
        load = loading->second;
        load->ready.wait(lock, [&] { return load->complete; });
        return load->type == std::type_index(typeid(T))
            ? std::static_pointer_cast<T>(load->value)
            : nullptr;
      }
      load = std::make_shared<LoadState>();
      load->type = std::type_index(typeid(T));
      loads_.emplace(key, load);
    }

    uint64_t reservation = 0;
    try {
      reservation = declared_bytes();
    } catch (...) {
      finishFailedLoad(key, load, 0);
      return nullptr;
    }
    {
      std::lock_guard<std::mutex> lock(mu_);
      bool admitted = reservation > 0 && reservation <= capacity_bytes_;
      while (admitted && !fits(reservation)) {
        if (!evictLRU()) {
          admitted = false;
        }
      }
      if (!admitted) {
        completeFailedLoad(key, load);
        return nullptr;
      }
      reserved_bytes_ += reservation;
    }

    LoadedArtifact<T> loaded;
    try {
      loaded = loader(reservation);
    } catch (...) {
      finishFailedLoad(key, load, reservation);
      return nullptr;
    }
    std::lock_guard<std::mutex> lock(mu_);
    reserved_bytes_ -= reservation;
    const bool admitted = loaded.value && loaded.resident_bytes > 0 &&
        loaded.resident_bytes <= reservation &&
        loaded.resident_bytes <= capacity_bytes_ - used_bytes_;
    if (admitted) {
      cache_[key] = Entry{
          loaded.value, loaded.resident_bytes, std::type_index(typeid(T))};
      used_bytes_ += loaded.resident_bytes;
      lru_.push_front(key);
      lru_index_[key] = lru_.begin();
      load->value = loaded.value;
    }
    load->complete = true;
    loads_.erase(key);
    load->ready.notify_all();
    return admitted ? loaded.value : nullptr;
  }

  void configure(uint64_t capacity_bytes);
  void shutdown();
  void evict(const std::string& key);
  bool enabled() const;
  uint64_t capacityBytes() const;
  uint64_t usedBytes() const;
  uint64_t reservedBytes() const;
  size_t size() const;

 private:
  struct Entry final {
    std::shared_ptr<void> value;
    uint64_t bytes{0};
    std::type_index type{typeid(void)};
  };

  struct LoadState final {
    bool complete{false};
    std::shared_ptr<void> value;
    std::type_index type{typeid(void)};
    std::condition_variable ready;
  };

  ParsedArtifactCache() = default;
  bool fits(uint64_t bytes) const;
  bool evictLRU();
  void touch(const std::string& key);
  void completeFailedLoad(
      const std::string& key,
      const std::shared_ptr<LoadState>& load);
  void finishFailedLoad(
      const std::string& key,
      const std::shared_ptr<LoadState>& load,
      uint64_t reservation);

  mutable std::mutex mu_;
  std::unordered_map<std::string, Entry> cache_;
  std::unordered_map<std::string, std::shared_ptr<LoadState>> loads_;
  std::list<std::string> lru_;
  std::unordered_map<std::string, std::list<std::string>::iterator> lru_index_;
  uint64_t capacity_bytes_{0};
  uint64_t used_bytes_{0};
  uint64_t reserved_bytes_{0};
};

}  // namespace pangrova::artifact
