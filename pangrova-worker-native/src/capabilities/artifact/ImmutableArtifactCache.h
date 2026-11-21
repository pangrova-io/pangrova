// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

namespace pangrova::resource { class TaskObjectReadBudget; }

#include "pangrova/protocol/v2/execution_binding.pb.h"

namespace pangrova::artifact {

/** A pinned, checksum-verified immutable object in Worker-local storage. */
struct MaterializedArtifact final {
  MaterializedArtifact() = default;
  MaterializedArtifact(
      std::string path,
      uint64_t size,
      uint64_t downloaded)
      : local_path(std::move(path)),
        size_bytes(size),
        downloaded_bytes(downloaded) {}

  std::string local_path;
  uint64_t size_bytes{0};
  uint64_t downloaded_bytes{0};

 private:
  // Keeps the cache entry pinned while a task may still open/read local_path.
  std::shared_ptr<const void> lease_;
  friend class ImmutableArtifactCache;
};

/**
 * Process-wide content-addressed artifact cache.
 *
 * The cache is quota-isolated from Velox's data-page SSD cache. Concurrent
 * misses for one checksum are coalesced, publication is atomic, every first
 * use is verified against the canonical SHA-256, and LRU eviction skips
 * task-pinned entries.
 */
class ImmutableArtifactCache final {
 public:
  static ImmutableArtifactCache& instance();

  void configure(std::string root_directory, uint64_t capacity_bytes);
  void shutdown();

  bool enabled() const;
  uint64_t capacityBytes() const;
  uint64_t usedBytes() const;

  MaterializedArtifact materialize(
      const protocol::v2::ArtifactObject& object,
      uint64_t remaining_object_store_read_bytes,
      std::shared_ptr<resource::TaskObjectReadBudget> task_budget = nullptr);

 private:
  struct State;
  ImmutableArtifactCache();
  ~ImmutableArtifactCache();

  ImmutableArtifactCache(const ImmutableArtifactCache&) = delete;
  ImmutableArtifactCache& operator=(const ImmutableArtifactCache&) = delete;

  std::unique_ptr<State> state_;
};

}  // namespace pangrova::artifact
