// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include "vector/diskann/DiskAnnIndexCache.h"

#include "artifact/ParsedArtifactCache.h"

namespace pangrova::vector::diskann {
namespace {

std::string cacheKey(const std::string& path) {
  return "diskann:" + path;
}

}  // namespace

DiskAnnIndexCache& DiskAnnIndexCache::instance() {
  static DiskAnnIndexCache cache;
  return cache;
}

std::shared_ptr<VamanaIndex> DiskAnnIndexCache::getOrLoad(
    const std::string& path,
    int64_t dimension) {
  auto index = artifact::ParsedArtifactCache::instance().getOrLoad<VamanaIndex>(
      cacheKey(path),
      [path, dimension] {
        return VamanaIndex::residentBytesFromDisk(path, dimension).value_or(0);
      },
      [path, dimension](uint64_t reservation) {
        VamanaConfig config;
        config.dimension = dimension;
        auto value = std::make_shared<VamanaIndex>(config, path);
        if (!value->loadFromDisk(path, reservation)) {
          value.reset();
        }
        return artifact::ParsedArtifactCache::LoadedArtifact<VamanaIndex>{
            value, value ? value->memoryBytes() : 0};
      });
  return index && index->dimension() == dimension ? index : nullptr;
}

void DiskAnnIndexCache::configure(uint64_t capacity_bytes) {
  artifact::ParsedArtifactCache::instance().configure(capacity_bytes);
}

void DiskAnnIndexCache::shutdown() {
  artifact::ParsedArtifactCache::instance().shutdown();
}

bool DiskAnnIndexCache::enabled() const {
  return artifact::ParsedArtifactCache::instance().enabled();
}

uint64_t DiskAnnIndexCache::capacityBytes() const {
  return artifact::ParsedArtifactCache::instance().capacityBytes();
}

uint64_t DiskAnnIndexCache::usedBytes() const {
  return artifact::ParsedArtifactCache::instance().usedBytes();
}

uint64_t DiskAnnIndexCache::reservedBytes() const {
  return artifact::ParsedArtifactCache::instance().reservedBytes();
}

void DiskAnnIndexCache::evict(const std::string& path) {
  artifact::ParsedArtifactCache::instance().evict(cacheKey(path));
}

size_t DiskAnnIndexCache::size() const {
  return artifact::ParsedArtifactCache::instance().size();
}

}  // namespace pangrova::vector::diskann
