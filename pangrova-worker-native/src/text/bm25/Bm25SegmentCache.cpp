// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include "text/bm25/Bm25SegmentCache.h"

#include "artifact/ParsedArtifactCache.h"

namespace pangrova::text::bm25 {
namespace {

std::string cacheKey(const std::string& path) {
  return "bm25:" + path;
}

}  // namespace

Bm25SegmentCache& Bm25SegmentCache::instance() {
  static Bm25SegmentCache cache;
  return cache;
}

std::shared_ptr<Bm25Segment> Bm25SegmentCache::getOrLoad(
    const std::string& path) {
  return artifact::ParsedArtifactCache::instance().getOrLoad<Bm25Segment>(
      cacheKey(path),
      [path] { return Bm25Segment::residentBytesFromDisk(path); },
      [path](uint64_t reservation) {
        auto value = std::make_shared<Bm25Segment>(
            Bm25Segment::load(path, reservation));
        return artifact::ParsedArtifactCache::LoadedArtifact<Bm25Segment>{
            value, value->residentBytes()};
      });
}

void Bm25SegmentCache::evict(const std::string& path) {
  artifact::ParsedArtifactCache::instance().evict(cacheKey(path));
}

}  // namespace pangrova::text::bm25
