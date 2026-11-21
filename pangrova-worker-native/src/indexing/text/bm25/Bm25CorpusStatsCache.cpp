// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include "indexing/text/bm25/Bm25CorpusStatsCache.h"

#include "capabilities/artifact/ParsedArtifactCache.h"

namespace pangrova::text::bm25 {
namespace {

std::string cacheKey(const std::string& path) {
  return "bm25-corpus:" + path;
}

}  // namespace

Bm25CorpusStatsCache& Bm25CorpusStatsCache::instance() {
  static Bm25CorpusStatsCache cache;
  return cache;
}

std::shared_ptr<Bm25CorpusStats> Bm25CorpusStatsCache::getOrLoad(
    const std::string& path) {
  return artifact::ParsedArtifactCache::instance().getOrLoad<Bm25CorpusStats>(
      cacheKey(path),
      [path] { return Bm25CorpusStats::residentBytesFromDisk(path); },
      [path](uint64_t reservation) {
        auto value = std::make_shared<Bm25CorpusStats>(
            Bm25CorpusStats::load(path, reservation));
        return artifact::ParsedArtifactCache::LoadedArtifact<Bm25CorpusStats>{
            value, value->residentBytes()};
      });
}

void Bm25CorpusStatsCache::evict(const std::string& path) {
  artifact::ParsedArtifactCache::instance().evict(cacheKey(path));
}

}  // namespace pangrova::text::bm25
