// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <memory>
#include <string>

#include "indexing/text/bm25/Bm25CorpusStats.h"

namespace pangrova::text::bm25 {

/** Generation statistics backed by a bounded sparse index and read-only mmap. */
class Bm25CorpusStatsCache final {
 public:
  static Bm25CorpusStatsCache& instance();
  std::shared_ptr<Bm25CorpusStats> getOrLoad(const std::string& path);
  void evict(const std::string& path);

 private:
  Bm25CorpusStatsCache() = default;
};

}  // namespace pangrova::text::bm25
