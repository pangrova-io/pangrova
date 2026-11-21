// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <memory>
#include <string>

#include "indexing/text/bm25/Bm25Segment.h"

namespace pangrova::text::bm25 {

/** BM25 typed access path backed by the process-wide parsed artifact cache. */
class Bm25SegmentCache final {
 public:
  static Bm25SegmentCache& instance();
  std::shared_ptr<Bm25Segment> getOrLoad(const std::string& path);
  void evict(const std::string& path);

 private:
  Bm25SegmentCache() = default;
};

}  // namespace pangrova::text::bm25
