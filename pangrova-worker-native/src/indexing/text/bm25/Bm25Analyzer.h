// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <string>
#include <string_view>
#include <vector>

namespace pangrova::text::bm25 {

inline constexpr std::string_view kStandardAnalyzer = "standard-v1";

/** Deterministic Unicode tokenizer shared by BM25 build and retrieval. */
class Bm25Analyzer final {
 public:
  static std::vector<std::string> tokenize(
      std::string_view text,
      std::string_view analyzer = kStandardAnalyzer);
};

}  // namespace pangrova::text::bm25
