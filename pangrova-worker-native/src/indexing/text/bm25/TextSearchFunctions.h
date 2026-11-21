// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <string_view>

namespace pangrova::text::bm25 {

/** Exact boolean semantics shared by scan-time MATCH and indexed BM25 retrieval. */
bool matchesAnalyzedText(std::string_view text, std::string_view query);

/** Registers the Native MATCH(VARCHAR, VARCHAR) scalar function. */
void registerTextSearchFunctions();

}  // namespace pangrova::text::bm25
