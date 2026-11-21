// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include "indexing/text/bm25/TextSearchFunctions.h"

#include <gtest/gtest.h>

namespace pangrova::text::bm25 {
namespace {

TEST(TextSearchFunctionsTest, MatchesAnyAnalyzedQueryTerm) {
  EXPECT_TRUE(matchesAnalyzedText(
      "A distributed SQL query engine", "missing DISTRIBUTED"));
}

TEST(TextSearchFunctionsTest, RejectsDocumentsWithoutAnyQueryTerm) {
  EXPECT_FALSE(matchesAnalyzedText(
      "columnar execution", "transaction coordinator"));
}

TEST(TextSearchFunctionsTest, EmptyQueryNeverMatches) {
  EXPECT_FALSE(matchesAnalyzedText("distributed search", "  "));
}

TEST(TextSearchFunctionsTest, UsesTheSameUnicodeAnalyzerAsBm25Artifacts) {
  EXPECT_TRUE(matchesAnalyzedText("分布式查询引擎", "查询"));
}

}  // namespace
}  // namespace pangrova::text::bm25
