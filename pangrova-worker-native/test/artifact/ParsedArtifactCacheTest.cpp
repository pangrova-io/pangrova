// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include <memory>

#include "artifact/ParsedArtifactCache.h"

namespace pangrova::artifact::test {
namespace {

struct ParsedValue final {
  int value;
};

class ParsedArtifactCacheTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (ParsedArtifactCache::instance().enabled()) {
      ParsedArtifactCache::instance().shutdown();
    }
  }

  void TearDown() override {
    if (ParsedArtifactCache::instance().enabled()) {
      ParsedArtifactCache::instance().shutdown();
    }
  }
};

std::shared_ptr<ParsedValue> load(const std::string& key, uint64_t bytes) {
  return ParsedArtifactCache::instance().getOrLoad<ParsedValue>(
      key,
      [bytes] { return bytes; },
      [bytes](uint64_t reservation) {
        EXPECT_EQ(reservation, bytes);
        return ParsedArtifactCache::LoadedArtifact<ParsedValue>{
            std::make_shared<ParsedValue>(ParsedValue{7}), bytes};
      });
}

TEST_F(ParsedArtifactCacheTest, SharesOneHardQuotaAcrossArtifactKinds) {
  ParsedArtifactCache::instance().configure(100);
  auto pinned = load("diskann:first", 60);
  ASSERT_NE(pinned, nullptr);

  EXPECT_EQ(load("bm25:second", 60), nullptr);
  EXPECT_EQ(ParsedArtifactCache::instance().usedBytes(), 60);
  pinned.reset();

  EXPECT_NE(load("bm25:second", 60), nullptr);
  EXPECT_EQ(ParsedArtifactCache::instance().size(), 1);
  EXPECT_EQ(ParsedArtifactCache::instance().usedBytes(), 60);
}

TEST_F(ParsedArtifactCacheTest, RejectsUnderstatedLoadedFootprint) {
  ParsedArtifactCache::instance().configure(100);
  auto result = ParsedArtifactCache::instance().getOrLoad<ParsedValue>(
      "bm25:understated",
      [] { return 40; },
      [](uint64_t) {
        return ParsedArtifactCache::LoadedArtifact<ParsedValue>{
            std::make_shared<ParsedValue>(ParsedValue{7}), 41};
      });

  EXPECT_EQ(result, nullptr);
  EXPECT_EQ(ParsedArtifactCache::instance().usedBytes(), 0);
  EXPECT_EQ(ParsedArtifactCache::instance().reservedBytes(), 0);
}

}  // namespace
}  // namespace pangrova::artifact::test
