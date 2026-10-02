// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <string>
#include <vector>

#include "text/bm25/Bm25Analyzer.h"
#include "text/bm25/Bm25CorpusStats.h"
#include "text/bm25/Bm25Segment.h"

namespace pangrova::text::bm25::test {
namespace {

class TestFile final {
 public:
  TestFile() {
    path_ = std::filesystem::temp_directory_path() /
        ("pangrova-bm25-" + std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count()));
  }
  ~TestFile() {
    std::error_code ignored;
    std::filesystem::remove(path_, ignored);
  }
  std::string path() const { return path_.string(); }

 private:
  std::filesystem::path path_;
};

TEST(Bm25AnalyzerTest, ProducesDeterministicLatinAndCjkTokens) {
  EXPECT_EQ(
      Bm25Analyzer::tokenize("Hello, HELLO 世界"),
      (std::vector<std::string>{"hello", "hello", "世", "界"}));
  EXPECT_THROW(
      Bm25Analyzer::tokenize(std::string("\xC0\xAF", 2)),
      std::invalid_argument);
  EXPECT_THROW(
      Bm25Analyzer::tokenize("value", "unknown"),
      std::invalid_argument);
}

TEST(Bm25SegmentTest, RoundTripsAndRanksByCorpusStatistics) {
  Bm25SegmentBuilder builder;
  builder.addDocument("distributed native search search");
  builder.addDocument("distributed query engine");
  builder.addDocument("unrelated document");
  TestFile file;
  TestFile statistics_file;
  builder.write(file.path());
  builder.writeStatistics(statistics_file.path());

  const uint64_t resident = Bm25Segment::residentBytesFromDisk(file.path());
  auto segment = Bm25Segment::load(file.path(), resident);
  auto corpus_stats = Bm25CorpusStats::load(
      statistics_file.path(),
      Bm25CorpusStats::residentBytesFromDisk(statistics_file.path()));
  auto results = segment.search(
      Bm25Analyzer::tokenize("distributed search"), 2, 1.2, 0.75,
      corpus_stats);

  ASSERT_EQ(segment.documentCount(), 3);
  ASSERT_EQ(results.size(), 2);
  EXPECT_EQ(results[0].document, 0);
  EXPECT_EQ(results[1].document, 1);
  EXPECT_GT(results[0].score, results[1].score);
  EXPECT_EQ(segment.residentBytes(), resident);
}

TEST(Bm25SegmentTest, RejectsLoadBeforeExceedingResidentQuota) {
  Bm25SegmentBuilder builder;
  builder.addDocument("bounded immutable segment");
  TestFile file;
  builder.write(file.path());
  const uint64_t resident = Bm25Segment::residentBytesFromDisk(file.path());
  ASSERT_GT(resident, 0);

  EXPECT_THROW(
      Bm25Segment::load(file.path(), resident - 1),
      std::runtime_error);
}

TEST(Bm25SegmentTest, RejectsTruncatedSegment) {
  Bm25SegmentBuilder builder;
  builder.addDocument("complete segment");
  TestFile file;
  builder.write(file.path());
  std::filesystem::resize_file(
      file.path(), std::filesystem::file_size(file.path()) - 1);

  EXPECT_THROW(
      Bm25Segment::load(file.path(), 1024 * 1024),
      std::runtime_error);
}

TEST(Bm25CorpusStatsTest, MergesSortedShardsWithBoundedFanIn) {
  TestFile first;
  TestFile second;
  TestFile merged;
  Bm25CorpusStats::writeShard(
      first.path(), 2, 6, {{"alpha", 2}, {"beta", 1}});
  Bm25CorpusStats::writeShard(
      second.path(), 3, 9, {{"alpha", 1}, {"gamma", 3}});

  Bm25CorpusStats::mergeSortedShards(
      {first.path(), second.path()}, merged.path());
  auto statistics = Bm25CorpusStats::load(
      merged.path(), Bm25CorpusStats::residentBytesFromDisk(merged.path()));

  EXPECT_EQ(statistics.documentCount(), 5);
  EXPECT_EQ(statistics.totalDocumentLength(), 15);
  EXPECT_DOUBLE_EQ(statistics.averageDocumentLength(), 3.0);
  EXPECT_EQ(statistics.documentFrequency("alpha"), 3);
  EXPECT_EQ(statistics.documentFrequency("beta"), 1);
  EXPECT_EQ(statistics.documentFrequency("gamma"), 3);
  EXPECT_EQ(statistics.documentFrequency("missing"), 0);
}

TEST(Bm25CorpusStatsTest, RejectsUnboundedReductionFanIn) {
  TestFile output;
  EXPECT_THROW(
      Bm25CorpusStats::mergeSortedShards(
          std::vector<std::string>(65, output.path()), output.path()),
      std::invalid_argument);
}

TEST(Bm25CorpusStatsTest, SparseIndexBoundsMemoryAndFindsBlockEdges) {
  TestFile file;
  std::vector<std::pair<std::string, uint64_t>> frequencies;
  frequencies.reserve(1025);
  for (int index = 0; index < 1025; ++index) {
    std::string suffix = std::to_string(index);
    frequencies.emplace_back(
        "term-" + std::string(4 - suffix.size(), '0') + suffix,
        static_cast<uint64_t>(1 + index % 7));
  }
  Bm25CorpusStats::writeShard(file.path(), 10, 100, frequencies);

  const uint64_t resident = Bm25CorpusStats::residentBytesFromDisk(file.path());
  auto statistics = Bm25CorpusStats::load(file.path(), resident);

  EXPECT_LT(resident, std::filesystem::file_size(file.path()) / 8);
  EXPECT_EQ(statistics.documentFrequency("term-0000"), 1);
  EXPECT_EQ(statistics.documentFrequency("term-0255"), 4);
  EXPECT_EQ(statistics.documentFrequency("term-0256"), 5);
  EXPECT_EQ(statistics.documentFrequency("term-1024"), 3);
  EXPECT_EQ(statistics.documentFrequency("term-1025"), 0);
  EXPECT_THROW(
      Bm25CorpusStats::load(file.path(), resident - 1),
      std::runtime_error);
}

TEST(Bm25CorpusStatsTest, RejectsTruncatedSparseIndex) {
  TestFile file;
  Bm25CorpusStats::writeShard(file.path(), 2, 4, {{"alpha", 1}});
  std::filesystem::resize_file(
      file.path(), std::filesystem::file_size(file.path()) - 1);

  EXPECT_THROW(
      Bm25CorpusStats::load(file.path(), 1024 * 1024),
      std::runtime_error);
}

}  // namespace
}  // namespace pangrova::text::bm25::test
