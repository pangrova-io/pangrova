// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include <unistd.h>
#include <stdexcept>

#include <boost/multiprecision/cpp_bin_float.hpp>

#include <chrono>
#include <cmath>
#include <filesystem>
#include <limits>
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
    // CTest executes one case per process concurrently. A clock value is not
    // a filesystem reservation; another owner could truncate/remove our file.
    auto pattern = (std::filesystem::temp_directory_path() / "pangrova-bm25-XXXXXX").string();
    std::vector<char> name(pattern.begin(), pattern.end());
    name.push_back('\0');
    const int descriptor = ::mkstemp(name.data());
    if (descriptor < 0) throw std::runtime_error("Cannot reserve BM25 test file");
    path_ = name.data();
    if (::close(descriptor) != 0) {
      std::error_code ignored;
      std::filesystem::remove(path_, ignored);
      throw std::runtime_error("Cannot close reserved BM25 test file");
    }
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

// Independent 100-decimal-digit arithmetic evaluates the unreduced BM25
// equation, including log, from exact counts and the original binary64 inputs.
// These fixture scores are numerical oracles, never performance measurements.
double bm25Oracle(uint64_t documents, uint64_t tokens, uint64_t document_frequency,
    uint64_t term_frequency, uint64_t document_length, double k1, double b) {
  using Precise = boost::multiprecision::cpp_bin_float_100;
  const Precise average = Precise(tokens) / Precise(documents);
  const Precise inverse_document_frequency = log(Precise(1) +
      (Precise(documents) - Precise(document_frequency) + Precise(0.5)) /
      (Precise(document_frequency) + Precise(0.5)));
  const Precise frequency(term_frequency);
  const Precise score = inverse_document_frequency * frequency * (Precise(k1) + 1) /
      (frequency + Precise(k1) * (Precise(1) - Precise(b) +
          Precise(b) * Precise(document_length) / average));
  return score.convert_to<double>();
}

TEST(Bm25SegmentTest, SparseGlobalAverageUsesActualMergedZeroLengthDocuments) {
  Bm25SegmentBuilder indexed;
  indexed.addDocument("alpha alpha beta beta");
  indexed.addDocument("alpha");
  Bm25SegmentBuilder empty_documents;
  for (int index = 0; index < 10; ++index) {
    empty_documents.addDocument("");
  }
  TestFile segment_file;
  TestFile indexed_statistics;
  TestFile empty_statistics;
  TestFile merged_statistics;
  indexed.write(segment_file.path());
  indexed.writeStatistics(indexed_statistics.path());
  empty_documents.writeStatistics(empty_statistics.path());
  Bm25CorpusStats::mergeSortedShards(
      {indexed_statistics.path(), empty_statistics.path()}, merged_statistics.path());
  auto segment = Bm25Segment::load(segment_file.path(),
      Bm25Segment::residentBytesFromDisk(segment_file.path()));
  auto corpus = Bm25CorpusStats::load(merged_statistics.path(),
      Bm25CorpusStats::residentBytesFromDisk(merged_statistics.path()));

  ASSERT_EQ(corpus.documentCount(), 12);
  ASSERT_EQ(corpus.totalDocumentLength(), 5);
  ASSERT_EQ(corpus.documentFrequency("alpha"), 2);
  ASSERT_EQ(corpus.documentFrequency("beta"), 1);
  EXPECT_DOUBLE_EQ(corpus.averageDocumentLength(), 5.0 / 12.0);
  const auto matches = segment.search({"alpha"}, 2, 1.2, 0.75, corpus);
  ASSERT_EQ(matches.size(), 2);
  EXPECT_EQ(matches[0].document, 1);
  EXPECT_EQ(matches[1].document, 0);
  for (const auto& match : matches) {
    const double expected = bm25Oracle(12, 5, 2,
        match.document == 0 ? 2 : 1, match.document == 0 ? 4 : 1, 1.2, 0.75);
    EXPECT_TRUE(std::isfinite(match.score));
    EXPECT_NEAR(match.score, expected, expected * 1e-13);
  }
}

TEST(Bm25SegmentTest, FiniteSaturationBoundariesPreserveTheSameHighPrecisionEquation) {
  Bm25SegmentBuilder builder;
  builder.addDocument("alpha alpha beta beta");
  builder.addDocument("alpha");
  TestFile segment_file;
  TestFile statistics_file;
  builder.write(segment_file.path());
  builder.writeStatistics(statistics_file.path());
  auto segment = Bm25Segment::load(segment_file.path(),
      Bm25Segment::residentBytesFromDisk(segment_file.path()));
  auto corpus = Bm25CorpusStats::load(statistics_file.path(),
      Bm25CorpusStats::residentBytesFromDisk(statistics_file.path()));
  ASSERT_EQ(corpus.documentCount(), 2);
  ASSERT_EQ(corpus.totalDocumentLength(), 5);
  EXPECT_DOUBLE_EQ(corpus.averageDocumentLength(), 2.5);
  for (double k1 : {std::numeric_limits<double>::denorm_min(),
      std::numeric_limits<double>::min(), 1.2,
      std::numeric_limits<double>::max()}) {
    for (double b : {0.0, 0.75, 1.0}) {
      const auto matches = segment.search({"alpha", "alpha"}, 2, k1, b, corpus);
      ASSERT_EQ(matches.size(), 2);
      EXPECT_GE(matches[0].score, matches[1].score);
      if (matches[0].score == matches[1].score) {
        EXPECT_LT(matches[0].document, matches[1].document);
      }
      for (const auto& match : matches) {
        const double expected = bm25Oracle(2, 5, 2,
            match.document == 0 ? 2 : 1, match.document == 0 ? 4 : 1, k1, b);
        EXPECT_TRUE(std::isfinite(match.score));
        EXPECT_NEAR(match.score, expected, expected * 1e-13);
      }
    }
  }
}

TEST(Bm25SegmentTest, UnitAverageRetainsTheExactCorpusDefinition) {
  Bm25SegmentBuilder builder;
  builder.addDocument("alpha alpha");
  builder.addDocument("");
  TestFile segment_file;
  TestFile statistics_file;
  builder.write(segment_file.path());
  builder.writeStatistics(statistics_file.path());
  auto segment = Bm25Segment::load(segment_file.path(),
      Bm25Segment::residentBytesFromDisk(segment_file.path()));
  auto corpus = Bm25CorpusStats::load(statistics_file.path(),
      Bm25CorpusStats::residentBytesFromDisk(statistics_file.path()));
  EXPECT_DOUBLE_EQ(corpus.averageDocumentLength(), 1.0);
  const auto matches = segment.search({"alpha"}, 2, 1.2, 0.75, corpus);
  ASSERT_EQ(matches.size(), 1);
  EXPECT_EQ(matches[0].document, 0);
  const double expected = bm25Oracle(2, 2, 1, 2, 2, 1.2, 0.75);
  EXPECT_NEAR(matches[0].score, expected, expected * 1e-13);
}

TEST(Bm25SegmentTest, AllZeroLengthDocumentsRemainLegalEmptyResults) {
  Bm25SegmentBuilder builder;
  for (int index = 0; index < 4; ++index) {
    builder.addDocument("");
  }
  TestFile segment_file;
  TestFile statistics_file;
  builder.write(segment_file.path());
  builder.writeStatistics(statistics_file.path());
  auto segment = Bm25Segment::load(segment_file.path(),
      Bm25Segment::residentBytesFromDisk(segment_file.path()));
  auto corpus = Bm25CorpusStats::load(statistics_file.path(),
      Bm25CorpusStats::residentBytesFromDisk(statistics_file.path()));
  EXPECT_EQ(corpus.documentCount(), 4);
  EXPECT_EQ(corpus.totalDocumentLength(), 0);
  EXPECT_DOUBLE_EQ(corpus.averageDocumentLength(), 0);
  EXPECT_TRUE(segment.search({"alpha"}, 2, 1.2, 0.75, corpus).empty());
  EXPECT_TRUE(segment.search({}, 2, 1.2, 0.75, corpus).empty());
}

TEST(Bm25SegmentTest, ContradictoryGlobalTokenCountsFailWithoutFabricatingScores) {
  Bm25SegmentBuilder builder;
  builder.addDocument("alpha alpha beta beta");
  builder.addDocument("alpha");
  TestFile segment_file;
  builder.write(segment_file.path());
  auto segment = Bm25Segment::load(segment_file.path(),
      Bm25Segment::residentBytesFromDisk(segment_file.path()));
  for (uint64_t tokens : {uint64_t{0}, uint64_t{4}}) {
    TestFile statistics_file;
    Bm25CorpusStats::writeShard(statistics_file.path(), 2, tokens,
        {{"alpha", 2}, {"beta", 1}});
    auto corpus = Bm25CorpusStats::load(statistics_file.path(),
        Bm25CorpusStats::residentBytesFromDisk(statistics_file.path()));
    EXPECT_THROW(segment.search({"alpha"}, 2, 1.2, 0.75, corpus), std::invalid_argument);
    EXPECT_THROW(segment.search({"absent"}, 2, 1.2, 0.75, corpus), std::invalid_argument);
  }
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
