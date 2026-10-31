#include <gtest/gtest.h>

#include <boost/multiprecision/cpp_bin_float.hpp>

#include <algorithm>
#include <cmath>
#include <limits>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "indexing/retrieval/RetrievalFusion.h"

namespace pangrova::retrieval::test {
namespace {

constexpr size_t kMaxRows = 1'000;
constexpr size_t kMaxFanIn = 8;

std::vector<SourceConfig> twoSources() {
  return {
      {"vector", ScoreDirection::kLowerIsBetter, 0.5},
      {"text", ScoreDirection::kHigherIsBetter, 0.5}};
}

// Independent oracle: raw differences and the unscaled raw weight sum are
// evaluated with 100 decimal digits and a wider exponent range. This does not
// rely on long double, which has DOUBLE precision/range on Apple arm64.
using HighPrecision = boost::multiprecision::cpp_bin_float_100;

std::map<std::string, HighPrecision> linearOracle(
    const std::vector<Candidate>& candidates,
    const std::vector<SourceConfig>& sources) {
  HighPrecision total_weight = 0;
  for (const auto& source : sources) {
    total_weight += HighPrecision(source.weight);
  }
  std::map<std::string, HighPrecision> scores;
  for (const auto& source : sources) {
    const auto first = std::find_if(
        candidates.begin(), candidates.end(), [&](const Candidate& candidate) {
          return candidate.source_id == source.source_id;
        });
    if (first == candidates.end()) {
      continue;
    }
    HighPrecision low(first->score);
    HighPrecision high(first->score);
    for (const auto& candidate : candidates) {
      if (candidate.source_id == source.source_id) {
        low = std::min(low, HighPrecision(candidate.score));
        high = std::max(high, HighPrecision(candidate.score));
      }
    }
    for (const auto& candidate : candidates) {
      if (candidate.source_id != source.source_id) {
        continue;
      }
      HighPrecision normalized = 1;
      if (low != high) {
        if (source.score_direction == ScoreDirection::kHigherIsBetter) {
          normalized = (HighPrecision(candidate.score) - low) / (high - low);
        } else {
          normalized = (high - HighPrecision(candidate.score)) / (high - low);
        }
      }
      scores[candidate.stable_key] +=
          HighPrecision(source.weight) * normalized / total_weight;
    }
  }
  return scores;
}

void expectLinearOracle(
    const std::vector<Candidate>& candidates,
    const std::vector<SourceConfig>& sources,
    const std::vector<FusedCandidate>& result) {
  const auto expected = linearOracle(candidates, sources);
  ASSERT_EQ(result.size(), expected.size());
  for (const auto& candidate : result) {
    SCOPED_TRACE(candidate.stable_key);
    const auto score = expected.find(candidate.stable_key);
    ASSERT_NE(score, expected.end());
    const double rounded = score->second.convert_to<double>();
    const double tolerance = std::max(
        8.0 * std::numeric_limits<double>::epsilon() * std::abs(rounded),
        4.0 * std::numeric_limits<double>::denorm_min());
    EXPECT_TRUE(std::isfinite(candidate.fused_score));
    EXPECT_GE(candidate.fused_score, 0.0);
    EXPECT_LE(candidate.fused_score,
              1.0 + 8.0 * std::numeric_limits<double>::epsilon());
    EXPECT_NEAR(candidate.fused_score, rounded, tolerance);
  }
}

TEST(RetrievalFusionTest, RrfFusesAcrossSourcesDeterministically) {
  std::vector<Candidate> candidates = {
      {"a", "vector", 0.1, 0},
      {"b", "vector", 0.2, 1},
      {"b", "text", 12.0, 2},
      {"c", "text", 11.0, 3}};

  auto result = RetrievalFusion::rrf(
      candidates, twoSources(), 60, 2, kMaxRows, kMaxFanIn);

  ASSERT_EQ(result.size(), 2);
  EXPECT_EQ(result[0].stable_key, "b");
  EXPECT_EQ(result[0].representative_ordinal, 2);
  EXPECT_GT(result[0].fused_score, result[1].fused_score);
  EXPECT_EQ(result[1].stable_key, "a");
}

TEST(RetrievalFusionTest, RrfDerivesGlobalRanksFromRawScores) {
  std::vector<Candidate> candidates = {
      {"weak", "vector", 0.9, 0},
      {"shared", "text", 100.0, 1},
      {"shared", "vector", 0.1, 2},
      {"weak-text", "text", 1.0, 3}};

  auto result = RetrievalFusion::rrf(
      candidates, twoSources(), 60, 3, kMaxRows, kMaxFanIn);

  ASSERT_EQ(result.size(), 3);
  EXPECT_EQ(result[0].stable_key, "shared");
  EXPECT_EQ(result[0].representative_ordinal, 1);
  EXPECT_NEAR(result[0].fused_score, 2.0 / 61.0, 1e-12);
  EXPECT_EQ(result[1].stable_key, "weak");
  EXPECT_EQ(result[2].stable_key, "weak-text");
}

TEST(RetrievalFusionTest, LinearNormalizesOppositeScoreDirectionsGlobally) {
  std::vector<Candidate> candidates = {
      {"a", "vector", 0.1, 0},
      {"b", "vector", 0.9, 1},
      {"a", "text", 0.0, 2},
      {"b", "text", 100.0, 3}};

  auto result = RetrievalFusion::linear(
      candidates, twoSources(), 2, kMaxRows, kMaxFanIn);

  ASSERT_EQ(result.size(), 2);
  EXPECT_EQ(result[0].stable_key, "a");
  EXPECT_EQ(result[1].stable_key, "b");
  EXPECT_DOUBLE_EQ(result[0].fused_score, 0.5);
  EXPECT_DOUBLE_EQ(result[1].fused_score, 0.5);
}

TEST(RetrievalFusionTest, TieBreakUsesStableKeyAndTopKIsBounded) {
  std::vector<Candidate> candidates = {
      {"z", "vector", 1.0, 0},
      {"a", "vector", 1.0, 1},
      {"m", "vector", 1.0, 2}};
  std::vector<SourceConfig> source = {
      {"vector", ScoreDirection::kLowerIsBetter, 1.0}};

  auto result = RetrievalFusion::linear(
      candidates, source, 2, kMaxRows, kMaxFanIn);

  ASSERT_EQ(result.size(), 2);
  EXPECT_EQ(result[0].stable_key, "a");
  EXPECT_EQ(result[1].stable_key, "m");
}

TEST(RetrievalFusionTest, RejectsDuplicateKeyWithinSource) {
  std::vector<Candidate> candidates = {
      {"a", "vector", 0.1, 0},
      {"a", "vector", 0.2, 1}};

  EXPECT_THROW(
      RetrievalFusion::rrf(
          candidates, twoSources(), 60, 10, kMaxRows, kMaxFanIn),
      std::invalid_argument);
}

TEST(RetrievalFusionTest, RejectsUnknownSourceAndNonFiniteScore) {
  EXPECT_THROW(
      RetrievalFusion::rrf(
          {{"a", "unknown", 1.0, 0}}, twoSources(),
          60, 10, kMaxRows, kMaxFanIn),
      std::invalid_argument);
  EXPECT_THROW(
      RetrievalFusion::rrf(
          {{"a", "vector", std::numeric_limits<double>::quiet_NaN(), 0}},
          twoSources(), 60, 10, kMaxRows, kMaxFanIn),
      std::invalid_argument);
}

TEST(RetrievalFusionTest, EnforcesCandidateAndFanInLimits) {
  std::vector<Candidate> candidates = {{"a", "vector", 1.0, 0}};

  EXPECT_THROW(
      RetrievalFusion::rrf(candidates, twoSources(), 60, 1, 0, kMaxFanIn),
      std::invalid_argument);
  EXPECT_THROW(
      RetrievalFusion::rrf(candidates, twoSources(), 60, 1, kMaxRows, 1),
      std::invalid_argument);
}

TEST(RetrievalFusionTest, LinearFiniteExtremeRangesMatchHighPrecisionOracle) {
  const double max = std::numeric_limits<double>::max();
  for (const auto direction : {ScoreDirection::kHigherIsBetter,
                               ScoreDirection::kLowerIsBetter}) {
    for (const auto& bounds : std::vector<std::pair<double, double>>{
             {-1e308, 1e308}, {-max, max}, {-max / 4.0, max},
             {-max, max / 4.0}, {-1e307, 1e308}}) {
      SCOPED_TRACE(bounds.first);
      SCOPED_TRACE(bounds.second);
      std::vector<Candidate> candidates = {
          {"low", "score", bounds.first, 0},
          {"zero", "score", 0.0, 1},
          {"high", "score", bounds.second, 2}};
      std::vector<SourceConfig> sources = {{"score", direction, 1.0}};

      const auto result = RetrievalFusion::linear(
          candidates, sources, 3, kMaxRows, kMaxFanIn);

      expectLinearOracle(candidates, sources, result);
      ASSERT_EQ(result.size(), 3);
      EXPECT_EQ(result.front().stable_key,
                direction == ScoreDirection::kHigherIsBetter ? "high" : "low");
      EXPECT_DOUBLE_EQ(result.front().fused_score, 1.0);
      EXPECT_DOUBLE_EQ(result.back().fused_score, 0.0);
    }
  }
}

TEST(RetrievalFusionTest, LinearPreservesAdjacentHugeAndSubnormalRanges) {
  const double max = std::numeric_limits<double>::max();
  const double tiny = std::numeric_limits<double>::denorm_min();
  for (const auto& candidates : std::vector<std::vector<Candidate>>{
           {{"low", "score", std::nextafter(max, 0.0), 0},
            {"high", "score", max, 1}},
           {{"low", "score", -tiny, 0}, {"zero", "score", 0.0, 1},
            {"high", "score", tiny, 2}}}) {
    for (const auto direction : {ScoreDirection::kHigherIsBetter,
                                 ScoreDirection::kLowerIsBetter}) {
      std::vector<SourceConfig> sources = {{"score", direction, 1.0}};

      const auto result = RetrievalFusion::linear(
          candidates, sources, candidates.size(), kMaxRows, kMaxFanIn);

      expectLinearOracle(candidates, sources, result);
      ASSERT_EQ(result.size(), candidates.size());
      EXPECT_DOUBLE_EQ(result.front().fused_score, 1.0);
      EXPECT_DOUBLE_EQ(result.back().fused_score, 0.0);
    }
  }
}

TEST(RetrievalFusionTest, LinearEqualExtremeScoresRetainUnitNormalization) {
  const double max = std::numeric_limits<double>::max();
  for (const double score : {-max, 0.0, max}) {
    std::vector<Candidate> candidates = {
        {"a", "vector", score, 0}, {"b", "vector", score, 1},
        {"a", "text", score, 2}, {"b", "text", score, 3}};
    std::vector<SourceConfig> sources = {
        {"vector", ScoreDirection::kLowerIsBetter, 3.0},
        {"text", ScoreDirection::kHigherIsBetter, 7.0}};

    const auto result = RetrievalFusion::linear(
        candidates, sources, 2, kMaxRows, kMaxFanIn);

    expectLinearOracle(candidates, sources, result);
    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result[0].stable_key, "a");
    EXPECT_EQ(result[1].stable_key, "b");
    EXPECT_DOUBLE_EQ(result[0].fused_score, 1.0);
    EXPECT_DOUBLE_EQ(result[1].fused_score, 1.0);
  }
}

TEST(RetrievalFusionTest, LinearMissingSourceRetainsConfiguredWeightShare) {
  std::vector<Candidate> candidates = {
      {"best", "text", 9.0, 0}, {"worst", "text", -3.0, 1}};
  std::vector<SourceConfig> sources = {
      {"text", ScoreDirection::kHigherIsBetter, 1.0},
      {"absent", ScoreDirection::kLowerIsBetter, 3.0}};

  const auto result = RetrievalFusion::linear(
      candidates, sources, 2, kMaxRows, kMaxFanIn);

  expectLinearOracle(candidates, sources, result);
  ASSERT_EQ(result.size(), 2);
  EXPECT_EQ(result[0].stable_key, "best");
  EXPECT_DOUBLE_EQ(result[0].fused_score, 0.25);
  EXPECT_DOUBLE_EQ(result[1].fused_score, 0.0);
}

TEST(RetrievalFusionTest, LinearFiniteWeightsUseOverflowSafeRelativeShares) {
  const double max = std::numeric_limits<double>::max();
  const double tiny = std::numeric_limits<double>::denorm_min();
  std::vector<Candidate> candidates = {
      {"a", "vector", -1e308, 0}, {"b", "vector", 1e308, 1},
      {"a", "text", 2.0, 2}, {"b", "text", 8.0, 3}};
  for (const auto& weights : std::vector<std::pair<double, double>>{
           {max, max}, {max, max / 2.0}, {tiny, tiny}, {tiny, 2.0 * tiny},
           {std::numeric_limits<double>::min(), tiny}, {max, tiny}}) {
    SCOPED_TRACE(weights.first);
    SCOPED_TRACE(weights.second);
    std::vector<SourceConfig> sources = {
        {"vector", ScoreDirection::kLowerIsBetter, weights.first},
        {"text", ScoreDirection::kHigherIsBetter, weights.second}};

    const auto result = RetrievalFusion::linear(
        candidates, sources, 2, kMaxRows, kMaxFanIn);

    expectLinearOracle(candidates, sources, result);
  }
}

TEST(RetrievalFusionTest, LinearNormalScoresAcrossThreeSourcesMatchOracle) {
  std::vector<Candidate> candidates = {
      {"a", "distance", 2.0, 0}, {"b", "distance", 6.0, 1},
      {"c", "distance", 8.0, 2}, {"a", "text", -5.0, 3},
      {"b", "text", 2.0, 4}, {"d", "text", 9.0, 5},
      {"b", "other", 0.5, 6}, {"c", "other", 0.5, 7}};
  std::vector<SourceConfig> sources = {
      {"distance", ScoreDirection::kLowerIsBetter, 0.7},
      {"text", ScoreDirection::kHigherIsBetter, 1.3},
      {"other", ScoreDirection::kHigherIsBetter, 3.1},
      {"absent", ScoreDirection::kHigherIsBetter, 0.2}};

  const auto result = RetrievalFusion::linear(
      candidates, sources, 4, kMaxRows, kMaxFanIn);

  expectLinearOracle(candidates, sources, result);
  ASSERT_EQ(result.size(), 4);
  EXPECT_EQ(result.front().stable_key, "b");
}

TEST(RetrievalFusionTest, LinearEmptySourcesReturnEmptyWithExtremeWeights) {
  const double max = std::numeric_limits<double>::max();
  std::vector<SourceConfig> sources = {
      {"vector", ScoreDirection::kLowerIsBetter, max},
      {"text", ScoreDirection::kHigherIsBetter, max}};

  EXPECT_TRUE(RetrievalFusion::linear(
      {}, sources, 2, kMaxRows, kMaxFanIn).empty());
}

TEST(RetrievalFusionTest, LinearRejectsInvalidWeightsAndNonFiniteScores) {
  for (const double weight : {0.0, -1.0,
                              std::numeric_limits<double>::infinity(),
                              std::numeric_limits<double>::quiet_NaN()}) {
    std::vector<SourceConfig> sources = {
        {"score", ScoreDirection::kHigherIsBetter, weight}};
    EXPECT_THROW(
        RetrievalFusion::linear(
            {{"a", "score", 1.0, 0}}, sources, 1, kMaxRows, kMaxFanIn),
        std::invalid_argument);
  }
  for (const double score : {std::numeric_limits<double>::infinity(),
                             -std::numeric_limits<double>::infinity(),
                             std::numeric_limits<double>::quiet_NaN()}) {
    EXPECT_THROW(
        RetrievalFusion::linear(
            {{"a", "vector", score, 0}}, twoSources(), 1, kMaxRows, kMaxFanIn),
        std::invalid_argument);
  }
}

}  // namespace
}  // namespace pangrova::retrieval::test
