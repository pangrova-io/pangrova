#include <gtest/gtest.h>

#include <limits>
#include <vector>

#include "retrieval/RetrievalFusion.h"

namespace pangrova::retrieval::test {
namespace {

constexpr size_t kMaxRows = 1'000;
constexpr size_t kMaxFanIn = 8;

std::vector<SourceConfig> twoSources() {
  return {
      {"vector", ScoreDirection::kLowerIsBetter, 0.5},
      {"text", ScoreDirection::kHigherIsBetter, 0.5}};
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

}  // namespace
}  // namespace pangrova::retrieval::test
