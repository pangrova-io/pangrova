#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace pangrova::retrieval {

enum class ScoreDirection : uint8_t {
  kHigherIsBetter = 1,
  kLowerIsBetter = 2,
};

struct Candidate {
  std::string stable_key;
  std::string source_id;
  double score;
  size_t input_ordinal;
};

struct SourceConfig {
  std::string source_id;
  ScoreDirection score_direction;
  double weight{1.0};
};

struct FusedCandidate {
  std::string stable_key;
  double fused_score;
  size_t representative_ordinal;
};

/** Bounded, deterministic Native implementations of cross-source fusion. */
class RetrievalFusion final {
 public:
  static std::vector<FusedCandidate> rrf(
      const std::vector<Candidate>& candidates,
      const std::vector<SourceConfig>& sources,
      uint64_t rrf_k,
      size_t output_limit,
      size_t max_candidate_rows,
      size_t max_fan_in);

  static std::vector<FusedCandidate> linear(
      const std::vector<Candidate>& candidates,
      const std::vector<SourceConfig>& sources,
      size_t output_limit,
      size_t max_candidate_rows,
      size_t max_fan_in);
};

}  // namespace pangrova::retrieval
