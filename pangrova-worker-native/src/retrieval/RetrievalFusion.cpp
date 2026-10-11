#include "retrieval/RetrievalFusion.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <stdexcept>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

namespace pangrova::retrieval {
namespace {

struct Aggregate {
  double score{0.0};
  size_t representative_ordinal{0};
  uint64_t representative_rank{std::numeric_limits<uint64_t>::max()};
  bool initialized{false};
};

struct SourceStats {
  ScoreDirection direction;
  double weight;
  double min_score{std::numeric_limits<double>::infinity()};
  double max_score{-std::numeric_limits<double>::infinity()};
};

using SourceMap = std::unordered_map<std::string, SourceStats>;

SourceMap validateInputs(
    const std::vector<Candidate>& candidates,
    const std::vector<SourceConfig>& sources,
    size_t output_limit,
    size_t max_candidate_rows,
    size_t max_fan_in) {
  if (output_limit == 0) {
    throw std::invalid_argument("retrieval fusion output_limit must be positive");
  }
  if (max_candidate_rows == 0 || candidates.size() > max_candidate_rows) {
    throw std::invalid_argument("retrieval fusion candidate row limit exceeded");
  }
  if (sources.empty() || sources.size() > max_fan_in) {
    throw std::invalid_argument("retrieval fusion source fan-in is invalid");
  }

  SourceMap source_map;
  source_map.reserve(sources.size());
  for (const auto& source : sources) {
    if (source.source_id.empty() || !std::isfinite(source.weight) ||
        source.weight <= 0.0) {
      throw std::invalid_argument("retrieval fusion source configuration is invalid");
    }
    if (!source_map.emplace(
            source.source_id,
            SourceStats{source.score_direction, source.weight})
             .second) {
      throw std::invalid_argument(
          "duplicate retrieval source configuration: " + source.source_id);
    }
  }

  std::unordered_set<std::string> source_key_pairs;
  source_key_pairs.reserve(candidates.size());
  for (const auto& candidate : candidates) {
    if (candidate.stable_key.empty() || candidate.source_id.empty() ||
        !std::isfinite(candidate.score)) {
      throw std::invalid_argument("retrieval candidate contract violation");
    }
    auto source = source_map.find(candidate.source_id);
    if (source == source_map.end()) {
      throw std::invalid_argument(
          "candidate references undeclared source: " + candidate.source_id);
    }
    std::string pair_key;
    pair_key.reserve(
        candidate.source_id.size() + candidate.stable_key.size() + 1);
    pair_key.append(candidate.source_id);
    pair_key.push_back('\0');
    pair_key.append(candidate.stable_key);
    if (!source_key_pairs.emplace(std::move(pair_key)).second) {
      throw std::invalid_argument(
          "duplicate candidate stable key within retrieval source");
    }
    source->second.min_score =
        std::min(source->second.min_score, candidate.score);
    source->second.max_score =
        std::max(source->second.max_score, candidate.score);
  }
  return source_map;
}

std::vector<uint64_t> globalSourceRanks(
    const std::vector<Candidate>& candidates,
    const SourceMap& sources) {
  std::vector<size_t> order(candidates.size());
  for (size_t index = 0; index < order.size(); ++index) {
    order[index] = index;
  }
  std::sort(order.begin(), order.end(), [&](size_t left, size_t right) {
    const auto& lhs = candidates[left];
    const auto& rhs = candidates[right];
    if (lhs.source_id != rhs.source_id) {
      return lhs.source_id < rhs.source_id;
    }
    if (lhs.score != rhs.score) {
      return sources.at(lhs.source_id).direction ==
              ScoreDirection::kHigherIsBetter
          ? lhs.score > rhs.score
          : lhs.score < rhs.score;
    }
    if (lhs.stable_key != rhs.stable_key) {
      return lhs.stable_key < rhs.stable_key;
    }
    return lhs.input_ordinal < rhs.input_ordinal;
  });

  std::vector<uint64_t> ranks(candidates.size());
  std::string current_source;
  uint64_t rank = 0;
  for (size_t index : order) {
    const auto& source = candidates[index].source_id;
    if (source != current_source) {
      current_source = source;
      rank = 1;
    } else {
      ++rank;
    }
    ranks[index] = rank;
  }
  return ranks;
}

void considerRepresentative(
    Aggregate& aggregate,
    const Candidate& candidate,
    uint64_t source_rank) {
  if (!aggregate.initialized ||
      source_rank < aggregate.representative_rank ||
      (source_rank == aggregate.representative_rank &&
       candidate.input_ordinal < aggregate.representative_ordinal)) {
    aggregate.representative_rank = source_rank;
    aggregate.representative_ordinal = candidate.input_ordinal;
    aggregate.initialized = true;
  }
}

std::vector<FusedCandidate> finish(
    std::unordered_map<std::string, Aggregate> aggregates,
    size_t output_limit) {
  std::vector<FusedCandidate> result;
  result.reserve(aggregates.size());
  for (auto& [key, aggregate] : aggregates) {
    if (!std::isfinite(aggregate.score)) {
      throw std::invalid_argument("retrieval fusion produced a non-finite score");
    }
    result.push_back(FusedCandidate{
        std::move(key), aggregate.score, aggregate.representative_ordinal});
  }
  const auto better = [](const FusedCandidate& left,
                         const FusedCandidate& right) {
    if (left.fused_score != right.fused_score) {
      return left.fused_score > right.fused_score;
    }
    return left.stable_key < right.stable_key;
  };
  if (result.size() > output_limit) {
    auto boundary = result.begin() + static_cast<ptrdiff_t>(output_limit);
    std::nth_element(result.begin(), boundary, result.end(), better);
    result.resize(output_limit);
  }
  std::sort(result.begin(), result.end(), better);
  return result;
}

double normalizedScore(double score, const SourceStats& source) {
  if (source.max_score == source.min_score) {
    return 1.0;
  }
  double low = source.min_score;
  double high = source.max_score;
  const double half_max = std::numeric_limits<double>::max() / 2.0;
  if (low < 0.0 && high > 0.0 && (low < -half_max || high > half_max)) {
    // A common power-of-two scale preserves min/max normalization while
    // bounding every difference by DOUBLE_MAX, even for [-DOUBLE_MAX, +DOUBLE_MAX].
    // Apply it only to huge opposite-sign endpoints: tiny ranges must retain
    // their subnormal differences. No wider long double is required.
    low *= 0.5;
    high *= 0.5;
    score *= 0.5;
  }
  const double range = high - low;
  const double normalized =
      source.direction == ScoreDirection::kHigherIsBetter
          ? (score - low) / range
          : (high - score) / range;
  if (!std::isfinite(normalized) || normalized < 0.0 || normalized > 1.0) {
    throw std::invalid_argument("retrieval fusion normalized score is invalid");
  }
  return normalized;
}

}  // namespace

std::vector<FusedCandidate> RetrievalFusion::rrf(
    const std::vector<Candidate>& candidates,
    const std::vector<SourceConfig>& sources,
    uint64_t rrf_k,
    size_t output_limit,
    size_t max_candidate_rows,
    size_t max_fan_in) {
  SourceMap source_map = validateInputs(
      candidates, sources, output_limit, max_candidate_rows, max_fan_in);
  if (rrf_k == 0) {
    throw std::invalid_argument("RRF k must be positive");
  }

  auto ranks = globalSourceRanks(candidates, source_map);
  std::unordered_map<std::string, Aggregate> aggregates;
  aggregates.reserve(candidates.size());
  for (size_t index = 0; index < candidates.size(); ++index) {
    const auto& candidate = candidates[index];
    auto& aggregate = aggregates[candidate.stable_key];
    long double denominator = static_cast<long double>(rrf_k) +
        static_cast<long double>(ranks[index]);
    aggregate.score += static_cast<double>(1.0L / denominator);
    considerRepresentative(aggregate, candidate, ranks[index]);
  }
  return finish(std::move(aggregates), output_limit);
}

std::vector<FusedCandidate> RetrievalFusion::linear(
    const std::vector<Candidate>& candidates,
    const std::vector<SourceConfig>& sources,
    size_t output_limit,
    size_t max_candidate_rows,
    size_t max_fan_in) {
  SourceMap source_map = validateInputs(
      candidates, sources, output_limit, max_candidate_rows, max_fan_in);
  auto ranks = globalSourceRanks(candidates, source_map);
  double max_weight = 0.0;
  for (const auto& [source_id, source] : source_map) {
    (void)source_id;
    max_weight = std::max(max_weight, source.weight);
  }
  // Each configured weight is positive and finite. Scaling by the maximum
  // preserves their relative shares without overflowing or losing an entire
  // subnormal total. Absent sources still participate in the denominator.
  double total_scaled_weight = 0.0;
  for (const auto& [source_id, source] : source_map) {
    (void)source_id;
    total_scaled_weight += source.weight / max_weight;
  }
  if (!std::isfinite(total_scaled_weight) || total_scaled_weight <= 0.0) {
    throw std::invalid_argument("retrieval fusion weights are invalid");
  }

  std::unordered_map<std::string, Aggregate> aggregates;
  aggregates.reserve(candidates.size());
  for (size_t index = 0; index < candidates.size(); ++index) {
    const auto& candidate = candidates[index];
    const auto& source = source_map.at(candidate.source_id);
    auto& aggregate = aggregates[candidate.stable_key];
    aggregate.score +=
        ((source.weight / max_weight) / total_scaled_weight) *
        normalizedScore(candidate.score, source);
    considerRepresentative(aggregate, candidate, ranks[index]);
  }
  return finish(std::move(aggregates), output_limit);
}

}  // namespace pangrova::retrieval
