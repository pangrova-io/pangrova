package org.pangrova.worker.ai.rerank;

import org.pangrova.spi.ai.rerank.RankedResult;
import org.pangrova.spi.ai.rerank.Reranker;
import org.pangrova.spi.ai.rerank.ScoredCandidate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Weighted linear combination reranker that fuses candidates using normalized score blending.
 *
 * <p>Each retrieval source is assigned a weight. Scores within each source are min-max normalized
 * to [0, 1], then the fused score for document d is:
 * {@code score(d) = sum(weight_i * normalized_score_i(d))}.</p>
 *
 * <p>Sources not present in the weight map receive equal share of the remaining weight.
 * If all sources are specified, weights must sum to 1.0 (within floating-point tolerance).</p>
 */
final class WeightedReranker implements Reranker {

    private final Map<String, Double> weights;

    WeightedReranker(Map<String, Double> weights) {
        Objects.requireNonNull(weights, "weights must not be null");
        if (weights.isEmpty()) {
            throw new IllegalArgumentException("weights must not be empty");
        }
        for (Map.Entry<String, Double> entry : weights.entrySet()) {
            if (entry.getValue() <= 0.0) {
                throw new IllegalArgumentException(
                        "weight for source '" + entry.getKey() + "' must be positive, got: " + entry.getValue());
            }
        }
        this.weights = Map.copyOf(weights);
    }

    @Override
    public List<RankedResult> rerank(String query, List<ScoredCandidate> candidates, int topK) {
        Objects.requireNonNull(candidates, "candidates must not be null");
        if (topK <= 0) {
            throw new IllegalArgumentException("topK must be positive, got: " + topK);
        }

        Map<String, List<ScoredCandidate>> bySource = candidates.stream()
                .collect(Collectors.groupingBy(ScoredCandidate::source));

        // Resolve effective weights: use configured weights, distribute remainder equally
        Map<String, Double> effectiveWeights = resolveWeights(bySource.keySet());

        // Normalize scores per source and compute weighted sum
        Map<Long, Double> fusedScores = new HashMap<>();
        Map<Long, Map<String, String>> payloads = new HashMap<>();

        for (Map.Entry<String, List<ScoredCandidate>> entry : bySource.entrySet()) {
            String source = entry.getKey();
            List<ScoredCandidate> sourceCandidates = entry.getValue();
            double weight = effectiveWeights.getOrDefault(source, 0.0);

            double minScore = sourceCandidates.stream()
                    .mapToDouble(ScoredCandidate::score).min().orElse(0.0);
            double maxScore = sourceCandidates.stream()
                    .mapToDouble(ScoredCandidate::score).max().orElse(0.0);
            double range = maxScore - minScore;

            for (ScoredCandidate candidate : sourceCandidates) {
                long rowOffset = candidate.rowOffset();
                double normalized = range > 0.0
                        ? (candidate.score() - minScore) / range
                        : 1.0;
                double weightedScore = weight * normalized;

                fusedScores.merge(rowOffset, weightedScore, Double::sum);
                payloads.computeIfAbsent(rowOffset, ignored -> new HashMap<>())
                        .put(source + "_normalized", String.valueOf(normalized));
            }
        }

        return fusedScores.entrySet().stream()
                .sorted(Map.Entry.<Long, Double>comparingByValue().reversed())
                .limit(topK)
                .map(e -> new RankedResult(
                        e.getKey(),
                        e.getValue(),
                        payloads.getOrDefault(e.getKey(), Map.of())))
                .toList();
    }

    @Override
    public String name() {
        return "weighted";
    }

    private Map<String, Double> resolveWeights(java.util.Set<String> sources) {
        Map<String, Double> resolved = new HashMap<>();
        double totalConfigured = 0.0;
        int unconfiguredCount = 0;

        for (String source : sources) {
            if (weights.containsKey(source)) {
                resolved.put(source, weights.get(source));
                totalConfigured += weights.get(source);
            } else {
                unconfiguredCount++;
            }
        }

        if (unconfiguredCount > 0) {
            double remaining = Math.max(0.0, 1.0 - totalConfigured);
            double equalShare = remaining / unconfiguredCount;
            for (String source : sources) {
                if (!weights.containsKey(source)) {
                    resolved.put(source, equalShare);
                }
            }
        }

        return resolved;
    }
}
