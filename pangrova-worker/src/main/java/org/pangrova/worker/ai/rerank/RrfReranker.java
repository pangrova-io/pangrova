package org.pangrova.worker.ai.rerank;

import org.pangrova.spi.ai.rerank.RankedResult;
import org.pangrova.spi.ai.rerank.Reranker;
import org.pangrova.spi.ai.rerank.ScoredCandidate;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Reciprocal Rank Fusion reranker that fuses candidates from multiple retrieval sources.
 *
 * <p>For each document d, the fused score is computed as:
 * {@code score(d) = sum(1 / (k + rank_i(d)))} where rank_i(d) is the 1-based rank
 * of document d in retrieval source i. The parameter k (default 60) controls how much
 * influence lower-ranked documents have on the fused score.</p>
 */
final class RrfReranker implements Reranker {

    private final int k;

    RrfReranker(int k) {
        if (k <= 0) {
            throw new IllegalArgumentException("k must be positive, got: " + k);
        }
        this.k = k;
    }

    @Override
    public List<RankedResult> rerank(String query, List<ScoredCandidate> candidates, int topK) {
        Objects.requireNonNull(candidates, "candidates must not be null");
        if (topK <= 0) {
            throw new IllegalArgumentException("topK must be positive, got: " + topK);
        }

        // Group candidates by source, then rank within each source by descending score
        Map<String, List<ScoredCandidate>> bySource = candidates.stream()
                .collect(Collectors.groupingBy(ScoredCandidate::source));

        // Compute per-source ranks (1-based) and accumulate RRF scores per rowOffset
        Map<Long, Double> rrfScores = new HashMap<>();
        Map<Long, Map<String, String>> payloads = new HashMap<>();

        for (Map.Entry<String, List<ScoredCandidate>> entry : bySource.entrySet()) {
            String source = entry.getKey();
            List<ScoredCandidate> sorted = entry.getValue().stream()
                    .sorted(Comparator.comparingDouble(ScoredCandidate::score).reversed())
                    .toList();

            for (int rank = 0; rank < sorted.size(); rank++) {
                ScoredCandidate candidate = sorted.get(rank);
                long rowOffset = candidate.rowOffset();
                double rrfContribution = 1.0 / (k + rank + 1);

                rrfScores.merge(rowOffset, rrfContribution, Double::sum);
                payloads.computeIfAbsent(rowOffset, ignored -> new HashMap<>())
                        .put(source + "_rank", String.valueOf(rank + 1));
            }
        }

        return rrfScores.entrySet().stream()
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
        return "rrf";
    }

    int k() {
        return k;
    }
}
