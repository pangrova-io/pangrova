package org.pangrova.worker.ai.rerank;

import org.pangrova.spi.ai.rerank.RankedResult;
import org.pangrova.spi.ai.rerank.Reranker;
import org.pangrova.spi.ai.rerank.ScoredCandidate;

import java.util.List;
import java.util.Objects;

/**
 * Cascade reranker that chains multiple rerankers in sequence.
 *
 * <p>Each stage receives the output of the previous stage as its input candidates.
 * The first stage receives the original candidates; subsequent stages receive
 * {@link RankedResult} entries converted back to {@link ScoredCandidate} for re-scoring.
 * The final stage's output is returned as the cascade result.</p>
 *
 * <p>This enables patterns like "fast RRF first, then precise cross-encoder on top-N"
 * where each stage progressively refines the candidate set.</p>
 */
final class CascadeReranker implements Reranker {

    private final List<Reranker> stages;

    CascadeReranker(List<Reranker> stages) {
        Objects.requireNonNull(stages, "stages must not be null");
        if (stages.isEmpty()) {
            throw new IllegalArgumentException("cascade requires at least one reranker stage");
        }
        this.stages = List.copyOf(stages);
    }

    @Override
    public List<RankedResult> rerank(String query, List<ScoredCandidate> candidates, int topK) {
        Objects.requireNonNull(candidates, "candidates must not be null");
        if (topK <= 0) {
            throw new IllegalArgumentException("topK must be positive, got: " + topK);
        }

        List<ScoredCandidate> currentCandidates = candidates;
        List<RankedResult> results = List.of();

        for (Reranker stage : stages) {
            results = stage.rerank(query, currentCandidates, topK);
            currentCandidates = toScoredCandidates(results, stage.name());
        }

        return results;
    }

    @Override
    public String name() {
        return "cascade";
    }

    private List<ScoredCandidate> toScoredCandidates(List<RankedResult> results, String source) {
        return results.stream()
                .map(r -> new ScoredCandidate(r.rowOffset(), r.score(), source))
                .toList();
    }
}
