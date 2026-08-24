package org.pangrova.worker.ai.rerank;

import org.pangrova.spi.ai.rerank.RankedResult;
import org.pangrova.spi.ai.rerank.Reranker;
import org.pangrova.spi.ai.rerank.ScoredCandidate;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies cascade reranker chains stages correctly.
 */
class CascadeRerankerTest {

    @Test
    void singleStagePassesThrough() {
        Reranker rrf = new RrfReranker(60);
        Reranker cascade = new CascadeReranker(List.of(rrf));

        List<ScoredCandidate> candidates = List.of(
                new ScoredCandidate(100L, 0.9, "vector"),
                new ScoredCandidate(200L, 0.8, "vector"));

        List<RankedResult> results = cascade.rerank("query", candidates, 2);

        assertThat(results).hasSize(2);
        assertThat(results.get(0).rowOffset()).isEqualTo(100L);
    }

    @Test
    void twoStagesChainCorrectly() {
        // First stage: RRF with k=60
        // Second stage: another RRF with k=1 (amplifies rank differences)
        Reranker stage1 = new RrfReranker(60);
        Reranker stage2 = new RrfReranker(1);
        Reranker cascade = new CascadeReranker(List.of(stage1, stage2));

        List<ScoredCandidate> candidates = List.of(
                new ScoredCandidate(100L, 0.9, "vector"),
                new ScoredCandidate(200L, 0.8, "vector"),
                new ScoredCandidate(300L, 0.7, "vector"));

        List<RankedResult> results = cascade.rerank("query", candidates, 3);

        // After stage1, order is 100, 200, 300
        // Stage2 re-ranks them (same order since input is already sorted by score)
        assertThat(results).hasSize(3);
        assertThat(results.get(0).rowOffset()).isEqualTo(100L);
        assertThat(results.get(1).rowOffset()).isEqualTo(200L);
        assertThat(results.get(2).rowOffset()).isEqualTo(300L);
    }

    @Test
    void topKAppliedAtEachStage() {
        Reranker stage1 = new RrfReranker(60);
        Reranker stage2 = new RrfReranker(60);
        Reranker cascade = new CascadeReranker(List.of(stage1, stage2));

        List<ScoredCandidate> candidates = List.of(
                new ScoredCandidate(100L, 0.9, "vector"),
                new ScoredCandidate(200L, 0.8, "vector"),
                new ScoredCandidate(300L, 0.7, "vector"));

        List<RankedResult> results = cascade.rerank("query", candidates, 2);

        assertThat(results).hasSize(2);
    }

    @Test
    void emptyCandidatesReturnsEmpty() {
        Reranker cascade = new CascadeReranker(List.of(new RrfReranker(60)));

        List<RankedResult> results = cascade.rerank("query", List.of(), 10);

        assertThat(results).isEmpty();
    }

    @Test
    void emptyStagesThrows() {
        assertThatThrownBy(() -> new CascadeReranker(List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void nameReturnsCascade() {
        assertThat(new CascadeReranker(List.of(new RrfReranker(60))).name()).isEqualTo("cascade");
    }
}
