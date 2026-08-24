package org.pangrova.worker.ai.rerank;

import org.pangrova.spi.ai.rerank.RankedResult;
import org.pangrova.spi.ai.rerank.Reranker;
import org.pangrova.spi.ai.rerank.ScoredCandidate;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Verifies RRF reranker scoring formula and edge cases.
 */
class RrfRerankerTest {

    @Test
    void singleSourceRanksCorrectly() {
        Reranker reranker = new RrfReranker(60);
        List<ScoredCandidate> candidates = List.of(
                new ScoredCandidate(100L, 0.9, "vector"),
                new ScoredCandidate(200L, 0.8, "vector"),
                new ScoredCandidate(300L, 0.7, "vector"));

        List<RankedResult> results = reranker.rerank("query", candidates, 3);

        assertThat(results).hasSize(3);
        // rank 1: 1/(60+1) = 0.01639
        assertThat(results.get(0).rowOffset()).isEqualTo(100L);
        assertThat(results.get(0).score()).isCloseTo(1.0 / 61, within(1e-10));
        // rank 2: 1/(60+2) = 0.01613
        assertThat(results.get(1).rowOffset()).isEqualTo(200L);
        assertThat(results.get(1).score()).isCloseTo(1.0 / 62, within(1e-10));
    }

    @Test
    void multiSourceFusesScores() {
        Reranker reranker = new RrfReranker(60);
        // Doc 100 appears in both sources at rank 1
        List<ScoredCandidate> candidates = List.of(
                new ScoredCandidate(100L, 0.9, "vector"),
                new ScoredCandidate(200L, 0.8, "vector"),
                new ScoredCandidate(100L, 0.95, "fts"),
                new ScoredCandidate(300L, 0.85, "fts"));

        List<RankedResult> results = reranker.rerank("query", candidates, 3);

        assertThat(results).hasSize(3);
        // Doc 100: rank 1 in vector + rank 1 in fts = 1/61 + 1/61
        assertThat(results.get(0).rowOffset()).isEqualTo(100L);
        assertThat(results.get(0).score()).isCloseTo(2.0 / 61, within(1e-10));
    }

    @Test
    void topKLimitsResults() {
        Reranker reranker = new RrfReranker(60);
        List<ScoredCandidate> candidates = List.of(
                new ScoredCandidate(100L, 0.9, "vector"),
                new ScoredCandidate(200L, 0.8, "vector"),
                new ScoredCandidate(300L, 0.7, "vector"));

        List<RankedResult> results = reranker.rerank("query", candidates, 1);

        assertThat(results).hasSize(1);
        assertThat(results.get(0).rowOffset()).isEqualTo(100L);
    }

    @Test
    void emptyCandidatesReturnsEmpty() {
        Reranker reranker = new RrfReranker(60);

        List<RankedResult> results = reranker.rerank("query", List.of(), 10);

        assertThat(results).isEmpty();
    }

    @Test
    void invalidKThrows() {
        assertThatThrownBy(() -> new RrfReranker(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RrfReranker(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void invalidTopKThrows() {
        Reranker reranker = new RrfReranker(60);
        assertThatThrownBy(() -> reranker.rerank("q", List.of(), 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void nullCandidatesThrows() {
        Reranker reranker = new RrfReranker(60);
        assertThatThrownBy(() -> reranker.rerank("q", null, 5))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void nameReturnsRrf() {
        assertThat(new RrfReranker(60).name()).isEqualTo("rrf");
    }

    @Test
    void factoryCreatesWithDefaultK() {
        RrfRerankerFactory factory = new RrfRerankerFactory();
        Reranker reranker = factory.create(Map.of());

        assertThat(reranker.name()).isEqualTo("rrf");
        assertThat(((RrfReranker) reranker).k()).isEqualTo(60);
    }

    @Test
    void factoryCreatesWithCustomK() {
        RrfRerankerFactory factory = new RrfRerankerFactory();
        Reranker reranker = factory.create(Map.of("k", "30"));

        assertThat(((RrfReranker) reranker).k()).isEqualTo(30);
    }
}
