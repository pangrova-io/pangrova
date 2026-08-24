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
 * Verifies weighted linear combination reranker scoring and normalization.
 */
class WeightedRerankerTest {

    @Test
    void singleSourceWithEqualScoresNormalizesToOne() {
        Reranker reranker = new WeightedReranker(Map.of("vector", 1.0));
        List<ScoredCandidate> candidates = List.of(
                new ScoredCandidate(100L, 0.5, "vector"),
                new ScoredCandidate(200L, 0.5, "vector"));

        List<RankedResult> results = reranker.rerank("query", candidates, 2);

        // All same score → normalized to 1.0, weighted by 1.0
        assertThat(results).hasSize(2);
        assertThat(results.get(0).score()).isCloseTo(1.0, within(1e-10));
    }

    @Test
    void twoSourcesWithExplicitWeights() {
        Reranker reranker = new WeightedReranker(Map.of("vector", 0.7, "fts", 0.3));
        List<ScoredCandidate> candidates = List.of(
                new ScoredCandidate(100L, 1.0, "vector"),
                new ScoredCandidate(200L, 0.0, "vector"),
                new ScoredCandidate(100L, 0.0, "fts"),
                new ScoredCandidate(200L, 1.0, "fts"));

        List<RankedResult> results = reranker.rerank("query", candidates, 2);

        // Doc 100: vector normalized=1.0*0.7 + fts normalized=0.0*0.3 = 0.7
        // Doc 200: vector normalized=0.0*0.7 + fts normalized=1.0*0.3 = 0.3
        assertThat(results.get(0).rowOffset()).isEqualTo(100L);
        assertThat(results.get(0).score()).isCloseTo(0.7, within(1e-10));
        assertThat(results.get(1).rowOffset()).isEqualTo(200L);
        assertThat(results.get(1).score()).isCloseTo(0.3, within(1e-10));
    }

    @Test
    void emptyCandidatesReturnsEmpty() {
        Reranker reranker = new WeightedReranker(Map.of("vector", 0.5, "fts", 0.5));

        List<RankedResult> results = reranker.rerank("query", List.of(), 10);

        assertThat(results).isEmpty();
    }

    @Test
    void emptyWeightsThrows() {
        assertThatThrownBy(() -> new WeightedReranker(Map.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void negativeWeightThrows() {
        assertThatThrownBy(() -> new WeightedReranker(Map.of("vector", -0.5)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void factoryCreatesFromWeightProperties() {
        WeightedRerankerFactory factory = new WeightedRerankerFactory();
        Reranker reranker = factory.create(Map.of("weight.vector", "0.7", "weight.fts", "0.3"));

        assertThat(reranker.name()).isEqualTo("weighted");
    }

    @Test
    void factoryUsesDefaultWeightsWhenNoneProvided() {
        WeightedRerankerFactory factory = new WeightedRerankerFactory();
        Reranker reranker = factory.create(Map.of());

        assertThat(reranker.name()).isEqualTo("weighted");
    }
}
