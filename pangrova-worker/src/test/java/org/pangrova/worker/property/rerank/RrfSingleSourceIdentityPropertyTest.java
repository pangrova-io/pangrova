package org.pangrova.worker.property.rerank;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;

import org.pangrova.spi.ai.rerank.RankedResult;
import org.pangrova.spi.ai.rerank.ScoredCandidate;
import org.pangrova.worker.ai.rerank.RrfRerankerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for RRF single-source identity (P9).
 *
 * <p>Validates that when RRF receives candidates from a single retrieval source,
 * the output ordering preserves the original ranking order. With one source,
 * RRF score is {@code 1/(k + rank)} which is monotonically decreasing with rank,
 * so the original order must be preserved.</p>
 *
 * <p><b>Validates: Requirements P9</b></p>
 */
class RrfSingleSourceIdentityPropertyTest {

    private static final String SOURCE = "vector";
    private static final String QUERY = "test query";

    @Provide
    Arbitrary<Integer> kValues() {
        return Arbitraries.of(1, 10, 30, 60, 100, 200);
    }

    @Provide
    Arbitrary<List<ScoredCandidate>> singleSourceCandidates() {
        Arbitrary<Long> offsets = Arbitraries.longs().between(0, 100_000);
        Arbitrary<Double> scores = Arbitraries.doubles().between(0.01, 1.0);

        return Arbitraries.lazy(() ->
                offsets.flatMap(offset ->
                        scores.map(score -> new ScoredCandidate(offset, score, SOURCE))
                ).list().ofMinSize(2).ofMaxSize(50)
                        .filter(list -> list.stream()
                                .map(ScoredCandidate::rowOffset)
                                .distinct().count() == list.size())
        );
    }

    /**
     * P9: RRF with a single source preserves the original ranking order.
     * The candidate with the highest score in the source should remain first
     * in the reranked output.
     */
    @Property(tries = 100)
    void singleSourcePreservesOriginalOrder(
            @ForAll("kValues") int k,
            @ForAll("singleSourceCandidates") List<ScoredCandidate> candidates) {

        var factory = new RrfRerankerFactory();
        var reranker = factory.create(Map.of("k", String.valueOf(k)));

        int topK = candidates.size();
        List<RankedResult> results = reranker.rerank(QUERY, candidates, topK);

        // Expected order: candidates sorted by descending score (original ranking)
        List<Long> expectedOrder = candidates.stream()
                .sorted(Comparator.comparingDouble(ScoredCandidate::score).reversed())
                .map(ScoredCandidate::rowOffset)
                .toList();

        List<Long> actualOrder = results.stream()
                .map(RankedResult::rowOffset)
                .toList();

        assertThat(actualOrder).isEqualTo(expectedOrder);
    }

    /**
     * P9: RRF scores with a single source are strictly monotonically decreasing.
     * Each successive result has a lower RRF score than the previous one.
     */
    @Property(tries = 100)
    void singleSourceScoresAreStrictlyDecreasing(
            @ForAll("kValues") int k,
            @ForAll("singleSourceCandidates") List<ScoredCandidate> candidates) {

        var factory = new RrfRerankerFactory();
        var reranker = factory.create(Map.of("k", String.valueOf(k)));

        int topK = candidates.size();
        List<RankedResult> results = reranker.rerank(QUERY, candidates, topK);

        for (int i = 1; i < results.size(); i++) {
            assertThat(results.get(i - 1).score())
                    .as("Score at position %d should be > score at position %d", i - 1, i)
                    .isGreaterThan(results.get(i).score());
        }
    }

    /**
     * P9: With a single source, the RRF score for rank r (1-based) equals 1/(k+r).
     * Verifies the exact formula for each result position.
     */
    @Property(tries = 50)
    void singleSourceScoresMatchFormula(
            @ForAll("kValues") int k,
            @ForAll("singleSourceCandidates") List<ScoredCandidate> candidates) {

        var factory = new RrfRerankerFactory();
        var reranker = factory.create(Map.of("k", String.valueOf(k)));

        int topK = candidates.size();
        List<RankedResult> results = reranker.rerank(QUERY, candidates, topK);

        for (int i = 0; i < results.size(); i++) {
            double expectedScore = 1.0 / (k + i + 1);
            assertThat(results.get(i).score())
                    .as("RRF score at rank %d should be 1/(%d+%d)", i + 1, k, i + 1)
                    .isCloseTo(expectedScore, org.assertj.core.data.Offset.offset(1e-10));
        }
    }

    /**
     * P9: topK truncation with single source still preserves order of the top elements.
     */
    @Property(tries = 50)
    void topKTruncationPreservesOrderPrefix(
            @ForAll("kValues") int k,
            @ForAll("singleSourceCandidates") List<ScoredCandidate> candidates,
            @ForAll @IntRange(min = 1, max = 10) int topK) {

        var factory = new RrfRerankerFactory();
        var reranker = factory.create(Map.of("k", String.valueOf(k)));

        int effectiveTopK = Math.min(topK, candidates.size());
        List<RankedResult> results = reranker.rerank(QUERY, candidates, effectiveTopK);

        // Full ranking for comparison
        List<Long> fullOrder = candidates.stream()
                .sorted(Comparator.comparingDouble(ScoredCandidate::score).reversed())
                .map(ScoredCandidate::rowOffset)
                .toList();

        List<Long> actualOrder = results.stream()
                .map(RankedResult::rowOffset)
                .toList();

        // Truncated result should be a prefix of the full ordering
        assertThat(actualOrder).isEqualTo(fullOrder.subList(0, effectiveTopK));
    }
}
