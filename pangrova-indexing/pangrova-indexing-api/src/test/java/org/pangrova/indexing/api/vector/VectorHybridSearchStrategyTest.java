package org.pangrova.indexing.api.vector;

import org.pangrova.indexing.api.enums.HybridSearchMode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for {@link VectorHybridSearchStrategy} and {@link Mode}.
 */
class VectorHybridSearchStrategyTest {

    @Test
    void modeEnumHasThreeValues() {
        assertEquals(3, HybridSearchMode.values().length);
    }

    @Test
    void modeValueOf() {
        assertEquals(HybridSearchMode.ANN_THEN_FILTER, HybridSearchMode.valueOf("ANN_THEN_FILTER"));
        assertEquals(HybridSearchMode.FILTER_THEN_BRUTEFORCE, HybridSearchMode.valueOf("FILTER_THEN_BRUTEFORCE"));
        assertEquals(HybridSearchMode.FILTERED_ANN, HybridSearchMode.valueOf("FILTERED_ANN"));
    }

    @Test
    void annThenFilterStrategy() {
        var strategy = new VectorHybridSearchStrategy(HybridSearchMode.ANN_THEN_FILTER, 100);
        assertEquals(HybridSearchMode.ANN_THEN_FILTER, strategy.mode());
        assertEquals(100, strategy.effectiveK());
    }

    @Test
    void filterThenBruteforceStrategy() {
        var strategy = new VectorHybridSearchStrategy(HybridSearchMode.FILTER_THEN_BRUTEFORCE, 10);
        assertEquals(HybridSearchMode.FILTER_THEN_BRUTEFORCE, strategy.mode());
        assertEquals(10, strategy.effectiveK());
    }

    @Test
    void filteredAnnStrategy() {
        var strategy = new VectorHybridSearchStrategy(HybridSearchMode.FILTERED_ANN, 50);
        assertEquals(HybridSearchMode.FILTERED_ANN, strategy.mode());
        assertEquals(50, strategy.effectiveK());
    }

    @Test
    void modeOrdinalOrder() {
        assertEquals(0, HybridSearchMode.ANN_THEN_FILTER.ordinal());
        assertEquals(1, HybridSearchMode.FILTER_THEN_BRUTEFORCE.ordinal());
        assertEquals(2, HybridSearchMode.FILTERED_ANN.ordinal());
    }
}
