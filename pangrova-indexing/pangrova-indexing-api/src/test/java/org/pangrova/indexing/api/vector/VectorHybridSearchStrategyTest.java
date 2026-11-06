package org.pangrova.indexing.api.vector;

import org.pangrova.indexing.api.vector.VectorHybridSearchStrategy.Mode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for {@link VectorHybridSearchStrategy} and {@link Mode}.
 */
class VectorHybridSearchStrategyTest {

    @Test
    void modeEnumHasThreeValues() {
        assertEquals(3, Mode.values().length);
    }

    @Test
    void modeValueOf() {
        assertEquals(Mode.ANN_THEN_FILTER, Mode.valueOf("ANN_THEN_FILTER"));
        assertEquals(Mode.FILTER_THEN_BRUTEFORCE, Mode.valueOf("FILTER_THEN_BRUTEFORCE"));
        assertEquals(Mode.FILTERED_ANN, Mode.valueOf("FILTERED_ANN"));
    }

    @Test
    void annThenFilterStrategy() {
        var strategy = new VectorHybridSearchStrategy(Mode.ANN_THEN_FILTER, 100);
        assertEquals(Mode.ANN_THEN_FILTER, strategy.mode());
        assertEquals(100, strategy.effectiveK());
    }

    @Test
    void filterThenBruteforceStrategy() {
        var strategy = new VectorHybridSearchStrategy(Mode.FILTER_THEN_BRUTEFORCE, 10);
        assertEquals(Mode.FILTER_THEN_BRUTEFORCE, strategy.mode());
        assertEquals(10, strategy.effectiveK());
    }

    @Test
    void filteredAnnStrategy() {
        var strategy = new VectorHybridSearchStrategy(Mode.FILTERED_ANN, 50);
        assertEquals(Mode.FILTERED_ANN, strategy.mode());
        assertEquals(50, strategy.effectiveK());
    }

    @Test
    void modeOrdinalOrder() {
        assertEquals(0, Mode.ANN_THEN_FILTER.ordinal());
        assertEquals(1, Mode.FILTER_THEN_BRUTEFORCE.ordinal());
        assertEquals(2, Mode.FILTERED_ANN.ordinal());
    }
}
