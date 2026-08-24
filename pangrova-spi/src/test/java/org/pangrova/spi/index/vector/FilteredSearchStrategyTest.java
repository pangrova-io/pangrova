package org.pangrova.spi.index.vector;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Unit tests for {@link FilteredSearchStrategy}.
 */
class FilteredSearchStrategyTest {

    @Test
    void hasFourValues() {
        assertEquals(4, FilteredSearchStrategy.values().length);
    }

    @Test
    void valueOfRoundTrips() {
        for (FilteredSearchStrategy strategy : FilteredSearchStrategy.values()) {
            assertEquals(strategy, FilteredSearchStrategy.valueOf(strategy.name()));
        }
    }

    @Test
    void enumConstants() {
        assertNotNull(FilteredSearchStrategy.POST_FILTER);
        assertNotNull(FilteredSearchStrategy.PRE_FILTER);
        assertNotNull(FilteredSearchStrategy.PREDICATE_AWARE);
        assertNotNull(FilteredSearchStrategy.ITERATIVE_SCAN);
    }

    @Test
    void defaultStrategyIsPostFilter() {
        assertSame(FilteredSearchStrategy.POST_FILTER,
                FilteredSearchStrategy.defaultStrategy());
    }

    @Test
    void ordinalOrder() {
        assertEquals(0, FilteredSearchStrategy.POST_FILTER.ordinal());
        assertEquals(1, FilteredSearchStrategy.PRE_FILTER.ordinal());
        assertEquals(2, FilteredSearchStrategy.PREDICATE_AWARE.ordinal());
        assertEquals(3, FilteredSearchStrategy.ITERATIVE_SCAN.ordinal());
    }

    @Test
    void searchRequestWithNullStrategyDefaultsToPostFilter() {
        var request = new VectorSearchRequest(
                "col", new float[]{1.0f}, 10, DistanceMetric.L2);
        assertSame(FilteredSearchStrategy.POST_FILTER,
                request.effectiveFilteredSearchStrategy());
    }

    @Test
    void searchRequestWithExplicitStrategy() {
        var request = new VectorSearchRequest(
                "col", new float[]{1.0f}, 10, DistanceMetric.L2,
                FilteredSearchStrategy.PREDICATE_AWARE);
        assertSame(FilteredSearchStrategy.PREDICATE_AWARE,
                request.effectiveFilteredSearchStrategy());
        assertSame(FilteredSearchStrategy.PREDICATE_AWARE,
                request.filteredSearchStrategy());
    }

    @Test
    void searchRequestBackwardCompatibleConstructorSetsNullStrategy() {
        var request = new VectorSearchRequest(
                "col", new float[]{1.0f}, 10, DistanceMetric.COSINE);
        // The 4-arg constructor sets filteredSearchStrategy to null
        assertEquals(null, request.filteredSearchStrategy());
        // But effectiveFilteredSearchStrategy resolves to the default
        assertSame(FilteredSearchStrategy.POST_FILTER,
                request.effectiveFilteredSearchStrategy());
    }

    @Test
    void searchRequestWithIterativeScan() {
        var request = new VectorSearchRequest(
                "col", new float[]{1.0f, 2.0f}, 5, DistanceMetric.INNER_PRODUCT,
                FilteredSearchStrategy.ITERATIVE_SCAN);
        assertSame(FilteredSearchStrategy.ITERATIVE_SCAN,
                request.effectiveFilteredSearchStrategy());
    }
}
