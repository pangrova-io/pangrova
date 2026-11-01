package org.pangrova.spi.indexing.vector;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Unit tests for {@link DistanceMetric}.
 */
class DistanceMetricTest {

    @Test
    void enumContainsExactlyThreeValues() {
        assertEquals(3, DistanceMetric.values().length);
    }

    @Test
    void l2ValueExists() {
        assertNotNull(DistanceMetric.valueOf("L2"));
    }

    @Test
    void cosineValueExists() {
        assertNotNull(DistanceMetric.valueOf("COSINE"));
    }

    @Test
    void innerProductValueExists() {
        assertNotNull(DistanceMetric.valueOf("INNER_PRODUCT"));
    }

    @Test
    void ordinalOrder() {
        assertEquals(0, DistanceMetric.L2.ordinal());
        assertEquals(1, DistanceMetric.COSINE.ordinal());
        assertEquals(2, DistanceMetric.INNER_PRODUCT.ordinal());
    }

    @Test
    void valueOfRoundTripsForAllValues() {
        for (DistanceMetric metric : DistanceMetric.values()) {
            assertEquals(metric, DistanceMetric.valueOf(metric.name()));
        }
    }
}
