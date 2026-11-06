package org.pangrova.planner.core.statistics;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link HyperLogLog} NDV estimator.
 */
class HyperLogLogTest {

    @Test
    void emptyEstimateIsZero() {
        HyperLogLog hll = new HyperLogLog();
        assertThat(hll.estimate()).isZero();
    }

    @Test
    void singleValueEstimatesOne() {
        HyperLogLog hll = new HyperLogLog();
        hll.add("hello");
        assertThat(hll.estimate()).isBetween(1L, 2L);
    }

    @Test
    void duplicatesDoNotIncrease() {
        HyperLogLog hll = new HyperLogLog();
        for (int i = 0; i < 1000; i++) {
            hll.add("same");
        }
        assertThat(hll.estimate()).isBetween(1L, 2L);
    }

    @Test
    void estimatesDistinctWithReasonableError() {
        HyperLogLog hll = new HyperLogLog();
        int expected = 10_000;
        for (int i = 0; i < expected; i++) {
            hll.add(i);
        }
        long estimate = hll.estimate();
        // Allow 10% error for HLL with precision 14
        assertThat(estimate).isBetween((long) (expected * 0.9), (long) (expected * 1.1));
    }

    @Test
    void mergeUnionsDistinctValues() {
        HyperLogLog hll1 = new HyperLogLog();
        HyperLogLog hll2 = new HyperLogLog();
        for (int i = 0; i < 5000; i++) {
            hll1.add(i);
        }
        for (int i = 3000; i < 8000; i++) {
            hll2.add(i);
        }
        hll1.merge(hll2);
        long estimate = hll1.estimate();
        // Union should be ~8000
        assertThat(estimate).isBetween(7000L, 9000L);
    }

    @Test
    void mergeRejectsDifferentPrecision() {
        HyperLogLog hll1 = new HyperLogLog(10);
        HyperLogLog hll2 = new HyperLogLog(12);
        assertThatThrownBy(() -> hll1.merge(hll2))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void resetClearsState() {
        HyperLogLog hll = new HyperLogLog();
        for (int i = 0; i < 100; i++) {
            hll.add(i);
        }
        hll.reset();
        assertThat(hll.estimate()).isZero();
    }

    @Test
    void invalidPrecisionThrows() {
        assertThatThrownBy(() -> new HyperLogLog(3))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new HyperLogLog(19))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void nullValuesIgnored() {
        HyperLogLog hll = new HyperLogLog();
        hll.add(null);
        hll.add(null);
        assertThat(hll.estimate()).isZero();
    }

    @Test
    void registerCountMatchesPrecision() {
        HyperLogLog hll = new HyperLogLog(10);
        assertThat(hll.registerCount()).isEqualTo(1024);
    }
}
