package org.pangrova.spi.cache;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link CacheStoreStats} record.
 * G-CACHE-08: distributed/detached cache layer SPI.
 */
class CacheStoreStatsTest {

    @Test
    void unknownSentinelHasNegativeFields() {
        CacheStoreStats stats = CacheStoreStats.UNKNOWN;
        assertThat(stats.hitCount()).isEqualTo(-1);
        assertThat(stats.missCount()).isEqualTo(-1);
        assertThat(stats.putCount()).isEqualTo(-1);
        assertThat(stats.evictionCount()).isEqualTo(-1);
        assertThat(stats.estimatedSizeBytes()).isEqualTo(-1);
        assertThat(stats.estimatedEntryCount()).isEqualTo(-1);
    }

    @Test
    void hitRateUnknownWhenCountsNegative() {
        assertThat(CacheStoreStats.UNKNOWN.hitRate()).isEqualTo(-1.0);
    }

    @Test
    void hitRateZeroWhenNoCalls() {
        CacheStoreStats stats = new CacheStoreStats(0, 0, 0, 0, 0, 0);
        assertThat(stats.hitRate()).isEqualTo(0.0);
    }

    @Test
    void hitRateComputedCorrectly() {
        CacheStoreStats stats = new CacheStoreStats(75, 25, 100, 10, 1024, 50);
        assertThat(stats.hitRate()).isEqualTo(0.75);
    }

    @Test
    void hitRateOneWhenAllHits() {
        CacheStoreStats stats = new CacheStoreStats(100, 0, 100, 0, 1024, 100);
        assertThat(stats.hitRate()).isEqualTo(1.0);
    }

    @Test
    void hitRateZeroWhenAllMisses() {
        CacheStoreStats stats = new CacheStoreStats(0, 100, 100, 0, 0, 0);
        assertThat(stats.hitRate()).isEqualTo(0.0);
    }

    @Test
    void hitRateUnknownWhenOnlyHitCountNegative() {
        CacheStoreStats stats = new CacheStoreStats(-1, 100, 50, 0, 0, 0);
        assertThat(stats.hitRate()).isEqualTo(-1.0);
    }

    @Test
    void hitRateUnknownWhenOnlyMissCountNegative() {
        CacheStoreStats stats = new CacheStoreStats(100, -1, 50, 0, 0, 0);
        assertThat(stats.hitRate()).isEqualTo(-1.0);
    }
}
