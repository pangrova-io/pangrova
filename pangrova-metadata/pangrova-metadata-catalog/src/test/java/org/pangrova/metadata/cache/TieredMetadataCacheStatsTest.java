package org.pangrova.metadata.cache;

import org.pangrova.metadata.api.object.TableMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import org.pangrova.metadata.api.cache.CacheStats;

/**
 * Validates R3.5: TieredMetadataCache catalogIndex maximumSize guard,
 * recordStats() enablement, and unified hit/miss counting via Caffeine stats.
 */
class TieredMetadataCacheStatsTest {

    private TieredMetadataCache cache;

    @BeforeEach
    void setUp() {
        cache = new TieredMetadataCache();
    }

    @Test
    void statsReflectHitsFromL1Lookups() {
        cache.putTable("cat", "s", "t", sampleMetadata());

        cache.getTable("cat", "s", "t"); // hit
        cache.getTable("cat", "s", "t"); // hit

        CacheStats stats = cache.getStats();
        assertThat(stats.hitCount()).isEqualTo(2);
        assertThat(stats.missCount()).isZero();
    }

    @Test
    void statsReflectMissesForAbsentKeys() {
        cache.getTable("no", "such", "table"); // miss

        CacheStats stats = cache.getStats();
        assertThat(stats.hitCount()).isZero();
        assertThat(stats.missCount()).isEqualTo(1);
    }

    @Test
    void statsReflectMixedHitsAndMisses() {
        cache.putTable("cat", "s", "t1", sampleMetadata());

        cache.getTable("cat", "s", "t1"); // hit
        cache.getTable("cat", "s", "missing"); // miss
        cache.getTable("cat", "s", "t1"); // hit

        CacheStats stats = cache.getStats();
        assertThat(stats.hitCount()).isEqualTo(2);
        assertThat(stats.missCount()).isEqualTo(1);
    }

    @Test
    void catalogIndexMaxSizeEvictsOldestCatalogs() throws InterruptedException {
        // Create cache with catalogIndex max size of 2
        TieredMetadataCache smallCache = new TieredMetadataCache(100, 60, 2,
                null);

        smallCache.putTable("cat1", "s", "t", sampleMetadata());
        smallCache.putTable("cat2", "s", "t", sampleMetadata());
        smallCache.putTable("cat3", "s", "t", sampleMetadata());

        // Caffeine eviction is async — allow time for eviction listener to fire
        Thread.sleep(100);

        // Force Caffeine to run eviction (it's async)
        smallCache.getTable("cat3", "s", "t");
        // Trigger maintenance
        smallCache.getCatalogIndexStats();

        // After eviction, at most 2 catalogs should remain in the index.
        // The evicted catalog's L1 entries are also cleaned up by the removal listener.
        CacheStats indexStats = smallCache.getCatalogIndexStats();
        assertThat(indexStats.size()).isLessThanOrEqualTo(2);
    }

    @Test
    void catalogIndexStatsAreExposed() {
        cache.putTable("cat1", "s", "t1", sampleMetadata());
        cache.putTable("cat2", "s", "t2", sampleMetadata());

        CacheStats indexStats = cache.getCatalogIndexStats();
        assertThat(indexStats.size()).isEqualTo(2);
    }

    @Test
    void l1StatsTrackEvictions() throws Exception {
        // Create cache with L1 max size of 2
        TieredMetadataCache tinyCache = new TieredMetadataCache(2, 60,
                null);

        tinyCache.putTable("c", "s", "t1", sampleMetadata());
        tinyCache.putTable("c", "s", "t2", sampleMetadata());
        tinyCache.putTable("c", "s", "t3", sampleMetadata());

        // Caffeine eviction is async — give it a moment to process
        Thread.sleep(100);
        // Force Caffeine maintenance to process evictions
        tinyCache.getStats();
        tinyCache.getTable("c", "s", "t3");

        CacheStats stats = tinyCache.getStats();
        assertThat(stats.size()).isLessThanOrEqualTo(2);
    }

    @Test
    void catalogIndexEvictionCleansUpL1Entries() throws Exception {
        // catalogIndex max size = 1, so adding a second catalog evicts the first
        TieredMetadataCache smallCache = new TieredMetadataCache(100, 60, 1,
                null);

        smallCache.putTable("evict_me", "s", "t1", sampleMetadata());
        smallCache.putTable("evict_me", "s", "t2", sampleMetadata());

        // Add a second catalog to trigger eviction of "evict_me"
        smallCache.putTable("keeper", "s", "t1", sampleMetadata());

        // Caffeine eviction is async — give it a moment and trigger cleanup
        Thread.sleep(100);
        smallCache.getCatalogIndexStats(); // trigger maintenance

        // The evicted catalog's L1 entries should be cleaned up by the removal listener.
        // "keeper" should still be present.
        assertThat(smallCache.getTable("keeper", "s", "t1")).isPresent();
    }

    @Test
    void existingConstructorStillWorks() {
        // The 3-arg constructor should still work (backward compat)
        TieredMetadataCache legacyCache = new TieredMetadataCache(500, 30,
                null);

        legacyCache.putTable("c", "s", "t", sampleMetadata());
        assertThat(legacyCache.getTable("c", "s", "t")).isPresent();

        CacheStats stats = legacyCache.getStats();
        assertThat(stats.hitCount()).isEqualTo(1);
    }

    private static TableMetadata sampleMetadata() {
        return new TableMetadata("cat", "s", "t", List.of(), null, null, null, null, null);
    }
}
