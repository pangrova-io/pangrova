package org.pangrova.connector.paimon.index;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link PaimonSnapshotAwareMetadata} verifying Caffeine cache
 * migration: bounded size ({@code maximumSize(10_000)}), expireAfterAccess (5 min),
 * and cache invalidation behavior.
 *
 * <p>Core diff computation logic depends on Paimon's {@code FileStoreTable} and
 * cannot be unit-tested without a real Paimon catalog. These tests focus on the
 * cache layer by injecting pre-populated Caffeine caches.</p>
 */
class PaimonSnapshotAwareMetadataTest {

    @Test
    void cacheConfigConstants() {
        assertThat(PaimonSnapshotAwareMetadata.MAXIMUM_SIZE).isEqualTo(10_000);
        assertThat(PaimonSnapshotAwareMetadata.EXPIRE_AFTER_ACCESS_MINUTES).isEqualTo(5);
    }

    @Test
    void clearDiffCacheInvalidatesAllEntries() {
        Cache<String, PaimonSnapshotAwareMetadata.SnapshotDiff> cache =
                Caffeine.newBuilder().maximumSize(100).build();
        cache.put("1:2", new PaimonSnapshotAwareMetadata.SnapshotDiff(
                List.of("added.parquet"), List.of("removed.parquet")));
        cache.put("3:4", new PaimonSnapshotAwareMetadata.SnapshotDiff(
                List.of("a.parquet"), List.of()));

        var metadata = new PaimonSnapshotAwareMetadata(null, cache);
        metadata.clearDiffCache();

        assertThat(cache.estimatedSize()).isZero();
    }

    @Test
    void cacheEvictsWhenMaxSizeExceeded() {
        Cache<String, PaimonSnapshotAwareMetadata.SnapshotDiff> tinyCache =
                Caffeine.newBuilder().maximumSize(2).build();

        tinyCache.put("1:2", new PaimonSnapshotAwareMetadata.SnapshotDiff(
                List.of("a.parquet"), List.of()));
        tinyCache.put("3:4", new PaimonSnapshotAwareMetadata.SnapshotDiff(
                List.of("b.parquet"), List.of()));
        tinyCache.put("5:6", new PaimonSnapshotAwareMetadata.SnapshotDiff(
                List.of("c.parquet"), List.of()));

        tinyCache.cleanUp();

        assertThat(tinyCache.estimatedSize()).isLessThanOrEqualTo(2);
    }

    @Test
    void cacheEvictsAfterAccessExpiry() {
        Cache<String, PaimonSnapshotAwareMetadata.SnapshotDiff> expiringCache =
                Caffeine.newBuilder()
                        .maximumSize(10_000)
                        .expireAfterAccess(1, TimeUnit.NANOSECONDS)
                        .build();

        expiringCache.put("1:2", new PaimonSnapshotAwareMetadata.SnapshotDiff(
                List.of("added.parquet"), List.of()));

        expiringCache.cleanUp();

        assertThat(expiringCache.getIfPresent("1:2")).isNull();
    }

    @Test
    void snapshotDiffRecordHoldsAddedAndRemoved() {
        var diff = new PaimonSnapshotAwareMetadata.SnapshotDiff(
                List.of("new1.parquet", "new2.parquet"),
                List.of("old1.parquet"));

        assertThat(diff.added()).containsExactly("new1.parquet", "new2.parquet");
        assertThat(diff.removed()).containsExactly("old1.parquet");
    }
}
