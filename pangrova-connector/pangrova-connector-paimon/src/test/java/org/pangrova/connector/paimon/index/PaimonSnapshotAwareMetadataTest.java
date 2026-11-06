package org.pangrova.connector.paimon.index;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.junit.jupiter.api.Test;
import org.pangrova.connector.api.index.SnapshotDataFile;
import org.pangrova.connector.api.index.SnapshotFileDelta;

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
        Cache<String, SnapshotFileDelta> cache =
                Caffeine.newBuilder().maximumSize(100).build();
        cache.put("1:2", delta("added.parquet", "removed.parquet"));
        cache.put("3:4", delta("a.parquet", null));

        var metadata = new PaimonSnapshotAwareMetadata(null, cache);
        metadata.clearDiffCache();

        assertThat(cache.estimatedSize()).isZero();
    }

    @Test
    void cacheEvictsWhenMaxSizeExceeded() {
        Cache<String, SnapshotFileDelta> tinyCache =
                Caffeine.newBuilder().maximumSize(2).build();

        tinyCache.put("1:2", delta("a.parquet", null));
        tinyCache.put("3:4", delta("b.parquet", null));
        tinyCache.put("5:6", delta("c.parquet", null));

        tinyCache.cleanUp();

        assertThat(tinyCache.estimatedSize()).isLessThanOrEqualTo(2);
    }

    @Test
    void cacheEvictsAfterAccessExpiry() {
        Cache<String, SnapshotFileDelta> expiringCache =
                Caffeine.newBuilder()
                        .maximumSize(10_000)
                        .expireAfterAccess(1, TimeUnit.NANOSECONDS)
                        .build();

        expiringCache.put("1:2", delta("added.parquet", null));

        expiringCache.cleanUp();

        assertThat(expiringCache.getIfPresent("1:2")).isNull();
    }

    @Test
    void snapshotDiffRecordHoldsAddedAndRemoved() {
        var diff = new SnapshotFileDelta(
                List.of(file("new1.parquet"), file("new2.parquet")),
                List.of("old1.parquet"));

        assertThat(diff.added()).extracting(SnapshotDataFile::path)
                .containsExactly("new1.parquet", "new2.parquet");
        assertThat(diff.removed()).containsExactly("old1.parquet");
    }

    private static SnapshotFileDelta delta(String added, String removed) {
        return new SnapshotFileDelta(
                List.of(file(added)), removed == null ? List.of() : List.of(removed));
    }

    private static SnapshotDataFile file(String path) {
        return new SnapshotDataFile(path, 1, 1, "parquet");
    }
}
