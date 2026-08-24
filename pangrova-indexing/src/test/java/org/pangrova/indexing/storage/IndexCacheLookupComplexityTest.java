package org.pangrova.indexing.storage;

import org.pangrova.spi.index.data.BloomFilterIndexData;
import org.pangrova.spi.index.data.IndexData;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests verifying O(1) lookup complexity on cache miss (F-INDEX-06 fix).
 *
 * <p>Previously, {@code invalidateOlderGenerations} scanned ALL entries for an indexId
 * on every cache miss (O(K) per miss, O(K²) for cold scan). After the fix, it uses
 * a nested map {@code indexId -> dataFilePath -> Set<CacheKey>} for direct O(1) lookup
 * to the exact (indexId, dataFilePath) bucket.</p>
 */
class IndexCacheLookupComplexityTest {

    private static IndexData testData(int marker) {
        return new BloomFilterIndexData(new byte[]{(byte) marker}, 3, 8);
    }

    /**
     * Verifies that a cache miss for a specific (indexId, dataFilePath) does NOT
     * affect or scan entries for other data files under the same index.
     * Populates many data files for one index, then triggers a miss on a new file.
     * Other entries must remain intact (proving no accidental removal from scanning).
     */
    @Test
    void cacheMissDoesNotScanOtherDataFilesForSameIndex() {
        IndexCache cache = new IndexCache(10 * 1024 * 1024);
        String indexId = "idx-shared";

        // Populate 1000 different data files for the same index
        int fileCount = 1000;
        for (int i = 0; i < fileCount; i++) {
            cache.put(indexId, "/data/file-" + i + ".parquet", 0, testData(i % 127));
        }
        assertThat(cache.size()).isEqualTo(fileCount);

        // Cache miss on a completely new file triggers invalidateOlderGenerations.
        // With O(1) lookup, this should only examine the bucket for the new file
        // (which is empty), NOT scan all 1000 existing entries.
        IndexData result = cache.get(indexId, "/data/new-file.parquet", 1);
        assertThat(result).isNull();

        // All original entries must still be present (no collateral damage)
        for (int i = 0; i < fileCount; i++) {
            assertThat(cache.get(indexId, "/data/file-" + i + ".parquet", 0))
                    .as("Entry for file-%d must still exist", i)
                    .isNotNull();
        }
    }

    /**
     * Verifies that older-generation invalidation on miss only removes entries
     * for the specific (indexId, dataFilePath) pair, not for other files.
     */
    @Test
    void olderGenerationInvalidationIsTargeted() {
        IndexCache cache = new IndexCache(10 * 1024 * 1024);
        String indexId = "idx-1";

        // Insert generation 0 for multiple files
        cache.put(indexId, "/data/file-A.parquet", 0, testData(1));
        cache.put(indexId, "/data/file-B.parquet", 0, testData(2));
        cache.put(indexId, "/data/file-C.parquet", 0, testData(3));

        // Miss on file-A at generation 2 should invalidate ONLY file-A gen 0
        IndexData result = cache.get(indexId, "/data/file-A.parquet", 2);
        assertThat(result).isNull();

        // file-A gen 0 should be invalidated
        assertThat(cache.get(indexId, "/data/file-A.parquet", 0)).isNull();

        // file-B and file-C must NOT be affected
        assertThat(cache.get(indexId, "/data/file-B.parquet", 0)).isNotNull();
        assertThat(cache.get(indexId, "/data/file-C.parquet", 0)).isNotNull();
    }

    /**
     * Performance sanity check: cold scan of K entries completes in bounded time.
     * With the old O(K²) implementation, 10000 misses on 10000 entries would be
     * prohibitively slow. With O(1) lookup per miss, this should be fast.
     */
    @Test
    void coldScanOfManyEntriesCompletesInBoundedTime() {
        IndexCache cache = new IndexCache(100 * 1024 * 1024);
        String indexId = "idx-perf";
        int entryCount = 10_000;

        // Populate cache with many entries (all gen 0)
        for (int i = 0; i < entryCount; i++) {
            cache.put(indexId, "/data/file-" + i + ".parquet", 0, testData(i % 127));
        }

        // Cold scan: miss every entry at generation 1 (triggers invalidateOlderGenerations)
        long startNanos = System.nanoTime();
        for (int i = 0; i < entryCount; i++) {
            cache.get(indexId, "/data/file-" + i + ".parquet", 1);
        }
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

        // With O(K²) this would take seconds; with O(1) per miss it should be < 2s
        // even on slow hardware. We use a generous bound to avoid flakiness.
        assertThat(elapsedMs)
                .as("Cold scan of %d entries must complete in bounded time (was %d ms)", entryCount, elapsedMs)
                .isLessThan(5000);
    }

    /**
     * Verifies that miss on an index with no entries is a no-op (null safety).
     */
    @Test
    void missOnEmptyCacheIsNoOp() {
        IndexCache cache = new IndexCache(1024 * 1024);
        IndexData result = cache.get("non-existent-idx", "/data/file.parquet", 0);
        assertThat(result).isNull();
        assertThat(cache.size()).isEqualTo(0);
    }

    /**
     * Verifies that miss on a known index but unknown dataFilePath is a no-op.
     */
    @Test
    void missOnKnownIndexButUnknownFileIsNoOp() {
        IndexCache cache = new IndexCache(1024 * 1024);
        cache.put("idx-1", "/data/file-A.parquet", 0, testData(1));

        IndexData result = cache.get("idx-1", "/data/unknown-file.parquet", 0);
        assertThat(result).isNull();

        // Original entry unaffected
        assertThat(cache.get("idx-1", "/data/file-A.parquet", 0)).isNotNull();
    }

    /**
     * Verifies correctness of invalidateIndex with the nested structure.
     */
    @Test
    void invalidateIndexWithNestedStructure() {
        IndexCache cache = new IndexCache(10 * 1024 * 1024);
        String targetIndex = "idx-target";
        String otherIndex = "idx-other";

        for (int i = 0; i < 100; i++) {
            cache.put(targetIndex, "/data/file-" + i + ".parquet", 0, testData(i % 127));
            cache.put(otherIndex, "/data/file-" + i + ".parquet", 0, testData(i % 127));
        }
        assertThat(cache.size()).isEqualTo(200);

        cache.invalidateIndex(targetIndex);

        // All target entries gone
        for (int i = 0; i < 100; i++) {
            assertThat(cache.get(targetIndex, "/data/file-" + i + ".parquet", 0)).isNull();
        }
        // Other index entries untouched
        for (int i = 0; i < 100; i++) {
            assertThat(cache.get(otherIndex, "/data/file-" + i + ".parquet", 0)).isNotNull();
        }
    }
}
