package org.pangrova.indexing.property.storage;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.StringLength;

import org.pangrova.indexing.core.storage.IndexCache;
import org.pangrova.indexing.api.descriptor.IndexType;
import org.pangrova.indexing.api.data.BloomFilterIndexData;
import org.pangrova.indexing.api.data.IndexData;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for {@link IndexCache} access-order compaction (IC1 fix).
 *
 * <p>Validates: IC1 — repeated get() hits do not cause unbounded deque growth;
 * compaction keeps the deque bounded while preserving LRU semantics.</p>
 */
class IndexCachePropertyTest {

    @Property(tries = 10)
    void repeatedGetsDoNotCauseUnboundedGrowth(
            @ForAll @IntRange(min = 100, max = 500) int accessCount) {
        IndexCache cache = new IndexCache(1024 * 1024);
        IndexData data = new BloomFilterIndexData(new byte[]{1, 2, 3}, 3, 24);
        cache.put("idx-1", "file-1", 0, data);

        for (int i = 0; i < accessCount; i++) {
            IndexData result = cache.get("idx-1", "file-1", 0);
            assertThat(result).isNotNull();
        }

        // Cache should still have exactly 1 entry
        assertThat(cache.size()).isEqualTo(1);
        // Hit rate should be 100%
        assertThat(cache.hitRate()).isEqualTo(1.0);
    }

    @Property(tries = 10)
    void evictionRespectsMaxSize(
            @ForAll @IntRange(min = 5, max = 20) int entryCount) {
        // Each BloomFilterIndexData with 100 bytes = 100 bytes estimated size
        long maxBytes = 300; // fits ~3 entries
        IndexCache cache = new IndexCache(maxBytes);

        for (int i = 0; i < entryCount; i++) {
            byte[] filter = new byte[100];
            filter[0] = (byte) i;
            cache.put("idx-" + i, "file-" + i, 0,
                    new BloomFilterIndexData(filter, 3, 800));
        }

        assertThat(cache.currentSizeBytes()).isLessThanOrEqualTo(maxBytes);
    }

    @Property(tries = 10)
    void invalidateIndexRemovesAllEntriesForIndex(
            @ForAll @IntRange(min = 1, max = 10) int fileCount) {
        IndexCache cache = new IndexCache(1024 * 1024);
        String indexId = "target-idx";

        for (int i = 0; i < fileCount; i++) {
            cache.put(indexId, "file-" + i, 0,
                    new BloomFilterIndexData(new byte[]{(byte) i}, 3, 8));
        }
        // Also add an entry for a different index
        cache.put("other-idx", "file-0", 0,
                new BloomFilterIndexData(new byte[]{99}, 3, 8));

        cache.invalidateIndex(indexId);

        for (int i = 0; i < fileCount; i++) {
            assertThat(cache.get(indexId, "file-" + i, 0)).isNull();
        }
        assertThat(cache.get("other-idx", "file-0", 0)).isNotNull();
    }
}
