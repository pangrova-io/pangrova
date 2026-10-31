package org.pangrova.indexing.property.storage;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

import org.pangrova.indexing.core.storage.IndexCache;
import org.pangrova.indexing.api.data.BloomFilterIndexData;
import org.pangrova.indexing.api.data.IndexData;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for {@link IndexCache} access-order compaction (IC1 fix).
 *
 * <p><b>Validates: Requirements 3.1, 3.2, 3.3, 3.4</b></p>
 *
 * <p>Property: after N cache hits (get calls) on K distinct keys,
 * the access order deque size should be &le; K after compaction runs.
 * This verifies that compaction prevents unbounded deque growth.</p>
 */
class IndexCacheCompactionPropertyTest {

    private static final int COMPACTION_THRESHOLD = 64;

    /**
     * Core property: after N random get() calls on K distinct keys,
     * accessOrderSize() never grows unboundedly. Between compaction
     * cycles up to COMPACTION_THRESHOLD new entries accumulate, so
     * the invariant is: accessOrderSize &le; K + COMPACTION_THRESHOLD.
     */
    @Property(tries = 10)
    void dequeStaysBoundedDuringRandomAccess(
            @ForAll @IntRange(min = 1, max = 50) int distinctKeys,
            @ForAll @IntRange(min = 64, max = 500) int totalAccesses) {

        IndexCache cache = new IndexCache(1024L * 1024 * 100);
        List<String[]> keys = populateCache(cache, distinctKeys);

        ThreadLocalRandom rng = ThreadLocalRandom.current();
        for (int i = 0; i < totalAccesses; i++) {
            String[] key = keys.get(rng.nextInt(distinctKeys));
            cache.get(key[0], key[1], 0);
        }

        assertThat(cache.accessOrderSize())
                .as("accessOrderSize after %d accesses on %d keys",
                        totalAccesses, distinctKeys)
                .isLessThanOrEqualTo(distinctKeys + COMPACTION_THRESHOLD);
    }

    /**
     * Stronger property: immediately after compaction completes,
     * accessOrderSize() &le; K. We achieve this by starting from a
     * clean counter (0) and performing exactly COMPACTION_THRESHOLD
     * get() calls so the last call triggers compaction as its final
     * deque mutation.
     */
    @Property(tries = 10)
    void dequeSizeAtMostKRightAfterCompaction(
            @ForAll @IntRange(min = 1, max = 50) int distinctKeys) {

        IndexCache cache = new IndexCache(1024L * 1024 * 100);
        List<String[]> keys = populateCache(cache, distinctKeys);

        // Phase 1: create duplicates. Use exact multiple of threshold
        // so counter resets to 0 after the last compaction.
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        for (int i = 0; i < COMPACTION_THRESHOLD * 3; i++) {
            String[] key = keys.get(rng.nextInt(distinctKeys));
            cache.get(key[0], key[1], 0);
        }

        // Phase 2: exactly COMPACTION_THRESHOLD gets. The last one
        // triggers compaction -- no entries added after it.
        for (int i = 0; i < COMPACTION_THRESHOLD; i++) {
            String[] key = keys.get(rng.nextInt(distinctKeys));
            cache.get(key[0], key[1], 0);
        }

        assertThat(cache.accessOrderSize())
                .as("accessOrderSize right after compaction with %d keys",
                        distinctKeys)
                .isLessThanOrEqualTo(distinctKeys);
    }

    /**
     * Without compaction the deque would grow proportionally to total
     * accesses. With compaction it stays bounded — even after many
     * accesses the size is dramatically smaller than total access count.
     */
    @Property(tries = 10)
    void compactionPreventsUnboundedGrowth(
            @ForAll @IntRange(min = 1, max = 10) int distinctKeys) {

        IndexCache cache = new IndexCache(1024L * 1024 * 100);
        List<String[]> keys = populateCache(cache, distinctKeys);

        int totalAccesses = COMPACTION_THRESHOLD * 10;
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        for (int i = 0; i < totalAccesses; i++) {
            String[] key = keys.get(rng.nextInt(distinctKeys));
            cache.get(key[0], key[1], 0);
        }

        // Without compaction: deque would have totalAccesses + K entries.
        // With compaction: bounded by K + COMPACTION_THRESHOLD.
        assertThat(cache.accessOrderSize())
                .as("deque size vs %d total accesses", totalAccesses)
                .isLessThanOrEqualTo(distinctKeys + COMPACTION_THRESHOLD);
    }

    private List<String[]> populateCache(IndexCache cache, int count) {
        List<String[]> keys = new ArrayList<>();
        for (int k = 0; k < count; k++) {
            String indexId = "idx-" + k;
            String filePath = "file-" + k;
            IndexData data = new BloomFilterIndexData(new byte[]{1, 2, 3}, 3, 24L);
            cache.put(indexId, filePath, 0, data);
            keys.add(new String[]{indexId, filePath});
        }
        return keys;
    }
}
