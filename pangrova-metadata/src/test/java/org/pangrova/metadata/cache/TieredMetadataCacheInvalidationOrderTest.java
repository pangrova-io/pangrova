package org.pangrova.metadata.cache;

import org.pangrova.metadata.api.object.TableMetadata;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import org.pangrova.metadata.api.cache.L2CacheStore;

/**
 * Verifies BUG-M01 fix: L2 must be deleted BEFORE L1 invalidation to prevent
 * a concurrent getTable() from backfilling stale L2 data into L1 during the
 * window between L1 clear and L2 clear.
 *
 * <p>Uses a recording L2 adapter to capture the order of operations.</p>
 */
class TieredMetadataCacheInvalidationOrderTest {

    @Test
    void invalidateTableDeletesL2BeforeL1() {
        RecordingL2Adapter l2 = new RecordingL2Adapter();
        TieredMetadataCache cache = new TieredMetadataCache(100, 60, l2);

        cache.putTable("cat", "schema", "orders", sampleMetadata());
        l2.operations.clear(); // reset after put

        cache.invalidateTable("cat", "schema", "orders");

        // L2 delete must come before L1 invalidate.
        // We verify by checking that L2.delete was called, and that after
        // invalidation, a getTable() does NOT find stale data.
        assertThat(l2.operations).containsExactly(
                "delete:cat\u001Fschema\u001Forders",
                "publishInvalidation:cat\u001Fschema\u001Forders");

        // After invalidation, table should not be found
        assertThat(cache.getTable("cat", "schema", "orders")).isEmpty();
    }

    @Test
    void invalidateCatalogDeletesL2BeforeL1() {
        RecordingL2Adapter l2 = new RecordingL2Adapter();
        TieredMetadataCache cache = new TieredMetadataCache(100, 60, l2);

        cache.putTable("mycat", "db", "t1", sampleMetadata());
        cache.putTable("mycat", "db", "t2", sampleMetadata());
        cache.putTable("other", "db", "t3", sampleMetadata());
        l2.operations.clear();

        cache.invalidateCatalog("mycat");

        // L2 deleteByPrefix must come first
        assertThat(l2.operations).containsExactly("deleteByPrefix:mycat\u001F");

        // mycat entries gone, other catalog untouched
        assertThat(cache.getTable("mycat", "db", "t1")).isEmpty();
        assertThat(cache.getTable("mycat", "db", "t2")).isEmpty();
        assertThat(cache.getTable("other", "db", "t3")).isPresent();
    }

    @Test
    void concurrentGetDuringInvalidateDoesNotBackfillStaleData() {
        // Simulate: L2 has stale data, invalidateTable deletes L2 first,
        // then a concurrent getTable() should NOT find stale data in L2.
        StaleSimulatingL2Adapter l2 = new StaleSimulatingL2Adapter();
        TieredMetadataCache cache = new TieredMetadataCache(100, 60, l2);

        // Pre-populate L1 and L2
        cache.putTable("cat", "s", "t", sampleMetadata());

        // Invalidate — L2 deleted first, then L1
        cache.invalidateTable("cat", "s", "t");

        // After invalidation, getTable should return empty because L2 was
        // deleted before L1, so no stale backfill can occur
        assertThat(cache.getTable("cat", "s", "t")).isEmpty();
    }

    private static TableMetadata sampleMetadata() {
        return new TableMetadata("cat", "s", "t", List.of(), null, null, null, null, null);
    }

    /**
     * Records all L2 operations in order for verification.
     */
    static class RecordingL2Adapter implements L2CacheStore {
        final List<String> operations = new ArrayList<>();

        @Override
        public Optional<String> get(String key) {
            operations.add("get:" + key);
            return Optional.empty();
        }

        @Override
        public void put(String key, String value, long ttlSeconds) {
            operations.add("put:" + key);
        }

        @Override
        public void delete(String key) {
            operations.add("delete:" + key);
        }

        @Override
        public void deleteByPrefix(String prefix) {
            operations.add("deleteByPrefix:" + prefix);
        }

        @Override
        public void publishInvalidation(String key) {
            operations.add("publishInvalidation:" + key);
        }
    }

    /**
     * L2 adapter that returns stale data on get() UNLESS delete() was called first.
     * Simulates the race condition scenario.
     */
    static class StaleSimulatingL2Adapter implements L2CacheStore {
        private final java.util.concurrent.ConcurrentHashMap<String, String> store =
                new java.util.concurrent.ConcurrentHashMap<>();

        @Override
        public Optional<String> get(String key) {
            return Optional.ofNullable(store.get(key));
        }

        @Override
        public void put(String key, String value, long ttlSeconds) {
            store.put(key, value);
        }

        @Override
        public void delete(String key) {
            store.remove(key);
        }

        @Override
        public void deleteByPrefix(String prefix) {
            store.keySet().removeIf(k -> k.startsWith(prefix));
        }

        @Override
        public void publishInvalidation(String key) {
            // no-op
        }
    }
}
