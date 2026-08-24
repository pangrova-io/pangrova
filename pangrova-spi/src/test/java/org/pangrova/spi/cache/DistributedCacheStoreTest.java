package org.pangrova.spi.cache;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Tests for the {@link DistributedCacheStore} SPI contract,
 * including the NOOP sentinel and a simple in-memory implementation
 * that validates the interface semantics.
 *
 * G-CACHE-08: distributed/detached cache layer SPI.
 */
class DistributedCacheStoreTest {

    // ── NOOP sentinel tests ──

    @Test
    void noopGetReturnsEmpty() {
        Optional<CacheEntry> result = DistributedCacheStore.NOOP.get(
                new CacheKey("s3://bucket/file.parquet", 0, 100));
        assertThat(result).isEmpty();
    }

    @Test
    void noopPutDoesNotThrow() {
        CacheKey key = new CacheKey("s3://bucket/file.parquet", 0, 100);
        CacheEntry entry = CacheEntry.of(new byte[]{1, 2, 3}, "table1");
        assertThatCode(() -> DistributedCacheStore.NOOP.put(key, entry))
                .doesNotThrowAnyException();
    }

    @Test
    void noopInvalidateDoesNotThrow() {
        assertThatCode(() -> DistributedCacheStore.NOOP.invalidate(
                new CacheKey("s3://bucket/file.parquet", 0, 100)))
                .doesNotThrowAnyException();
    }

    @Test
    void noopInvalidateAllDoesNotThrow() {
        assertThatCode(() -> DistributedCacheStore.NOOP.invalidateAll("table1"))
                .doesNotThrowAnyException();
    }

    @Test
    void noopCloseDoesNotThrow() {
        assertThatCode(() -> DistributedCacheStore.NOOP.close())
                .doesNotThrowAnyException();
    }

    @Test
    void noopStatsReturnsUnknown() {
        assertThat(DistributedCacheStore.NOOP.stats()).isEqualTo(CacheStoreStats.UNKNOWN);
    }

    @Test
    void noopToStringIsDescriptive() {
        assertThat(DistributedCacheStore.NOOP.toString())
                .isEqualTo("DistributedCacheStore.NOOP");
    }

    // ── Semantic contract tests using a simple in-memory store ──

    @Test
    void putAndGetRoundTrip() {
        DistributedCacheStore store = new InMemoryCacheStore();
        CacheKey key = new CacheKey("s3://bucket/data/001.parquet", 0, 4096);
        CacheEntry entry = CacheEntry.of(new byte[]{10, 20, 30}, "catalog.db.t1");

        store.put(key, entry);
        Optional<CacheEntry> result = store.get(key);

        assertThat(result).isPresent();
        assertThat(result.get().data()).isEqualTo(new byte[]{10, 20, 30});
        assertThat(result.get().tableId()).isEqualTo("catalog.db.t1");
    }

    @Test
    void getMissReturnsEmpty() {
        DistributedCacheStore store = new InMemoryCacheStore();
        CacheKey key = new CacheKey("s3://bucket/missing.parquet", 0, 100);
        assertThat(store.get(key)).isEmpty();
    }

    @Test
    void invalidateRemovesEntry() {
        DistributedCacheStore store = new InMemoryCacheStore();
        CacheKey key = new CacheKey("s3://bucket/data/001.parquet", 0, 4096);
        store.put(key, CacheEntry.of(new byte[]{1}, "t1"));

        store.invalidate(key);
        assertThat(store.get(key)).isEmpty();
    }

    @Test
    void invalidateNonExistentKeyDoesNotThrow() {
        DistributedCacheStore store = new InMemoryCacheStore();
        CacheKey key = new CacheKey("s3://bucket/nonexistent.parquet", 0, 100);
        assertThatCode(() -> store.invalidate(key)).doesNotThrowAnyException();
    }

    @Test
    void invalidateAllRemovesEntriesForTable() {
        DistributedCacheStore store = new InMemoryCacheStore();
        CacheKey k1 = new CacheKey("s3://bucket/t1/001.parquet", 0, 100);
        CacheKey k2 = new CacheKey("s3://bucket/t1/002.parquet", 0, 100);
        CacheKey k3 = new CacheKey("s3://bucket/t2/001.parquet", 0, 100);

        store.put(k1, CacheEntry.of(new byte[]{1}, "catalog.db.t1"));
        store.put(k2, CacheEntry.of(new byte[]{2}, "catalog.db.t1"));
        store.put(k3, CacheEntry.of(new byte[]{3}, "catalog.db.t2"));

        store.invalidateAll("catalog.db.t1");

        assertThat(store.get(k1)).isEmpty();
        assertThat(store.get(k2)).isEmpty();
        assertThat(store.get(k3)).isPresent(); // t2 entry survives
    }

    @Test
    void putOverwritesExistingEntry() {
        DistributedCacheStore store = new InMemoryCacheStore();
        CacheKey key = new CacheKey("s3://bucket/data/001.parquet", 0, 4096);

        store.put(key, CacheEntry.of(new byte[]{1}, "t1"));
        store.put(key, CacheEntry.of(new byte[]{2}, "t1"));

        Optional<CacheEntry> result = store.get(key);
        assertThat(result).isPresent();
        assertThat(result.get().data()).isEqualTo(new byte[]{2});
    }

    @Test
    void closeIsIdempotent() {
        DistributedCacheStore store = new InMemoryCacheStore();
        assertThatCode(() -> {
            store.close();
            store.close();
        }).doesNotThrowAnyException();
    }

    // ── Simple in-memory implementation for testing ──

    /**
     * Minimal in-memory implementation that validates the
     * {@link DistributedCacheStore} contract without external dependencies.
     */
    private static final class InMemoryCacheStore implements DistributedCacheStore {

        private final ConcurrentHashMap<String, CacheEntry> store = new ConcurrentHashMap<>();

        @Override
        public Optional<CacheEntry> get(CacheKey key) {
            return Optional.ofNullable(store.get(key.toKeyString()));
        }

        @Override
        public void put(CacheKey key, CacheEntry entry) {
            store.put(key.toKeyString(), entry);
        }

        @Override
        public void invalidate(CacheKey key) {
            store.remove(key.toKeyString());
        }

        @Override
        public void invalidateAll(String tableId) {
            store.entrySet().removeIf(e -> tableId.equals(e.getValue().tableId()));
        }

        @Override
        public void close() {
            store.clear();
        }
    }
}
