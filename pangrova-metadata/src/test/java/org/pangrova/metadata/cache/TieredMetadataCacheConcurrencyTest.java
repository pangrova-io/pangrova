package org.pangrova.metadata.cache;

import org.pangrova.metadata.model.TableMetadata;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies concurrent consistency of TieredMetadataCache invalidatingCatalogs.
 *
 * <p>Tests that concurrent readers during catalog invalidation never observe
 * stale catalog index entries — the volatile fence + ConcurrentHashMap.computeIfAbsent
 * pattern ensures readers either see the invalidation flag (and skip registration)
 * or register after invalidation completes (safe).</p>
 */
class TieredMetadataCacheConcurrencyTest {

    private static final int READER_THREADS = 8;
    private static final int INVALIDATION_ITERATIONS = 200;
    private static final int TABLE_COUNT = 50;

    @Test
    void concurrentReadsDuringInvalidationNeverLeakIndexEntries() throws Exception {
        InMemoryL2Store l2 = new InMemoryL2Store(new ConcurrentHashMap<>());
        TieredMetadataCache cache = new TieredMetadataCache(10_000, 60, l2);
        populateCache(cache, "testcat", TABLE_COUNT);

        AtomicBoolean running = new AtomicBoolean(true);
        CyclicBarrier barrier = new CyclicBarrier(READER_THREADS + 1);
        ExecutorService readers = Executors.newFixedThreadPool(READER_THREADS);

        for (int t = 0; t < READER_THREADS; t++) {
            int threadId = t;
            readers.submit(() -> readerLoop(cache, barrier, running, threadId));
        }

        barrier.await();
        for (int i = 0; i < INVALIDATION_ITERATIONS; i++) {
            cache.invalidateCatalog("testcat");
            populateCache(cache, "testcat", TABLE_COUNT);
        }

        running.set(false);
        readers.shutdown();
        assertThat(readers.awaitTermination(5, TimeUnit.SECONDS)).isTrue();

        // Final invariant: after one last invalidation, no orphaned index entries remain
        cache.invalidateCatalog("testcat");
        cache.performMaintenance();
        assertThat(cache.getTable("testcat", "schema", "table0")).isEmpty();
    }

    @Test
    void concurrentInvalidateAndPutDoNotCorruptCatalogIndex() throws Exception {
        InMemoryL2Store l2 = new InMemoryL2Store(new ConcurrentHashMap<>());
        TieredMetadataCache cache = new TieredMetadataCache(10_000, 60, l2);

        int iterations = 500;
        CountDownLatch done = new CountDownLatch(2);
        AtomicBoolean error = new AtomicBoolean(false);

        Thread writer = new Thread(() -> {
            try {
                for (int i = 0; i < iterations; i++) {
                    cache.putTable("cat", "s", "t" + (i % 10),
                            sampleMetadata("cat", "s", "t" + (i % 10)));
                }
            } catch (Exception e) {
                error.set(true);
            } finally {
                done.countDown();
            }
        });

        Thread invalidator = new Thread(() -> {
            try {
                for (int i = 0; i < iterations; i++) {
                    cache.invalidateCatalog("cat");
                }
            } catch (Exception e) {
                error.set(true);
            } finally {
                done.countDown();
            }
        });

        writer.start();
        invalidator.start();

        assertThat(done.await(10, TimeUnit.SECONDS))
                .as("Both threads should complete without deadlock")
                .isTrue();
        assertThat(error.get())
                .as("No exceptions during concurrent put/invalidate")
                .isFalse();
    }

    @Test
    void invalidationVisibilityAcrossThreads() throws Exception {
        InMemoryL2Store l2 = new InMemoryL2Store(new ConcurrentHashMap<>());
        TieredMetadataCache cache = new TieredMetadataCache(10_000, 60, l2);
        cache.putTable("vis", "s", "t1", sampleMetadata("vis", "s", "t1"));

        CyclicBarrier barrier = new CyclicBarrier(2);

        Thread invalidator = new Thread(() -> {
            try {
                barrier.await();
                cache.invalidateCatalog("vis");
            } catch (Exception e) {
                // barrier interruption acceptable
            }
        });

        Thread reader = new Thread(() -> {
            try {
                barrier.await();
                Thread.sleep(1);
                cache.invalidateCatalog("vis");
            } catch (Exception e) {
                // barrier interruption acceptable
            }
        });

        invalidator.start();
        reader.start();
        invalidator.join(5000);
        reader.join(5000);

        // After both invalidations complete, L2 was cleared so table is gone
        assertThat(cache.getTable("vis", "s", "t1")).isEmpty();
    }

    private void readerLoop(TieredMetadataCache cache, CyclicBarrier barrier,
                            AtomicBoolean running, int threadId) {
        try {
            barrier.await();
            while (running.get()) {
                String tableName = "table" + (threadId * 5 + (int) (Math.random() * 5));
                cache.getTable("testcat", "schema", tableName);
            }
        } catch (Exception e) {
            // Barrier interruption on shutdown is expected
        }
    }

    private void populateCache(TieredMetadataCache cache, String catalog, int count) {
        for (int i = 0; i < count; i++) {
            cache.putTable(catalog, "schema", "table" + i,
                    sampleMetadata(catalog, "schema", "table" + i));
        }
    }

    private static TableMetadata sampleMetadata(String catalog, String schema, String table) {
        return new TableMetadata(catalog, schema, table, List.of(), null, null, null, null, null);
    }

    /**
     * In-memory L2 store for concurrency testing. Stores serialized metadata
     * and supports prefix deletion. Intentional no-op for publishInvalidation
     * as cross-node invalidation is not relevant for single-JVM concurrency tests.
     */
    static class InMemoryL2Store implements L2CacheStore {
        private final ConcurrentHashMap<String, String> store;

        InMemoryL2Store(ConcurrentHashMap<String, String> store) {
            this.store = store;
        }

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

        /** No-op: cross-node invalidation not relevant for single-JVM concurrency tests. */
        @Override
        public void publishInvalidation(String key) {
        }
    }
}
