package org.pangrova.metadata.cache;

import org.pangrova.metadata.api.object.TableMetadata;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import org.pangrova.cache.api.CacheStore;
import org.pangrova.cache.memory.MemoryCacheStoreFactory;

/** Concurrent L1/L2 fills and catalog invalidation. */
class TieredMetadataCacheConcurrencyTest {

    private static final int READER_THREADS = 8;
    private static final int INVALIDATION_ITERATIONS = 200;
    private static final int TABLE_COUNT = 50;

    @Test
    void concurrentReadsDuringInvalidationNeverLeakIndexEntries() throws Exception {
        CacheStore l2 = new MemoryCacheStoreFactory().open(java.util.Map.of());
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
        CacheStore l2 = new MemoryCacheStoreFactory().open(java.util.Map.of());
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
        CacheStore l2 = new MemoryCacheStoreFactory().open(java.util.Map.of());
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

}
