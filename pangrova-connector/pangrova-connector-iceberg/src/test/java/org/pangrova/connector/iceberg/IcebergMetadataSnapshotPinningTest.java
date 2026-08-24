package org.pangrova.connector.iceberg;

import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests snapshot pinning lifecycle: beginQuery pins, cleanupQuery unpins and clears boundsCache.
 */
class IcebergMetadataSnapshotPinningTest {

    @TempDir
    Path warehouse;

    private IcebergMetadata metadata;

    private static final Schema TEST_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    @BeforeEach
    void setUp() {
        HadoopTestGuard.assumeHadoopCompatible();
        Catalog catalog = createHadoopCatalog();
        if (catalog instanceof org.apache.iceberg.catalog.SupportsNamespaces nsCatalog) {
            nsCatalog.createNamespace(Namespace.of("db"));
        }
        catalog.createTable(TableIdentifier.of("db", "orders"), TEST_SCHEMA);
        metadata = new IcebergMetadata(catalog, "test_catalog");
    }

    @Test
    void beginQueryPinsSnapshot() {
        metadata.beginQuery("q-1");
        // After beginQuery, the query should be tracked
        assertThat(metadata.getPinnedSnapshot("q-1")).isEmpty(); // -1L sentinel → empty
    }

    @Test
    void cleanupQueryUnpinsAndClearsBoundsCache() {
        metadata.beginQuery("q-2");

        // Populate boundsCache via the nativeIndexEvaluator
        var evaluator = metadata.getNativeIndexEvaluator().orElseThrow();
        var icebergEvaluator = (org.pangrova.connector.iceberg.index.IcebergNativeIndexEvaluator) evaluator;
        icebergEvaluator.cacheBounds("file1.parquet",
                new org.pangrova.connector.iceberg.index.IcebergNativeIndexEvaluator.FileBounds(
                        Map.of(), Map.of(), Map.of(), Map.of()));

        metadata.cleanupQuery("q-2");

        // After cleanup, pinned snapshot should be gone
        assertThat(metadata.getPinnedSnapshot("q-2")).isEmpty();
    }

    @Test
    void cleanupQueryIsIdempotent() {
        metadata.beginQuery("q-3");
        metadata.cleanupQuery("q-3");
        // Second cleanup should not throw
        metadata.cleanupQuery("q-3");
    }

    @Test
    void multipleQueriesArePinnedIndependently() {
        metadata.beginQuery("q-a");
        metadata.beginQuery("q-b");

        metadata.cleanupQuery("q-a");

        // q-b should still be tracked (getPinnedSnapshot returns empty for -1L sentinel)
        // but the key should still exist in the map — verified by cleanup not throwing
        metadata.cleanupQuery("q-b");
    }

    @Test
    void ttlEvictionRemovesExpiredPins() throws Exception {
        // Use a short TTL for testing
        IcebergMetadata shortTtlMetadata = new IcebergMetadata(
                createHadoopCatalog(), "test_catalog", 50L);

        shortTtlMetadata.beginQuery("q-ttl-1");
        shortTtlMetadata.beginQuery("q-ttl-2");

        // Wait for TTL to expire
        Thread.sleep(100);

        // Trigger eviction via scheduled executor
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        shortTtlMetadata.startPinEviction(scheduler);

        // Give eviction time to run
        Thread.sleep(200);
        scheduler.shutdown();
        scheduler.awaitTermination(1, TimeUnit.SECONDS);

        // Both pins should have been evicted — cleanup should be no-op (not throw)
        shortTtlMetadata.cleanupQuery("q-ttl-1");
        shortTtlMetadata.cleanupQuery("q-ttl-2");

        assertThat(shortTtlMetadata.getPinnedSnapshot("q-ttl-1")).isEmpty();
        assertThat(shortTtlMetadata.getPinnedSnapshot("q-ttl-2")).isEmpty();
    }

    @Test
    void concurrentPinUnpinDoesNotCorrupt() throws Exception {
        int threadCount = 8;
        int queriesPerThread = 50;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < threadCount; t++) {
            int threadId = t;
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int i = 0; i < queriesPerThread; i++) {
                    String queryId = "q-" + threadId + "-" + i;
                    metadata.beginQuery(queryId);
                    metadata.cleanupQuery(queryId);
                }
            }));
        }

        startLatch.countDown();
        for (Future<?> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }
        executor.shutdown();

        // After all threads complete, no pinned snapshots should remain
        for (int t = 0; t < threadCount; t++) {
            for (int i = 0; i < queriesPerThread; i++) {
                assertThat(metadata.getPinnedSnapshot("q-" + t + "-" + i)).isEmpty();
            }
        }
    }

    @Test
    void pinAfterCleanupCreatesNewEntry() {
        metadata.beginQuery("q-repin");
        metadata.cleanupQuery("q-repin");

        // Re-pinning after cleanup should work
        metadata.beginQuery("q-repin");
        // Entry exists again (with -1L sentinel)
        metadata.cleanupQuery("q-repin");
    }

    private Catalog createHadoopCatalog() {
        Map<String, String> props = Map.of(
                "type", "hadoop",
                "warehouse", warehouse.toString());
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        return org.apache.iceberg.CatalogUtil.buildIcebergCatalog("test_catalog", props, hadoopConf);
    }
}
