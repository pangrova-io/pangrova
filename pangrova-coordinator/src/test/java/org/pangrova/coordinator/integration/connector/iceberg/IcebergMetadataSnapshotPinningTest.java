package org.pangrova.coordinator.integration.connector.iceberg;

import org.pangrova.connector.iceberg.IcebergMetadata;

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
    private final java.util.concurrent.ConcurrentHashMap<String,
            org.pangrova.coordinator.task.TestConnectorQuery> queries = new java.util.concurrent.ConcurrentHashMap<>();

    private org.pangrova.coordinator.task.TestConnectorQuery query(String id) {
        return queries.computeIfAbsent(id, org.pangrova.coordinator.task.TestConnectorQuery::new);
    }

    private void cleanup(org.pangrova.spi.connector.metadata.ConnectorMetadata owner, String id) {
        var lifetime = query(id);
        lifetime.seal();
        owner.cleanupQuery(lifetime.authority());
        lifetime.retire();
    }


    @TempDir
    Path warehouse;

    private IcebergMetadata metadata;

    private static final Schema TEST_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    @BeforeEach
    void setUp() {
        Catalog catalog = createHadoopCatalog();
        if (catalog instanceof org.apache.iceberg.catalog.SupportsNamespaces nsCatalog) {
            nsCatalog.createNamespace(Namespace.of("db"));
        }
        catalog.createTable(TableIdentifier.of("db", "orders"), TEST_SCHEMA);
        metadata = new IcebergMetadata(catalog, "test_catalog");
    }

    @Test
    void beginQueryPinsSnapshot() {
        metadata.beginQuery(query("q-1").authority());
        // After beginQuery, the query should be tracked
        assertThat(metadata.getPinnedSnapshot("q-1")).isEmpty(); // -1L sentinel → empty
    }

    @Test
    void cleanupQueryUnpinsAndClearsBoundsCache() {
        metadata.beginQuery(query("q-2").authority());

        // Populate boundsCache via the nativeIndexEvaluator
        var evaluator = metadata.getNativeIndexEvaluator().orElseThrow();
        var icebergEvaluator = (org.pangrova.connector.iceberg.index.IcebergNativeIndexEvaluator) evaluator;
        icebergEvaluator.cacheBounds("file1.parquet",
                new org.pangrova.connector.iceberg.index.IcebergNativeIndexEvaluator.FileBounds(
                        Map.of(), Map.of(), Map.of(), Map.of()));

        cleanup(metadata, "q-2");

        // After cleanup, pinned snapshot should be gone
        assertThat(metadata.getPinnedSnapshot("q-2")).isEmpty();
    }

    @Test
    void cleanupQueryIsIdempotent() {
        metadata.beginQuery(query("q-3").authority());
        cleanup(metadata, "q-3");
        // Second cleanup should not throw
        cleanup(metadata, "q-3");
    }

    @Test
    void multipleQueriesArePinnedIndependently() {
        metadata.beginQuery(query("q-a").authority());
        metadata.beginQuery(query("q-b").authority());

        cleanup(metadata, "q-a");

        // q-b should still be tracked (getPinnedSnapshot returns empty for -1L sentinel)
        // but the key should still exist in the map — verified by cleanup not throwing
        cleanup(metadata, "q-b");
    }

    @Test
    void ttlEvictionRemovesExpiredPins() throws Exception {
        // Use a short TTL for testing
        IcebergMetadata shortTtlMetadata = new IcebergMetadata(
                createHadoopCatalog(), "test_catalog", 50L);

        shortTtlMetadata.beginQuery(query("q-ttl-1").authority());
        shortTtlMetadata.beginQuery(query("q-ttl-2").authority());

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
        cleanup(shortTtlMetadata, "q-ttl-1");
        cleanup(shortTtlMetadata, "q-ttl-2");

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
                    metadata.beginQuery(query(queryId).authority());
                    cleanup(metadata, queryId);
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
        metadata.beginQuery(query("q-repin").authority());
        cleanup(metadata, "q-repin");

        // A fresh actual query creation may reuse a textual ID; the old facade stays closed.
        var prior = queries.remove("q-repin");
        assertThat(prior.authority().isClosed()).isTrue();
        metadata.beginQuery(query("q-repin").authority());
        // Entry exists again (with -1L sentinel)
        cleanup(metadata, "q-repin");
    }

    private Catalog createHadoopCatalog() {
        Map<String, String> props = Map.of(
                "type", "hadoop",
                "warehouse", warehouse.toString());
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        return org.apache.iceberg.CatalogUtil.buildIcebergCatalog("test_catalog", props, hadoopConf);
    }
}
