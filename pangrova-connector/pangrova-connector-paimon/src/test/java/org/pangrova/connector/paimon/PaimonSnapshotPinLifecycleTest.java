package org.pangrova.connector.paimon;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the pin/unpin lifecycle with real snapshot IDs for Paimon, verifying:
 * 1. Query start pins the real snapshot ID (not -1L)
 * 2. Query end unpins the snapshot
 * 3. Multiple concurrent queries pin different snapshots independently
 * 4. Abnormal query termination still triggers unpin (no leak)
 */
class PaimonSnapshotPinLifecycleTest {

    @TempDir
    Path warehouse;

    private Catalog catalog;
    private PaimonMetadata metadata;

    @BeforeEach
    void setUp() throws Exception {
        Options options = new Options();
        options.set("warehouse", warehouse.toUri().toString());
        catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        catalog.createDatabase("db", false);
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.BIGINT())
                .column("name", DataTypes.STRING())
                .primaryKey("id")
                .option("bucket", "1")
                .build();
        catalog.createTable(Identifier.create("db", "orders"), schema, false);
        metadata = new PaimonMetadata(catalog, "test_catalog");
    }

    @Test
    void queryStartPinsRealSnapshotId() throws Exception {
        long realSnapshotId = commitDataAndGetSnapshotId();
        assertThat(realSnapshotId).isGreaterThan(0L);

        metadata.beginQuery("q-real-1");
        metadata.pinSnapshotForQuery("q-real-1", realSnapshotId);

        Optional<Long> pinned = metadata.getPinnedSnapshot("q-real-1");
        assertThat(pinned).isPresent();
        assertThat(pinned.get()).isEqualTo(realSnapshotId);
        assertThat(pinned.get()).isNotEqualTo(-1L);

        metadata.cleanupQuery("q-real-1");
    }

    @Test
    void queryEndUnpinsSnapshot() throws Exception {
        long realSnapshotId = commitDataAndGetSnapshotId();

        metadata.beginQuery("q-unpin-1");
        metadata.pinSnapshotForQuery("q-unpin-1", realSnapshotId);

        // Verify pinned
        assertThat(metadata.getPinnedSnapshot("q-unpin-1")).isPresent();

        // Cleanup (query end)
        metadata.cleanupQuery("q-unpin-1");

        // Verify unpinned
        assertThat(metadata.getPinnedSnapshot("q-unpin-1")).isEmpty();
    }

    @Test
    void multipleConcurrentQueriesPinDifferentSnapshotsIndependently() throws Exception {
        // Create first snapshot
        long snapshot1 = commitDataAndGetSnapshotId();

        // Create second snapshot
        long snapshot2 = commitDataAndGetSnapshotId();

        assertThat(snapshot1).isNotEqualTo(snapshot2);

        // Query A pins snapshot1
        metadata.beginQuery("q-a");
        metadata.pinSnapshotForQuery("q-a", snapshot1);

        // Query B pins snapshot2
        metadata.beginQuery("q-b");
        metadata.pinSnapshotForQuery("q-b", snapshot2);

        // Both should be independently pinned
        assertThat(metadata.getPinnedSnapshot("q-a")).hasValue(snapshot1);
        assertThat(metadata.getPinnedSnapshot("q-b")).hasValue(snapshot2);

        // Cleanup query A should not affect query B
        metadata.cleanupQuery("q-a");
        assertThat(metadata.getPinnedSnapshot("q-a")).isEmpty();
        assertThat(metadata.getPinnedSnapshot("q-b")).hasValue(snapshot2);

        metadata.cleanupQuery("q-b");
        assertThat(metadata.getPinnedSnapshot("q-b")).isEmpty();
    }

    @Test
    void abnormalTerminationCleanupQueryStillWorks() {
        // Simulate abnormal termination: cleanupQuery called without prior pin
        // This verifies no exception is thrown and no leak occurs
        metadata.beginQuery("q-abnormal");
        // No pinSnapshotForQuery called (simulating crash before table access)

        // cleanupQuery should still work without error
        metadata.cleanupQuery("q-abnormal");
        assertThat(metadata.getPinnedSnapshot("q-abnormal")).isEmpty();
    }

    @Test
    void pinSnapshotForQueryIsIdempotent() throws Exception {
        long snapshot1 = commitDataAndGetSnapshotId();
        long snapshot2 = commitDataAndGetSnapshotId();

        metadata.beginQuery("q-idempotent");
        metadata.pinSnapshotForQuery("q-idempotent", snapshot1);

        // Second pin attempt with different snapshot should be no-op
        metadata.pinSnapshotForQuery("q-idempotent", snapshot2);

        // Should still have the first snapshot pinned
        assertThat(metadata.getPinnedSnapshot("q-idempotent")).hasValue(snapshot1);

        metadata.cleanupQuery("q-idempotent");
    }

    @Test
    void pinSnapshotForQueryWithoutBeginQueryIsNoOp() {
        // If beginQuery was never called, pinSnapshotForQuery should not create an entry
        metadata.pinSnapshotForQuery("q-never-began", 12345L);
        assertThat(metadata.getPinnedSnapshot("q-never-began")).isEmpty();
    }

    @Test
    void concurrentPinAndUnpinWithRealSnapshotsDoesNotCorrupt() throws Exception {
        long realSnapshotId = commitDataAndGetSnapshotId();

        int threadCount = 8;
        int queriesPerThread = 50;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        Set<Long> observedSnapshots = ConcurrentHashMap.newKeySet();
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
                    metadata.pinSnapshotForQuery(queryId, realSnapshotId);

                    Optional<Long> pinned = metadata.getPinnedSnapshot(queryId);
                    pinned.ifPresent(observedSnapshots::add);

                    metadata.cleanupQuery(queryId);
                }
            }));
        }

        startLatch.countDown();
        for (Future<?> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }
        executor.shutdown();

        // All observed snapshots should be the real one
        assertThat(observedSnapshots).containsExactly(realSnapshotId);

        // No leaked pins
        for (int t = 0; t < threadCount; t++) {
            for (int i = 0; i < queriesPerThread; i++) {
                assertThat(metadata.getPinnedSnapshot("q-" + t + "-" + i)).isEmpty();
            }
        }
    }

    private long commitDataAndGetSnapshotId() throws Exception {
        Table table = catalog.getTable(Identifier.create("db", "orders"));
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
             BatchTableCommit commit = writeBuilder.newCommit()) {
            org.apache.paimon.data.GenericRow row =
                    org.apache.paimon.data.GenericRow.of(System.nanoTime(),
                            org.apache.paimon.data.BinaryString.fromString("test"));
            write.write(row);
            commit.commit(write.prepareCommit());
        }
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        return fileStoreTable.snapshotManager().latestSnapshotId();
    }
}
