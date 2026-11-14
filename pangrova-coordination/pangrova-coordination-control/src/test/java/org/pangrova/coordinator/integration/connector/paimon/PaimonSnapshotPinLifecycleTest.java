package org.pangrova.coordinator.integration.connector.paimon;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.paimon.PaimonMetadata;
import org.pangrova.connector.paimon.PaimonSplitRegistry;
import org.pangrova.connector.paimon.handle.PaimonTableHandle;
import org.pangrova.coordinator.task.TestConnectorQuery;
import org.pangrova.connector.api.metadata.table.ConnectorReadBinding;
import org.pangrova.connector.api.metadata.table.SchemaTableName;

import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real table-scoped SDK snapshot identity and engine-owned cleanup component oracles. */
class PaimonSnapshotPinLifecycleTest {
    @TempDir Path warehouse;
    private final ConcurrentHashMap<String, TestConnectorQuery> queries = new ConcurrentHashMap<>();
    private Catalog catalog;
    private PaimonSplitRegistry registry;
    private PaimonMetadata metadata;
    private FileStoreTable orders;

    @BeforeEach
    void setUp() throws Exception {
        var options = new Options();
        options.setString("warehouse", warehouse.toString());
        catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        catalog.createDatabase("db", false);
        orders = create("orders");
        registry = new PaimonSplitRegistry();
        metadata = new PaimonMetadata(catalog, "test_catalog", registry);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (var query : queries.values()) {
            cleanup(query);
            query.retire();
        }
        registry.close();
        catalog.close();
    }

    @Test
    void queryStartPinsRealSnapshotId() throws Exception {
        long version = write(orders, 17);
        var query = query("q-real-1");
        var binding = bind(query, "orders");
        assertThat(version).isGreaterThan(0);
        assertThat(binding.snapshotId()).hasValue(version);
        assertThat(binding.snapshotId().orElseThrow()).isNotEqualTo(-1L);
    }

    @Test
    void queryEndUnpinsSnapshot() throws Exception {
        long version = write(orders, 17);
        var query = query("q-unpin-1");
        assertThat(bind(query, "orders").snapshotId()).hasValue(version);
        cleanup(query);
        assertThatThrownBy(() -> bind(query, "orders")).hasMessageContaining("admission is closed");
        assertThatThrownBy(() -> metadata.beginQuery(query.authority())).hasMessageContaining("admission is closed");
    }

    @Test
    void multipleConcurrentQueriesPinDifferentSnapshotsIndependently() throws Exception {
        long firstVersion = write(orders, 17);
        var first = query("q-a");
        var original = bind(first, "orders");
        long secondVersion = write(orders, 23);
        var second = query("q-b");
        var newer = bind(second, "orders");
        assertThat(firstVersion).isNotEqualTo(secondVersion);
        assertThat(original.snapshotId()).hasValue(firstVersion);
        assertThat(newer.snapshotId()).hasValue(secondVersion);
        cleanup(first);
        assertThat(bind(second, "orders")).isSameAs(newer);
        cleanup(second);
    }

    @Test
    void abnormalTerminationCleanupQueryStillWorks() {
        var query = query("q-abnormal");
        cleanup(query);
        assertThat(registry.size()).isZero();
        assertThatThrownBy(() -> bind(query, "orders")).hasMessageContaining("admission is closed");
    }

    @Test
    void pinSnapshotForQueryIsIdempotent() throws Exception {
        long firstVersion = write(orders, 17);
        var query = query("q-idempotent");
        var first = bind(query, "orders");
        long nextVersion = write(orders, 23);
        assertThat(nextVersion).isNotEqualTo(firstVersion);
        assertThat(bind(query, "orders")).isSameAs(first);
        assertThat(first.snapshotId()).hasValue(firstVersion);
        assertThat(first.statistics().rowCount()).isEqualTo(1);
        var actor = query.session("test_catalog", "db");
        var changed = ((PaimonTableHandle) metadata.getTableHandle(actor, new SchemaTableName("db", "orders")))
                .withTimeTravelSnapshot(nextVersion);
        assertThatThrownBy(() -> metadata.bindRead(actor, changed)).hasMessageContaining("first-access binding");
        var narrowed = metadata.bindRead(actor, ((PaimonTableHandle) first.handle()).withLimit(1));
        assertThat(narrowed.sourceIdentity()).isEqualTo(first.sourceIdentity());
        assertThat(((PaimonTableHandle) narrowed.handle()).pushedLimit()).isEqualTo(1);
    }

    @Test
    void pinSnapshotForQueryWithoutBeginQueryIsNoOp() throws Exception {
        write(orders, 17);
        var neverBegun = new TestConnectorQuery("q-never-began");
        try {
            assertThatThrownBy(() -> bind(neverBegun, "orders")).hasMessageContaining("No active Paimon query scope");
            assertThat(registry.size()).isZero();
        } finally {
            neverBegun.seal();
            neverBegun.retire();
        }
    }

    @Test
    void concurrentPinAndUnpinWithRealSnapshotsDoesNotCorrupt() throws Exception {
        long version = write(orders, 17);
        var executor = Executors.newFixedThreadPool(8);
        var start = new CountDownLatch(1);
        var tasks = new java.util.ArrayList<java.util.concurrent.Future<?>>();
        try {
            for (int worker = 0; worker < 8; worker++) {
                int thread = worker;
                tasks.add(executor.submit(() -> {
                    if (!start.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("Start timed out");
                    for (int round = 0; round < 50; round++) {
                        var query = query("q-" + thread + "-" + round);
                        assertThat(bind(query, "orders").snapshotId()).hasValue(version);
                        cleanup(query);
                        assertThatThrownBy(() -> bind(query, "orders")).hasMessageContaining("admission is closed");
                    }
                    return null;
                }));
            }
            start.countDown();
            for (var task : tasks) task.get(30, TimeUnit.SECONDS);
            assertThat(registry.size()).isZero();
        } finally {
            start.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void tablesDoNotShareAQueryGlobalSnapshotAndEmptyFirstAccessRemainsEmpty() throws Exception {
        var other = create("other");
        long orderVersion = write(orders, 17);
        write(orders, 23);
        long otherVersion = write(other, 31);
        var query = query("q-two-tables");
        var first = bind(query, "orders");
        var second = bind(query, "other");
        assertThat(first.snapshotId().orElseThrow()).isGreaterThan(orderVersion);
        assertThat(second.snapshotId()).hasValue(otherVersion);
        assertThat(first.statistics().rowCount()).isEqualTo(2);
        assertThat(second.statistics().rowCount()).isEqualTo(1);
        var emptyTable = create("empty");
        var empty = bind(query, "empty");
        write(emptyTable, 41);
        assertThat(bind(query, "empty")).isSameAs(empty);
        assertThat(empty.snapshotId()).isEmpty();
        assertThat(empty.statistics().rowCount()).isZero();
    }

    private TestConnectorQuery query(String id) {
        return queries.computeIfAbsent(id, key -> {
            var query = new TestConnectorQuery(key);
            query.authority().beginMetadata("test_catalog", () -> metadata);
            return query;
        });
    }

    private ConnectorReadBinding bind(TestConnectorQuery query, String table) {
        var session = query.session("test_catalog", "db");
        return metadata.bindRead(session, metadata.getTableHandle(session, new SchemaTableName("db", table)));
    }

    private static void cleanup(TestConnectorQuery query) {
        query.seal();
        query.authority().cleanupMetadata().requireSuccess();
    }

    private FileStoreTable create(String name) throws Exception {
        var identifier = Identifier.create("db", name);
        catalog.createTable(identifier, Schema.newBuilder().column("id", DataTypes.BIGINT())
                .option("bucket", "-1").option("file.format", "parquet")
                .option("write-only", "true").build(), false);
        return (FileStoreTable) catalog.getTable(identifier);
    }

    private static long write(Table table, long value) throws Exception {
        var builder = table.newBatchWriteBuilder();
        try (var write = builder.newWrite(); var commit = builder.newCommit()) {
            write.write(GenericRow.of(value));
            commit.commit(write.prepareCommit());
        }
        return ((FileStoreTable) table).snapshotManager().latestSnapshotId();
    }
}
