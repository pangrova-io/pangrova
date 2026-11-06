package org.pangrova.coordinator.integration.connector.paimon;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.paimon.PaimonMetadata;
import org.pangrova.connector.paimon.PaimonSplitRegistry;
import org.pangrova.coordinator.task.TestConnectorQuery;
import org.pangrova.connector.api.metadata.table.ConnectorReadBinding;
import org.pangrova.connector.api.metadata.table.SchemaTableName;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real first-access SDK bindings and cleanup, not fabricated query-global pin identifiers. */
class PaimonMetadataSnapshotPinningTest {
    @TempDir Path warehouse;
    private final Map<String, TestConnectorQuery> queries = new LinkedHashMap<>();
    private Catalog catalog;
    private PaimonSplitRegistry registry;
    private PaimonMetadata metadata;
    private Table table;

    @BeforeEach
    void setUp() throws Exception {
        var options = new Options();
        options.setString("warehouse", warehouse.toString());
        catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        catalog.createDatabase("db", false);
        var name = Identifier.create("db", "orders");
        catalog.createTable(name, Schema.newBuilder().column("id", DataTypes.BIGINT())
                .option("bucket", "-1").option("file.format", "parquet").build(), false);
        table = catalog.getTable(name);
        registry = new PaimonSplitRegistry();
        metadata = new PaimonMetadata(catalog, "test_catalog", registry);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (var query : queries.values()) {
            query.seal();
            query.authority().cleanupMetadata().requireSuccess();
            query.retire();
        }
        registry.close();
        catalog.close();
    }

    @Test
    void beginQueryPinsSnapshot() {
        var query = query("q-1");
        var empty = bind(query);
        assertThat(empty.snapshotId()).isEmpty();
        assertThat(empty.state()).isEqualTo(ConnectorReadBinding.SnapshotState.EMPTY);
        assertThat(bind(query)).isSameAs(empty);
    }

    @Test
    void cleanupQueryUnpinsAndClearsBoundsCache() throws Exception {
        write(17);
        var query = query("q-2");
        var original = bind(query);
        assertThat(original.snapshotId()).isPresent();
        var evaluator = (org.pangrova.connector.paimon.index.PaimonNativeIndexEvaluator)
                metadata.getNativeIndexEvaluator().orElseThrow();
        evaluator.cacheBounds("file1.parquet",
                new org.pangrova.connector.paimon.index.PaimonNativeIndexEvaluator.FileBounds(Map.of(), Map.of()));
        cleanup(query);
        assertThat(registry.size()).isZero();
        assertThatThrownBy(() -> bind(query)).hasMessageContaining("admission is closed");
        write(23);
        var fresh = query("q-after-cleanup");
        assertThat(bind(fresh).snapshotId().orElseThrow()).isGreaterThan(original.snapshotId().orElseThrow());
    }

    @Test
    void cleanupQueryIsIdempotent() {
        var query = query("q-3");
        bind(query);
        cleanup(query);
        cleanup(query);
        assertThatThrownBy(() -> bind(query)).hasMessageContaining("admission is closed");
    }

    @Test
    void multipleQueriesArePinnedIndependently() throws Exception {
        write(17);
        var first = query("q-a");
        var firstBinding = bind(first);
        write(23);
        var second = query("q-b");
        var secondBinding = bind(second);
        assertThat(firstBinding.snapshotId()).isNotEqualTo(secondBinding.snapshotId());
        cleanup(first);
        assertThat(bind(second)).isSameAs(secondBinding);
        assertThat(secondBinding.statistics().rowCount()).isEqualTo(2);
        cleanup(second);
    }

    private TestConnectorQuery query(String id) {
        return queries.computeIfAbsent(id, key -> {
            var query = new TestConnectorQuery(key);
            query.authority().beginMetadata("test_catalog", () -> metadata);
            return query;
        });
    }

    private ConnectorReadBinding bind(TestConnectorQuery query) {
        var session = query.session("test_catalog", "db");
        return metadata.bindRead(session, metadata.getTableHandle(session, new SchemaTableName("db", "orders")));
    }

    private static void cleanup(TestConnectorQuery query) {
        query.seal();
        query.authority().cleanupMetadata().requireSuccess();
    }

    private void write(long value) throws Exception {
        var builder = table.newBatchWriteBuilder();
        try (var write = builder.newWrite(); var commit = builder.newCommit()) {
            write.write(GenericRow.of(value));
            commit.commit(write.prepareCommit());
        }
    }
}
