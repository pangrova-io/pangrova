package org.pangrova.coordinator.integration.connector.paimon;

import org.pangrova.connector.paimon.PaimonSplitRegistry;
import org.pangrova.connector.paimon.PaimonSplitManager;
import org.pangrova.connector.paimon.PaimonMetadata;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.ConnectorTransactionHandle;
import org.pangrova.spi.connector.metadata.Constraint;
import org.pangrova.spi.connector.metadata.DynamicFilter;
import org.pangrova.spi.connector.metadata.table.ConnectorReadBinding;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual SDK Parquet commits and canonical split serialization, not Native/SQL E2E. */
class PaimonReadBindingIntegrationTest {
    @TempDir Path warehouse;
    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override
        public java.util.Optional<org.pangrova.spi.connector.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

        @Override public String getUser() { return "reader"; }
        @Override public Optional<String> getCatalog() { return Optional.of("bound"); }
        @Override public Optional<String> getSchema() { return Optional.of("db"); }
        @Override public Map<String, String> getProperties() { return Map.of("queryId", "read", "tenant_id", "tenant"); }
    };

    @Test
    void selectedStatisticsAndSplitsDoNotAdvanceAfterAnotherCommit() throws Exception {
        try (var catalog = catalog()) {
            var table = create(catalog, "rows");
            write(table, 3);
            var metadata = new PaimonMetadata(catalog, "bound", new org.pangrova.connector.paimon.PaimonSplitRegistry());
            var binding = bind(metadata, "rows");
            long version = binding.snapshotId().orElseThrow();
            write(table, 5);
            metadata.validateReadBinding(SESSION, binding);
            assertThat(binding.statistics().rowCount()).isEqualTo(3);
            var splits = splits(catalog, binding);
            assertThat(splits).isNotEmpty();
            assertThat(splits).allSatisfy(split -> assertThat(split.getSourceSnapshot()).isEqualTo(Long.toString(version)));
            assertThat(bind(metadata, "rows").statistics().rowCount()).isEqualTo(8);
        }
    }

    @Test
    void currentAndHistoricalSchemasAreDistinguishedBeforeSplits() throws Exception {
        try (var catalog = catalog()) {
            var table = create(catalog, "schema");
            write(table, 2);
            long version = table.snapshotManager().latestSnapshotId();
            catalog.alterTable(Identifier.create("db", "schema"),
                    List.of(SchemaChange.addColumn("added", DataTypes.STRING())), false);
            var metadata = new PaimonMetadata(catalog, "bound", new org.pangrova.connector.paimon.PaimonSplitRegistry());
            var current = bind(metadata, "schema");
            assertThat(current.snapshotId().orElseThrow()).isEqualTo(version);
            assertThat(current.declaration().metadata().columns()).containsExactly("id", "added");
            var selector = metadata.getTableHandle(SESSION, new SchemaTableName("db", "schema"))
                    .withTimeTravelSnapshot(version);
            var historical = metadata.bindRead(SESSION, selector);
            assertThat(historical.declaration().requireExactDeclaration().columns()).containsExactly("id");
            assertThat(splits(catalog, current)).isNotEmpty();
            assertThat(splits(catalog, historical)).isNotEmpty();
        }
    }

    @Test
    void capturedCurrentSchemaOrOptionsCannotBeSilentlyRebound() throws Exception {
        try (var catalog = catalog()) {
            var table = create(catalog, "drift");
            write(table, 2);
            var metadata = new PaimonMetadata(catalog, "bound", new org.pangrova.connector.paimon.PaimonSplitRegistry());
            var selected = bind(metadata, "drift");
            catalog.alterTable(Identifier.create("db", "drift"),
                    List.of(SchemaChange.addColumn("added", DataTypes.STRING())), false);
            assertThatThrownBy(() -> splits(catalog, selected))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("schema or read options changed");
        }
    }

    @Test
    void scopedHistoricalBindingAcceptsItsOwnExactHandleAfterSchemaEvolution() throws Exception {
        try (var catalog = catalog(); var registry = new PaimonSplitRegistry()) {
            var table = create(catalog, "scoped_history");
            write(table, 2);
            long version = table.snapshotManager().latestSnapshotId();
            catalog.alterTable(Identifier.create("db", "scoped_history"),
                    List.of(SchemaChange.addColumn("added", DataTypes.STRING())), false);
            var query = new org.pangrova.coordinator.task.TestConnectorQuery("historical-owner");
            var actor = query.session("bound", "db");
            var metadata = new PaimonMetadata(catalog, "bound", registry);
            query.authority().beginMetadata("bound", () -> metadata);
            try {
                var currentSelector = metadata.getTableHandle(actor, new SchemaTableName("db", "scoped_history"));
                var original = metadata.bindRead(actor, currentSelector.withTimeTravelSnapshot(version));
                assertThat(original.declaration().metadata().columns()).containsExactly("id");
                assertThat(metadata.bindRead(actor, original.handle())).isSameAs(original);
                assertThat(metadata.getTableStatistics(actor, original.handle())).isSameAs(original.statistics());
                var narrowedHandle = ((org.pangrova.connector.paimon.handle.PaimonTableHandle) original.handle())
                        .withLimit(1);
                assertThat(metadata.bindRead(actor, narrowedHandle).sourceIdentity()).isEqualTo(original.sourceIdentity());
                var forged = new org.pangrova.connector.paimon.handle.PaimonTableHandle(
                        narrowedHandle.schemaName(), narrowedHandle.tableName(), narrowedHandle.tableLocation(),
                        narrowedHandle.snapshotId(), ((org.pangrova.connector.paimon.handle.PaimonTableHandle)
                                currentSelector).rowTypeJson(), narrowedHandle.primaryKeys(),
                        narrowedHandle.partitionKeys(), narrowedHandle.properties(), narrowedHandle.pushedLimit());
                assertThatThrownBy(() -> metadata.bindRead(actor, forged)).hasMessageContaining("first-access binding");
            } finally {
                query.seal();
                query.authority().cleanupMetadata().requireSuccess();
                query.retire();
            }
        }
    }

    @Test
    void emptyViewSurvivesFirstCommitButRejectsAReplacementTable() throws Exception {
        try (var catalog = catalog()) {
            var table = create(catalog, "empty");
            var metadata = new PaimonMetadata(catalog, "bound", new org.pangrova.connector.paimon.PaimonSplitRegistry());
            var selected = bind(metadata, "empty");
            assertThat(selected.state()).isEqualTo(ConnectorReadBinding.SnapshotState.EMPTY);
            assertThat(selected.statistics().rowCount()).isZero();
            assertThat(selected.snapshotId()).isEmpty();
            write(table, 2);
            metadata.validateReadBinding(SESSION, selected);
            assertThat(selected.statistics().rowCount()).isZero();
            catalog.dropTable(Identifier.create("db", "empty"), false);
            create(catalog, "empty");
            assertThatThrownBy(() -> metadata.validateReadBinding(SESSION, selected))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("identity changed");
        }
    }

    @Test
    void removedSnapshotIsRejectedWithoutReadingTheLatest() throws Exception {
        try (var catalog = catalog()) {
            var table = create(catalog, "expired");
            write(table, 2);
            var metadata = new PaimonMetadata(catalog, "bound", new org.pangrova.connector.paimon.PaimonSplitRegistry());
            var selected = bind(metadata, "expired");
            write(table, 4);
            var file = table.snapshotManager().snapshotPath(selected.snapshotId().orElseThrow());
            assertThat(table.fileIO().delete(file, false)).isTrue();
            assertThatThrownBy(() -> metadata.validateReadBinding(SESSION, selected))
                    .isInstanceOf(org.pangrova.spi.connector.base.ConnectorException.class)
                    .hasMessageContaining("unavailable");
            assertThatThrownBy(() -> splits(catalog, selected))
                    .isInstanceOf(org.pangrova.spi.connector.base.ConnectorException.class)
                    .hasMessageContaining("unavailable");
        }
    }

    private Catalog catalog() throws Exception {
        var options = new Options();
        options.setString("warehouse", warehouse.toString());
        var catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        catalog.createDatabase("db", false);
        return catalog;
    }

    private static FileStoreTable create(Catalog catalog, String name) throws Exception {
        var id = Identifier.create("db", name);
        catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.BIGINT())
                .option("bucket", "-1").option("file.format", "parquet").build(), false);
        return (FileStoreTable) catalog.getTable(id);
    }

    private static ConnectorReadBinding bind(PaimonMetadata metadata, String name) {
        return metadata.bindRead(SESSION, metadata.getTableHandle(SESSION, new SchemaTableName("db", name)));
    }

    private static void write(FileStoreTable table, int rows) throws Exception {
        var builder = table.newBatchWriteBuilder();
        try (var writer = builder.newWrite(); var commit = builder.newCommit()) {
            for (int row = 0; row < rows; row++) writer.write(GenericRow.of((long) row));
            commit.commit(writer.prepareCommit());
        }
    }

    private static List<org.pangrova.protocol.v2.DataSplit> splits(Catalog catalog,
            ConnectorReadBinding binding) throws Exception {
        var registry = new PaimonSplitRegistry();
        var query = new org.pangrova.coordinator.task.TestConnectorQuery("read", "reader", "tenant", Map.of());
        var session = query.session("bound", "db");
        registry.beginQuery(query.authority());
        var manager = new PaimonSplitManager(catalog, registry);
        var source = manager.getSplits(ConnectorTransactionHandle.EMPTY, session, binding.handle(),
                DynamicFilter.ALL, Constraint.EMPTY);
        List<org.pangrova.protocol.v2.DataSplit> result = new ArrayList<>();
        try {
            while (!source.isFinished()) {
                for (var split : source.getNextBatch(100)) {
                    result.add(org.pangrova.protocol.v2.DataSplit.parseFrom(split.toSplitInfoProto()));
                }
            }
        } finally {
            source.close();
            query.seal();
            registry.releaseQuery(query.authority());
            query.retire();
            registry.close();
        }
        return result;
    }
}
