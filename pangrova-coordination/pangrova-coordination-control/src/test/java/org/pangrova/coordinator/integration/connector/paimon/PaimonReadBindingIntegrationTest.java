package org.pangrova.coordinator.integration.connector.paimon;

import org.pangrova.protocol.source.CanonicalConnectorSplit;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.connector.api.exception.ConnectorException;
import org.pangrova.connector.api.enums.ReadSnapshotState;
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
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.transaction.ConnectorTransactionHandle;
import org.pangrova.connector.api.metadata.SplitPlanningBudget;
import org.pangrova.connector.api.metadata.Constraint;
import org.pangrova.connector.api.metadata.DynamicFilter;
import org.pangrova.connector.api.metadata.table.ConnectorReadBinding;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.coordinator.task.TestConnectorQuery;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual SDK Parquet commits and canonical split serialization, not Native/SQL E2E. */
class PaimonReadBindingIntegrationTest {
    @TempDir Path warehouse;
    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override
        public java.util.Optional<org.pangrova.connector.api.query.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

        @Override public String getUser() { return "reader"; }
        @Override public Optional<String> getCatalog() { return Optional.of("bound"); }
        @Override public Optional<String> getSchema() { return Optional.of("db"); }
        @Override public Map<String, String> getProperties() { return Map.of("queryId", "read", "tenant_id", "tenant"); }
    };

    @Test
    void validatingABoundScanAndPlanningItsFilesShareOneFreshCatalogLoad() throws Exception {
        try (var catalog = catalog()) {
            var table = create(catalog, "counted");
            write(table, 3);
            var invalidations = new AtomicInteger();
            var tableLoads = new AtomicInteger();
            var counted = (Catalog) Proxy.newProxyInstance(Catalog.class.getClassLoader(),
                    new Class<?>[] {Catalog.class}, (proxy, method, arguments) -> {
                        if (method.getName().equals("getTable")) tableLoads.incrementAndGet();
                        if (method.getName().equals("invalidateTable")) invalidations.incrementAndGet();
                        try {
                            return method.invoke(catalog, arguments);
                        } catch (InvocationTargetException failure) {
                            throw failure.getCause();
                        }
                    });
            try (var registry = new PaimonSplitRegistry()) {
                var metadata = new PaimonMetadata(counted, "bound", registry);
                var selected = bind(metadata, "counted");
                tableLoads.set(0);
                invalidations.set(0);
                assertThat(splits(counted, registry, metadata, selected)).isNotEmpty().allSatisfy(split ->
                        assertThat(split.getSourceSnapshot()).isEqualTo(Long.toString(selected.snapshotId().orElseThrow())));
                assertThat(tableLoads.get()).isOne();
                assertThat(invalidations.get()).isOne();
            }
        }
    }

    @Test
    void boundAppendScanCountsPhysicalFilesAgainstRemainingStageCapacity() throws Exception {
        try (var catalog = catalog(); var registry = new PaimonSplitRegistry()) {
            var table = create(catalog, "bounded");
            write(table, 2);
            write(table, 3);
            var metadata = new PaimonMetadata(catalog, "bound", registry);
            var selected = bind(metadata, "bounded");
            var deadline = SplitPlanningBudget.standard().deadlineNanos();

            var limited = new SplitPlanningBudget(deadline, 1);
            assertThatThrownBy(() -> splits(catalog, registry, metadata, selected, limited))
                    .isInstanceOfSatisfying(ConnectorException.class, failure ->
                            assertThat(failure.getErrorCode())
                                    .isEqualTo(PangrovaErrorCode.CONNECTOR_SPLIT_LIMIT_REACHED));
            assertThat(splits(catalog, registry, metadata, selected, new SplitPlanningBudget(deadline, 2)))
                    .hasSize(2).allSatisfy(split ->
                            assertThat(split.getSourceSnapshot())
                                    .isEqualTo(Long.toString(selected.snapshotId().orElseThrow())));
        }
    }

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
            String versionToken = new org.pangrova.connector.paimon.index.PaimonSnapshotAwareMetadata(table)
                    .currentSnapshotId().orElseThrow();
            assertThat(versionToken).isEqualTo(Long.toString(version));
            catalog.alterTable(Identifier.create("db", "schema"),
                    List.of(SchemaChange.addColumn("added", DataTypes.STRING())), false);
            var metadata = new PaimonMetadata(catalog, "bound", new org.pangrova.connector.paimon.PaimonSplitRegistry());
            var current = bind(metadata, "schema");
            assertThat(current.snapshotId().orElseThrow()).isEqualTo(version);
            assertThat(current.declaration().metadata().columns()).containsExactly("id", "added");
            var selector = metadata.getTableHandle(SESSION, new SchemaTableName("db", "schema"))
                    .withTimeTravelSnapshot(versionToken);
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
            String versionToken = new org.pangrova.connector.paimon.index.PaimonSnapshotAwareMetadata(table)
                    .currentSnapshotId().orElseThrow();
            assertThat(versionToken).isEqualTo(Long.toString(version));
            catalog.alterTable(Identifier.create("db", "scoped_history"),
                    List.of(SchemaChange.addColumn("added", DataTypes.STRING())), false);
            var query = new org.pangrova.coordinator.task.TestConnectorQuery("historical-owner");
            var actor = query.session("bound", "db");
            var metadata = new PaimonMetadata(catalog, "bound", registry);
            query.authority().beginMetadata("bound", () -> metadata);
            try {
                var currentSelector = metadata.getTableHandle(actor, new SchemaTableName("db", "scoped_history"));
                var original = metadata.bindRead(actor, currentSelector.withTimeTravelSnapshot(versionToken));
                assertThat(original.declaration().metadata().columns()).containsExactly("id");
                assertThat(metadata.bindRead(actor, original.handle())).isSameAs(original);
                assertThat(metadata.getTableStatistics(actor, original.handle())).isSameAs(original.statistics());
                var narrowedHandle = ((org.pangrova.connector.paimon.handle.PaimonTableHandle) original.handle())
                        .withLimit(1);
                var narrowed = metadata.bindRead(actor, narrowedHandle);
                assertThat(narrowed.sourceIdentity()).isEqualTo(original.sourceIdentity());
                assertThat(narrowed.schemaIdentity()).containsSame(original.schemaIdentity().orElseThrow());
                var forged = new org.pangrova.connector.paimon.handle.PaimonTableHandle(
                        narrowedHandle.schemaName(), narrowedHandle.tableName(), narrowedHandle.tableLocation(),
                        narrowedHandle.tableUuid(), narrowedHandle.schemaFingerprint(),
                        narrowedHandle.snapshotId(), ((org.pangrova.connector.paimon.handle.PaimonTableHandle)
                                currentSelector).rowTypeJson(), narrowedHandle.primaryKeys(),
                        narrowedHandle.partitionKeys(), narrowedHandle.properties(), narrowedHandle.pushedLimit(),
                        narrowedHandle.incrementalRange());
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
            assertThat(selected.state()).isEqualTo(ReadSnapshotState.EMPTY);
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
                    .isInstanceOf(org.pangrova.connector.api.exception.ConnectorException.class)
                    .hasMessageContaining("unavailable");
            assertThatThrownBy(() -> splits(catalog, selected))
                    .isInstanceOf(org.pangrova.connector.api.exception.ConnectorException.class)
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
        try (var registry = new PaimonSplitRegistry()) {
            return splits(catalog, registry, new PaimonMetadata(catalog, "bound", registry), binding);
        }
    }

    private static List<org.pangrova.protocol.v2.DataSplit> splits(Catalog catalog, PaimonSplitRegistry registry,
            PaimonMetadata metadata, ConnectorReadBinding binding) throws Exception {
        return splits(catalog, registry, metadata, binding, SplitPlanningBudget.standard());
    }

    private static List<org.pangrova.protocol.v2.DataSplit> splits(Catalog catalog, PaimonSplitRegistry registry,
            PaimonMetadata metadata, ConnectorReadBinding binding, SplitPlanningBudget budget) throws Exception {
        var query = new TestConnectorQuery("read", "reader", "tenant", Map.of());
        var session = query.session("bound", "db");
        registry.beginQuery(query.authority());
        var manager = new PaimonSplitManager(catalog, registry);
        List<org.pangrova.protocol.v2.DataSplit> result = new ArrayList<>();
        try {
            var source = manager.getBoundSplits(ConnectorTransactionHandle.empty(), session, metadata, binding,
                    DynamicFilter.ALL, Constraint.EMPTY, budget);
            try {
                assertThat(registry.size()).isZero();
                while (!source.isFinished()) {
                    for (var split : source.getNextBatch(100)) {
                        result.add(org.pangrova.protocol.v2.DataSplit.parseFrom(
                                CanonicalConnectorSplit.from(split).toByteArray()));
                    }
                }
            } finally {
                source.close();
            }
        } finally {
            query.seal();
            registry.releaseQuery(query.authority());
            query.retire();
        }
        return result;
    }
}
