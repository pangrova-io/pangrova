package org.pangrova.connector.paimon;

import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.stats.ColStats;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.connector.api.exception.ConnectorException;
import org.pangrova.connector.api.metadata.Constraint;
import org.pangrova.connector.api.metadata.Domain;
import org.pangrova.connector.api.metadata.DynamicFilter;
import org.pangrova.connector.api.metadata.SplitPlanningBudget;
import org.pangrova.connector.api.metadata.table.ConnectorReadBinding;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.query.ConnectorQueryAuthority;
import org.pangrova.connector.api.query.ConnectorQueryOperation;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.paimon.handle.PaimonColumnHandle;
import org.pangrova.connector.paimon.handle.PaimonSplit;
import org.pangrova.connector.paimon.handle.PaimonTableHandleFactory;
import org.pangrova.types.enums.PangrovaType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PaimonReadBindingIoIntegrationTest {
    @TempDir java.nio.file.Path warehouse;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void historicalBindingUsesOneSnapshotForSchemaAndStatisticsThenRevalidates(boolean withStatistics)
            throws Exception {
        var io = new CountingSnapshotIo();
        try (var catalog = new FileSystemCatalog(io, new Path(warehouse.toString()))) {
            catalog.createDatabase("db", false);
            var id = Identifier.create("db", "history");
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.BIGINT())
                    .option("bucket", "-1").option("file.format", "parquet").build(), false);
            var table = (FileStoreTable) catalog.getTable(id);
            write(table, GenericRow.of(10L));
            if (withStatistics) {
                try (var commit = table.newCommit("analyze-history")) {
                    commit.updateStatistics(new Statistics(1, table.schema().id(), 1L, 8L,
                            Map.of("id", ColStats.<Long>newColStats(0, 1L, 10L, 10L, 0L, 8L, 8L))));
                }
            }
            long snapshotId = table.snapshotManager().latestSnapshotId();
            catalog.alterTable(id, SchemaChange.addColumn("later", DataTypes.STRING()), false);
            var selector = PaimonTableHandleFactory.fromTable("db", "history", catalog.getTable(id))
                    .withTimeTravelSnapshot(Long.toString(snapshotId));
            io.snapshotPath = table.snapshotManager().snapshotPath(snapshotId);

            var bound = PaimonReadBinder.bind(catalog, selector);
            assertThat(bound.declaration().metadata().columns()).containsExactly("id");
            assertThat(bound.statistics().rowCount()).isEqualTo(1);
            assertThat(bound.statistics().columnStats().get("id").ndv()).isEqualTo(withStatistics ? 1 : -1);
            assertThat(new int[] {io.snapshotExists, io.snapshotOpens})
                    .as("snapshot existence probes, snapshot opens").containsExactly(0, 2);

            io.deleteAfterRead = true;
            assertThatThrownBy(() -> PaimonReadBinder.bind(catalog, selector))
                    .isInstanceOfSatisfying(ConnectorException.class, failure -> assertThat(failure.getErrorCode())
                            .isEqualTo(PangrovaErrorCode.CONNECTOR_SNAPSHOT_NOT_FOUND));
        }
    }

    @ParameterizedTest
    @CsvSource({"false,false", "true,false", "false,true", "true,true"})
    void ordinarySplitPlanningReusesTheValidatedSnapshotAndStillRejectsItsRemoval(
            boolean historical, boolean dataEvolution) throws Exception {
        var io = new CountingSnapshotIo();
        try (var catalog = new FileSystemCatalog(io, new Path(warehouse.toString()));
                var registry = new PaimonSplitRegistry()) {
            catalog.createDatabase("db", false);
            var id = Identifier.create("db", "history");
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.BIGINT())
                    .option("bucket", "-1").option("file.format", "parquet")
                    .option("data-evolution.enabled", Boolean.toString(dataEvolution))
                    .option("row-tracking.enabled", Boolean.toString(dataEvolution)).build(), false);
            var table = (FileStoreTable) catalog.getTable(id);
            write(table, GenericRow.of(10L));
            long snapshotId = table.snapshotManager().latestSnapshotId();
            io.snapshotPath = table.snapshotManager().snapshotPath(snapshotId);

            catalog.alterTable(id, SchemaChange.addColumn("later", DataTypes.STRING()), false);
            var metadata = new PaimonMetadata(catalog, "audit", registry);
            var session = new ReadSession();
            metadata.beginQuery(session);
            var selector = metadata.getTableHandle(session, new SchemaTableName("db", "history"));
            if (historical) selector = selector.withTimeTravelSnapshot(Long.toString(snapshotId));
            var binding = metadata.bindRead(session, selector);
            assertThat(binding.declaration().metadata().columns())
                    .hasSize(historical ? 1 : 2).contains("id");
            write((FileStoreTable) catalog.getTable(id), GenericRow.of(20L, BinaryString.fromString("later")));

            Constraint constraint = dataEvolution
                    ? () -> Map.of(new PaimonColumnHandle(0, "id", PangrovaType.BIGINT),
                            new Domain.Range(10L, 10L, true, true))
                    : Constraint.EMPTY;
            var splits = new PaimonSplitManager(catalog, registry);
            io.snapshotOpens = 0;
            io.snapshotExists = 0;
            assertThat(plan(splits, metadata, binding, session, constraint)).singleElement().satisfies(split -> {
                assertThat(split.snapshotId()).isEqualTo(snapshotId);
                assertThat(split.rowCount()).isEqualTo(1L);
            });
            assertThat(new int[] {io.snapshotExists, io.snapshotOpens})
                    .as("snapshot existence probes, snapshot opens during split planning").containsExactly(0, 2);

            io.deleteAfterRead = true;
            assertThatThrownBy(() -> plan(splits, metadata, binding, session, constraint))
                    .isInstanceOfSatisfying(ConnectorException.class, failure -> assertThat(failure.getErrorCode())
                            .isEqualTo(PangrovaErrorCode.CONNECTOR_SNAPSHOT_NOT_FOUND));
        }
    }

    private static List<PaimonSplit> plan(PaimonSplitManager splits, PaimonMetadata metadata,
            ConnectorReadBinding binding, ConnectorSession session, Constraint constraint) {
        return splits.getBoundSplits(null, session, metadata, binding, DynamicFilter.ALL,
                        constraint, SplitPlanningBudget.standard())
                .getNextBatch(8).stream().map(PaimonSplit.class::cast).toList();
    }

    private static void write(FileStoreTable table, GenericRow row) throws Exception {
        var builder = table.newBatchWriteBuilder();
        try (var writer = builder.newWrite(); var commit = builder.newCommit()) {
            writer.write(row);
            commit.commit(writer.prepareCommit());
        }
    }

    private static final class ReadSession implements ConnectorSession, ConnectorQueryAuthority {
        @Override public String queryId() { return "split-planning"; }
        @Override public boolean isClosed() { return false; }
        @Override public void requireDrained() { throw new AssertionError("Split planning cannot claim cleanup"); }
        @Override public ConnectorQueryOperation enterOperation() { return () -> { }; }
        @Override public String getUser() { return "audit-user"; }
        @Override public Optional<String> getCatalog() { return Optional.of("audit"); }
        @Override public Optional<String> getSchema() { return Optional.of("db"); }
        @Override public Map<String, String> getProperties() { return Map.of(); }
        @Override public Optional<ConnectorQueryAuthority> getQueryAuthority() { return Optional.of(this); }
    }

    private static final class CountingSnapshotIo extends LocalFileIO {
        private Path snapshotPath;
        private int snapshotOpens;
        private int snapshotExists;
        private boolean deleteAfterRead;

        @Override
        public boolean exists(Path path) throws IOException {
            if (path.equals(snapshotPath)) snapshotExists++;
            return super.exists(path);
        }

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            if (path.equals(snapshotPath)) snapshotOpens++;
            return super.newInputStream(path);
        }

        @Override
        public String readFileUtf8(Path path) throws IOException {
            String text = super.readFileUtf8(path);
            if (deleteAfterRead && path.equals(snapshotPath)) {
                deleteAfterRead = false;
                delete(path, false);
            }
            return text;
        }
    }
}
