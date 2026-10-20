package org.pangrova.connector.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.iceberg.read.IcebergExpressionToSubstrait;
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
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real SDK commits and physical Parquet files; not a SQL/JNI execution proof. */
class IcebergReadBindingIntegrationTest {
    @TempDir Path warehouse;
    private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
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
    void statisticsAndWireSplitsRetainTheSelectedCommitAfterAnotherAppend() throws Exception {
        try (var catalog = catalog()) {
            Table table = create(catalog, "rows");
            write(table, 3);
            var metadata = new IcebergMetadata(catalog, "bound");
            var binding = bind(metadata, "rows");
            long selected = table.currentSnapshot().snapshotId();
            write(table, 5);
            metadata.validateReadBinding(SESSION, binding);
            assertThat(binding.statistics().rowCount()).isEqualTo(3);
            assertThat(binding.statistics().dataSize()).isPositive();
            assertThat(binding.statistics().sourceWorkUnits()).isEqualTo(1);
            assertThat(binding.statistics().columnStats()).containsKey("id");
            assertThat(binding.statistics().columnStats().get("id").avgSize()).isEqualTo(-1.0);
            assertThat(binding.snapshotId().orElseThrow()).isEqualTo(selected);
            var splits = splits(catalog, binding);
            assertThat(splits).hasSize(1);
            assertThat(splits).allSatisfy(split -> assertThat(split.getSourceSnapshot()).isEqualTo(Long.toString(selected)));
            assertThat(bind(metadata, "rows").statistics().rowCount()).isEqualTo(8);
            assertThat(bind(metadata, "rows").statistics().sourceWorkUnits()).isEqualTo(2);
        }
    }

    @Test
    void ordinaryReadKeepsCurrentSchemaWhereasExplicitVersionUsesHistoricalSchema() throws Exception {
        try (var catalog = catalog()) {
            Table table = create(catalog, "schema");
            write(table, 2);
            long selected = table.currentSnapshot().snapshotId();
            table.updateSchema().addColumn("added", Types.StringType.get()).commit();
            var metadata = new IcebergMetadata(catalog, "bound");
            var current = bind(metadata, "schema");
            assertThat(current.snapshotId().orElseThrow()).isEqualTo(selected);
            assertThat(current.declaration().metadata().columns()).containsExactly("id", "added");
            var selectedHandle = (org.pangrova.connector.iceberg.handle.IcebergTableHandle) current.handle();
            String identityKey = org.pangrova.connector.iceberg.handle.IcebergTableHandle.SOURCE_IDENTITY_PROPERTY;
            assertThat(selectedHandle.withLimit(1).properties()).containsKey(identityKey);
            assertThat(selectedHandle.withSnapshotId(selected).properties()).doesNotContainKey(identityKey);
            assertThat(selectedHandle.withoutSnapshotId().properties()).doesNotContainKey(identityKey);
            assertThat(selectedHandle.withIncrementalRead(selected, selected).properties()).doesNotContainKey(identityKey);
            var oldHandle = metadata.getTableHandle(SESSION, new SchemaTableName("db", "schema"))
                    .withTimeTravelSnapshot(selected);
            var historical = metadata.bindRead(SESSION, oldHandle);
            assertThat(historical.declaration().requireExactDeclaration().columns()).containsExactly("id");
            assertThat(splits(catalog, current)).hasSize(1);
            assertThat(splits(catalog, historical)).hasSize(1);
        }
    }

    @Test
    void incrementalRangeDoesNotBorrowEndSnapshotTableStatistics() throws Exception {
        try (var catalog = catalog()) {
            Table table = create(catalog, "incremental");
            write(table, 3);
            long first = table.currentSnapshot().snapshotId();
            write(table, 5);
            long second = table.currentSnapshot().snapshotId();
            var metadata = new IcebergMetadata(catalog, "bound");
            var selector = (org.pangrova.connector.iceberg.handle.IcebergTableHandle)
                    metadata.getTableHandle(SESSION, new SchemaTableName("db", "incremental"));
            var interval = selector.withIncrementalRead(first, second);
            var binding = metadata.bindRead(SESSION, interval);

            assertThat(binding.statistics().rowCount()).isEqualTo(-1);
            assertThat(binding.statistics().dataSize()).isEqualTo(-1);
            assertThat(binding.statistics().sourceWorkUnits()).isEqualTo(-1);
            assertThat(metadata.getTableStatistics(SESSION, interval)).isSameAs(
                    org.pangrova.spi.connector.metadata.table.TableStatistics.UNKNOWN);
            assertThat(splits(catalog, binding)).hasSize(1);
            assertThat(bind(metadata, "incremental").statistics().sourceWorkUnits()).isEqualTo(2);
        }
    }

    @Test
    void emptyBindingStaysEmptyAcrossTheFirstCommitAndRejectsReplacement() throws Exception {
        try (var catalog = catalog()) {
            Table table = create(catalog, "empty");
            var metadata = new IcebergMetadata(catalog, "bound");
            var empty = bind(metadata, "empty");
            assertThat(empty.state()).isEqualTo(ConnectorReadBinding.SnapshotState.EMPTY);
            assertThat(empty.snapshotId()).isEmpty();
            assertThat(empty.statistics().rowCount()).isZero();
            assertThat(empty.statistics().sourceWorkUnits()).isZero();
            write(table, 2);
            metadata.validateReadBinding(SESSION, empty);
            // EMPTY is consumed without invoking a live SDK scan by SplitFetcher.
            assertThat(empty.statistics().rowCount()).isZero();
            catalog.dropTable(TableIdentifier.of("db", "empty"), false);
            create(catalog, "empty");
            assertThatThrownBy(() -> metadata.validateReadBinding(SESSION, empty))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("identity changed");
        }
    }

    @Test
    void expiredSelectedSnapshotFailsInsteadOfReadingTheLatest() throws Exception {
        try (var catalog = catalog()) {
            Table table = create(catalog, "expired");
            write(table, 2);
            var metadata = new IcebergMetadata(catalog, "bound");
            var selected = bind(metadata, "expired");
            write(table, 4);
            table.expireSnapshots().expireSnapshotId(selected.snapshotId().orElseThrow()).commit();
            assertThatThrownBy(() -> metadata.validateReadBinding(SESSION, selected))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("unavailable");
            assertThatThrownBy(() -> splits(catalog, selected))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("unavailable");
        }
    }

    @Test
    void refsBindingPinsMetadataObjectAcrossReferenceChanges() throws Exception {
        try (var catalog = catalog()) {
            Table table = create(catalog, "reference_rows");
            write(table, 2);
            long snapshot = table.currentSnapshot().snapshotId();
            table.manageSnapshots().createTag("release", snapshot).commit();
            var metadata = new IcebergMetadata(catalog, "bound");
            var selected = bind(metadata, "reference_rows$refs");
            assertThat(selected.state()).isEqualTo(ConnectorReadBinding.SnapshotState.METADATA);
            assertThat(selected.declaration().requireExactDeclaration().columns()).containsExactly(
                    "name", "type", "snapshot_id", "max_reference_age_in_ms",
                    "min_snapshots_to_keep", "max_snapshot_age_in_ms");
            var original = splits(catalog, selected).get(0);
            var detail = original.getConnectorDetail().unpack(org.pangrova.protocol.v2.FileSplitDetail.class);
            assertThat(detail.getSystemTable().getKind())
                    .isEqualTo(org.pangrova.protocol.v2.SystemTableSplitDetail.Kind.ICEBERG_REFS);
            assertThat(original.getSourceId()).endsWith("reference_rows$refs");
            assertThat(original.getSourceSnapshot()).isEqualTo(selected.sourceIdentity());
            table.manageSnapshots().removeTag("release").createBranch("audit", snapshot).commit();
            metadata.validateReadBinding(SESSION, selected);
            assertThat(splits(catalog, selected).get(0)).isEqualTo(original);
            assertThat(bind(metadata, "reference_rows$refs").sourceIdentity())
                    .isNotEqualTo(selected.sourceIdentity());
        }
    }

    private HadoopCatalog catalog() {
        var catalog = new HadoopCatalog(new Configuration(), warehouse.toString());
        catalog.createNamespace(Namespace.of("db"));
        return catalog;
    }

    private static Table create(HadoopCatalog catalog, String name) {
        return catalog.createTable(TableIdentifier.of("db", name), SCHEMA, PartitionSpec.unpartitioned(),
                Map.of("format-version", "2", "write.metadata.metrics.default", "full"));
    }

    private static ConnectorReadBinding bind(IcebergMetadata metadata, String name) {
        return metadata.bindRead(SESSION, metadata.getTableHandle(SESSION, new SchemaTableName("db", name)));
    }

    private static void write(Table table, int rows) throws Exception {
        var writer = Parquet.<Record>writeData(table.io().newOutputFile(table.location() + "/data/" + UUID.randomUUID() + ".parquet"))
                .schema(table.schema()).createWriterFunc(GenericParquetWriter::create)
                .withSpec(PartitionSpec.unpartitioned()).build();
        try (writer) {
            for (int row = 0; row < rows; row++) {
                var record = GenericRecord.create(table.schema());
                record.setField("id", (long) row);
                writer.write(record);
            }
        }
        table.newAppend().appendFile(writer.toDataFile()).commit();
        table.refresh();
    }

    private static List<org.pangrova.protocol.v2.DataSplit> splits(HadoopCatalog catalog,
            ConnectorReadBinding binding) throws Exception {
        var manager = new IcebergSplitManager("bound", catalog, new IcebergExpressionToSubstrait());
        var source = manager.getSplits(ConnectorTransactionHandle.EMPTY, SESSION, binding.handle(),
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
        }
        return result;
    }
}
