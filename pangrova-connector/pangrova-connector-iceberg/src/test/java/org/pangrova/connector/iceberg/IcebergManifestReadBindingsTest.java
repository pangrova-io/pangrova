package org.pangrova.connector.iceberg;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;

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
import org.pangrova.connector.iceberg.handle.IcebergManifestTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergManifestTableSplit;
import org.pangrova.connector.iceberg.handle.IcebergTableHandleFactory;
import org.pangrova.connector.iceberg.read.IcebergExpressionToSubstrait;
import org.pangrova.protocol.DataSplitProtoCodec;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.SystemTableSplitDetail;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.ConnectorTransactionHandle;
import org.pangrova.spi.connector.data.type.ArrayType;
import org.pangrova.spi.connector.metadata.Constraint;
import org.pangrova.spi.connector.metadata.DynamicFilter;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exact SDK manifest-list object binding; no Java metadata rows are generated. */
class IcebergManifestReadBindingsTest {
    @TempDir Path warehouse;

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()));

    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override public java.util.Optional<org.pangrova.spi.connector.ConnectorQueryAuthority>
                getQueryAuthority() { return java.util.Optional.empty(); }
        @Override public String getUser() { return "manifest-test"; }
        @Override public java.util.Optional<String> getCatalog() { return java.util.Optional.of("bound"); }
        @Override public java.util.Optional<String> getSchema() { return java.util.Optional.of("db"); }
        @Override public Map<String, String> getProperties() { return Map.of(); }
    };

    @Test
    void routesSqlHandleThroughPinnedTypedSplit() throws Exception {
        try (var catalog = catalog()) {
            Table table = catalog.createTable(TableIdentifier.of("db", "routed"), SCHEMA,
                    PartitionSpec.unpartitioned(), Map.of("format-version", "2"));
            var metadata = new IcebergMetadata(catalog, "bound");
            var manager = new IcebergSplitManager("bound", catalog, new IcebergExpressionToSubstrait());
            var name = new SchemaTableName("db", "routed$manifests");
            var empty = (IcebergManifestTableHandle) metadata.getTableHandle(SESSION, name);
            assertThat(empty.empty()).isTrue();
            var emptyBinding = metadata.bindRead(SESSION, empty);
            metadata.validateReadBinding(SESSION, emptyBinding);
            var emptySource = manager.getSplits(ConnectorTransactionHandle.EMPTY, SESSION,
                    emptyBinding.handle(), DynamicFilter.ALL, Constraint.EMPTY);
            try {
                assertThat(emptySource.getNextBatch(10)).isEmpty();
            } finally {
                emptySource.close();
            }

            append(table);
            var selected = (IcebergManifestTableHandle) metadata.getTableHandle(SESSION, name);
            assertThat(metadata.getTableMetadata(SESSION, selected).typedColumns())
                    .containsExactlyElementsOf(selected.typedColumns());
            var binding = metadata.bindRead(SESSION, selected);
            metadata.validateReadBinding(SESSION, binding);
            var source = manager.getSplits(ConnectorTransactionHandle.EMPTY, SESSION,
                    binding.handle(), DynamicFilter.ALL, Constraint.EMPTY);
            try {
                var splits = source.getNextBatch(10);
                assertThat(splits).singleElement().isInstanceOf(IcebergManifestTableSplit.class);
                DataSplit split = DataSplit.parseFrom(splits.get(0).toSplitInfoProto());
                assertThat(split.getOpaqueLocation()).isEqualTo(selected.manifestListLocation());
                assertThat(split.getLength()).isEqualTo(selected.manifestListLength());
                var detail = DataSplitProtoCodec.fileDetail(split);
                assertThat(detail.getFileFormat()).isEqualTo(FileFormat.FILE_FORMAT_METADATA_AVRO);
                assertThat(detail.getSystemTable().getKind())
                        .isEqualTo(SystemTableSplitDetail.Kind.ICEBERG_MANIFESTS);
                assertThat(detail.getSystemTable().getSnapshotId())
                        .isEqualTo(selected.snapshotId().orElseThrow());
                assertThat(detail.getSystemTable().getObjectSha256().size()).isEqualTo(32);
                assertThat(detail.getSystemTable().getIcebergPartitionSpecsCount()).isPositive();
            } finally {
                source.close();
            }
        }
    }

    @Test
    void pinsSdkSchemaAndDetectsManifestListMutation() throws Exception {
        try (var catalog = catalog()) {
            Table table = catalog.createTable(TableIdentifier.of("db", "items"), SCHEMA,
                    PartitionSpec.unpartitioned(), Map.of("format-version", "2"));
            var empty = IcebergManifestReadBindings.select(
                    IcebergTableHandleFactory.fromTable("db", "items", table), table);
            assertThat(empty.empty()).isTrue();
            assertThat(empty.typedColumns()).hasSize(12);
            assertThat(empty.typedColumns().get(11).name()).isEqualTo("partition_summaries");
            assertThat(empty.typedColumns().get(11).parameterizedType()).isInstanceOf(ArrayType.class);

            append(table);
            var selected = IcebergManifestReadBindings.select(
                    IcebergTableHandleFactory.fromTable("db", "items", table), table);
            assertThat(selected.empty()).isFalse();
            assertThat(selected.manifestListSha256()).matches("[0-9a-f]{64}");
            assertThat(selected.partitionSummarySpecs()).containsKey(0);
            IcebergManifestReadBindings.validate(selected, table);

            append(table);
            IcebergManifestReadBindings.validate(selected, table);
            URI manifestUri = URI.create(selected.manifestListLocation());
            Path manifestList = manifestUri.getScheme() == null
                    ? Path.of(selected.manifestListLocation()) : Path.of(manifestUri);
            byte[] bytes = Files.readAllBytes(manifestList);
            bytes[bytes.length - 1] ^= 1;
            Files.write(manifestList, bytes);
            Files.deleteIfExists(manifestList.resolveSibling("." + manifestList.getFileName() + ".crc"));
            assertThatThrownBy(() -> IcebergManifestReadBindings.validate(selected, table))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("bytes changed");
        }
    }

    @Test
    void rejectsLegacyFormatWithoutNativeReader() throws Exception {
        try (var catalog = catalog()) {
            Table table = catalog.createTable(TableIdentifier.of("db", "legacy"), SCHEMA,
                    PartitionSpec.unpartitioned(), Map.of("format-version", "1"));
            assertThatThrownBy(() -> IcebergManifestReadBindings.select(
                    IcebergTableHandleFactory.fromTable("db", "legacy", table), table))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("v2/v3");
        }
    }

    private HadoopCatalog catalog() {
        var catalog = new HadoopCatalog(new Configuration(), warehouse.toString());
        catalog.createNamespace(Namespace.of("db"));
        return catalog;
    }

    private static void append(Table table) throws Exception {
        var writer = Parquet.<Record>writeData(table.io().newOutputFile(
                        table.location() + "/data/" + UUID.randomUUID() + ".parquet"))
                .schema(table.schema()).createWriterFunc(GenericParquetWriter::create)
                .withSpec(PartitionSpec.unpartitioned()).build();
        try (writer) {
            var record = GenericRecord.create(table.schema());
            record.setField("id", 1L);
            writer.write(record);
        }
        table.newAppend().appendFile(writer.toDataFile()).commit();
        table.refresh();
    }
}
