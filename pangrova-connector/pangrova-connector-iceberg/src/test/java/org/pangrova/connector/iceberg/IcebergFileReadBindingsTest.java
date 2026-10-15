package org.pangrova.connector.iceberg;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
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
import org.pangrova.connector.iceberg.handle.IcebergFileTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergFileTableSplit;
import org.pangrova.connector.iceberg.read.IcebergExpressionToSubstrait;
import org.pangrova.protocol.DataSplitProtoCodec;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.SystemTableSplitDetail;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.ConnectorTransactionHandle;
import org.pangrova.spi.connector.metadata.Constraint;
import org.pangrova.spi.connector.metadata.DynamicFilter;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IcebergFileReadBindingsTest {
    @TempDir Path warehouse;

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()));

    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override public java.util.Optional<org.pangrova.spi.connector.ConnectorQueryAuthority>
                getQueryAuthority() { return java.util.Optional.empty(); }
        @Override public String getUser() { return "files-test"; }
        @Override public java.util.Optional<String> getCatalog() { return java.util.Optional.of("bound"); }
        @Override public java.util.Optional<String> getSchema() { return java.util.Optional.of("db"); }
        @Override public Map<String, String> getProperties() { return Map.of(); }
    };

    @Test
    void bindsSdkSchemaAndPinnedChildSplit() throws Exception {
        try (var catalog = catalog()) {
            Table table = catalog.createTable(TableIdentifier.of("db", "routed"), SCHEMA,
                    PartitionSpec.unpartitioned(), Map.of("format-version", "2"));
            var metadata = new IcebergMetadata(catalog, "bound");
            var manager = new IcebergSplitManager("bound", catalog, new IcebergExpressionToSubstrait());
            var name = new SchemaTableName("db", "routed$files");
            var empty = (IcebergFileTableHandle) metadata.getTableHandle(SESSION, name);
            assertThat(empty.manifests()).isEmpty();
            assertThat(empty.typedColumns()).hasSize(9);
            assertThat(empty.typedColumns().stream().map(c -> c.name()).toList())
                    .containsExactly("content", "file_path", "file_format", "spec_id",
                            "record_count", "file_size_in_bytes", "split_offsets",
                            "equality_ids", "sort_order_id");
            assertThat(metadata.getColumnHandles(SESSION, empty))
                    .containsKeys("split_offsets", "equality_ids", "sort_order_id");
            assertThat(metadata.getColumnHandles(SESSION, empty)).doesNotContainKey("column_sizes");
            metadata.validateColumnAccess(SESSION, empty,
                    List.of("content", "file_path", "file_format", "spec_id",
                            "record_count", "file_size_in_bytes", "split_offsets",
                            "equality_ids", "sort_order_id"));
            metadata.validateColumnAccess(SESSION, empty, List.of("FILE_PATH"));
            assertThatThrownBy(() -> metadata.validateColumnAccess(SESSION, empty,
                    List.of("column_sizes")))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("column_sizes");

            var emptySource = manager.getSplits(ConnectorTransactionHandle.EMPTY, SESSION,
                    empty, DynamicFilter.ALL, Constraint.EMPTY);
            try {
                assertThat(emptySource.getNextBatch(10)).isEmpty();
            } finally {
                emptySource.close();
            }

            append(table);
            var selected = (IcebergFileTableHandle) metadata.getTableHandle(SESSION, name);
            assertThat(selected.manifests()).hasSize(1);
            var binding = metadata.bindRead(SESSION, selected);
            metadata.validateReadBinding(SESSION, binding);
            var source = manager.getSplits(ConnectorTransactionHandle.EMPTY, SESSION,
                    binding.handle(), DynamicFilter.ALL, Constraint.EMPTY);
            try {
                var splits = source.getNextBatch(10);
                assertThat(splits).singleElement().isInstanceOf(IcebergFileTableSplit.class);
                DataSplit split = DataSplit.parseFrom(splits.get(0).toSplitInfoProto());
                assertThat(split.getSourceSnapshot())
                        .isEqualTo(selected.manifestSelection().manifestListLocation());
                assertThat(split.getDataObjectId()).isEqualTo(selected.manifests().get(0).uri());
                assertThat(split.getOpaqueLocation()).isEqualTo(selected.manifests().get(0).uri());
                assertThat(split.getLength()).isEqualTo(selected.manifests().get(0).length());
                assertThat(DataSplitProtoCodec.fileDetail(split).getSystemTable().getKind())
                        .isEqualTo(SystemTableSplitDetail.Kind.ICEBERG_FILES);
                assertThat(DataSplitProtoCodec.fileDetail(split).getSystemTable()
                        .getObjectSha256().size()).isEqualTo(32);
                assertThat(DataSplitProtoCodec.fileDetail(split).getSystemTable()
                        .getIcebergManifestListSha256().size()).isEqualTo(32);
                assertThat(DataSplitProtoCodec.fileDetail(split).getSystemTable()
                        .getIcebergManifestListLength())
                        .isEqualTo(selected.manifestSelection().manifestListLength());
            } finally {
                source.close();
            }
        }
    }

    @Test
    void rejectsSameLengthChildManifestMutation() throws Exception {
        try (var catalog = catalog()) {
            Table table = catalog.createTable(TableIdentifier.of("db", "tamper"), SCHEMA,
                    PartitionSpec.unpartitioned(), Map.of("format-version", "2"));
            append(table);
            var selected = IcebergFileReadBindings.select(
                    org.pangrova.connector.iceberg.handle.IcebergTableHandleFactory
                            .fromTable("db", "tamper", table), table);
            IcebergFileReadBindings.validate(selected, table);
            URI childUri = URI.create(selected.manifests().get(0).uri());
            Path child = childUri.getScheme() == null
                    ? Path.of(selected.manifests().get(0).uri()) : Path.of(childUri);
            byte[] bytes = Files.readAllBytes(child);
            bytes[bytes.length - 1] ^= 1;
            Files.write(child, bytes);
            Files.deleteIfExists(child.resolveSibling("." + child.getFileName() + ".crc"));
            assertThatThrownBy(() -> IcebergFileReadBindings.validate(selected, table))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("child manifests changed");
        }
    }

    @Test
    void rejectsVersionOneWithoutVerifiedManifestList() throws Exception {
        try (var catalog = catalog()) {
            Table table = catalog.createTable(TableIdentifier.of("db", "version_one"), SCHEMA,
                    PartitionSpec.unpartitioned(), Map.of("format-version", "1"));
            assertThatThrownBy(() -> IcebergFileReadBindings.select(
                    org.pangrova.connector.iceberg.handle.IcebergTableHandleFactory
                            .fromTable("db", "version_one", table), table))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("v2/v3 manifest list");
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
