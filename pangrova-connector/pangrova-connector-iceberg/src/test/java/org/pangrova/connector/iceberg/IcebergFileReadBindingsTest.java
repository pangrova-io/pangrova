package org.pangrova.connector.iceberg;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.DataFiles;
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
import org.pangrova.protocol.source.DataSplitProtoCodec;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.SystemTableSplitDetail;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.ConnectorTransactionHandle;
import org.pangrova.spi.connector.metadata.Constraint;
import org.pangrova.spi.connector.metadata.DynamicFilter;
import org.pangrova.spi.connector.data.type.RowType;
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
            assertThat(empty.typedColumns()).hasSize(10);
            assertThat(empty.typedColumns().stream().map(c -> c.name()).toList())
                    .containsExactly("content", "file_path", "file_format", "spec_id",
                            "record_count", "file_size_in_bytes", "column_sizes", "split_offsets",
                            "equality_ids", "sort_order_id");
            assertThat(metadata.getColumnHandles(SESSION, empty))
                    .containsKeys("column_sizes", "split_offsets", "equality_ids", "sort_order_id");
            assertThat(metadata.getColumnHandles(SESSION, empty)).doesNotContainKey("value_counts");
            metadata.validateColumnAccess(SESSION, empty,
                    List.of("content", "file_path", "file_format", "spec_id",
                            "record_count", "file_size_in_bytes", "column_sizes", "split_offsets",
                            "equality_ids", "sort_order_id"));
            metadata.validateColumnAccess(SESSION, empty, List.of("FILE_PATH"));
            assertThatThrownBy(() -> metadata.validateColumnAccess(SESSION, empty,
                    List.of("value_counts")))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("value_counts");

            var emptySource = manager.getSplits(ConnectorTransactionHandle.empty(), SESSION,
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
            var source = manager.getSplits(ConnectorTransactionHandle.empty(), SESSION,
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
    void entriesBindTheSamePinnedManifestGraphWithTheirOwnKind() throws Exception {
        try (var catalog = catalog()) {
            Table table = catalog.createTable(TableIdentifier.of("db", "lineage"), SCHEMA,
                    PartitionSpec.unpartitioned(), Map.of("format-version", "2"));
            var metadata = new IcebergMetadata(catalog, "bound");
            var manager = new IcebergSplitManager("bound", catalog, new IcebergExpressionToSubstrait());
            var base = metadata.getTableHandle(SESSION, new SchemaTableName("db", "lineage"));
            assertThat(metadata.getSystemTable(SESSION, base, "entries")).isEmpty();
            var empty = metadata.getTableHandle(SESSION,
                    new SchemaTableName("db", "lineage$entries"));
            var emptySource = manager.getSplits(ConnectorTransactionHandle.empty(), SESSION,
                    empty, DynamicFilter.ALL, Constraint.EMPTY);
            try {
                assertThat(emptySource.getNextBatch(10)).isEmpty();
            } finally {
                emptySource.close();
            }
            append(table);
            var selected = (IcebergFileTableHandle) metadata.getTableHandle(SESSION,
                    new SchemaTableName("db", "lineage$entries"));
            assertThat(selected.entries()).isTrue();
            assertThat(selected.typedColumns().stream().map(c -> c.name()).toList())
                    .containsExactly("status", "snapshot_id", "sequence_number",
                            "file_sequence_number", "data_file");
            metadata.validateColumnAccess(SESSION, selected, List.of("DATA_FILE"));
            var file = (RowType) selected.typedColumns().get(4).parameterizedType();
            assertThat(file.fields().stream().map(field -> field.name())).containsExactly(
                    "content", "file_path", "file_format", "spec_id", "record_count", "file_size_in_bytes",
                    "column_sizes", "split_offsets", "equality_ids", "sort_order_id", "first_row_id");
            assertThat(selected.typedColumns()).isEqualTo(
                    org.pangrova.connector.iceberg.type.IcebergTypeMapper.toPangrovaColumns(
                            org.apache.iceberg.MetadataTableUtils.createMetadataTableInstance(table,
                                    org.apache.iceberg.MetadataTableType.ENTRIES).schema().select(
                                            "status", "snapshot_id", "sequence_number", "file_sequence_number",
                                            "data_file.content", "data_file.file_path", "data_file.file_format",
                                            "data_file.spec_id", "data_file.record_count", "data_file.file_size_in_bytes",
                                            "data_file.column_sizes", "data_file.split_offsets", "data_file.equality_ids",
                                            "data_file.sort_order_id", "data_file.first_row_id")));
            assertThatThrownBy(() -> metadata.validateColumnAccess(SESSION, selected,
                    List.of("record_count"))).isInstanceOf(UnsupportedOperationException.class);
            var binding = metadata.bindRead(SESSION, selected);
            metadata.validateReadBinding(SESSION, binding);
            var fileSelection = metadata.getTableHandle(SESSION,
                    new SchemaTableName("db", "lineage$files"));
            assertThat(binding.sourceIdentity())
                    .isNotEqualTo(metadata.bindRead(SESSION, fileSelection).sourceIdentity());
            var source = manager.getSplits(ConnectorTransactionHandle.empty(), SESSION,
                    binding.handle(), DynamicFilter.ALL, Constraint.EMPTY);
            try {
                var splits = source.getNextBatch(10);
                assertThat(splits).hasSize(1);
                DataSplit split = DataSplit.parseFrom(splits.get(0).toSplitInfoProto());
                assertThat(split.getSourceId()).endsWith("$entries");
                assertThat(DataSplitProtoCodec.fileDetail(split).getSystemTable().getKind())
                        .isEqualTo(SystemTableSplitDetail.Kind.ICEBERG_ENTRIES);
            } finally {
                source.close();
            }
        }
    }

    @Test
    void partitionFieldIdentitySurvivesRenameAndSpecEvolution() throws Exception {
        try (var catalog = catalog()) {
            var spec = PartitionSpec.builderFor(SCHEMA).identity("id", "old_id").build();
            Table table = catalog.createTable(TableIdentifier.of("db", "partitioned"), SCHEMA,
                    spec, Map.of("format-version", "2"));
            table.newAppend().appendFile(DataFiles.builder(spec)
                    .withPath(table.location() + "/data/old.parquet")
                    .withFileSizeInBytes(100).withRecordCount(1)
                    .withPartitionPath("old_id=7").build()).commit();
            var metadata = new IcebergMetadata(catalog, "bound");
            var name = new SchemaTableName("db", "partitioned$files");
            var before = (IcebergFileTableHandle) metadata.getTableHandle(SESSION, name);
            var originalIdentity = metadata.bindRead(SESSION, before).sourceIdentity();
            table.updateSpec().renameField("old_id", "new_id")
                    .addField("id_bucket", org.apache.iceberg.expressions.Expressions.bucket("id", 16)).commit();
            var selected = (IcebergFileTableHandle) metadata.getTableHandle(SESSION, name);
            metadata.validateColumnAccess(SESSION, selected, List.of("PARTITION"));
            metadata.validateColumnAccess(SESSION, selected,
                    selected.typedColumns().stream().map(column -> column.name()).toList());
            assertThat(selected.manifests()).isEqualTo(before.manifests());
            assertThat(metadata.bindRead(SESSION, selected).sourceIdentity()).isNotEqualTo(originalIdentity);
            var partition = selected.typedColumns().stream().filter(c -> c.name().equals("partition"))
                    .findFirst().orElseThrow();
            assertThat(((RowType) partition.parameterizedType()).fields().stream().map(f -> f.name()))
                    .containsExactly("new_id", "id_bucket");
            var split = DataSplit.parseFrom(new IcebergFileTableSplit(
                    selected, selected.manifests().get(0)).toSplitInfoProto());
            var system = DataSplitProtoCodec.fileDetail(split).getSystemTable();
            assertThat(system.getIcebergChildSpecId()).isEqualTo(spec.specId());
            assertThat(system.getIcebergPartitionSpecsList()).hasSize(2);
            var original = system.getIcebergPartitionSpecs(0).getFields(0);
            var renamed = system.getIcebergPartitionSpecs(1).getFields(0);
            assertThat(original.getFieldId()).isEqualTo(spec.fields().get(0).fieldId());
            assertThat(renamed.getFieldId()).isEqualTo(original.getFieldId());
            assertThat(original.getName()).isEqualTo("new_id");
            assertThat(renamed.getName()).isEqualTo("new_id");
            IcebergFileReadBindings.validate(selected, table);
        }
    }

    @Test
    void concurrentSpecCommitCannotMixTheSelectedObjectGraphAndDeclaration() throws Exception {
        try (var catalog = catalog()) {
            var spec = PartitionSpec.builderFor(SCHEMA).identity("id", "old_id").build();
            Table table = catalog.createTable(TableIdentifier.of("db", "concurrent_spec"), SCHEMA,
                    spec, Map.of("format-version", "2"));
            table.newAppend().appendFile(DataFiles.builder(spec)
                    .withPath(table.location() + "/data/old.parquet").withFileSizeInBytes(100)
                    .withRecordCount(1).withPartitionPath("old_id=7").build()).commit();
            var operations = ((org.apache.iceberg.HasTableOperations) table).operations();
            var initial = operations.current();
            var changed = new java.util.concurrent.atomic.AtomicBoolean();
            var originalIo = table.io();
            var changingIo = new org.apache.iceberg.io.FileIO() {
                @Override public org.apache.iceberg.io.InputFile newInputFile(String location) {
                    if (changed.compareAndSet(false, true)) {
                        table.updateSpec().renameField("old_id", "new_id").commit();
                    }
                    return originalIo.newInputFile(location);
                }
                @Override public org.apache.iceberg.io.OutputFile newOutputFile(String location) {
                    return originalIo.newOutputFile(location);
                }
                @Override public void deleteFile(String location) { originalIo.deleteFile(location); }
            };
            var mutable = new org.apache.iceberg.BaseTable(
                    new org.apache.iceberg.StaticTableOperations(initial, changingIo) {
                        @Override public org.apache.iceberg.TableMetadata current() { return operations.current(); }
                    }, table.name());
            var base = org.pangrova.connector.iceberg.handle.IcebergTableHandleFactory
                    .fromTable("db", "concurrent_spec", table);
            var selected = IcebergFileReadBindings.selectEntries(base, mutable);
            assertThat(changed).isTrue();
            assertThat(table.spec().fields().get(0).name()).isEqualTo("new_id");
            assertThat(selected.manifestSelection().metadataLocation()).isEqualTo(initial.metadataFileLocation());
            var file = (RowType) selected.typedColumns().get(4).parameterizedType();
            var partition = (RowType) file.fields().stream().filter(field -> field.name().equals("partition"))
                    .findFirst().orElseThrow().type();
            assertThat(partition.fields().stream().map(field -> field.name())).containsExactly("old_id");
            IcebergFileReadBindings.validate(selected, table);
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
