package org.pangrova.connector.iceberg;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.connector.iceberg.handle.IcebergOutputHandleCodec;
import org.pangrova.connector.iceberg.handle.IcebergOutputTableHandle;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.base.ConnectorException;
import org.pangrova.spi.connector.metadata.DistributedWriteCommitContext;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.coordination.WriteCommitFingerprint;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.ContentFileParser;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual SDK Parquet, scoped snapshot commit and serialized recovery; not a Native/authentication E2E. */
class IcebergStaticOverwriteScopeTest {
    private static final String CATALOG = "scope_catalog";
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "day", Types.StringType.get()),
            Types.NestedField.required(3, "region", Types.StringType.get()),
            Types.NestedField.optional(4, "payload", Types.StringType.get()));
    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override
        public java.util.Optional<org.pangrova.spi.connector.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

        @Override public String getUser() { return "scope-user"; }
        @Override public Optional<String> getCatalog() { return Optional.of(CATALOG); }
        @Override public Optional<String> getSchema() { return Optional.of("db"); }
        @Override public Map<String, String> getProperties() { return Map.of("tenant_id", "scope-tenant"); }
    };

    @TempDir Path warehouse;
    private HadoopCatalog catalog;
    private IcebergMetadata metadata;
    private Table table;
    private final IcebergOutputHandleCodec codec = new IcebergOutputHandleCodec();

    @BeforeEach
    void createActualPartitionedTable() throws IOException {
        catalog = new HadoopCatalog(new Configuration(), warehouse.toString());
        catalog.createNamespace(Namespace.of("db"));
        table = catalog.createTable(TableIdentifier.of("db", "events"), SCHEMA,
                PartitionSpec.builderFor(SCHEMA).identity("day").identity("region").build());
        metadata = new IcebergMetadata(catalog, CATALOG);
        table.newAppend().appendFile(writeFile(101, "a", "east"))
                .appendFile(writeFile(205, "a", "west"))
                .appendFile(writeFile(309, "b", "east")).commit();
    }

    @AfterEach
    void closeCatalog() throws IOException {
        if (catalog != null) {
            catalog.close();
        }
    }

    @Test
    void emptyStaticOverwriteRetainsOtherPartitionsAndReplayIsIdempotent() throws IOException {
        var output = roundTrip(begin(Map.of("day", "a")));
        var context = context(output, List.of());
        metadata.finishDistributedWrite(SESSION, output, List.of(), context);
        assertThat(readRows()).containsExactly("309:b:east:null");
        table.newAppend().appendFile(writeFile(413, "b", "west")).commit();
        long latest = table.currentSnapshot().snapshotId();

        metadata.finishDistributedWrite(SESSION, roundTrip(output), List.of(), context);

        table.refresh();
        assertThat(table.currentSnapshot().snapshotId()).isEqualTo(latest);
        assertThat(readRows()).containsExactlyInAnyOrder("309:b:east:null", "413:b:west:null");
        assertThat(output.staticPartitions()).containsExactlyEntriesOf(Map.of("day", "a"));
    }

    @Test
    void partialStaticScopeClearsAllMatchingPartitionsNotOnlyNewFilePartition() throws IOException {
        var output = begin(Map.of("day", "a"));
        List<byte[]> fragments = fragments(writeFile(517, "a", "east"));
        metadata.finishDistributedWrite(SESSION, roundTrip(output), fragments, context(output, fragments));
        assertThat(readRows()).containsExactlyInAnyOrder("309:b:east:null", "517:a:east:null");
    }

    @Test
    void addedFileOutsideImmutableScopeFailsWithoutDeletingOriginalData() throws IOException {
        var output = begin(Map.of("day", "a"));
        List<byte[]> fragments = fragments(writeFile(517, "b", "west"));
        assertThatThrownBy(() -> metadata.finishDistributedWrite(SESSION, output, fragments))
                .hasMessageContaining("Cannot append file with rows that do not match filter");
        assertOriginalRows();
    }

    @Test
    void changedSchemaOrSpecDoesNotRebindStaticScopeToNewMetadata() throws IOException {
        var output = begin(Map.of("day", "a"));
        table.updateSchema().renameColumn("day", "event_day").commit();
        assertThatThrownBy(() -> metadata.finishDistributedWrite(SESSION, output, List.of()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("changed since allocation");
        assertThat(table.currentSnapshot().summary()).containsEntry("total-records", "3");
    }

    @Test
    void nonPartitionColumnsUnknownColumnsAndTransformsRejectBeforeAllocation() {
        for (String column : List.of("id", "missing")) {
            assertThatThrownBy(() -> begin(Map.of(column, "1")))
                    .isInstanceOfSatisfying(ConnectorException.class,
                            error -> assertThat(error.getErrorCode())
                                    .isEqualTo(PangrovaErrorCode.CONNECTOR_UNSUPPORTED_OPERATION))
                    .hasMessageContaining("identity partition column");
        }
        var transformed = catalog.createTable(TableIdentifier.of("db", "buckets"), SCHEMA,
                PartitionSpec.builderFor(SCHEMA).bucket("id", 8).build());
        var handle = metadata.getTableHandle(SESSION, new SchemaTableName("db", "buckets"));
        assertThat(handle).isNotNull();
        assertThatThrownBy(() -> metadata.beginOverwrite(SESSION, handle, Map.of("id", "1")))
                .isInstanceOfSatisfying(ConnectorException.class,
                        error -> assertThat(error.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.CONNECTOR_UNSUPPORTED_OPERATION))
                .hasMessageContaining("identity partition column");
        assertThat(transformed.currentSnapshot()).isNull();
    }

    @Test
    void newestCodecIsDeterministicScopeSensitiveAndRejectsPriorVersionAndTrailingData() {
        var left = begin(Map.of("region", "east", "day", "a"));
        var right = begin(Map.of("day", "a", "region", "east"));
        assertThat(codec.serialize(left)).isEqualTo(codec.serialize(right));
        assertThat(roundTrip(left)).isEqualTo(left);
        assertThat(codec.serialize(left)).isNotEqualTo(codec.serialize(begin(Map.of("day", "b"))));
        assertThatThrownBy(() -> left.staticPartitions().put("day", "b"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> codec.deserialize(2, codec.serialize(left)))
                .hasMessageContaining("Unsupported Iceberg output handle version");
        byte[] trailing = Arrays.copyOf(codec.serialize(left), codec.serialize(left).length + 1);
        assertThatThrownBy(() -> codec.deserialize(codec.codecVersion(), trailing))
                .hasMessageContaining("Trailing bytes");
    }

    @Test
    void typedIdentityScopeRejectsInvalidBooleanRatherThanParsingItAsFalse() {
        Schema typed = new Schema(Types.NestedField.required(1, "active", Types.BooleanType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(typed).identity("active").build();
        assertThatThrownBy(() -> IcebergStaticOverwriteScope.filter(typed, spec, Map.of("active", "garbage")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("boolean partition value");
        assertThat(IcebergStaticOverwriteScope.filter(typed, spec, Map.of("active", "false"))).isNotNull();
    }

    private IcebergOutputTableHandle begin(Map<String, String> scope) {
        var handle = metadata.getTableHandle(SESSION, new SchemaTableName("db", "events"));
        assertThat(handle).isNotNull();
        return (IcebergOutputTableHandle) metadata.beginOverwrite(SESSION, handle, scope);
    }

    private IcebergOutputTableHandle roundTrip(IcebergOutputTableHandle output) {
        return (IcebergOutputTableHandle) codec.deserialize(codec.codecVersion(), codec.serialize(output));
    }

    private DataFile writeFile(long id, String day, String region) throws IOException {
        String path = table.location() + "/data/row-" + id + ".parquet";
        PartitionData partition = new PartitionData(table.spec().partitionType());
        partition.set(0, day);
        partition.set(1, region);
        DataWriter<Record> writer = Parquet.writeData(table.io().newOutputFile(path)).schema(SCHEMA)
                .createWriterFunc(GenericParquetWriter::create).withSpec(table.spec())
                .withPartition(partition).build();
        try (writer) {
            GenericRecord record = GenericRecord.create(SCHEMA);
            record.setField("id", id);
            record.setField("day", day);
            record.setField("region", region);
            writer.write(record);
        }
        return writer.toDataFile();
    }

    private List<String> readRows() throws IOException {
        table.refresh();
        List<String> rows = new ArrayList<>();
        try (var tasks = table.newScan().planFiles()) {
            for (var task : tasks) {
                try (CloseableIterable<Record> records = Parquet.read(table.io().newInputFile(task.file().location()))
                        .project(SCHEMA)
                        .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(SCHEMA, fileSchema)).build()) {
                    for (Record record : records) {
                        rows.add(record.getField("id") + ":" + record.getField("day") + ":"
                                + record.getField("region") + ":" + record.getField("payload"));
                    }
                }
            }
        }
        return rows;
    }

    private void assertOriginalRows() throws IOException {
        assertThat(readRows()).containsExactlyInAnyOrder("101:a:east:null", "205:a:west:null", "309:b:east:null");
    }

    private List<byte[]> fragments(DataFile file) {
        String json = "{\"operation\":\"OVERWRITE\",\"dataFiles\":["
                + ContentFileParser.toJson(file, table.spec()) + "]}";
        return List.of(json.getBytes(StandardCharsets.UTF_8));
    }

    private DistributedWriteCommitContext context(IcebergOutputTableHandle output, List<byte[]> fragments) {
        byte[] binding = IcebergWriteBindingFixtures.binding(output);
        var receipt = IcebergWriteReceiptFixtures.receipt();
        String fingerprint = WriteCommitFingerprint.compute(new WriteCommitFingerprint.Intent(
                "scope-replay", "scope-tenant", CATALOG, "db.events", output.writeMode(),
                output.baseSnapshotId(), binding, SESSION.getUser(), receipt), fragments);
        return new DistributedWriteCommitContext("scope-replay", "scope-tenant", output.baseSnapshotId(),
                binding, fingerprint, SESSION.getUser(), receipt);
    }
}
