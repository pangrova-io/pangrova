package org.pangrova.connector.iceberg;

import org.pangrova.types.enums.VectorElementType;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ContentFileParser;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.TypeUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.connector.iceberg.handle.IcebergOutputTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergTableHandleFactory;
import org.pangrova.connector.iceberg.schema.IcebergSchemaBuilder;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.generation.codec.column.GeneratedColumnMetadataCodec;
import org.pangrova.protocol.v2.ExecutionMode;
import org.pangrova.protocol.v2.GeneratedColumnDefinition;
import org.pangrova.protocol.v2.GeneratedColumnSet;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.metadata.DeleteFilter;
import org.pangrova.types.ArrayType;
import org.pangrova.types.MapType;
import org.pangrova.types.enums.PangrovaType;
import org.pangrova.types.RowField;
import org.pangrova.types.RowType;
import org.pangrova.types.VectorType;
import org.pangrova.types.primitive.BigIntType;
import org.pangrova.types.primitive.VarcharType;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real PostgreSQL pointer CAS: data-only rebases succeed, declaration-changing rebases do not. */
class IcebergJdbcWriteDeclarationIntegrationTest extends ContainerTestBase {
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "title", Types.StringType.get()),
            Types.NestedField.optional(3, "embedding", Types.ListType.ofRequired(4, Types.FloatType.get())));
    private static final TableIdentifier ID = TableIdentifier.of("db", "events");
    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override
        public java.util.Optional<org.pangrova.connector.api.query.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

        @Override public String getUser() { return "writer"; }
        @Override public Optional<String> getCatalog() { return Optional.of("jdbc"); }
        @Override public Optional<String> getSchema() { return Optional.of("db"); }
        @Override public Map<String, String> getProperties() { return Map.of("tenant_id", "tenant"); }
    };

    @TempDir Path warehouse;
    private JdbcCatalog first;
    private JdbcCatalog second;
    private Table table;
    private IcebergMetadata metadata;

    @BeforeEach
    void createIndependentSdkCatalogs() {
        String name = "declaration_" + UUID.randomUUID().toString().replace("-", "");
        first = catalog(name);
        second = catalog(name);
        first.createNamespace(Namespace.of("db"));
        table = first.createTable(ID, SCHEMA);
        metadata = new IcebergMetadata(first, name);
    }

    @AfterEach
    void closeCatalogs() throws IOException {
        try {
            if (second != null) second.close();
        } finally {
            if (first != null) first.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void createdNestedSchemaUsesSdkCanonicalIdsBeforeAllocation(boolean jdbc) throws Exception {
        try (var local = new HadoopCatalog(new Configuration(), warehouse.resolve("nested-hadoop").toString())) {
            Catalog selected = jdbc ? first : local;
            if (!jdbc) local.createNamespace(Namespace.of("db"));
            var declared = nestedTableMetadata();
            var original = IcebergSchemaBuilder.buildIcebergSchema(declared.typedColumns(), declared.properties());
            var expected = TypeUtil.assignIncreasingFreshIds(original);
            var actualMetadata = new IcebergMetadata(selected, "created_schema");
            var output = (IcebergOutputTableHandle) actualMetadata.createTable(SESSION, declared);
            Table created = selected.loadTable(TableIdentifier.of("db", "nested_events"));
            var persisted = ((HasTableOperations) created).operations().refresh();

            assertThat(output.schemaJson()).isEqualTo(SchemaParser.toJson(expected))
                    .isEqualTo(SchemaParser.toJson(persisted.schema()));
            assertThat(SchemaParser.toJson(original)).isNotEqualTo(output.schemaJson());
            assertThat(output.tableUuid()).isEqualTo(persisted.uuid());
            assertThat(output.partitionSpecJson()).isEqualTo(PartitionSpecParser.toJson(persisted.spec()));
            assertThat(created.spec().fields()).singleElement().satisfies(field ->
                    assertThat(created.schema().findColumnName(field.sourceId())).isEqualTo("id"));
            assertThat(created.sortOrder().fields()).extracting(field ->
                    created.schema().findColumnName(field.sourceId())).containsExactly("title", "id");
            assertThat(created.sortOrder().fields().get(0).direction())
                    .isEqualTo(org.apache.iceberg.SortDirection.DESC);
            assertThat(created.sortOrder().fields().get(1).direction())
                    .isEqualTo(org.apache.iceberg.SortDirection.ASC);
            assertNestedTypes(created.schema());
            assertThat(created.properties()).containsAllEntriesOf(
                    IcebergSchemaBuilder.extractIcebergTableProperties(declared.properties()));
            assertThat(output.declarationProperties()).containsAllEntriesOf(
                    GeneratedColumnMetadataCodec.encode(nestedDefinitions()));
            assertThat(output.declarationProperties()).containsEntry("pangrova.vector.dimension.embedding", "3");
            assertThat(created.currentSnapshot()).isNull();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void realAlterStillRejectsTheExactCreatedSchemaAllocation(boolean jdbc) throws Exception {
        try (var local = new HadoopCatalog(new Configuration(), warehouse.resolve("nested-hadoop").toString())) {
            Catalog selected = jdbc ? first : local;
            if (!jdbc) local.createNamespace(Namespace.of("db"));
            var actualMetadata = new IcebergMetadata(selected, "created_schema");
            var output = (IcebergOutputTableHandle) actualMetadata.createTable(SESSION, nestedTableMetadata());
            var expected = SchemaParser.fromJson(output.schemaJson());
            Table created = selected.loadTable(TableIdentifier.of("db", "nested_events"));
            created.updateSchema().renameColumn("title", "changed_title").commit();
            assertThat(created.uuid().toString()).isEqualTo(output.tableUuid());
            assertThatThrownBy(() -> IcebergTableHandleFactory.outputFromTable(
                    "db", "nested_events", created, expected)).isInstanceOf(IllegalStateException.class)
                    .hasMessage("Iceberg created write schema changed before allocation");
            assertThat(created.currentSnapshot()).isNull();
        }
    }

    private static ConnectorTableMetadata nestedTableMetadata() {
        var columns = List.of(
                new ConnectorTableMetadata.Column("embedding", PangrovaType.VECTOR, OptionalInt.empty(),
                        new VectorType(3, VectorElementType.FLOAT)),
                new ConnectorTableMetadata.Column("items", PangrovaType.ARRAY, OptionalInt.empty(),
                        new ArrayType(BigIntType.INSTANCE)),
                new ConnectorTableMetadata.Column("attributes", PangrovaType.MAP, OptionalInt.empty(),
                        new MapType(VarcharType.UNBOUNDED, BigIntType.INSTANCE)),
                new ConnectorTableMetadata.Column("detail", PangrovaType.STRUCT, OptionalInt.empty(),
                        new RowType(List.of(new RowField("entries", new ArrayType(BigIntType.INSTANCE))))),
                new ConnectorTableMetadata.Column("title", PangrovaType.VARCHAR),
                new ConnectorTableMetadata.Column("id", PangrovaType.BIGINT));
        Map<String, String> properties = new LinkedHashMap<>(GeneratedColumnMetadataCodec.encode(nestedDefinitions()));
        properties.put("pangrova.vector.dimension.embedding", "3");
        properties.put("pangrova.array.element.items", "BIGINT");
        properties.put("partition-by", "id");
        properties.put("sorted-by", "title:desc,id:asc");
        return new ConnectorTableMetadata(new SchemaTableName("db", "nested_events"),
                columns.stream().map(ConnectorTableMetadata.Column::name).toList(), columns, properties);
    }

    private static GeneratedColumnSet nestedDefinitions() {
        return GeneratedColumnSet.newBuilder().setSchemaVersion(1)
                .addDefinitions(GeneratedColumnDefinition.newBuilder().setTargetColumn("embedding")
                        .setCapability(BuiltInCapabilityCatalog.aiEmbeddingPlanRef()).setLogicalModelName("first-model")
                        .addSourceColumns("title").setExecutionMode(ExecutionMode.EXECUTION_MODE_ASYNC)
                        .setOutputType(org.pangrova.protocol.type.VectorLogicalType.create(3))).build();
    }

    private static void assertNestedTypes(Schema actual) {
        assertThat(actual.findType("embedding").asListType().elementType()).isEqualTo(Types.FloatType.get());
        assertThat(actual.findType("embedding").asListType().isElementRequired()).isTrue();
        assertThat(actual.findType("items").asListType().elementType()).isEqualTo(Types.LongType.get());
        assertThat(actual.findType("attributes").asMapType().keyType()).isEqualTo(Types.StringType.get());
        assertThat(actual.findType("attributes").asMapType().valueType()).isEqualTo(Types.LongType.get());
        assertThat(actual.findType("detail.entries").asListType().elementType()).isEqualTo(Types.LongType.get());
    }

    @Test
    void schemaChangedAfterAllocationCannotPublishWorkerFragments() throws IOException {
        var handle = allocate();
        var file = file(table, 1);
        second.loadTable(ID).updateSchema().addColumn("extra", Types.StringType.get()).commit();
        assertThatThrownBy(() -> metadata.finishDistributedWrite(SESSION, handle, List.of(fragment(file))))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("write declaration changed");
        assertThat(second.loadTable(ID).currentSnapshot()).isNull();
    }

    @Test
    void introducedGeneratedDeclarationCannotPublishAnOrdinaryAllocation() throws IOException {
        var handle = allocate();
        var file = file(table, 1);
        setDefinitions(second.loadTable(ID), "first-model");
        assertThatThrownBy(() -> metadata.finishDistributedWrite(SESSION, handle, List.of(fragment(file))))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("write declaration changed");
        assertThat(second.loadTable(ID).currentSnapshot()).isNull();
    }

    @Test
    void partitionDeclarationChangingAfterAllocationCannotPublish() throws IOException {
        var handle = allocate();
        var file = file(table, 1);
        second.loadTable(ID).updateSpec().addField("title").commit();
        assertThatThrownBy(() -> metadata.finishDistributedWrite(SESSION, handle, List.of(fragment(file))))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("write declaration changed");
        assertThat(second.loadTable(ID).currentSnapshot()).isNull();
    }

    @Test
    void renamedColumnAtTheSameOrdinalCannotPublishAnOldAllocation() throws IOException {
        var handle = allocate();
        var file = file(table, 1);
        second.loadTable(ID).updateSchema().renameColumn("title", "renamed_title").commit();
        assertThatThrownBy(() -> metadata.finishDistributedWrite(SESSION, handle, List.of(fragment(file))))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("write declaration changed");
        assertThat(second.loadTable(ID).currentSnapshot()).isNull();
    }

    @Test
    void vectorDimensionChangingBetweenCheckedBaseAndCasCannotRebase() throws IOException {
        table.updateProperties().set("pangrova.vector.dimension.embedding", "3").commit();
        var handle = allocate();
        var file = file(table, 1);
        Table fenced = interleave(handle, () -> second.loadTable(ID).updateProperties()
                .set("pangrova.vector.dimension.embedding", "4").commit());
        assertThatThrownBy(() -> fenced.newAppend().appendFile(file).commit())
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("write declaration changed");
        assertThat(second.loadTable(ID).currentSnapshot()).isNull();
    }

    @Test
    void modelDeclarationChangingBetweenCheckedBaseAndCasCannotRebase() throws IOException {
        setDefinitions(table, "first-model");
        var handle = allocate();
        var file = file(table, 1);
        Table fenced = interleave(handle, () -> setDefinitions(second.loadTable(ID), "second-model"));
        assertThatThrownBy(() -> fenced.newAppend().appendFile(file).commit())
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("write declaration changed");
        assertThat(second.loadTable(ID).currentSnapshot()).isNull();
    }

    @Test
    void dataOnlyCommitBetweenCheckedBaseAndCasRebasesWithoutDuplicateFiles() throws IOException {
        var handle = allocate();
        var ours = file(table, 1);
        var other = file(table, 2);
        Table fenced = interleave(handle, () -> second.loadTable(ID).newAppend().appendFile(other).commit());
        var append = fenced.newAppend().appendFile(ours);
        IcebergSnapshotDeclarations.apply(append, handle);
        append.commit();
        Table current = second.loadTable(ID);
        assertThat(current.snapshots()).hasSize(2);
        assertThat(current.currentSnapshot().summary().get("total-records")).isEqualTo("2");
        assertThat(current.currentSnapshot().summary()).containsAllEntriesOf(handle.declarationProperties());
        try (var tasks = current.newScan().planFiles()) {
            assertThat(tasks).extracting(task -> task.file().location())
                    .containsExactlyInAnyOrder(ours.location(), other.location());
        }
    }

    @Test
    void productionCommitAcceptsEarlierDataOnlyMetadataRevision() throws IOException {
        var handle = allocate();
        var ours = file(table, 1);
        second.loadTable(ID).newAppend().appendFile(file(table, 2)).commit();
        metadata.finishDistributedWrite(SESSION, handle, List.of(fragment(ours)));
        Table current = second.loadTable(ID);
        assertThat(current.snapshots()).hasSize(2);
        assertThat(current.currentSnapshot().summary().get("total-records")).isEqualTo("2");
        assertThat(current.currentSnapshot().summary()).containsAllEntriesOf(handle.declarationProperties());
    }

    @Test
    void deleteByFilterPublishesThroughJdbcDeclarationFence() throws IOException {
        table.newAppend().appendFile(file(table, 1)).commit();
        var handle = metadata.getTableHandle(SESSION, new SchemaTableName("db", "events"));
        long snapshotBefore = table.currentSnapshot().snapshotId();

        metadata.deleteByFilter(SESSION, handle, new DeleteFilter.All());

        Table current = second.loadTable(ID);
        assertThat(current.currentSnapshot().snapshotId()).isNotEqualTo(snapshotBefore);
    }

    @Test
    void postCommitSdkRefreshDoesNotRevokeKnownPublicationAndCannotPublishAgain() throws IOException {
        var handle = allocate();
        var file = file(table, 1);
        var actual = ((HasTableOperations) first.loadTable(ID)).operations();
        var interleaved = new IcebergCommitInterleavingOperations(actual, () -> {},
                () -> second.loadTable(ID).updateSchema().addColumn("after_commit", Types.StringType.get()).commit());
        var operations = new IcebergDeclarationTableOperations(interleaved, handle);
        var fenced = new BaseTable(operations, "test");
        fenced.newAppend().appendFile(file).commit();
        assertThat(second.loadTable(ID).currentSnapshot().summary().get("total-records")).isEqualTo("1");
        assertThat(second.loadTable(ID).schema().findField("after_commit")).isNotNull();
        var current = operations.refresh();
        assertThatThrownBy(() -> operations.commit(current, current))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("already published a commit");
        assertThat(second.loadTable(ID).snapshots()).hasSize(1);
        assertThat(table.io().newInputFile(file.location()).exists()).isTrue();
    }

    private Table interleave(IcebergOutputTableHandle handle, Runnable concurrentCommit) {
        var actual = ((HasTableOperations) first.loadTable(ID)).operations();
        var interleaved = new IcebergCommitInterleavingOperations(actual, concurrentCommit, () -> {});
        return new BaseTable(new IcebergDeclarationTableOperations(interleaved, handle), "test");
    }

    private IcebergOutputTableHandle allocate() {
        var handle = metadata.getTableHandle(SESSION, new SchemaTableName("db", "events"));
        return (IcebergOutputTableHandle) metadata.beginInsert(SESSION, handle);
    }

    private byte[] fragment(DataFile file) {
        return ("[" + ContentFileParser.toJson(file, table.spec()) + "]").getBytes(StandardCharsets.UTF_8);
    }

    private JdbcCatalog catalog(String name) {
        var postgres = IcebergJdbcTestDatabase.instance();
        var options = Map.of("type", "jdbc", "uri", postgres.getJdbcUrl(), "warehouse", warehouse.toString(),
                "jdbc.user", postgres.getUsername(), "jdbc.password", postgres.getPassword(),
                "jdbc.schema-version", "V1");
        var configuration = new Configuration();
        configuration.set("fs.file.impl", org.apache.hadoop.fs.RawLocalFileSystem.class.getName());
        configuration.setBoolean("fs.file.impl.disable.cache", true);
        return (JdbcCatalog) CatalogUtil.buildIcebergCatalog(name, options, configuration);
    }

    private static DataFile file(Table target, long value) throws IOException {
        var output = target.io().newOutputFile(target.location() + "/data/row-" + value + ".parquet");
        DataWriter<Record> writer = Parquet.writeData(output).schema(SCHEMA)
                .createWriterFunc(GenericParquetWriter::create).withSpec(target.spec()).build();
        try (writer) {
            var row = GenericRecord.create(SCHEMA);
            row.setField("id", value);
            row.setField("title", "row " + value);
            writer.write(row);
        }
        return writer.toDataFile();
    }

    private static void setDefinitions(Table target, String model) {
        var definitions = GeneratedColumnSet.newBuilder().setSchemaVersion(1)
                .addDefinitions(GeneratedColumnDefinition.newBuilder().setTargetColumn("embedding")
                        .setCapability(BuiltInCapabilityCatalog.aiEmbeddingPlanRef()).setLogicalModelName(model)
                        .addSourceColumns("title").setExecutionMode(ExecutionMode.EXECUTION_MODE_ASYNC)
                        .setOutputType(org.pangrova.protocol.type.VectorLogicalType.create(3))).build();
        var update = target.updateProperties();
        GeneratedColumnMetadataCodec.encode(definitions).forEach(update::set);
        update.commit();
    }
}
