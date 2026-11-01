package org.pangrova.connector.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.iceberg.handle.IcebergTableHandleFactory;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.metadata.GeneratedColumnMetadataCodec;
import org.pangrova.protocol.v2.ExecutionMode;
import org.pangrova.protocol.v2.GeneratedColumnDefinition;
import org.pangrova.protocol.v2.GeneratedColumnSet;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.metadata.table.ConnectorTableReadMetadata;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** SDK table properties must reach admission and planning instead of becoming an empty declaration. */
class IcebergDeclarationMetadataIntegrationTest {
    @TempDir
    Path warehouse;

    @Test
    void rawLocalPathPreservesUnicodeQuotesAndSpacesForDeclarationFencing() throws Exception {
        try (var catalog = catalog()) {
            var table = catalog.createTable(TableIdentifier.of("db", "Orders.中文\"; select"),
                    new Schema(Types.NestedField.required(1, "id", Types.LongType.get())));
            assertThat(table.location()).startsWith("/").contains("Orders.中文\"; select");
            IcebergDeclarationCommit.requireSupported(catalog, table);
        }
    }

    @Test
    void currentMetadataPreservesCanonicalGeneratedPropertiesAndVectorType() throws Exception {
        var configuration = new Configuration();
        configuration.set("fs.file.impl", org.apache.hadoop.fs.RawLocalFileSystem.class.getName());
        try (var catalog = new HadoopCatalog(configuration, warehouse.toString())) {
            var definitions = definitions();
            var properties = new LinkedHashMap<>(GeneratedColumnMetadataCodec.encode(definitions));
            properties.put("pangrova.vector.dimension.embedding", "3");
            var schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(2, "title", Types.StringType.get()),
                    Types.NestedField.optional(3, "embedding", Types.ListType.ofOptional(4, Types.FloatType.get())));
            var table = catalog.createTable(TableIdentifier.of("db", "declared"), schema,
                    org.apache.iceberg.PartitionSpec.unpartitioned(), properties);
            var handle = IcebergTableHandleFactory.fromTable("db", "declared", table);
            var metadata = new IcebergCatalogTables(catalog).getTableMetadata(handle);
            assertThat(metadata.properties()).containsAllEntriesOf(properties);
            assertThat(GeneratedColumnMetadataCodec.decode(metadata.properties())).isEqualTo(definitions);
            assertThat(metadata.typedColumns().get(2).type()).isEqualTo(PangrovaType.VECTOR);
        }
    }

    @Test
    void thirdPartySnapshotDeclarationComesFromItsActualCreationMetadataNotCurrentProperties() throws Exception {
        try (var catalog = catalog()) {
            var table = createTable(catalog);
            append(table, false);
            var snapshot = table.currentSnapshot().snapshotId();
            table.updateProperties().remove(GeneratedColumnMetadataCodec.PAYLOAD_PROPERTY)
                    .remove(GeneratedColumnMetadataCodec.SHA256_PROPERTY)
                    .remove("pangrova.vector.dimension.embedding").commit();
            table.updateSchema().addColumn("later", Types.StringType.get()).commit();
            var handle = IcebergTableHandleFactory.fromTableWithSnapshot("db", "declared", table, snapshot);
            var evidence = IcebergReadSchema.resolve(handle, table);
            assertThat(evidence.schema().columns()).extracting(field -> field.name())
                    .containsExactly("id", "title", "embedding");
            assertThat(GeneratedColumnMetadataCodec.decode(evidence.declarationProperties().orElseThrow()))
                    .isEqualTo(definitions());
            assertThat(evidence.declarationProperties().orElseThrow())
                    .containsEntry("pangrova.vector.dimension.embedding", "3");
            var actual = new IcebergCatalogTables(catalog).getTableMetadata(handle);
            assertThat(actual.columns()).containsExactly("id", "title", "embedding");
            assertThat(GeneratedColumnMetadataCodec.decode(actual.properties())).isEqualTo(definitions());
            assertThat(new IcebergCatalogTables(catalog).getColumnHandles(handle))
                    .containsOnlyKeys("id", "title", "embedding");
        }
    }

    @Test
    void engineSnapshotSummaryRetainsOnlyCanonicalDeclarationAfterMetadataHistoryExpires() throws Exception {
        try (var catalog = catalog()) {
            var table = createTable(catalog);
            table.updateProperties().set("private.auth.token", "must-not-enter-snapshot")
                    .set(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "1")
                    .set(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true").commit();
            append(table, true);
            var snapshot = table.currentSnapshot();
            table.updateProperties().remove(GeneratedColumnMetadataCodec.PAYLOAD_PROPERTY)
                    .remove(GeneratedColumnMetadataCodec.SHA256_PROPERTY).commit();
            table.updateProperties().set("irrelevant", "revision").commit();
            assertThat(snapshot.summary()).doesNotContainKey("private.auth.token")
                    .containsAllEntriesOf(GeneratedColumnMetadataCodec.encode(definitions()));
            var handle = IcebergTableHandleFactory.fromTableWithSnapshot("db", "declared", table, snapshot.snapshotId());
            assertThat(GeneratedColumnMetadataCodec.decode(
                    IcebergReadSchema.resolve(handle, table).declarationProperties().orElseThrow()))
                    .isEqualTo(definitions());
        }
    }

    @Test
    void unavailableThirdPartyHistoryIsExplicitUnknownRatherThanCurrentAbsence() throws Exception {
        try (var catalog = catalog()) {
            var table = createTable(catalog);
            table.updateProperties().set(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "1")
                    .set(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true").commit();
            append(table, false);
            var snapshot = table.currentSnapshot().snapshotId();
            table.updateProperties().remove(GeneratedColumnMetadataCodec.PAYLOAD_PROPERTY)
                    .remove(GeneratedColumnMetadataCodec.SHA256_PROPERTY).commit();
            table.updateProperties().set("irrelevant", "revision").commit();
            var handle = IcebergTableHandleFactory.fromTableWithSnapshot("db", "declared", table, snapshot);
            assertThat(IcebergReadSchema.resolve(handle, table).declarationProperties()).isEmpty();
            var tables = new IcebergCatalogTables(catalog);
            assertThat(tables.getTableReadMetadata(handle).declarationEvidence())
                    .isEqualTo(ConnectorTableReadMetadata.DeclarationEvidence.UNAVAILABLE);
            assertThatThrownBy(() -> tables.getTableMetadata(handle))
                    .isInstanceOf(org.pangrova.spi.connector.base.ConnectorException.class)
                    .hasMessageContaining("Historical generated declaration evidence is unavailable");
            assertThat(tables.getTableMetadata(IcebergTableHandleFactory.fromTable("db", "declared", table))
                    .columns()).containsExactly("id", "title", "embedding");
        }
    }

    @Test
    void ordinaryThirdPartySnapshotHasProvenAbsenceWithoutChangingItsSdkCommit() throws Exception {
        try (var catalog = catalog()) {
            var table = createTable(catalog);
            table.updateProperties().remove(GeneratedColumnMetadataCodec.PAYLOAD_PROPERTY)
                    .remove(GeneratedColumnMetadataCodec.SHA256_PROPERTY).commit();
            append(table, false);
            var snapshot = table.currentSnapshot();
            assertThat(snapshot.summary()).doesNotContainKey(GeneratedColumnMetadataCodec.PAYLOAD_PROPERTY);
            table.updateProperties().set("irrelevant", "revision").commit();
            var handle = IcebergTableHandleFactory.fromTableWithSnapshot("db", "declared", table, snapshot.snapshotId());
            var historical = new IcebergCatalogTables(catalog).getTableReadMetadata(handle).requireExactDeclaration();
            assertThat(GeneratedColumnMetadataCodec.decode(historical.properties()).getDefinitionsCount()).isZero();
            assertThat(historical.columns()).containsExactly("id", "title", "embedding");
        }
    }

    @Test
    void explicitAbsenceSummaryDoesNotCopySecretsOrFutureDeclarations() {
        var properties = IcebergSnapshotDeclarations.properties(Map.of("private.auth.token", "secret"));
        assertThat(properties).hasSize(2);
        assertThat(GeneratedColumnMetadataCodec.decode(properties).getDefinitionsCount()).isZero();
    }

    private HadoopCatalog catalog() {
        var configuration = new Configuration();
        configuration.set("fs.file.impl", org.apache.hadoop.fs.RawLocalFileSystem.class.getName());
        return new HadoopCatalog(configuration, warehouse.toString());
    }

    private static Table createTable(HadoopCatalog catalog) {
        var properties = new LinkedHashMap<>(GeneratedColumnMetadataCodec.encode(definitions()));
        properties.put("pangrova.vector.dimension.embedding", "3");
        var schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "title", Types.StringType.get()),
                Types.NestedField.optional(3, "embedding", Types.ListType.ofOptional(4, Types.FloatType.get())));
        return catalog.createTable(TableIdentifier.of("db", "declared"), schema,
                org.apache.iceberg.PartitionSpec.unpartitioned(), properties);
    }

    private static void append(Table table, boolean engineDeclaration) throws Exception {
        var output = table.io().newOutputFile(table.location() + "/data/row.parquet");
        var writer = Parquet.writeData(output).schema(table.schema()).withSpec(table.spec())
                .createWriterFunc(GenericParquetWriter::create).build();
        try (writer) {
            var record = GenericRecord.create(table.schema());
            record.setField("id", 1L);
            record.setField("title", "title");
            writer.write(record);
        }
        var update = table.newAppend().appendFile(writer.toDataFile());
        if (engineDeclaration) IcebergSnapshotDeclarations.properties(table.properties()).forEach(update::set);
        update.commit();
    }

    private static GeneratedColumnSet definitions() {
        return GeneratedColumnSet.newBuilder().setSchemaVersion(GeneratedColumnMetadataCodec.SCHEMA_VERSION)
                .addDefinitions(GeneratedColumnDefinition.newBuilder().setTargetColumn("embedding")
                        .setCapability(BuiltInCapabilityCatalog.aiEmbeddingPlanRef()).setLogicalModelName("embedding")
                        .addSourceColumns("title").setExecutionMode(ExecutionMode.EXECUTION_MODE_ASYNC)
                        .setOutputType(VectorLogicalType.create(3)))
                .build();
    }
}
