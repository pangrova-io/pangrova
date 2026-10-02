package org.pangrova.connector.paimon;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.paimon.handle.PaimonTableHandle;
import org.pangrova.protocol.BuiltInCapabilityCatalog;
import org.pangrova.protocol.GeneratedColumnMetadataCodec;
import org.pangrova.protocol.v2.ExecutionMode;
import org.pangrova.protocol.v2.GeneratedColumnDefinition;
import org.pangrova.protocol.v2.GeneratedColumnSet;
import org.pangrova.protocol.v2.LogicalType;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual SDK schema revisions retain their own generated declaration after current options are removed. */
class PaimonDeclarationMetadataIntegrationTest {
    @TempDir
    Path warehouse;

    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override public String getUser() { return "test"; }
        @Override public Optional<String> getCatalog() { return Optional.of("paimon"); }
        @Override public Optional<String> getSchema() { return Optional.of("db"); }
        @Override public Map<String, String> getProperties() { return Map.of(); }
    };

    @Test
    void currentAndPinnedMetadataPreserveCanonicalDeclarationWithoutBorrowingCurrentOptions() throws Exception {
        var options = new Options();
        options.setString("warehouse", warehouse.toString());
        try (var catalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            catalog.createDatabase("db", false);
            var id = Identifier.create("db", "declared");
            var definitions = definitions();
            var properties = GeneratedColumnMetadataCodec.encode(definitions);
            var schema = Schema.newBuilder().column("id", DataTypes.BIGINT())
                    .column("title", DataTypes.STRING()).column("embedding", DataTypes.ARRAY(DataTypes.FLOAT()))
                    .option("bucket", "1").option("bucket-key", "id")
                    .option("pangrova.vector.dimension.embedding", "3");
            properties.forEach(schema::option);
            catalog.createTable(id, schema.build(), false);
            var metadata = new PaimonMetadata(catalog, "paimon");
            var original = (PaimonTableHandle) metadata.getTableHandle(SESSION, new SchemaTableName("db", "declared"));
            assertThat(GeneratedColumnMetadataCodec.decode(metadata.getTableMetadata(SESSION, original).properties()))
                    .isEqualTo(definitions);
            assertGeneratedAdmissionRejected(metadata, original);
            var table = catalog.getTable(id);
            var builder = table.newBatchWriteBuilder();
            try (var writer = builder.newWrite(); var commit = builder.newCommit()) {
                writer.write(GenericRow.of(1L, BinaryString.fromString("title"), null));
                commit.commit(writer.prepareCommit());
            }
            catalog.alterTable(id, List.of(
                    SchemaChange.removeOption(GeneratedColumnMetadataCodec.PAYLOAD_PROPERTY),
                    SchemaChange.removeOption(GeneratedColumnMetadataCodec.SHA256_PROPERTY),
                    SchemaChange.removeOption("pangrova.vector.dimension.embedding"),
                    SchemaChange.addColumn("new_column", DataTypes.STRING())), false);
            var current = (PaimonTableHandle) metadata.getTableHandle(SESSION, new SchemaTableName("db", "declared"));
            var currentMetadata = metadata.getTableMetadata(SESSION, current);
            assertThat(GeneratedColumnMetadataCodec.decode(currentMetadata.properties()).getDefinitionsCount()).isZero();
            assertThat(currentMetadata.columns()).contains("new_column");
            assertThat(currentMetadata.typedColumns().get(2).type()).isEqualTo(PangrovaType.ARRAY);
            assertThat(metadata.beginInsert(SESSION, current))
                    .isInstanceOf(org.pangrova.connector.paimon.handle.PaimonOutputTableHandle.class);
            var historical = metadata.getTableReadMetadata(SESSION, current.withTimeTravelSnapshot(1))
                    .requireExactDeclaration();
            assertThat(GeneratedColumnMetadataCodec.decode(historical.properties())).isEqualTo(definitions);
            assertThat(historical.properties()).containsAllEntriesOf(properties)
                    .containsEntry("pangrova.vector.dimension.embedding", "3");
            assertThat(historical.columns()).containsExactly("id", "title", "embedding");
            assertThat(historical.typedColumns().get(2).type()).isEqualTo(PangrovaType.VECTOR);
        }
    }

    private static void assertGeneratedAdmissionRejected(PaimonMetadata metadata, PaimonTableHandle handle) {
        assertThatThrownBy(() -> metadata.beginInsert(SESSION, handle))
                .isInstanceOf(org.pangrova.spi.connector.base.ConnectorException.class)
                .hasMessageContaining("atomic declaration-and-snapshot commit gate");
        assertThatThrownBy(() -> metadata.beginOverwrite(SESSION, handle))
                .isInstanceOf(org.pangrova.spi.connector.base.ConnectorException.class)
                .hasMessageContaining("atomic declaration-and-snapshot commit gate");
        var declared = metadata.getTableMetadata(SESSION, handle);
        var create = new org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata(
                new SchemaTableName("db", "rejected"), declared.columns(), declared.typedColumns(),
                declared.properties());
        assertThatThrownBy(() -> metadata.createTable(SESSION, create))
                .isInstanceOf(org.pangrova.spi.connector.base.ConnectorException.class)
                .hasMessageContaining("atomic declaration-and-snapshot commit gate");
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("db", "rejected"))).isNull();
    }

    private static GeneratedColumnSet definitions() {
        return GeneratedColumnSet.newBuilder().setSchemaVersion(GeneratedColumnMetadataCodec.SCHEMA_VERSION)
                .addDefinitions(GeneratedColumnDefinition.newBuilder().setTargetColumn("embedding")
                        .setCapability(BuiltInCapabilityCatalog.aiEmbeddingPlanRef()).setLogicalModelName("embedding")
                        .addSourceColumns("title").setExecutionMode(ExecutionMode.EXECUTION_MODE_ASYNC)
                        .setOutputType(LogicalType.newBuilder().setCanonicalSql("VECTOR(3, COSINE)")))
                .build();
    }
}
