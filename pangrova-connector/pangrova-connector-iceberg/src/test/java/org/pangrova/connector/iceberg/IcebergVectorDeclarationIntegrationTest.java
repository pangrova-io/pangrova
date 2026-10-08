package org.pangrova.connector.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.pangrova.connector.iceberg.schema.IcebergSchemaBuilder;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.ConnectorQueryAuthority;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.data.type.VectorType;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real connector CREATE, persisted SDK schema and metadata readback; no synthetic storage. */
class IcebergVectorDeclarationIntegrationTest {
    @TempDir Path warehouse;
    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override public Optional<ConnectorQueryAuthority> getQueryAuthority() { return Optional.empty(); }
        @Override public String getUser() { return "declaration"; }
        @Override public Optional<String> getCatalog() { return Optional.of("vectors"); }
        @Override public Optional<String> getSchema() { return Optional.of("db"); }
        @Override public Map<String, String> getProperties() { return Map.of(); }
    };

    @ParameterizedTest
    @EnumSource(VectorType.ElementType.class)
    void connectorCreatePreservesDeclaredPrecisionAndDimensionInActualSdkAndReadback(VectorType.ElementType element)
            throws Exception {
        var configuration = new Configuration();
        configuration.set("fs.file.impl", org.apache.hadoop.fs.RawLocalFileSystem.class.getName());
        try (var catalog = new HadoopCatalog(configuration, warehouse.toString())) {
            catalog.createNamespace(Namespace.of("db"));
            var metadata = new IcebergMetadata(catalog, "vectors");
            var declared = declaration(new VectorType(3, element), Map.of());
            metadata.createTable(SESSION, declared);
            var table = catalog.loadTable(TableIdentifier.of("db", "vectors"));
            assertThat(table.schema().findType("embedding").asListType().elementType()).isEqualTo(
                    element == VectorType.ElementType.DOUBLE ? Types.DoubleType.get() : Types.FloatType.get());
            assertThat(table.properties()).containsEntry("pangrova.vector.dimension.embedding", "3");
            var actual = metadata.getTableMetadata(SESSION, metadata.getTableHandle(SESSION, declared.table()));
            assertThat(actual.typedColumns().get(0).type()).isEqualTo(PangrovaType.VECTOR);
            assertThat(actual.typedColumns().get(0).parameterizedType()).isEqualTo(new VectorType(3, element));
        }
    }

    @Test
    void missingStorageParametersAndConflictingPropertyCannotInventAFloatVector() {
        var incomplete = List.of(new ConnectorTableMetadata.Column("embedding", PangrovaType.VECTOR));
        assertThatThrownBy(() -> IcebergSchemaBuilder.buildIcebergSchema(incomplete, Map.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("concrete storage declaration");
        var declared = declaration(new VectorType(3, VectorType.ElementType.DOUBLE),
                Map.of("pangrova.vector.dimension.embedding", "4"));
        assertThatThrownBy(() -> IcebergSchemaBuilder.buildIcebergSchema(declared.typedColumns(), declared.properties()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("dimension property disagrees");
        assertThatThrownBy(() -> IcebergSchemaBuilder.declaredTableProperties(
                declared.typedColumns(), declared.properties()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("dimension property disagrees");
    }

    private static ConnectorTableMetadata declaration(VectorType vector, Map<String, String> properties) {
        return new ConnectorTableMetadata(new SchemaTableName("db", "vectors"), List.of("embedding"),
                List.of(new ConnectorTableMetadata.Column("embedding", PangrovaType.VECTOR,
                        OptionalInt.empty(), vector)), properties);
    }
}
