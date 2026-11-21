package org.pangrova.connector.paimon;

import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that Paimon's partition evolution limitation is properly surfaced (G-CONN-08).
 *
 * <p>Unlike Iceberg, Paimon does not support changing partition keys on existing tables.
 * The connector must reject partition evolution with a clear error message rather than
 * silently failing or throwing an opaque exception.</p>
 */
class PaimonPartitionEvolutionTest {

    @TempDir
    Path warehouse;

    private Catalog catalog;
    private PaimonMetadata metadata;

    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override
        public java.util.Optional<org.pangrova.connector.api.query.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

        @Override public String getUser() { return "test"; }
        @Override public Optional<String> getCatalog() { return Optional.of("test_catalog"); }
        @Override public Optional<String> getSchema() { return Optional.empty(); }
        @Override public Map<String, String> getProperties() { return Map.of(); }
    };

    @BeforeEach
    void setUp() throws Exception {
        Options options = new Options();
        options.set("warehouse", warehouse.toUri().toString());
        catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        catalog.createDatabase("db", false);

        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.BIGINT())
                .column("name", DataTypes.STRING())
                .column("ts", DataTypes.TIMESTAMP(3))
                .column("region", DataTypes.STRING())
                .partitionKeys("region")
                .build();
        catalog.createTable(Identifier.create("db", "events"), schema, false);

        metadata = new PaimonMetadata(catalog, "test_catalog", new org.pangrova.connector.paimon.PaimonSplitRegistry());
    }

    @Test
    void setPartitionSpec_throwsUnsupportedWithClearMessage() {
        ConnectorTableHandle handle = metadata.getTableHandle(
                SESSION, new SchemaTableName("db", "events"));
        assertThat(handle).isNotNull();

        List<ConnectorMetadata.PartitionTransform> transforms = List.of(
                new ConnectorMetadata.PartitionTransform("month", List.of("ts")));

        assertThatThrownBy(() ->
                metadata.setPartitionSpec(SESSION, handle, transforms))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Paimon does not support partition evolution")
                .hasMessageContaining("db")
                .hasMessageContaining("events")
                .hasMessageContaining("fixed at creation time");
    }

    @Test
    void setPartitionSpec_emptyTransformsAlsoRejected() {
        ConnectorTableHandle handle = metadata.getTableHandle(
                SESSION, new SchemaTableName("db", "events"));
        assertThat(handle).isNotNull();

        assertThatThrownBy(() ->
                metadata.setPartitionSpec(SESSION, handle, List.of()))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Paimon does not support partition evolution");
    }

    @Test
    void setPartitionSpec_identityTransformAlsoRejected() {
        ConnectorTableHandle handle = metadata.getTableHandle(
                SESSION, new SchemaTableName("db", "events"));
        assertThat(handle).isNotNull();

        List<ConnectorMetadata.PartitionTransform> transforms = List.of(
                ConnectorMetadata.PartitionTransform.identity("name"));

        assertThatThrownBy(() ->
                metadata.setPartitionSpec(SESSION, handle, transforms))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Paimon does not support partition evolution");
    }
}
