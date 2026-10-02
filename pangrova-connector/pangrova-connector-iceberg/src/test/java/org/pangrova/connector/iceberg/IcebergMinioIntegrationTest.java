package org.pangrova.connector.iceberg;

import org.pangrova.spi.connector.Connector;
import org.pangrova.spi.connector.ConnectorContext;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.ConnectorSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for Iceberg connector using local filesystem (HadoopCatalog).
 * Validates connector factory creation and metadata listing via real Catalog.
 *
 * <p>No Docker dependency — uses {@code @TempDir} for local warehouse storage.
 */
class IcebergMinioIntegrationTest {

    @TempDir
    static Path warehouseDir;

    private static final ConnectorSession TEST_SESSION = new ConnectorSession() {
        @Override
        public String getUser() { return "test-user"; }

        @Override
        public Optional<String> getCatalog() { return Optional.of("iceberg"); }

        @Override
        public Optional<String> getSchema() { return Optional.empty(); }

        @Override
        public Map<String, String> getProperties() { return Map.of(); }
    };

    private static Map<String, String> config;

    @BeforeAll
    static void buildConfig() {
        config = Map.of(
                "type", "hadoop",
                "warehouse", warehouseDir.toAbsolutePath().toString()
        );
    }

    @Test
    void connectorFactoryCreatesConnector() {
        IcebergConnectorFactory factory = new IcebergConnectorFactory();
        assertThat(factory.getName()).isEqualTo("iceberg");

        ConnectorContext context = java.util.Optional::empty;
        Connector connector = factory.create("iceberg", config, context);
        assertThat(connector).isNotNull();
    }

    @Test
    void metadataListsSchemasEmpty() {
        Connector connector = createConnectorViaFactory();
        ConnectorMetadata metadata = connector.getMetadata(TEST_SESSION);

        List<String> schemas = metadata.listSchemaNames(TEST_SESSION);
        assertThat(schemas).isEmpty();
    }

    @Test
    void metadataReturnsNullForNonexistentTable() {
        Connector connector = createConnectorViaFactory();
        ConnectorMetadata metadata = connector.getMetadata(TEST_SESSION);

        SchemaTableName tableName = new SchemaTableName("db", "orders");
        var handle = metadata.getTableHandle(TEST_SESSION, tableName);
        assertThat(handle).isNull();
    }

    private Connector createConnectorViaFactory() {
        ConnectorContext context = java.util.Optional::empty;
        return new IcebergConnectorFactory().create("iceberg", config, context);
    }
}
