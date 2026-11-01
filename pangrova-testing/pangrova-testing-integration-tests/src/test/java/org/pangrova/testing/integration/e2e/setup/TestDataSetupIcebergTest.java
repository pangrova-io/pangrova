package org.pangrova.testing.integration.e2e.setup;

import org.pangrova.connector.iceberg.IcebergConnector;
import org.pangrova.spi.connector.plugin.Connector;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.testing.testkit.TestCluster;
import org.pangrova.testing.testkit.TestDataGenerator;
import org.pangrova.testing.integration.e2e.E2ETestFramework;
import org.pangrova.testing.integration.e2e.runner.SqlTestRunner;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test verifying TestDataSetup writes real Parquet files
 * via Connector SPI to a local filesystem warehouse.
 */
class TestDataSetupIcebergTest {

    @TempDir
    Path warehouseDir;

    private TestDataSetup setup;
    private Catalog icebergCatalog;

    @BeforeEach
    void setUp() {
        TestCluster cluster = new TestCluster(() -> () -> {}, () -> () -> {});
        E2ETestFramework framework = new E2ETestFramework(cluster, sql -> List.of());
        SqlTestRunner runner = new SqlTestRunner(framework);
        setup = new TestDataSetup(runner);
        icebergCatalog = IcebergCatalogFactory.create(warehouseDir);
    }

    private Connector createIcebergConnector() {
        return new IcebergConnector("test_warehouse", icebergCatalog);
    }

    @Test
    void loadTpcHDataCreatesAllIcebergTables() {
        Connector connector = createIcebergConnector();
        setup.loadTpcHDataViaConnector(connector, 1);

        assertThat(setup.getConnector()).isNotNull();
        ConnectorMetadata metadata = connector.getMetadata(testSession());
        int expectedTables = TestDataGenerator.getSchemas().size();
        List<SchemaTableName> tables = metadata.listTables(testSession(), Optional.of("default"));
        assertThat(tables).hasSize(expectedTables);
    }

    @Test
    void eachTableHasDataFiles() throws IOException {
        setup.loadTpcHDataViaConnector(createIcebergConnector(), 1);

        for (String tableName : TestDataGenerator.getSchemas().keySet()) {
            Table table = loadIcebergTable(tableName);
            long rowCount = countRecordsViaMetadata(table);
            assertThat(rowCount)
                .as("Table %s row count", tableName)
                .isEqualTo(100);
        }
    }

    @Test
    void scaleFactorMultipliesRowCount() throws IOException {
        setup.loadTpcHDataViaConnector(createIcebergConnector(), 2);

        Table region = loadIcebergTable("region");
        assertThat(countRecordsViaMetadata(region)).isEqualTo(200);
    }

    @Test
    void tableSchemaMatchesTpcHDefinition() {
        setup.loadTpcHDataViaConnector(createIcebergConnector(), 1);

        Table region = loadIcebergTable("region");
        List<String> columnNames = region.schema().columns().stream()
            .map(f -> f.name())
            .toList();
        assertThat(columnNames).containsExactly(
            "r_regionkey", "r_name", "r_comment");
    }

    @Test
    void parquetFilesExistOnDisk() throws IOException {
        setup.loadTpcHDataViaConnector(createIcebergConnector(), 1);

        Table region = loadIcebergTable("region");
        try (CloseableIterable<FileScanTask> tasks = region.newScan().planFiles()) {
            int fileCount = 0;
            for (FileScanTask task : tasks) {
                assertThat(task.file().path().toString()).endsWith(".parquet");
                fileCount++;
            }
            assertThat(fileCount).isGreaterThan(0);
        }
    }

    @Test
    void dataFileRecordCountMatchesExpected() throws IOException {
        setup.loadTpcHDataViaConnector(createIcebergConnector(), 1);

        Table lineitem = loadIcebergTable("lineitem");
        assertThat(countRecordsViaMetadata(lineitem)).isEqualTo(100);
        assertThat(lineitem.schema().columns()).hasSizeGreaterThan(5);
    }

    @Test
    void catalogCanLoadTablesDirectly() {
        setup.loadTpcHDataViaConnector(createIcebergConnector(), 1);

        TableIdentifier id = TableIdentifier.of("default", "lineitem");
        Table lineitem = icebergCatalog.loadTable(id);
        assertThat(lineitem).isNotNull();
        assertThat(lineitem.schema().columns()).hasSizeGreaterThan(5);
    }

    private Table loadIcebergTable(String tableName) {
        TableIdentifier id = TableIdentifier.of("default", tableName);
        return icebergCatalog.loadTable(id);
    }

    private static ConnectorSession testSession() {
        return new ConnectorSession() {
            @Override
            public java.util.Optional<org.pangrova.spi.connector.ConnectorQueryAuthority> getQueryAuthority() {
                return java.util.Optional.empty();
            }

            @Override public String getUser() { return "test"; }
            @Override public Optional<String> getCatalog() { return Optional.of("test_warehouse"); }
            @Override public Optional<String> getSchema() { return Optional.of("default"); }
            @Override public Map<String, String> getProperties() {
                return Map.of("tenant_id", "e2e-tenant");
            }
        };
    }

    /**
     * Count total records by summing DataFile.recordCount() from table metadata.
     * Avoids needing iceberg-data module for GenericRecord reading.
     */
    private static long countRecordsViaMetadata(Table table) throws IOException {
        long total = 0;
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                total += task.file().recordCount();
            }
        }
        return total;
    }
}
