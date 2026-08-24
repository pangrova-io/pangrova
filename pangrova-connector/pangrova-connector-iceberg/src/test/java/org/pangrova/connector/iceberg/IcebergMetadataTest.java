package org.pangrova.connector.iceberg;

import org.pangrova.connector.iceberg.handle.IcebergColumnHandle;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for IcebergMetadata using real HadoopCatalog on local filesystem.
 * Validates: Requirements 30.1, 30.2, 30.3, 30.4, 30.5.
 */
class IcebergMetadataTest {

    @TempDir
    Path warehouse;

    private Catalog catalog;
    private IcebergMetadata metadata;

    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override public String getUser() { return "test"; }
        @Override public Optional<String> getCatalog() { return Optional.of("test_catalog"); }
        @Override public Optional<String> getSchema() { return Optional.empty(); }
        @Override public Map<String, String> getProperties() { return Map.of(); }
    };

    private static final Schema TEST_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "amount", Types.DoubleType.get()));

    @BeforeEach
    void setUp() {
        HadoopTestGuard.assumeHadoopCompatible();
        catalog = createHadoopCatalog();
        metadata = new IcebergMetadata(catalog, "test_catalog");
    }

    @Test
    void listSchemaNames_returnsCreatedNamespaces() {
        createNamespace("sales");
        createNamespace("analytics");

        List<String> schemas = metadata.listSchemaNames(SESSION);

        assertThat(schemas).containsExactlyInAnyOrder("sales", "analytics");
    }

    @Test
    void listSchemaNames_emptyWhenNoNamespaces() {
        List<String> schemas = metadata.listSchemaNames(SESSION);
        assertThat(schemas).isEmpty();
    }

    @Test
    void listTables_returnsTablesInSchema() {
        createNamespace("db");
        catalog.createTable(TableIdentifier.of("db", "orders"), TEST_SCHEMA);
        catalog.createTable(TableIdentifier.of("db", "customers"), TEST_SCHEMA);

        List<SchemaTableName> tables = metadata.listTables(SESSION, Optional.of("db"));

        assertThat(tables).containsExactlyInAnyOrder(
                new SchemaTableName("db", "orders"),
                new SchemaTableName("db", "customers"));
    }

    @Test
    void listTables_emptyForNonexistentSchema() {
        List<SchemaTableName> tables = metadata.listTables(SESSION, Optional.of("nonexistent"));
        assertThat(tables).isEmpty();
    }

    @Test
    void getTableHandle_returnsHandleWithMetadataSnapshot() {
        createNamespace("db");
        catalog.createTable(TableIdentifier.of("db", "orders"), TEST_SCHEMA);

        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName("db", "orders"));

        assertThat(handle).isNotNull().isInstanceOf(IcebergTableHandle.class);
        IcebergTableHandle icebergHandle = (IcebergTableHandle) handle;
        assertThat(icebergHandle.schemaName()).isEqualTo("db");
        assertThat(icebergHandle.tableName()).isEqualTo("orders");
        assertThat(icebergHandle.tableLocation()).isNotBlank();
        assertThat(icebergHandle.schemaJson()).isNotBlank();
    }

    @Test
    void getTableHandle_returnsNullForNonexistentTable() {
        createNamespace("db");

        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName("db", "missing"));

        assertThat(handle).isNull();
    }

    @Test
    void getTableMetadata_returnsTypedColumns() {
        createNamespace("db");
        catalog.createTable(TableIdentifier.of("db", "orders"), TEST_SCHEMA);
        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName("db", "orders"));

        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, handle);

        assertThat(tableMetadata.table()).isEqualTo(new SchemaTableName("db", "orders"));
        assertThat(tableMetadata.columns()).containsExactly("id", "name", "amount");
        assertThat(tableMetadata.typedColumns()).hasSize(3);
        assertTypedColumn(tableMetadata.typedColumns().get(0), "id", PangrovaType.BIGINT, 1);
        assertTypedColumn(tableMetadata.typedColumns().get(1), "name", PangrovaType.VARCHAR, 2);
        assertTypedColumn(tableMetadata.typedColumns().get(2), "amount", PangrovaType.DOUBLE, 3);
    }

    @Test
    void getColumnHandles_returnsHandlesWithFieldIds() {
        createNamespace("db");
        catalog.createTable(TableIdentifier.of("db", "orders"), TEST_SCHEMA);
        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName("db", "orders"));

        Map<String, ColumnHandle> columns = metadata.getColumnHandles(SESSION, handle);

        assertThat(columns).hasSize(3).containsKeys("id", "name", "amount");
        assertColumnHandle(columns.get("id"), 1, "id", PangrovaType.BIGINT);
        assertColumnHandle(columns.get("name"), 2, "name", PangrovaType.VARCHAR);
        assertColumnHandle(columns.get("amount"), 3, "amount", PangrovaType.DOUBLE);
    }

    private Catalog createHadoopCatalog() {
        Map<String, String> props = Map.of(
                "type", "hadoop",
                "warehouse", warehouse.toString());
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        return org.apache.iceberg.CatalogUtil.buildIcebergCatalog("test_catalog", props, hadoopConf);
    }

    private void createNamespace(String name) {
        if (catalog instanceof org.apache.iceberg.catalog.SupportsNamespaces nsCatalog) {
            nsCatalog.createNamespace(Namespace.of(name));
        }
    }

    private void assertTypedColumn(ConnectorTableMetadata.Column col, String name, PangrovaType type, int fieldId) {
        assertThat(col.name()).isEqualTo(name);
        assertThat(col.type()).isEqualTo(type);
        assertThat(col.fieldId()).hasValue(fieldId);
    }

    private void assertColumnHandle(ColumnHandle handle, int fieldId, String name, PangrovaType type) {
        assertThat(handle).isInstanceOf(IcebergColumnHandle.class);
        IcebergColumnHandle col = (IcebergColumnHandle) handle;
        assertThat(col.fieldId()).isEqualTo(fieldId);
        assertThat(col.name()).isEqualTo(name);
        assertThat(col.type()).isEqualTo(type);
    }
}
