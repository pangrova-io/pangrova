package org.pangrova.connector.iceberg;

import org.pangrova.connector.iceberg.handle.IcebergOutputTableHandle;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.metadata.DeletionMode;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for IcebergMetadata.deletionMode() and beginDelete() format-version branching.
 * Uses real HadoopCatalog on local filesystem (no mocks per pangrova-rules §12).
 */
class IcebergMetadataDeletionModeTest {

    @TempDir
    Path warehouse;

    private Catalog catalog;
    private IcebergMetadata metadata;

    private static final String CATALOG_NAME = "dv_catalog";
    private static final String NAMESPACE = "dv_db";

    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override
        public java.util.Optional<org.pangrova.connector.api.query.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

        @Override public String getUser() { return "test"; }
        @Override public Optional<String> getCatalog() { return Optional.of(CATALOG_NAME); }
        @Override public Optional<String> getSchema() { return Optional.empty(); }
        @Override public Map<String, String> getProperties() { return Map.of(); }
    };

    private static final Schema TEST_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    @BeforeEach
    void setUp() {
        HadoopTestGuard.assumeHadoopCompatible();
        catalog = buildHadoopCatalog();
        createNamespace();
        metadata = new IcebergMetadata(catalog, CATALOG_NAME);
    }

    @Test
    void deletionMode_v3_returnsDeletionVector() {
        createTable("v3_table", 3);
        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName(NAMESPACE, "v3_table"));

        DeletionMode mode = metadata.deletionMode(SESSION, handle);

        assertThat(mode).isEqualTo(DeletionMode.DELETION_VECTOR);
    }

    @Test
    void deletionMode_v2_returnsPositionDelete() {
        createTable("v2_table", 2);
        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName(NAMESPACE, "v2_table"));

        DeletionMode mode = metadata.deletionMode(SESSION, handle);

        assertThat(mode).isEqualTo(DeletionMode.POSITION_DELETE);
    }

    @Test
    void beginDelete_v3_returnsHandleWithFormatVersion3() {
        createTable("v3_delete", 3);
        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName(NAMESPACE, "v3_delete"));

        var outputHandle = (IcebergOutputTableHandle) metadata.beginDelete(SESSION, handle);

        assertThat(outputHandle.formatVersion()).isEqualTo(3);
    }

    @Test
    void beginDelete_v2_returnsHandleWithFormatVersion2() {
        createTable("v2_delete", 2);
        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName(NAMESPACE, "v2_delete"));

        var outputHandle = (IcebergOutputTableHandle) metadata.beginDelete(SESSION, handle);

        assertThat(outputHandle.formatVersion()).isEqualTo(2);
    }

    @Test
    void beginDelete_v1_returnsCopyOnWriteHandle() {
        createTable("v1_delete", 1);
        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName(NAMESPACE, "v1_delete"));

        var outputHandle = (IcebergOutputTableHandle) metadata.beginDelete(SESSION, handle);

        assertThat(outputHandle.formatVersion()).isEqualTo(1);
    }

    @Test
    void deletionMode_v1_returnsCopyOnWrite() {
        createTable("v1_mode", 1);
        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName(NAMESPACE, "v1_mode"));

        DeletionMode mode = metadata.deletionMode(SESSION, handle);

        assertThat(mode).isEqualTo(DeletionMode.COPY_ON_WRITE);
    }

    private Catalog buildHadoopCatalog() {
        Map<String, String> props = Map.of(
                "type", "hadoop",
                "warehouse", warehouse.toString());
        return org.apache.iceberg.CatalogUtil.buildIcebergCatalog(
                CATALOG_NAME, props, new org.apache.hadoop.conf.Configuration());
    }

    private void createNamespace() {
        if (catalog instanceof SupportsNamespaces nsCatalog) {
            nsCatalog.createNamespace(Namespace.of(NAMESPACE));
        }
    }

    private void createTable(String tableName, int formatVersion) {
        TableIdentifier tableId = TableIdentifier.of(NAMESPACE, tableName);
        catalog.createTable(tableId, TEST_SCHEMA,
                org.apache.iceberg.PartitionSpec.unpartitioned(),
                Map.of("format-version", String.valueOf(formatVersion)));
    }
}
