package org.pangrova.connector.iceberg;

import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
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

/**
 * Verifies that LIMIT pushdown is correctly absorbed by the Iceberg connector.
 */
class IcebergLimitPushdownTest {

    @TempDir
    Path warehouse;

    private Catalog catalog;
    private IcebergMetadata metadata;

    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override
        public java.util.Optional<org.pangrova.spi.connector.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

        @Override public String getUser() { return "test"; }
        @Override public Optional<String> getCatalog() { return Optional.of("test_catalog"); }
        @Override public Optional<String> getSchema() { return Optional.empty(); }
        @Override public Map<String, String> getProperties() { return Map.of(); }
    };

    private static final Schema TEST_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    @BeforeEach
    void setUp() {
        HadoopTestGuard.assumeHadoopCompatible();
        catalog = createHadoopCatalog();
        metadata = new IcebergMetadata(catalog, "test_catalog");
    }

    @Test
    void applyLimit_returnsHandleWithLimit() {
        createNamespace("db");
        catalog.createTable(TableIdentifier.of("db", "orders"), TEST_SCHEMA);

        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName("db", "orders"));
        assertThat(handle).isNotNull();

        Optional<ConnectorTableHandle> result = metadata.applyLimit(SESSION, handle, 10);

        assertThat(result).isPresent();
        IcebergTableHandle limitHandle = (IcebergTableHandle) result.get();
        assertThat(limitHandle.pushedLimit()).isEqualTo(10);
        assertThat(limitHandle.hasLimit()).isTrue();
    }

    @Test
    void applyLimit_smallerLimitReplacesLarger() {
        createNamespace("db2");
        catalog.createTable(TableIdentifier.of("db2", "items"), TEST_SCHEMA);

        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName("db2", "items"));
        Optional<ConnectorTableHandle> first = metadata.applyLimit(SESSION, handle, 100);
        assertThat(first).isPresent();

        Optional<ConnectorTableHandle> second = metadata.applyLimit(SESSION, first.get(), 10);
        assertThat(second).isPresent();
        assertThat(((IcebergTableHandle) second.get()).pushedLimit()).isEqualTo(10);
    }

    @Test
    void applyLimit_largerLimitIsIgnored() {
        createNamespace("db3");
        catalog.createTable(TableIdentifier.of("db3", "products"), TEST_SCHEMA);

        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName("db3", "products"));
        Optional<ConnectorTableHandle> first = metadata.applyLimit(SESSION, handle, 10);
        assertThat(first).isPresent();

        Optional<ConnectorTableHandle> second = metadata.applyLimit(SESSION, first.get(), 100);
        assertThat(second).isEmpty();
    }

    @Test
    void applyFilter_emptyConstraintReturnsEmpty() {
        createNamespace("db4");
        catalog.createTable(TableIdentifier.of("db4", "events"), TEST_SCHEMA);

        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName("db4", "events"));
        Optional<ConnectorTableHandle> result = metadata.applyFilter(
                SESSION, handle, org.pangrova.spi.connector.metadata.Constraint.EMPTY);

        assertThat(result).isEmpty();
    }

    @Test
    void applyTopN_absorbsLimitPortion() {
        createNamespace("db5");
        catalog.createTable(TableIdentifier.of("db5", "metrics"), TEST_SCHEMA);

        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName("db5", "metrics"));
        assertThat(handle).isNotNull();

        var columnHandles = metadata.getColumnHandles(SESSION, handle);
        var idColumn = columnHandles.get("id");

        var topN = new org.pangrova.spi.connector.metadata.TopNInfo(
                java.util.List.of(new org.pangrova.spi.connector.metadata.TopNInfo.SortColumn(
                        idColumn, org.pangrova.spi.connector.metadata.TopNInfo.SortOrder.ASC_NULLS_LAST)),
                5);

        Optional<ConnectorTableHandle> result = metadata.applyTopN(SESSION, handle, topN);

        assertThat(result).isPresent();
        IcebergTableHandle topNHandle = (IcebergTableHandle) result.get();
        assertThat(topNHandle.pushedLimit()).isEqualTo(5);
        assertThat(topNHandle.hasLimit()).isTrue();
    }

    private Catalog createHadoopCatalog() {
        org.apache.iceberg.hadoop.HadoopCatalog hadoopCatalog = new org.apache.iceberg.hadoop.HadoopCatalog();
        hadoopCatalog.setConf(new org.apache.hadoop.conf.Configuration());
        hadoopCatalog.initialize("test_catalog", Map.of(
                "warehouse", warehouse.toString()));
        return hadoopCatalog;
    }

    private void createNamespace(String name) {
        ((SupportsNamespaces) catalog).createNamespace(Namespace.of(name));
    }
}
