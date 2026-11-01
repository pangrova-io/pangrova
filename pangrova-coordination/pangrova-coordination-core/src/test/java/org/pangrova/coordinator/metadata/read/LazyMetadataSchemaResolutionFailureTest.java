package org.pangrova.coordinator.metadata.read;

import org.pangrova.coordinator.metadata.MetadataPlanningTestSessions;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.core.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.core.catalog.handle.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.exception.MetadataResolutionException;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.catalog.MetadataStore;
import org.pangrova.metadata.api.catalog.SyncResult;
import org.pangrova.spi.connector.plugin.Connector;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.apache.calcite.schema.Table;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that metadata backend failure returns "metadata service unavailable" error
 * (MetadataResolutionException), not "table not found" (empty map / null).
 *
 * <p>Validates: Requirements 1.35, 1.36, 2.35, 2.36</p>
 */
class LazyMetadataSchemaResolutionFailureTest {

    private static final String CATALOG = "test_catalog";
    private static final String SCHEMA = "test_schema";

    @Test
    void listTablesBackendFailureThrowsMetadataResolutionException() {
        CatalogRegistry registry = buildRegistry(
                new FailingListTablesMetadata(new RuntimeException("Connection refused to metadata backend")));

        var lazySchema = new LazyMetadataSchema(registry, CATALOG, SCHEMA, MetadataPlanningTestSessions.session());

        assertThatThrownBy(lazySchema::getTableMap)
                .isInstanceOf(MetadataResolutionException.class)
                .hasMessageContaining("metadata backend")
                .hasMessageContaining(CATALOG)
                .hasMessageContaining(SCHEMA)
                .satisfies(ex -> {
                    var mre = (MetadataResolutionException) ex;
                    assertThat(mre.getErrorCode()).isEqualTo(PangrovaErrorCode.METADATA_RESOLUTION_FAILED);
                    assertThat(mre.getCatalogName()).isEqualTo(CATALOG);
                    assertThat(mre.getSchemaName()).isEqualTo(SCHEMA);
                    assertThat(mre.getCause()).isInstanceOf(RuntimeException.class);
                });
    }

    @Test
    void singleTableResolutionFailureAggregatesErrors() {
        CatalogRegistry registry = buildRegistry(
                new FailingGetTableHandleMetadata(new RuntimeException("Timeout reading table metadata")));

        var lazySchema = new LazyMetadataSchema(registry, CATALOG, SCHEMA, MetadataPlanningTestSessions.session());

        assertThatThrownBy(lazySchema::getTableMap)
                .isInstanceOf(MetadataResolutionException.class)
                .satisfies(ex -> {
                    var mre = (MetadataResolutionException) ex;
                    assertThat(mre.getErrorCode()).isEqualTo(PangrovaErrorCode.METADATA_RESOLUTION_FAILED);
                    assertThat(mre.getCatalogName()).isEqualTo(CATALOG);
                    assertThat(mre.getSchemaName()).isEqualTo(SCHEMA);
                });
    }

    @Test
    void backendFailureDoesNotReturnEmptyMap() {
        CatalogRegistry registry = buildRegistry(
                new FailingListTablesMetadata(new RuntimeException("Network unreachable")));

        var lazySchema = new LazyMetadataSchema(registry, CATALOG, SCHEMA, MetadataPlanningTestSessions.session());

        // Backend failure must NOT silently return empty map
        // (which would cause "table not found" semantic drift)
        assertThatThrownBy(lazySchema::getTableMap)
                .isInstanceOf(MetadataResolutionException.class);
    }

    @Test
    void normalTableNotFoundReturnsAbsentFromMap() {
        // When a table genuinely doesn't exist (getTableHandle returns null),
        // behavior is unchanged: no exception, table absent from map
        CatalogRegistry registry = buildRegistry(new MissingTableMetadata());

        var lazySchema = new LazyMetadataSchema(registry, CATALOG, SCHEMA, MetadataPlanningTestSessions.session());

        Map<String, Table> tableMap = lazySchema.getTableMap();
        assertThat(tableMap).doesNotContainKey("missing_table");
    }

    // ── Registry construction ──

    private CatalogRegistry buildRegistry(ConnectorMetadata metadata) {
        Connector connector = new TestConnector(metadata);
        CatalogDescriptor descriptor = new CatalogDescriptor(
                CATALOG, CatalogType.CONNECTOR, "test-provider",
                CatalogState.ACTIVE, Map.of(), "", null);

        EmptyMetadataStore store = new EmptyMetadataStore();
        CatalogRegistry registry = new CatalogRegistry(
                store,
                desc -> new ConnectorCatalogHandle(desc, connector),
                null,
                null);
        registry.register(descriptor);
        return registry;
    }

    // ── Test implementations (real, no mocks) ──

    /** Connector that delegates to a provided ConnectorMetadata. */
    private static final class TestConnector implements Connector {
        private final ConnectorMetadata metadata;

        TestConnector(ConnectorMetadata metadata) {
            this.metadata = metadata;
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            return metadata;
        }

        @Override
        public ConnectorSplitManager getSplitManager() {
            throw new UnsupportedOperationException("Not needed for this test");
        }

        @Override
        public ConnectorPageSourceProvider getPageSourceProvider() {
            throw new UnsupportedOperationException("Not needed for this test");
        }
    }

    /** ConnectorMetadata that throws on listTables (simulates backend unavailability). */
    private static final class FailingListTablesMetadata implements ConnectorMetadata {
        private final RuntimeException failure;

        FailingListTablesMetadata(RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public List<String> listSchemaNames(ConnectorSession session) {
            throw failure;
        }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
            throw failure;
        }

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
            throw failure;
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
            throw failure;
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
            throw failure;
        }

        @Override
        public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table) {
            throw failure;
        }
    }

    /** ConnectorMetadata that lists tables but fails on getTableHandle (per-table failure). */
    private static final class FailingGetTableHandleMetadata implements ConnectorMetadata {
        private final RuntimeException failure;

        FailingGetTableHandleMetadata(RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public List<String> listSchemaNames(ConnectorSession session) {
            return List.of(SCHEMA);
        }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
            return List.of(new SchemaTableName(SCHEMA, "orders"));
        }

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
            throw failure;
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
            throw failure;
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
            return Map.of();
        }

        @Override
        public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table) {
            return TableStatistics.UNKNOWN;
        }
    }

    /** ConnectorMetadata where table genuinely doesn't exist (getTableHandle returns null). */
    private static final class MissingTableMetadata implements ConnectorMetadata {
        @Override
        public List<String> listSchemaNames(ConnectorSession session) {
            return List.of(SCHEMA);
        }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
            return List.of(new SchemaTableName(SCHEMA, "missing_table"));
        }

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
            return null;
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
            return null;
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
            return Map.of();
        }

        @Override
        public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table) {
            return TableStatistics.UNKNOWN;
        }
    }

    /** Minimal MetadataStore that stores nothing (sufficient for test registry bootstrap). */
    private static final class EmptyMetadataStore implements MetadataStore {
        @Override public void saveTable(TableMetadata metadata) { }
        @Override public Optional<TableMetadata> loadTable(String catalog, String schema, String table) { return Optional.empty(); }
        @Override public void deleteTable(String catalog, String schema, String table) { }
        @Override public List<QualifiedTableRef> searchTables(String pattern) { return List.of(); }
        @Override public void saveCatalogDescriptor(CatalogDescriptor descriptor) { }
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String name) { return Optional.empty(); }
        @Override public void deleteCatalogDescriptor(String name) { }
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.of(); }
        @Override public Optional<Instant> getLastSyncTime(String catalogName) { return Optional.empty(); }
        @Override public void updateSyncTime(String catalogName, Instant syncTime) { }
        @Override public void recordSyncHistory(SyncResult result) { }
        @Override public void saveSchemas(String catalogName, List<String> schemas) { }
    }
}
