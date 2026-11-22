package org.pangrova.coordinator.metadata.read;

import org.pangrova.planner.metadata.catalog.LazyMetadataSchema;

import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

import org.pangrova.coordinator.metadata.MetadataPlanningTestSessions;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.api.exception.MetadataResolutionException;
import org.pangrova.persistence.catalog.api.catalog.CatalogDeploymentRepository;
import org.pangrova.connector.api.plugin.Connector;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.data.ConnectorPageSourceProvider;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.ConnectorSplitManager;
import org.pangrova.connector.api.metadata.table.ColumnHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.apache.calcite.schema.Table;
import org.junit.jupiter.api.Test;

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

        assertThatThrownBy(lazySchema::getTableNames)
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
    void tableResolutionFailureSurfacesWhenTheTableIsRequested() {
        CatalogRegistry registry = buildRegistry(
                new FailingGetTableHandleMetadata(new RuntimeException("Timeout reading table metadata")));

        var lazySchema = new LazyMetadataSchema(registry, CATALOG, SCHEMA, MetadataPlanningTestSessions.session());

        assertThatThrownBy(() -> lazySchema.getTable("test_table"))
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
        assertThatThrownBy(() -> lazySchema.getTableMap().entrySet())
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

        EmptyCatalogDeployments store = new EmptyCatalogDeployments();
        CatalogRegistry registry = new CatalogRegistry(memoryTableStores(),
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

    /** Minimal CatalogDeploymentRepository that stores nothing (sufficient for test registry bootstrap). */
    private static final class EmptyCatalogDeployments implements CatalogDeploymentRepository {
        @Override
        public java.util.Optional<org.pangrova.metadata.api.catalog.CatalogDescriptor>
                findCatalogDescriptorRevision(String name, String incarnationId, long revision) {
            throw new UnsupportedOperationException("This fixture does not store deployment history");
        }

        @Override public void saveCatalogDescriptor(CatalogDescriptor descriptor) { }
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String name) { return Optional.empty(); }
        @Override public boolean deleteCatalogDescriptor(CatalogDeploymentIdentity expected) { throw new UnsupportedOperationException("Read-only catalog fixture"); }
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.of(); }

    }
}
