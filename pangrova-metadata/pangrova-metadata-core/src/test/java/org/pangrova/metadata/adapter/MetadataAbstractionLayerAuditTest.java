package org.pangrova.metadata.adapter;

import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.catalog.handle.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.catalog.handle.federation.FederationCatalogHandle;
import org.pangrova.metadata.catalog.handle.internal.InternalCatalogHandle;
import org.pangrova.metadata.api.object.ColumnMetadata;
import org.pangrova.metadata.api.object.ColumnType;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.connector.spi.plugin.Connector;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.transaction.ConnectorTransactionHandle;
import org.pangrova.connector.spi.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.connector.spi.metadata.ConnectorSplitManager;
import org.pangrova.connector.spi.metadata.ConnectorSplitSource;
import org.pangrova.connector.api.metadata.Constraint;
import org.pangrova.connector.api.metadata.DynamicFilter;
import org.pangrova.connector.api.metadata.HealthCheckResult;
import org.pangrova.connector.api.metadata.table.ColumnHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.pangrova.connector.spi.data.ConnectorPageSource;
import org.pangrova.connector.spi.data.ConnectorPageSourceProvider;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Audit test for Metadata abstraction layers.
 *
 * <p>Validates that the three-layer hierarchy is justified:
 * <ul>
 *   <li>{@link FederatedCatalog} — SPI for external catalog federation</li>
 *   <li>{@link FederationConnectorMetadata} — bridge from FederatedCatalog to ConnectorMetadata SPI</li>
 *   <li>{@link CatalogHandle} — unified handle with three implementations for different catalog types</li>
 * </ul>
 *
 * <p><b>Validates: Requirements 3.6</b></p>
 */
class MetadataAbstractionLayerAuditTest {

    @Test
    void catalogHandleHierarchy_threeTypesServeDifferentPurposes() {
        // ConnectorCatalogHandle: delegates to Connector SPI
        CatalogDescriptor connDesc = createDescriptor("iceberg_cat", CatalogType.CONNECTOR);
        Connector stubConnector = new StubConnector();
        CatalogHandle connHandle = new ConnectorCatalogHandle(connDesc, stubConnector);

        assertNotNull(connHandle.getMetadata(StubConnectorSession.INSTANCE));
        assertNotNull(connHandle.getSplitManager());
        assertNotNull(connHandle.getPageSourceProvider());

        // FederationCatalogHandle: delegates to FederatedCatalog via FederationConnectorMetadata
        CatalogDescriptor fedDesc = createDescriptor("external_cat", CatalogType.FEDERATION);
        FederatedCatalog stubAdapter = new StubFederatedCatalog();
        CatalogHandle fedHandle = new FederationCatalogHandle(fedDesc, stubAdapter);

        ConnectorMetadata fedMeta = fedHandle.getMetadata(StubConnectorSession.INSTANCE);
        assertInstanceOf(FederationConnectorMetadata.class, fedMeta);
        // Federation catalogs are read-only: split/page operations not supported
        assertThrows(Exception.class, fedHandle::getSplitManager);
        assertThrows(Exception.class, fedHandle::getPageSourceProvider);

        // InternalCatalogHandle: system catalog, no external delegation
        CatalogDescriptor intDesc = createDescriptor("system", CatalogType.INTERNAL);
        CatalogHandle intHandle = new InternalCatalogHandle(intDesc);

        assertThrows(Exception.class, () -> intHandle.getMetadata(StubConnectorSession.INSTANCE));
        assertTrue(intHandle.checkHealth().isHealthy());
    }

    @Test
    void federationConnectorMetadata_bridgesFederatedCatalogToConnectorMetadataSpi() {
        StubFederatedCatalog adapter = new StubFederatedCatalog();
        FederationConnectorMetadata metadata = new FederationConnectorMetadata(adapter);

        List<String> schemas = metadata.listSchemaNames(StubConnectorSession.INSTANCE);
        assertEquals(List.of("default"), schemas);

        List<SchemaTableName> tables = metadata.listTables(
                StubConnectorSession.INSTANCE, Optional.of("default"));
        assertEquals(1, tables.size());
        assertEquals("orders", tables.get(0).table());

        ConnectorTableHandle handle = metadata.getTableHandle(
                StubConnectorSession.INSTANCE, new SchemaTableName("default", "orders"));
        assertNotNull(handle);

        ConnectorTableMetadata tableMeta = metadata.getTableMetadata(
                StubConnectorSession.INSTANCE, handle);
        assertNotNull(tableMeta);

        TableStatistics stats = metadata.getTableStatistics(
                StubConnectorSession.INSTANCE, handle);
        assertNotNull(stats);
    }

    @Test
    void federationConnectorMetadata_isReadOnly() {
        FederationConnectorMetadata metadata = new FederationConnectorMetadata(new StubFederatedCatalog());
        ConnectorSession session = StubConnectorSession.INSTANCE;

        // DDL operations should throw UnsupportedOperationException (inherited defaults)
        assertThrows(UnsupportedOperationException.class,
                () -> metadata.createTable(session, null));
        assertThrows(UnsupportedOperationException.class,
                () -> metadata.beginInsert(session, null));
        assertThrows(UnsupportedOperationException.class,
                () -> metadata.dropTable(session, null));
    }

    @Test
    void catalogHandleState_managedConsistently() {
        CatalogDescriptor desc = createDescriptor("test", CatalogType.CONNECTOR);
        ConnectorCatalogHandle handle = new ConnectorCatalogHandle(desc, new StubConnector());

        assertEquals(CatalogState.ACTIVE, handle.getState());
        handle.setState(CatalogState.SUSPENDED);
        assertEquals(CatalogState.SUSPENDED, handle.getState());
    }

    private CatalogDescriptor createDescriptor(String name, CatalogType type) {
        return new CatalogDescriptor(name, type, "test-provider",
                CatalogState.ACTIVE, Map.of(), "", Set.of());
    }

    // --- Stubs (minimal, no mock frameworks) ---

    private enum StubConnectorSession implements ConnectorSession {
        INSTANCE;

        @Override
        public java.util.Optional<org.pangrova.connector.api.query.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

        @Override
        public String getUser() {
            return "test-user";
        }

        @Override
        public Optional<String> getCatalog() {
            return Optional.empty();
        }

        @Override
        public Optional<String> getSchema() {
            return Optional.empty();
        }

        @Override
        public Map<String, String> getProperties() {
            return Map.of();
        }
    }

    private static class StubFederatedCatalog implements FederatedCatalog {
        @Override
        public List<String> listSchemas() {
            return List.of("default");
        }

        @Override
        public List<String> listTables(String schema) {
            return List.of("orders");
        }

        @Override
        public TableMetadata getTable(String schema, String table) {
            return new TableMetadata(
                    "test-catalog", schema, table,
                    List.of(new ColumnMetadata("id", ColumnType.of("BIGINT"), false, "", Map.of())),
                    Map.of(), List.of(), Map.of(), null, null);
        }

        @Override
        public TableStatistics getTableStatistics(String schema, String table) {
            return new TableStatistics(100L, 1024L, -1, Map.of());
        }

        @Override
        public org.pangrova.metadata.api.catalog.MetadataChangeBatch readChangesSince(Instant since) {
            return new org.pangrova.metadata.api.catalog.MetadataChangeBatch(List.of(), java.util.Optional.empty());
        }

        @Override
        public void close() {
            // no-op for test
        }
    }

    private static class StubConnector implements Connector {
        private final StubConnectorMetadata metadata = new StubConnectorMetadata();

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            return metadata;
        }

        @Override
        public ConnectorSplitManager getSplitManager() {
            return new StubSplitManager();
        }

        @Override
        public ConnectorPageSourceProvider getPageSourceProvider() {
            return new StubPageSourceProvider();
        }

        @Override
        public HealthCheckResult healthCheck() {
            return HealthCheckResult.healthy();
        }

        @Override
        public void shutdown() {
            // no-op
        }
    }

    private static class StubConnectorMetadata implements ConnectorMetadata {
        @Override
        public List<String> listSchemaNames(ConnectorSession session) {
            return List.of("default");
        }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
            return List.of(new SchemaTableName("default", "t1"));
        }

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
            return null;
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
            return new ConnectorTableMetadata(new SchemaTableName("default", "t1"), List.of(), Map.of());
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
            return Map.of();
        }

        @Override
        public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table) {
            return new TableStatistics(0L, 0L, 0, Map.of());
        }
    }

    private static class StubSplitManager implements ConnectorSplitManager {
        @Override
        public ConnectorSplitSource getSplits(
                ConnectorTransactionHandle transaction,
                ConnectorSession session,
                ConnectorTableHandle table,
                DynamicFilter dynamicFilter,
                Constraint constraint) {
            return null;
        }
    }

    private static class StubPageSourceProvider implements ConnectorPageSourceProvider {
        @Override
        public ConnectorPageSource createPageSource(
                ConnectorTransactionHandle transaction,
                ConnectorSession session,
                ConnectorSplit split,
                ConnectorTableHandle table,
                List<ColumnHandle> columns,
                DynamicFilter dynamicFilter) {
            return null;
        }
    }
}
