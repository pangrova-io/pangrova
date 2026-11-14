package org.pangrova.metadata.service;

import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

import org.pangrova.metadata.api.resolution.ResolvedTable;
import org.pangrova.metadata.api.session.MetadataSessionContext;

import org.pangrova.metadata.cache.TieredMetadataCache;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.api.catalog.ConnectorHandleFactory;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.schema.SchemaEvolutionValidator;
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
import org.pangrova.types.enums.PangrovaType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that MetadataCache is wired into the resolveTable main path,
 * verifying cache hits avoid JDBC queries and DDL invalidates the cache.
 */
class DefaultMetadataServiceCacheTest {

    private CountingConnectorMetadata connectorMetadata;
    private DefaultMetadataService service;
    private CatalogRegistry registry;
    private MetadataEventDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        connectorMetadata = new CountingConnectorMetadata();
        TestCatalogDeployments store = new TestCatalogDeployments();
        dispatcher = new MetadataEventDispatcher();
        ConnectorHandleFactory chf = desc ->
                new ConnectorCatalogHandle(desc, new TestConnector(connectorMetadata));
        registry = new CatalogRegistry(memoryTableStores(), store, chf, null, dispatcher);
        CatalogDescriptor descriptor = new CatalogDescriptor(
                "test_catalog", CatalogType.CONNECTOR, "test",
                CatalogState.ACTIVE, Map.of(), "", Set.of());
        registry.register(descriptor);
        registry.initialize();

        service = new DefaultMetadataService(
                registry,
                new TieredMetadataCache(),
                dispatcher,
                new SchemaEvolutionValidator(),
                10_000L,
                Duration.ofMinutes(5));
    }

    @AfterEach
    void tearDown() {
        registry.shutdown();
        dispatcher.shutdown();
    }

    @Test
    void resolveTableTwiceSecondCallHitsCache() {
        QualifiedTableRef tableRef = new QualifiedTableRef("test_catalog", "public", "orders");
        MetadataSessionContext session = testSession();

        ResolvedTable first = service.resolveTable(session, tableRef);
        assertThat(first).isNotNull();
        assertThat(first.tableRef()).isEqualTo(tableRef);
        assertThat(connectorMetadata.getTableMetadataCallCount()).isEqualTo(1);

        ResolvedTable second = service.resolveTable(session, tableRef);
        assertThat(second).isNotNull();
        assertThat(second.tableRef()).isEqualTo(tableRef);
        // Second call must NOT hit the connector — cache serves the result
        assertThat(connectorMetadata.getTableMetadataCallCount()).isEqualTo(1);

        // Both calls return the same resolved table
        assertThat(second).isSameAs(first);
    }

    @Test
    void invalidateTableCachesForcesReResolution() {
        QualifiedTableRef tableRef = new QualifiedTableRef("test_catalog", "public", "orders");
        MetadataSessionContext session = testSession();

        service.resolveTable(session, tableRef);
        assertThat(connectorMetadata.getTableMetadataCallCount()).isEqualTo(1);

        // Invalidate simulates DDL (CREATE/ALTER/DROP TABLE)
        service.invalidateTableCaches(tableRef);

        service.resolveTable(session, tableRef);
        // After invalidation, connector must be called again
        assertThat(connectorMetadata.getTableMetadataCallCount()).isEqualTo(2);
    }

    @Test
    void invalidateCatalogCachesClearsAllTablesInCatalog() {
        MetadataSessionContext session = testSession();
        QualifiedTableRef orders = new QualifiedTableRef("test_catalog", "public", "orders");
        QualifiedTableRef users = new QualifiedTableRef("test_catalog", "public", "users");

        service.resolveTable(session, orders);
        service.resolveTable(session, users);
        assertThat(connectorMetadata.getTableMetadataCallCount()).isEqualTo(2);

        // Invalidate entire catalog
        service.invalidateCatalogCaches("test_catalog");

        service.resolveTable(session, orders);
        service.resolveTable(session, users);
        // Both tables must be re-resolved from connector
        assertThat(connectorMetadata.getTableMetadataCallCount()).isEqualTo(4);
    }

    private static MetadataSessionContext testSession() {
        return new MetadataSessionContext(
                "test-user", "test_catalog", "public", "UTC", "en",
                Map.of(), "default", "", "test-query-001");
    }

    @Test
    void resolutionCacheSeparatesTenantPrincipalPolicyAndConnectorProperties() {
        var table = new QualifiedTableRef("test_catalog", "public", "orders");
        var firstSession = scopedSession("tenant-a", "alice", "policy-1", Map.of("role", "reader"));
        var first = service.resolveTable(firstSession, table);
        assertThat(service.resolveTable(firstSession, table)).isSameAs(first);
        var sessions = List.of(
                scopedSession("tenant-b", "alice", "policy-1", Map.of("role", "reader")),
                scopedSession("tenant-a", "bob", "policy-1", Map.of("role", "reader")),
                scopedSession("tenant-a", "alice", "policy-2", Map.of("role", "reader")),
                scopedSession("tenant-a", "alice", "policy-1", Map.of("role", "admin")));
        sessions.forEach(session -> assertThat(service.resolveTable(session, table)).isNotSameAs(first));
        assertThat(connectorMetadata.getTableMetadataCallCount()).isEqualTo(5);
        service.invalidateTableCaches(table);
        service.resolveTable(firstSession, table);
        sessions.forEach(session -> service.resolveTable(session, table));
        assertThat(connectorMetadata.getTableMetadataCallCount()).isEqualTo(10);
    }

    private static MetadataSessionContext scopedSession(
            String tenant, String user, String token, Map<String, String> properties) {
        return new MetadataSessionContext(user, "test_catalog", "public", "UTC", "en",
                properties, tenant, token, "query");
    }

    // ── Test infrastructure ──

    static final class CountingConnectorMetadata implements ConnectorMetadata {
        private final AtomicInteger getTableMetadataCount = new AtomicInteger();

        int getTableMetadataCallCount() {
            return getTableMetadataCount.get();
        }

        @Override
        public void beginQuery(org.pangrova.connector.api.query.ConnectorQueryAuthority authority) { }

        @Override
        public void cleanupQuery(org.pangrova.connector.api.query.ConnectorQueryAuthority authority) { }

        @Override
        public List<String> listSchemaNames(ConnectorSession session) {
            return List.of("public");
        }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schema) {
            return List.of(new SchemaTableName("public", "orders"),
                    new SchemaTableName("public", "users"));
        }

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName name) {
            return new StubTableHandle(name.schema(), name.table());
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle h) {
            getTableMetadataCount.incrementAndGet();
            StubTableHandle handle = (StubTableHandle) h;
            SchemaTableName name = new SchemaTableName(handle.schema(), handle.table());
            return new ConnectorTableMetadata(
                    name,
                    List.of("id", "name"),
                    List.of(
                            new ConnectorTableMetadata.Column("id", PangrovaType.BIGINT),
                            new ConnectorTableMetadata.Column("name", PangrovaType.VARCHAR)),
                    Map.of());
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle h) {
            return Map.of();
        }

        @Override
        public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle h) {
            return TableStatistics.UNKNOWN;
        }
    }

    record StubTableHandle(String schema, String table) implements ConnectorTableHandle { }

    static final class TestConnector implements Connector {
        private final ConnectorMetadata metadata;

        TestConnector(ConnectorMetadata metadata) {
            this.metadata = metadata;
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) { return metadata; }

        @Override
        public ConnectorSplitManager getSplitManager() { throw new UnsupportedOperationException(); }

        @Override
        public ConnectorPageSourceProvider getPageSourceProvider() { throw new UnsupportedOperationException(); }

        @Override
        public void shutdown() { }
    }

    static final class TestCatalogDeployments implements CatalogDeploymentRepository {
        @Override
        public java.util.Optional<org.pangrova.metadata.api.catalog.CatalogDescriptor>
                findCatalogDescriptorRevision(String name, String incarnationId, long revision) {
            throw new UnsupportedOperationException("This fixture does not store deployment history");
        }

        private final CopyOnWriteArrayList<CatalogDescriptor> catalogs = new CopyOnWriteArrayList<>();

        @Override public void saveCatalogDescriptor(CatalogDescriptor d) {
            catalogs.removeIf(x -> x.name().equals(d.name()));
            catalogs.add(d);
        }
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String n) {
            return catalogs.stream().filter(d -> d.name().equals(n)).findFirst();
        }
        @Override public boolean deleteCatalogDescriptor(CatalogDeploymentIdentity expected) { return catalogs.removeIf(d -> d.identity().equals(expected)); }
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.copyOf(catalogs); }

    }
}
