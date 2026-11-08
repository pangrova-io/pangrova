package org.pangrova.coordinator.integration.metadata.service;

import org.pangrova.connector.spi.metadata.ConnectorMetadata;
import org.pangrova.connector.spi.metadata.ConnectorSplitManager;

import org.pangrova.metadata.service.DefaultMetadataService;
import org.pangrova.metadata.cache.TieredMetadataCache;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.spi.catalog.ConnectorHandleFactory;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.metadata.schema.SchemaEvolutionValidator;
import org.pangrova.persistence.catalog.api.catalog.CatalogDeploymentRepository;
import org.pangrova.connector.spi.plugin.Connector;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.spi.data.ConnectorPageSourceProvider;

import org.pangrova.connector.api.metadata.table.ColumnHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Unit tests for DefaultMetadataService query-level transaction management.
 *
 * <p><b>Validates: Requirements 6.1, 6.2, 6.3, 6.4, 6.5</b></p>
 */
class DefaultMetadataServiceTransactionTest {
    private final java.util.Map<String, org.pangrova.coordinator.task.TestConnectorQuery> queries =
            new java.util.LinkedHashMap<>();

    private org.pangrova.coordinator.task.TestConnectorQuery query(String id) {
        return queries.computeIfAbsent(id, org.pangrova.coordinator.task.TestConnectorQuery::new);
    }

    private void cleanup(String id) {
        var lifetime = query(id);
        lifetime.seal();
        service.cleanupQuery(lifetime.authority());
        lifetime.retire();
    }

    private RecordingConnectorMetadata connectorMetadata;
    private DefaultMetadataService service;
    private CatalogRegistry registry;
    private MetadataEventDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        connectorMetadata = new RecordingConnectorMetadata();
        TestCatalogDeployments store = new TestCatalogDeployments();
        dispatcher = new MetadataEventDispatcher();
        ConnectorHandleFactory chf = desc ->
                new ConnectorCatalogHandle(desc, new TestConnector(connectorMetadata));
        registry = new CatalogRegistry(store, chf, null, dispatcher);
        CatalogDescriptor descriptor = new CatalogDescriptor(
                "test_catalog", CatalogType.CONNECTOR, "test",
                CatalogState.ACTIVE, Map.of(), "", Set.of());
        registry.register(descriptor);
        registry.initialize();

        service = new DefaultMetadataService(
                registry,
                new TieredMetadataCache(),
                dispatcher,
                new SchemaEvolutionValidator());
    }

    @AfterEach
    void tearDown() {
        registry.shutdown();
        dispatcher.shutdown();
    }

    @Test
    void beginQueryDelegatesToConnectorMetadata() {
        service.beginQuery(query("q-001").metadataSession());

        assertThat(connectorMetadata.beginQueryIds).containsExactly("q-001");
    }

    @Test
    void cleanupQueryDelegatesToConnectorMetadata() {
        service.beginQuery(query("q-002").metadataSession());
        cleanup("q-002");

        assertThat(connectorMetadata.cleanupQueryIds).containsExactly("q-002");
    }

    @Test
    void beginQueryIdempotentForSameQueryId() {
        service.beginQuery(query("q-003").metadataSession());
        service.beginQuery(query("q-003").metadataSession());

        assertThat(connectorMetadata.beginQueryIds).containsExactly("q-003");
    }

    @Test
    void cleanupQueryIdempotentForSameQueryId() {
        service.beginQuery(query("q-004").metadataSession());
        cleanup("q-004");
        cleanup("q-004");

        assertThat(connectorMetadata.cleanupQueryIds).containsExactly("q-004");
    }

    @Test
    void cleanupQueryOnUnknownQueryIdIsNoOp() {
        assertThatCode(() -> cleanup("never-started"))
                .doesNotThrowAnyException();
        assertThat(connectorMetadata.cleanupQueryIds).isEmpty();
    }

    @Test
    void multipleQueriesAreIndependent() {
        service.beginQuery(query("q-a").metadataSession());
        service.beginQuery(query("q-b").metadataSession());
        cleanup("q-a");

        assertThat(connectorMetadata.beginQueryIds).containsExactly("q-a", "q-b");
        assertThat(connectorMetadata.cleanupQueryIds).containsExactly("q-a");
    }

    @Test
    void queryCanBeRestartedAfterCleanup() {
        service.beginQuery(query("q-restart").metadataSession());
        cleanup("q-restart");
        var prior = queries.remove("q-restart");
        assertThat(prior.authority().isClosed()).isTrue();
        service.beginQuery(query("q-restart").metadataSession());

        assertThat(connectorMetadata.beginQueryIds).containsExactly("q-restart", "q-restart");
    }

    // ── Test infrastructure ──

    static final class RecordingConnectorMetadata implements ConnectorMetadata {
        final CopyOnWriteArrayList<String> beginQueryIds = new CopyOnWriteArrayList<>();
        final CopyOnWriteArrayList<String> cleanupQueryIds = new CopyOnWriteArrayList<>();

        @Override
        public void beginQuery(org.pangrova.connector.api.query.ConnectorQueryAuthority authority) {
            String queryId = authority.queryId();
            beginQueryIds.add(queryId);
        }

        @Override
        public void cleanupQuery(org.pangrova.connector.api.query.ConnectorQueryAuthority authority) {
            String queryId = authority.queryId();
            cleanupQueryIds.add(queryId);
        }

        @Override
        public List<String> listSchemaNames(ConnectorSession session) { return List.of(); }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schema) {
            return List.of();
        }

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName name) {
            return null;
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle h) {
            return null;
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
        @Override public void deleteCatalogDescriptor(String n) { catalogs.removeIf(d -> d.name().equals(n)); }
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.copyOf(catalogs); }

    }
}
