package org.pangrova.coordinator.integration.metadata.query;

import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

import org.pangrova.metadata.catalog.CatalogMetadata;
import org.pangrova.metadata.cache.TieredMetadataCache;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.api.catalog.ConnectorHandleFactory;
import org.pangrova.metadata.event.MetadataEventDispatcher;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises closure, draining and release of the exact query scope retained by the catalog registry. */
class CatalogMetadataQueryEndCleanupTest {
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
    private CatalogMetadata service;
    private CatalogRegistry registry;
    private MetadataEventDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        connectorMetadata = new RecordingConnectorMetadata();
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

        service = new CatalogMetadata(
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
    void closedAuthorityReleasesTheActiveScope() {
        service.beginQuery(query("q-normal").metadataSession());
        assertThat(service.queryScopes().activeQueryCount()).isEqualTo(1);

        query("q-normal").seal();
        service.cleanupQuery(query("q-normal").authority());
        assertThat(service.queryScopes().activeQueryCount()).isZero();
    }

    @Test
    void plannerFirstResolutionPinsSplitProvidersAcrossAlterAndEagerMetadataBegin() {
        var first = query("before-alter");
        var session = org.pangrova.metadata.connector.DefaultConnectorSession.fromSessionContext(
                first.metadataSession(), "test_catalog", "default");
        var before = registry.queryCatalog("test_catalog", session);
        registry.alterProperties("test_catalog", null, Map.of("updated", "true"));
        service.beginQuery(first.metadataSession());
        assertThat(registry.queryCatalog("test_catalog", session).handle()).isSameAs(before.handle());
        var next = query("after-alter");
        var newSession = org.pangrova.metadata.connector.DefaultConnectorSession.fromSessionContext(
                next.metadataSession(), "test_catalog", "default");
        service.beginQuery(next.metadataSession());
        assertThat(registry.queryCatalog("test_catalog", newSession).handle().getDescriptor().revision()).isEqualTo(2);
        cleanup("before-alter");
        cleanup("after-alter");
    }

    @Test
    void openAuthorityCannotReleaseTheCatalogScope() {
        var issued = query("q-open");
        service.beginQuery(issued.metadataSession());
        assertThatThrownBy(() -> service.cleanupQuery(issued.authority())).isInstanceOf(IllegalStateException.class);
        assertThat(service.queryScopes().activeQueryCount()).isOne();
        assertThat(connectorMetadata.cleanupQueryIds).isEmpty();
        cleanup("q-open");
        assertThat(connectorMetadata.cleanupQueryIds).containsExactly("q-open");
    }

    @Test
    void releaseInvokesTheRetainedConnector() {
        service.beginQuery(query("q-cleanup").metadataSession());
        query("q-cleanup").seal();
        service.cleanupQuery(query("q-cleanup").authority());

        assertThat(connectorMetadata.cleanupQueryIds).containsExactly("q-cleanup");
    }

    @Test
    void duplicateReleaseIsIdempotent() {
        service.beginQuery(query("q-idem").metadataSession());
        query("q-idem").seal();
        service.cleanupQuery(query("q-idem").authority());
        query("q-idem").seal();
        service.cleanupQuery(query("q-idem").authority());

        assertThat(service.queryScopes().activeQueryCount()).isZero();
        assertThat(connectorMetadata.cleanupQueryIds).containsExactly("q-idem");
    }

    @Test
    void differentAuthorityWithTheSameQueryIdCannotReleaseTheRetainedScope() {
        var issued = query("q-owner");
        service.beginQuery(issued.metadataSession());
        var other = new org.pangrova.coordinator.task.TestConnectorQuery("q-owner");
        other.seal();
        assertThatThrownBy(() -> service.cleanupQuery(other.authority()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("lifetime");
        assertThat(connectorMetadata.cleanupQueryIds).isEmpty();
        cleanup("q-owner");
        assertThat(connectorMetadata.cleanupQueryIds).containsExactly("q-owner");
        other.retire();
    }

    @Test
    void lateReleaseAfterRetirementIsIdempotent() {
        service.beginQuery(query("q-reverse").metadataSession());

        cleanup("q-reverse");
        query("q-reverse").seal();
        service.cleanupQuery(query("q-reverse").authority());

        assertThat(service.queryScopes().activeQueryCount()).isZero();
        assertThat(connectorMetadata.cleanupQueryIds).containsExactly("q-reverse");
    }

    @Test
    void sealedAuthorityWithNoCatalogScopeIsIdempotent() {
        query("never-started").seal();
        service.cleanupQuery(query("never-started").authority());

        assertThat(service.queryScopes().activeQueryCount()).isZero();
        assertThat(connectorMetadata.cleanupQueryIds).isEmpty();
    }

    @Test
    void multipleQueriesCleanedIndependently() {
        service.beginQuery(query("q-a").metadataSession());
        service.beginQuery(query("q-b").metadataSession());
        service.beginQuery(query("q-c").metadataSession());
        assertThat(service.queryScopes().activeQueryCount()).isEqualTo(3);

        query("q-a").seal();
        service.cleanupQuery(query("q-a").authority());
        assertThat(service.queryScopes().activeQueryCount()).isEqualTo(2);

        query("q-b").seal();
        service.cleanupQuery(query("q-b").authority());
        assertThat(service.queryScopes().activeQueryCount()).isEqualTo(1);

        query("q-c").seal();
        service.cleanupQuery(query("q-c").authority());
        assertThat(service.queryScopes().activeQueryCount()).isZero();
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
        @Override public boolean deleteCatalogDescriptor(CatalogDeploymentIdentity expected) { return catalogs.removeIf(d -> d.identity().equals(expected)); }
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.copyOf(catalogs); }

    }
}
