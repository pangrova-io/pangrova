package org.pangrova.coordinator.integration.metadata.service;

import org.pangrova.metadata.service.DefaultMetadataService;
import org.pangrova.metadata.core.cache.TieredMetadataCache;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.core.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.core.catalog.handle.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.core.catalog.handle.connector.ConnectorHandleFactory;
import org.pangrova.metadata.core.event.MetadataEventDispatcher;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.core.schema.SchemaEvolutionValidator;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that DefaultMetadataService cleans up activeQueries via
 * QueryLifecycleListener.onQueryEnd, preventing unbounded Set growth
 * when queries terminate abnormally without calling cleanupQuery.
 *
 * <p><b>Validates: Requirements 9.7</b></p>
 */
class DefaultMetadataServiceQueryEndCleanupTest {
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
        TestMetadataStore store = new TestMetadataStore();
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
    void onQueryEndRemovesActiveQueryAfterNormalCompletion() {
        service.beginQuery(query("q-normal").metadataSession());
        assertThat(service.activeQueryCount()).isEqualTo(1);

        query("q-normal").seal();
        service.onQueryCompleted("q-normal");
        assertThat(service.activeQueryCount()).isZero();
    }

    @Test
    void onQueryEndRemovesActiveQueryAfterFailure() {
        service.beginQuery(query("q-failed").metadataSession());
        assertThat(service.activeQueryCount()).isEqualTo(1);

        query("q-failed").seal();
        service.onQueryFailed("q-failed", new RuntimeException("simulated failure"));
        assertThat(service.activeQueryCount()).isZero();
    }

    @Test
    void onQueryEndTriggersConnectorCleanup() {
        service.beginQuery(query("q-cleanup").metadataSession());
        query("q-cleanup").seal();
        service.onQueryEnd("q-cleanup");

        assertThat(connectorMetadata.cleanupQueryIds).containsExactly("q-cleanup");
    }

    @Test
    void onQueryEndIsIdempotent() {
        service.beginQuery(query("q-idem").metadataSession());
        query("q-idem").seal();
        service.onQueryEnd("q-idem");
        query("q-idem").seal();
        service.onQueryEnd("q-idem");

        assertThat(service.activeQueryCount()).isZero();
        assertThat(connectorMetadata.cleanupQueryIds).containsExactly("q-idem");
    }

    @Test
    void onQueryEndAndCleanupQueryDoNotDoubleClean() {
        service.beginQuery(query("q-double").metadataSession());

        query("q-double").seal();
        service.onQueryEnd("q-double");
        cleanup("q-double");

        assertThat(service.activeQueryCount()).isZero();
        assertThat(connectorMetadata.cleanupQueryIds).containsExactly("q-double");
    }

    @Test
    void cleanupQueryThenOnQueryEndDoesNotDoubleClean() {
        service.beginQuery(query("q-reverse").metadataSession());

        cleanup("q-reverse");
        query("q-reverse").seal();
        service.onQueryEnd("q-reverse");

        assertThat(service.activeQueryCount()).isZero();
        assertThat(connectorMetadata.cleanupQueryIds).containsExactly("q-reverse");
    }

    @Test
    void onQueryEndForUnknownQueryIsNoOp() {
        query("never-started").seal();
        service.onQueryEnd("never-started");

        assertThat(service.activeQueryCount()).isZero();
        assertThat(connectorMetadata.cleanupQueryIds).isEmpty();
    }

    @Test
    void multipleQueriesCleanedIndependently() {
        service.beginQuery(query("q-a").metadataSession());
        service.beginQuery(query("q-b").metadataSession());
        service.beginQuery(query("q-c").metadataSession());
        assertThat(service.activeQueryCount()).isEqualTo(3);

        query("q-a").seal();
        service.onQueryFailed("q-a", new RuntimeException("crash"));
        assertThat(service.activeQueryCount()).isEqualTo(2);

        query("q-b").seal();
        service.onQueryCompleted("q-b");
        assertThat(service.activeQueryCount()).isEqualTo(1);

        query("q-c").seal();
        service.onQueryEnd("q-c");
        assertThat(service.activeQueryCount()).isZero();
    }

    // ── Test infrastructure ──

    static final class RecordingConnectorMetadata implements ConnectorMetadata {
        final CopyOnWriteArrayList<String> beginQueryIds = new CopyOnWriteArrayList<>();
        final CopyOnWriteArrayList<String> cleanupQueryIds = new CopyOnWriteArrayList<>();

        @Override
        public void beginQuery(org.pangrova.spi.connector.ConnectorQueryAuthority authority) {
            String queryId = authority.queryId();
            beginQueryIds.add(queryId);
        }

        @Override
        public void cleanupQuery(org.pangrova.spi.connector.ConnectorQueryAuthority authority) {
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

    static final class TestMetadataStore implements MetadataStore {
        private final CopyOnWriteArrayList<CatalogDescriptor> catalogs = new CopyOnWriteArrayList<>();

        @Override public void saveTable(TableMetadata m) { }
        @Override public Optional<TableMetadata> loadTable(String c, String s, String t) { return Optional.empty(); }
        @Override public void deleteTable(String c, String s, String t) { }
        @Override public List<QualifiedTableRef> searchTables(String p) { return List.of(); }
        @Override public void saveCatalogDescriptor(CatalogDescriptor d) {
            catalogs.removeIf(x -> x.name().equals(d.name()));
            catalogs.add(d);
        }
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String n) {
            return catalogs.stream().filter(d -> d.name().equals(n)).findFirst();
        }
        @Override public void deleteCatalogDescriptor(String n) { catalogs.removeIf(d -> d.name().equals(n)); }
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.copyOf(catalogs); }
        @Override public Optional<Instant> getLastSyncTime(String n) { return Optional.empty(); }
        @Override public void updateSyncTime(String n, Instant t) { }
        @Override public void recordSyncHistory(SyncResult r) { }
        @Override public void saveSchemas(String n, List<String> s) { }
    }
}
