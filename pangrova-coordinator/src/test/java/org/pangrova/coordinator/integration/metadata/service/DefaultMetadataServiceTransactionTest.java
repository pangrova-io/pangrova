package org.pangrova.coordinator.integration.metadata.service;

import org.pangrova.metadata.service.DefaultMetadataService;

import org.pangrova.metadata.cache.TieredMetadataCache;
import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.catalog.CatalogState;
import org.pangrova.metadata.catalog.CatalogType;
import org.pangrova.metadata.catalog.ConnectorCatalogHandle;
import org.pangrova.metadata.catalog.ConnectorHandleFactory;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.metadata.model.TableMetadata;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.metadata.schema.SchemaEvolutionValidator;
import org.pangrova.metadata.store.MetadataStore;
import org.pangrova.metadata.sync.SyncResult;
import org.pangrova.spi.connector.Connector;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.metadata.*;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
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
