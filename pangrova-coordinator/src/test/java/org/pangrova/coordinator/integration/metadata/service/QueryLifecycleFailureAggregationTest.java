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
import org.pangrova.metadata.exception.MetadataResolutionException;
import org.pangrova.metadata.model.TableMetadata;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.metadata.schema.SchemaEvolutionValidator;
import org.pangrova.metadata.store.MetadataStore;
import org.pangrova.metadata.sync.SyncResult;
import org.pangrova.spi.connector.Connector;
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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for query lifecycle failure aggregation in DefaultMetadataService.
 *
 * <p>Verifies that partial catalog failures during beginQuery/cleanupQuery are
 * aggregated into structured reports, exposed via metrics counters, and do not
 * silently return empty results.</p>
 *
 * <p><b>Validates: Requirements TASK-M1-4</b></p>
 */
class QueryLifecycleFailureAggregationTest {
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


    private DefaultMetadataService service;
    private CatalogRegistry registry;
    private MetadataEventDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        dispatcher = new MetadataEventDispatcher();
    }

    @AfterEach
    void tearDown() {
        if (registry != null) {
            registry.shutdown();
        }
        dispatcher.shutdown();
    }

    @Test
    void partialBeginQueryFailureIsObservableViaMetrics() {
        RecordingMetadata healthy = new RecordingMetadata();
        FailingMetadata failing = new FailingMetadata("simulated begin failure");
        buildServiceWithCatalogs(
                catalogEntry("healthy_catalog", healthy),
                catalogEntry("failing_catalog", failing));

        assertThatThrownBy(() -> service.beginQuery(query("q-metrics").metadataSession()))
                .isInstanceOf(MetadataResolutionException.class);

        assertThat(service.beginQueryFailureTotal()).isEqualTo(1);
        assertThat(service.beginQuerySuccessTotal()).isEqualTo(1);
    }

    @Test
    void partialBeginQueryFailureThrowsWithAggregatedError() {
        RecordingMetadata healthy = new RecordingMetadata();
        FailingMetadata failing = new FailingMetadata("catalog down");
        buildServiceWithCatalogs(
                catalogEntry("good", healthy),
                catalogEntry("bad", failing));

        assertThatThrownBy(() -> service.beginQuery(query("q-throw").metadataSession()))
                .isInstanceOf(MetadataResolutionException.class)
                .satisfies(ex -> {
                    MetadataResolutionException mre = (MetadataResolutionException) ex;
                    assertThat(mre.getCatalogFailures()).containsKey("bad");
                    assertThat(mre.getCatalogFailures()).hasSize(1);
                    assertThat(mre.getMessage()).contains("beginQuery");
                    assertThat(mre.getMessage()).contains("q-throw");
                });

        // healthy catalog should still have received beginQuery
        assertThat(healthy.beginQueryIds).containsExactly("q-throw");
    }

    @Test
    void allCatalogsSucceedNoException() {
        RecordingMetadata cat1 = new RecordingMetadata();
        RecordingMetadata cat2 = new RecordingMetadata();
        buildServiceWithCatalogs(
                catalogEntry("alpha", cat1),
                catalogEntry("beta", cat2));

        assertThatCode(() -> service.beginQuery(query("q-ok").metadataSession())).doesNotThrowAnyException();

        assertThat(service.beginQuerySuccessTotal()).isEqualTo(2);
        assertThat(service.beginQueryFailureTotal()).isEqualTo(0);
        assertThat(cat1.beginQueryIds).containsExactly("q-ok");
        assertThat(cat2.beginQueryIds).containsExactly("q-ok");
    }

    @Test
    void partialCleanupFailureRetainsRetryOwnerAndStillCleansHealthyCatalog() {
        RecordingMetadata healthy = new RecordingMetadata();
        FailingMetadata failing = new FailingMetadata("cleanup boom");
        buildServiceWithCatalogs(
                catalogEntry("ok_catalog", healthy),
                catalogEntry("broken_catalog", failing));

        // beginQuery will fail because of the failing catalog, but we need
        // the query to be in activeQueries for cleanupQuery to execute.
        // So use all-healthy catalogs for begin, then swap.
        // Alternative: register only healthy catalogs for begin, then add failing for cleanup.
        // Simpler approach: use a metadata that only fails on cleanup.
        registry.shutdown();
        dispatcher.shutdown();

        dispatcher = new MetadataEventDispatcher();
        CleanupFailingMetadata cleanupFailing = new CleanupFailingMetadata("cleanup error");
        RecordingMetadata healthyMeta = new RecordingMetadata();
        buildServiceWithCatalogs(
                catalogEntry("ok_catalog", healthyMeta),
                catalogEntry("cleanup_fail_catalog", cleanupFailing));

        service.beginQuery(query("q-cleanup").metadataSession());
        assertThatThrownBy(() -> cleanup("q-cleanup")).isInstanceOf(MetadataResolutionException.class);
        assertThat(service.activeQueryCount()).isEqualTo(1);

        assertThat(healthyMeta.cleanupQueryIds).containsExactly("q-cleanup");
        assertThatThrownBy(() -> cleanup("q-cleanup")).isInstanceOf(MetadataResolutionException.class);
        assertThat(cleanupFailing.failure.getSuppressed()).isEmpty();
        assertThat(service.activeQueryCount()).isEqualTo(1);
        assertThat(healthyMeta.cleanupQueryIds).containsExactly("q-cleanup");
        cleanupFailing.recovered = true;
        cleanup("q-cleanup");
        assertThat(healthyMeta.cleanupQueryIds).containsExactly("q-cleanup");
        assertThat(service.activeQueryCount()).isZero();
    }

    @Test
    void cleanupFailureCounterIncrementsOnPartialFailure() {
        CleanupFailingMetadata cleanupFailing = new CleanupFailingMetadata("cleanup fail");
        RecordingMetadata healthy = new RecordingMetadata();
        buildServiceWithCatalogs(
                catalogEntry("working", healthy),
                catalogEntry("broken_cleanup", cleanupFailing));

        service.beginQuery(query("q-counter").metadataSession());
        assertThatThrownBy(() -> cleanup("q-counter")).isInstanceOf(MetadataResolutionException.class);

        assertThat(service.cleanupQueryFailureTotal()).isEqualTo(1);
        assertThat(service.cleanupQuerySuccessTotal()).isEqualTo(1);
    }

    @Test
    void singleCatalogFailureAmongManyReportsCorrectBreakdown() {
        RecordingMetadata cat1 = new RecordingMetadata();
        RecordingMetadata cat2 = new RecordingMetadata();
        FailingMetadata catFail = new FailingMetadata("one bad apple");
        RecordingMetadata cat3 = new RecordingMetadata();
        buildServiceWithCatalogs(
                catalogEntry("cat_a", cat1),
                catalogEntry("cat_b", cat2),
                catalogEntry("cat_fail", catFail),
                catalogEntry("cat_c", cat3));

        assertThatThrownBy(() -> service.beginQuery(query("q-breakdown").metadataSession()))
                .isInstanceOf(MetadataResolutionException.class)
                .satisfies(ex -> {
                    MetadataResolutionException mre = (MetadataResolutionException) ex;
                    assertThat(mre.getCatalogFailures()).hasSize(1);
                    assertThat(mre.getCatalogFailures()).containsKey("cat_fail");
                });

        assertThat(service.beginQuerySuccessTotal()).isEqualTo(3);
        assertThat(service.beginQueryFailureTotal()).isEqualTo(1);
        // all healthy catalogs still received the call
        assertThat(cat1.beginQueryIds).containsExactly("q-breakdown");
        assertThat(cat2.beginQueryIds).containsExactly("q-breakdown");
        assertThat(cat3.beginQueryIds).containsExactly("q-breakdown");
    }

    // ── Test infrastructure ──

    @Test
    void concurrentResolvedBeginUsesOneLookupAndTheSameActualMetadataOwner() throws Exception {
        var healthy = new RecordingMetadata();
        buildServiceWithCatalogs(catalogEntry("selected", healthy));
        var lifetime = query("q-single-flight");
        var authority = lifetime.authority();
        var entered = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        var lookups = new java.util.concurrent.atomic.AtomicInteger();
        var executor = java.util.concurrent.Executors.newFixedThreadPool(2);
        try {
            var first = executor.submit(() -> authority.beginMetadata("selected", () -> {
                lookups.incrementAndGet();
                entered.countDown();
                try {
                    if (!release.await(5, java.util.concurrent.TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Metadata lookup release timed out");
                    }
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(failure);
                }
                return registry.getCatalog("selected").getMetadata(lifetime.session("selected", "db"));
            }));
            assertThat(entered.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            var second = executor.submit(() -> authority.beginMetadata("selected", () -> {
                throw new AssertionError("The retained owner must not be looked up twice");
            }));
            release.countDown();
            assertThat(first.get(5, java.util.concurrent.TimeUnit.SECONDS)).isSameAs(healthy);
            assertThat(second.get(5, java.util.concurrent.TimeUnit.SECONDS)).isSameAs(healthy);
            assertThat(lookups).hasValue(1);
            assertThat(healthy.beginQueryIds).containsExactly("q-single-flight");
            lifetime.seal();
            authority.cleanupMetadata().requireSuccess();
            assertThat(healthy.cleanupQueryIds).containsExactly("q-single-flight");
            lifetime.retire();
        } finally {
            release.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void failedBeginCannotBecomeAnIdempotentSuccessAndItsOriginalOwnerIsCleaned() {
        var failing = new FailingMetadata("original begin failed");
        buildServiceWithCatalogs(catalogEntry("failed", failing));
        var lifetime = query("q-failed-begin");
        assertThatThrownBy(() -> service.beginQuery(lifetime.metadataSession()))
                .isInstanceOf(MetadataResolutionException.class);
        assertThatThrownBy(() -> service.beginQuery(lifetime.metadataSession()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("begin is incomplete");
        assertThatThrownBy(() -> lifetime.authority().beginMetadata("failed", () -> {
            throw new AssertionError("Failed BEGIN must retain its original instance, not retry lookup");
        })).hasMessage("original begin failed");
        lifetime.seal();
        service.cleanupQuery(lifetime.authority());
        assertThat(service.activeQueryCount()).isZero();
        lifetime.retire();
    }

    private record CatalogEntry(String name, ConnectorMetadata metadata) {}

    private static CatalogEntry catalogEntry(String name, ConnectorMetadata metadata) {
        return new CatalogEntry(name, metadata);
    }

    private void buildServiceWithCatalogs(CatalogEntry... entries) {
        TestMetadataStore store = new TestMetadataStore();
        ConnectorHandleFactory chf = desc -> {
            for (CatalogEntry entry : entries) {
                if (entry.name().equals(desc.name())) {
                    return new ConnectorCatalogHandle(desc, new SimpleConnector(entry.metadata()));
                }
            }
            throw new IllegalStateException("Unknown catalog: " + desc.name());
        };
        registry = new CatalogRegistry(store, chf, null, dispatcher);
        for (CatalogEntry entry : entries) {
            CatalogDescriptor descriptor = new CatalogDescriptor(
                    entry.name(), CatalogType.CONNECTOR, "test",
                    CatalogState.ACTIVE, Map.of(), "", Set.of());
            registry.register(descriptor);
        }
        registry.initialize();
        service = new DefaultMetadataService(
                registry, new TieredMetadataCache(), dispatcher, new SchemaEvolutionValidator());
    }

    /** Records beginQuery/cleanupQuery calls without failing. */
    static final class RecordingMetadata implements ConnectorMetadata {
        final CopyOnWriteArrayList<String> beginQueryIds = new CopyOnWriteArrayList<>();
        final CopyOnWriteArrayList<String> cleanupQueryIds = new CopyOnWriteArrayList<>();

        @Override public void beginQuery(org.pangrova.spi.connector.ConnectorQueryAuthority authority) {
            String queryId = authority.queryId(); beginQueryIds.add(queryId); }
        @Override public void cleanupQuery(org.pangrova.spi.connector.ConnectorQueryAuthority authority) {
            String queryId = authority.queryId(); cleanupQueryIds.add(queryId); }
        @Override public List<String> listSchemaNames(ConnectorSession s) { return List.of(); }
        @Override public List<SchemaTableName> listTables(ConnectorSession s, Optional<String> schema) { return List.of(); }
        @Override public ConnectorTableHandle getTableHandle(ConnectorSession s, SchemaTableName n) { return null; }
        @Override public ConnectorTableMetadata getTableMetadata(ConnectorSession s, ConnectorTableHandle h) { return null; }
        @Override public Map<String, ColumnHandle> getColumnHandles(ConnectorSession s, ConnectorTableHandle h) { return Map.of(); }
        @Override public TableStatistics getTableStatistics(ConnectorSession s, ConnectorTableHandle h) { return TableStatistics.UNKNOWN; }
    }

    /** Throws on beginQuery; cleanupQuery succeeds. */
    static final class FailingMetadata implements ConnectorMetadata {
        private final String errorMessage;

        FailingMetadata(String errorMessage) { this.errorMessage = errorMessage; }

        @Override public void beginQuery(org.pangrova.spi.connector.ConnectorQueryAuthority authority) {
            String queryId = authority.queryId(); throw new RuntimeException(errorMessage); }
        @Override public void cleanupQuery(org.pangrova.spi.connector.ConnectorQueryAuthority authority) {
            String queryId = authority.queryId(); /* no-op */ }
        @Override public List<String> listSchemaNames(ConnectorSession s) { return List.of(); }
        @Override public List<SchemaTableName> listTables(ConnectorSession s, Optional<String> schema) { return List.of(); }
        @Override public ConnectorTableHandle getTableHandle(ConnectorSession s, SchemaTableName n) { return null; }
        @Override public ConnectorTableMetadata getTableMetadata(ConnectorSession s, ConnectorTableHandle h) { return null; }
        @Override public Map<String, ColumnHandle> getColumnHandles(ConnectorSession s, ConnectorTableHandle h) { return Map.of(); }
        @Override public TableStatistics getTableStatistics(ConnectorSession s, ConnectorTableHandle h) { return TableStatistics.UNKNOWN; }
    }

    /** Succeeds on beginQuery; throws on cleanupQuery. */
    static final class CleanupFailingMetadata implements ConnectorMetadata {
        private boolean recovered;
        private final RuntimeException failure;

        CleanupFailingMetadata(String errorMessage) { this.failure = new RuntimeException(errorMessage); }

        @Override public void beginQuery(org.pangrova.spi.connector.ConnectorQueryAuthority authority) {
            String queryId = authority.queryId(); /* no-op */ }
        @Override public void cleanupQuery(org.pangrova.spi.connector.ConnectorQueryAuthority authority) {
            String queryId = authority.queryId();
            if (!recovered) throw failure;
        }
        @Override public List<String> listSchemaNames(ConnectorSession s) { return List.of(); }
        @Override public List<SchemaTableName> listTables(ConnectorSession s, Optional<String> schema) { return List.of(); }
        @Override public ConnectorTableHandle getTableHandle(ConnectorSession s, SchemaTableName n) { return null; }
        @Override public ConnectorTableMetadata getTableMetadata(ConnectorSession s, ConnectorTableHandle h) { return null; }
        @Override public Map<String, ColumnHandle> getColumnHandles(ConnectorSession s, ConnectorTableHandle h) { return Map.of(); }
        @Override public TableStatistics getTableStatistics(ConnectorSession s, ConnectorTableHandle h) { return TableStatistics.UNKNOWN; }
    }

    static final class SimpleConnector implements Connector {
        private final ConnectorMetadata metadata;
        SimpleConnector(ConnectorMetadata metadata) { this.metadata = metadata; }
        @Override public ConnectorMetadata getMetadata(ConnectorSession session) { return metadata; }
        @Override public ConnectorSplitManager getSplitManager() { throw new UnsupportedOperationException(); }
        @Override public ConnectorPageSourceProvider getPageSourceProvider() { throw new UnsupportedOperationException(); }
        @Override public void shutdown() { }
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
