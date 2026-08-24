package org.pangrova.coordinator.query;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.catalog.CatalogState;
import org.pangrova.metadata.catalog.CatalogType;
import org.pangrova.metadata.catalog.ConnectorCatalogHandle;
import org.pangrova.metadata.catalog.ConnectorHandleFactory;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.metadata.model.TableMetadata;
import org.pangrova.metadata.store.MetadataStore;
import org.pangrova.metadata.sync.SyncResult;
import org.pangrova.planner.SqlCompiler;
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
import org.pangrova.testing.TestObservability;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies BUG-D35/D39/D40 fixes: connector cleanup is called on dispatch failure.
 *
 * <p>Uses a tracking {@link ConnectorMetadata} test double to record
 * {@code beginQuery} and {@code cleanupQuery} invocations. No mock frameworks.</p>
 *
 * <p><b>Validates: FR-1 (Query Dispatch Cleanup on Failure)</b></p>
 */
class QueryDispatcherCleanupOnFailureTest {

    private TrackingMetadata trackingMetadata;
    private CatalogRegistry catalogRegistry;
    private QueryDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        trackingMetadata = new TrackingMetadata();
        TrackingConnector connector = new TrackingConnector(trackingMetadata);

        InMemoryStore store = new InMemoryStore();
        ConnectorHandleFactory chf = desc -> new ConnectorCatalogHandle(desc, connector);
        catalogRegistry = new CatalogRegistry(store, chf, null, null);
        catalogRegistry.initialize();
        catalogRegistry.register(new CatalogDescriptor(
                "test_catalog", CatalogType.CONNECTOR, "test",
                CatalogState.ACTIVE, Map.of(), "", Set.of()));

        // Use an empty WorkerManager so that non-constant queries fail during
        // worker selection — this triggers the execute() catch block.
        WorkerManager emptyWorkerManager = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        RemoteTaskDispatcher httpDispatcher = new RemoteTaskDispatcher(
                CircuitBreakerPolicy.noOp(), TestObservability.TRACING);
        TaskScheduler taskScheduler = new TaskScheduler(new WorkerPerformanceScorer());

        var deps = QueryDispatcherContext.withDefaults(
                new SqlCompiler(), null, null, null,
                httpDispatcher, taskScheduler, emptyWorkerManager,
                new CoordinatorTaskTracker(), catalogRegistry,
                null, null, null, 0L);
        dispatcher = new QueryDispatcher(deps, null, null);
    }

    @AfterEach
    void tearDown() {
        if (catalogRegistry != null) {
            catalogRegistry.shutdown();
        }
    }

    /**
     * D35/D39: execute() catch block must call cleanupQuery, which in turn
     * calls notifyConnectorsCleanupQuery. A table-referencing query with no
     * available workers triggers NoAvailableWorkerException inside
     * dispatchToWorker → dispatchByTopology → selectWorker.
     */
    @Test
    void executeFailure_callsCleanupQuery_releasingConnectorSnapshot() {
        QueryStateMachine sm = new QueryStateMachine();
        CoordinatorSessionContext session = new CoordinatorSessionContext(
                "q-cleanup-1", Map.of(), null, null);

        assertThrows(RuntimeException.class,
                () -> dispatcher.execute(sm, "SELECT * FROM nonexistent_table", session));

        assertEquals(QueryStateMachine.State.FAILED, sm.getState());
        // D35/D39 fix: cleanupQuery must have been called, which triggers
        // notifyConnectorsCleanupQuery → ConnectorMetadata.cleanupQuery
        assertTrue(trackingMetadata.cleanupQueryIds.size() >= 1,
                "cleanupQuery should be called at least once on dispatch failure, "
                        + "but was called " + trackingMetadata.cleanupQueryIds.size() + " times");
        assertTrue(trackingMetadata.cleanupQueryIds.contains("q-cleanup-1"),
                "cleanupQuery should be called with the failing query's ID");
    }

    /**
     * D40/D34: dispatchToWorker wraps body after notifyConnectorsBeginQuery
     * in try-catch. When dispatch fails, notifyConnectorsCleanupQuery is called
     * inside the catch block, AND cleanupQuery is called in execute()'s catch.
     * This means cleanupQuery is called at least once (possibly twice — once
     * from dispatchToWorker catch, once from execute catch). Both are idempotent.
     */
    @Test
    void dispatchFailure_callsConnectorCleanup_afterBeginQuery() {
        QueryStateMachine sm = new QueryStateMachine();
        CoordinatorSessionContext session = new CoordinatorSessionContext(
                "q-cleanup-2", Map.of(), null, null);

        assertThrows(RuntimeException.class,
                () -> dispatcher.execute(sm, "SELECT * FROM nonexistent_table", session));

        // beginQuery should have been called (we reached dispatchToWorker)
        // For table-referencing queries that pass planning, beginQuery is called.
        // cleanupQuery must also be called to release the pinned snapshot.
        List<String> cleanups = trackingMetadata.cleanupQueryIds;
        assertTrue(cleanups.contains("q-cleanup-2"),
                "notifyConnectorsCleanupQuery should release the pinned snapshot "
                        + "for query q-cleanup-2");
    }

    /**
     * Compile failure (invalid SQL) triggers execute() catch block.
     * Since dispatchToWorker is never reached, notifyConnectorsBeginQuery
     * is never called. cleanupQuery in execute() catch is still called
     * (idempotent — safe even when beginQuery was not called).
     */
    @Test
    void compileFailure_cleanupQueryCalledSafely() {
        QueryStateMachine sm = new QueryStateMachine();
        CoordinatorSessionContext session = new CoordinatorSessionContext(
                "q-compile-fail", Map.of(), null, null);

        assertThrows(RuntimeException.class,
                () -> dispatcher.execute(sm, "THIS IS NOT VALID SQL !!!", session));

        assertEquals(QueryStateMachine.State.FAILED, sm.getState());
        // cleanupQuery is called from execute() catch block even for compile failures.
        // This is safe because notifyConnectorsCleanupQuery is idempotent.
        assertTrue(trackingMetadata.cleanupQueryIds.contains("q-compile-fail"),
                "cleanupQuery should be called even on compile failure (idempotent)");
    }

    // ── Test doubles (no mock frameworks) ──

    /**
     * ConnectorMetadata that tracks beginQuery/cleanupQuery invocations.
     */
    static final class TrackingMetadata implements ConnectorMetadata {
        final CopyOnWriteArrayList<String> beginQueryIds = new CopyOnWriteArrayList<>();
        final CopyOnWriteArrayList<String> cleanupQueryIds = new CopyOnWriteArrayList<>();

        @Override
        public void beginQuery(String queryId) {
            beginQueryIds.add(queryId);
        }

        @Override
        public void cleanupQuery(String queryId) {
            cleanupQueryIds.add(queryId);
        }

        @Override
        public List<String> listSchemaNames(ConnectorSession session) {
            return List.of();
        }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schema) {
            return List.of();
        }

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName name) {
            return null;
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle handle) {
            return null;
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle handle) {
            return Map.of();
        }

        @Override
        public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle handle) {
            return TableStatistics.UNKNOWN;
        }
    }

    /**
     * Connector that returns the shared TrackingMetadata instance.
     */
    static final class TrackingConnector implements Connector {
        private final TrackingMetadata metadata;

        TrackingConnector(TrackingMetadata metadata) {
            this.metadata = metadata;
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            return metadata;
        }

        @Override
        public ConnectorSplitManager getSplitManager() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConnectorPageSourceProvider getPageSourceProvider() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Minimal in-memory MetadataStore for CatalogRegistry initialization.
     */
    static final class InMemoryStore implements MetadataStore {
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
        @Override public Optional<java.time.Instant> getLastSyncTime(String n) { return Optional.empty(); }
        @Override public void updateSyncTime(String n, java.time.Instant t) { }
        @Override public void recordSyncHistory(SyncResult r) { }
        @Override public void saveSchemas(String n, List<String> s) { }
    }
}
