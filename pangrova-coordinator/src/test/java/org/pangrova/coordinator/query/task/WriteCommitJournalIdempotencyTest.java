package org.pangrova.coordinator.query.task;

import org.pangrova.coordinator.coordination.inmemory.InMemoryWriteCommitJournal;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryManagerContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
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
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.protocol.rest.data.WriteResultDescriptor;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.resource.MemoryPressure;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.spi.connector.Connector;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.pangrova.spi.coordination.WriteCommitEntry;
import org.pangrova.spi.coordination.WriteCommitJournal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the WriteCommitJournal provides idempotent commit behavior
 * for distributed writes through the TaskStatusReceiver commit path.
 *
 * <p>Tests the full lifecycle: intent recording, commit execution, completion
 * marking, and crash-recovery skip of already-committed writes.</p>
 */
class WriteCommitJournalIdempotencyTest {

    private static final String QUERY_ID = "q-journal-1";
    private static final String WORKER_ID = "w-1";
    private static final String CATALOG_NAME = "test-catalog";

    private CoordinatorTaskTracker tracker;
    private NativeWriteCommitTracker writeCommitTracker;
    private RecordingConnectorMetadata recordingMetadata;
    private InMemoryWriteCommitJournal writeCommitJournal;
    private TaskStatusReceiver receiver;

    private long sequenceCounter = 100;

    @BeforeEach
    void setUp() {
        tracker = new CoordinatorTaskTracker();
        WorkerManager workerManager = new WorkerManager(20_000, 60_000,
                new org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry());
        workerManager.registerWorker(workerInfo(WORKER_ID));
        writeCommitTracker = new NativeWriteCommitTracker();
        recordingMetadata = new RecordingConnectorMetadata();
        writeCommitJournal = new InMemoryWriteCommitJournal();

        CatalogRegistry catalogRegistry = buildCatalogRegistry(
                CATALOG_NAME, recordingMetadata);
        catalogRegistry.initialize();

        QueryManager qm = buildQueryManager(workerManager, tracker);
        TaskStatusReceiverContext deps = new TaskStatusReceiverContext(
                qm, tracker, workerManager,
                null, null, null, null,
                catalogRegistry, writeCommitTracker, null, null,
                writeCommitJournal);
        receiver = new TaskStatusReceiver(deps);
    }

    @Test
    void commitRecordsIntentAndMarksCompleted() {
        byte[] fragment = {1, 2, 3};

        writeCommitTracker.registerWriteContext(QUERY_ID, CATALOG_NAME,
                new TestOutputTableHandle(), new TestConnectorSession());

        registerSingleTask("t-1");
        receiver.onTaskStatus(finishedWithWriteResult("t-1", fragment));

        // Commit should have been called
        assertThat(recordingMetadata.finishCalls).hasSize(1);

        // Journal entry should be purged after successful completion
        assertThat(writeCommitJournal.getEntry(QUERY_ID)).isEmpty();
    }

    @Test
    void alreadyCommittedQueryIsSkipped() {
        byte[] fragment = {1, 2, 3};

        // Pre-mark the query as committed (simulates crash recovery scenario)
        writeCommitJournal.recordIntent(new WriteCommitEntry(
                QUERY_ID, CATALOG_NAME, "test-table",
                WriteCommitEntry.State.PENDING, System.currentTimeMillis()));
        writeCommitJournal.markCommitted(QUERY_ID);

        writeCommitTracker.registerWriteContext(QUERY_ID, CATALOG_NAME,
                new TestOutputTableHandle(), new TestConnectorSession());

        registerSingleTask("t-1");
        receiver.onTaskStatus(finishedWithWriteResult("t-1", fragment));

        // finishDistributedWrite should NOT have been called (idempotency skip)
        assertThat(recordingMetadata.finishCalls).isEmpty();
    }

    @Test
    void failedQueryPurgesJournalEntry() {
        writeCommitTracker.registerWriteContext(QUERY_ID, CATALOG_NAME,
                new TestOutputTableHandle(), new TestConnectorSession());

        // Manually record an intent
        writeCommitJournal.recordIntent(new WriteCommitEntry(
                QUERY_ID, CATALOG_NAME, "test-table",
                WriteCommitEntry.State.PENDING, System.currentTimeMillis()));

        tracker.registerTask(QUERY_ID, "t-1");
        tracker.markScheduled(QUERY_ID, "t-1");

        receiver.onTaskStatus(failedReport("t-1"));

        // Journal entry should be purged on failure
        assertThat(writeCommitJournal.getEntry(QUERY_ID)).isEmpty();
        // Write state should also be cleaned up
        assertThat(writeCommitTracker.hasWriteContext(QUERY_ID)).isFalse();
    }

    @Test
    void journalReplayReturnsPendingEntries() {
        String queryId1 = "q-pending-1";
        String queryId2 = "q-pending-2";
        String queryId3 = "q-committed-3";

        writeCommitJournal.recordIntent(new WriteCommitEntry(
                queryId1, CATALOG_NAME, "table1",
                WriteCommitEntry.State.PENDING, 1000L));
        writeCommitJournal.recordIntent(new WriteCommitEntry(
                queryId2, CATALOG_NAME, "table2",
                WriteCommitEntry.State.PENDING, 2000L));
        writeCommitJournal.recordIntent(new WriteCommitEntry(
                queryId3, CATALOG_NAME, "table3",
                WriteCommitEntry.State.PENDING, 3000L));
        writeCommitJournal.markCommitted(queryId3);

        List<WriteCommitEntry> pending = writeCommitJournal.replayPendingEntries();
        assertThat(pending).hasSize(2);
        assertThat(pending.get(0).queryId()).isEqualTo(queryId1);
        assertThat(pending.get(1).queryId()).isEqualTo(queryId2);
    }

    @Test
    void replayableIntentPreservesFragmentsThroughCommitAndReplay() {
        // A-COMMIT-01: fragments (the replay payload) must survive recordIntent, be preserved by
        // markCommitted, and pending replay must exclude committed entries.
        java.util.List<byte[]> fragments = java.util.List.of(new byte[]{9, 8, 7});
        writeCommitJournal.recordIntent(new WriteCommitEntry(
                "q-replayable", CATALOG_NAME, "analytics.events",
                WriteCommitEntry.State.PENDING, System.currentTimeMillis(),
                fragments, "OVERWRITE", "q-replayable", "coord-A", 3L));

        WriteCommitEntry pendingEntry = writeCommitJournal.getEntry("q-replayable").orElseThrow();
        assertThat(pendingEntry.isReplayable()).isTrue();
        assertThat(pendingEntry.fragments().get(0)).isEqualTo(new byte[]{9, 8, 7});
        assertThat(pendingEntry.writeMode()).isEqualTo("OVERWRITE");

        writeCommitJournal.markCommitted("q-replayable");
        WriteCommitEntry committed = writeCommitJournal.getEntry("q-replayable").orElseThrow();
        assertThat(committed.state()).isEqualTo(WriteCommitEntry.State.COMMITTED);
        // Replay payload preserved across the state transition (not dropped).
        assertThat(committed.fragments().get(0)).isEqualTo(new byte[]{9, 8, 7});
        assertThat(committed.idempotencyKey()).isEqualTo("q-replayable");

        // Committed entries are excluded from pending replay.
        assertThat(writeCommitJournal.replayPendingEntries())
                .extracting(WriteCommitEntry::queryId)
                .doesNotContain("q-replayable");
    }

    @Test
    void inMemoryJournalSingletonIsNoOpSafe() {
        InMemoryWriteCommitJournal journal = InMemoryWriteCommitJournal.INSTANCE;

        // Operations should not throw
        journal.recordIntent(new WriteCommitEntry(
                "q-1", "cat", "table",
                WriteCommitEntry.State.PENDING, System.currentTimeMillis()));
        assertThat(journal.isCommitted("q-1")).isFalse();

        journal.markCommitted("q-1");
        assertThat(journal.isCommitted("q-1")).isTrue();

        journal.purge("q-1");
        assertThat(journal.isCommitted("q-1")).isFalse();
    }

    @Test
    void multiTaskWriteJournalsCorrectly() {
        byte[] fragment1 = {1, 2, 3};
        byte[] fragment2 = {4, 5, 6};

        writeCommitTracker.registerWriteContext(QUERY_ID, CATALOG_NAME,
                new TestOutputTableHandle(), new TestConnectorSession());

        registerTwoTasks();

        // First task finishes — not all tasks done yet
        receiver.onTaskStatus(finishedWithWriteResult("t-1", fragment1));
        // No commit yet
        assertThat(recordingMetadata.finishCalls).isEmpty();

        // Second task finishes — triggers commit
        receiver.onTaskStatus(finishedWithWriteResult("t-2", fragment2));

        // Commit should happen once with both fragments
        assertThat(recordingMetadata.finishCalls).hasSize(1);
        assertThat(recordingMetadata.finishCalls.get(0).fragments()).hasSize(2);

        // Journal should be purged after success
        assertThat(writeCommitJournal.getEntry(QUERY_ID)).isEmpty();
    }

    // --- Helpers ---

    private void registerSingleTask(String taskId) {
        tracker.registerTask(QUERY_ID, taskId);
        tracker.markScheduled(QUERY_ID, taskId);
        tracker.updateTaskState(QUERY_ID, taskId, "RUNNING");
    }

    private void registerTwoTasks() {
        for (String taskId : List.of("t-1", "t-2")) {
            tracker.registerTask(QUERY_ID, taskId);
            tracker.markScheduled(QUERY_ID, taskId);
            tracker.updateTaskState(QUERY_ID, taskId, "RUNNING");
        }
    }

    private TaskStatusReport finishedWithWriteResult(String taskId, byte[] commitMetadata) {
        WriteResultDescriptor wr = new WriteResultDescriptor(
                List.of(), 100, 1024, commitMetadata);
        return new TaskStatusReport(
                taskId, QUERY_ID, 0, 0, "COMPUTE", WORKER_ID, "FINISHED",
                null, null, wr, sequenceCounter++, System.currentTimeMillis());
    }

    private TaskStatusReport failedReport(String taskId) {
        return new TaskStatusReport(
                taskId, QUERY_ID, 0, 0, "COMPUTE", WORKER_ID, "FAILED",
                null, null, null, sequenceCounter++, System.currentTimeMillis());
    }

    private static WorkerInfo workerInfo(String workerId) {
        return new WorkerInfo(workerId, "host-1", 8080, 8081, 0,
                HealthState.ACTIVE, MemoryPressure.NONE, 0.0, 0,
                1024L * 1024 * 1024, Instant.now(), 1.0, null, 1,
                List.of(), null, List.of());
    }

    private static QueryManager buildQueryManager(WorkerManager wm,
                                                   CoordinatorTaskTracker tracker) {
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer());
        RemoteTaskDispatcher td = new RemoteTaskDispatcher(
                CircuitBreakerPolicy.noOp(), new NoOpTracingProvider());
        QueryDispatcherContext qdDeps = QueryDispatcherContext.withDefaults(
                new SqlCompiler(), null, null, null,
                td, scheduler, wm, tracker,
                null, null, null, null, 0L);
        QueryDispatcher dispatcher = new QueryDispatcher(
                qdDeps, null, null);
        return new QueryManager(
                QueryManagerContext.minimal(dispatcher, new NoOpTracingProvider()));
    }

    private static CatalogRegistry buildCatalogRegistry(String catalogName,
                                                         ConnectorMetadata metadata) {
        CatalogDescriptor descriptor = new CatalogDescriptor(
                catalogName, CatalogType.CONNECTOR, "test-provider",
                CatalogState.ACTIVE, Map.of(), "", Set.of());
        ConnectorCatalogHandle handle = new ConnectorCatalogHandle(
                descriptor, new TestConnector(metadata));
        ConnectorHandleFactory factory = desc -> handle;
        return new CatalogRegistry(
                new ReadOnlyMetadataStore(List.of(descriptor)), factory, null, null);
    }

    // --- Test doubles ---

    static final class RecordingConnectorMetadata implements ConnectorMetadata {
        final List<FinishCall> finishCalls =
                Collections.synchronizedList(new ArrayList<>());

        @Override
        public void finishDistributedWrite(ConnectorSession session,
                                           ConnectorOutputTableHandle outputHandle,
                                           List<byte[]> fragments) {
            finishCalls.add(new FinishCall(session, outputHandle, List.copyOf(fragments)));
        }

        record FinishCall(ConnectorSession session,
                          ConnectorOutputTableHandle outputHandle,
                          List<byte[]> fragments) {}

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession s, SchemaTableName n) {
            return null;
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession s,
                                                        ConnectorTableHandle t) {
            return null;
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession s,
                                                           ConnectorTableHandle t) {
            return Map.of();
        }

        @Override
        public TableStatistics getTableStatistics(ConnectorSession s,
                                                   ConnectorTableHandle t) {
            return null;
        }

        @Override
        public List<String> listSchemaNames(ConnectorSession s) {
            return List.of();
        }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession s, Optional<String> schema) {
            return List.of();
        }
    }

    static final class TestConnector implements Connector {
        private final ConnectorMetadata metadata;

        TestConnector(ConnectorMetadata metadata) {
            this.metadata = metadata;
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            return metadata;
        }

        @Override
        public ConnectorSplitManager getSplitManager() { return null; }

        @Override
        public ConnectorPageSourceProvider getPageSourceProvider() { return null; }
    }

    static final class TestOutputTableHandle implements ConnectorOutputTableHandle {}

    static final class TestConnectorSession implements ConnectorSession {
        @Override public String getUser() { return "test-user"; }
        @Override public Optional<String> getCatalog() { return Optional.of(CATALOG_NAME); }
        @Override public Optional<String> getSchema() { return Optional.of("default"); }
        @Override public Map<String, String> getProperties() { return Map.of(); }
    }

    static final class ReadOnlyMetadataStore implements MetadataStore {
        private final List<CatalogDescriptor> descriptors;

        ReadOnlyMetadataStore(List<CatalogDescriptor> descriptors) {
            this.descriptors = List.copyOf(descriptors);
        }

        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return descriptors; }
        @Override public void saveCatalogDescriptor(CatalogDescriptor d) {}
        @Override public void deleteCatalogDescriptor(String name) {}
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String name) {
            return descriptors.stream().filter(d -> d.name().equals(name)).findFirst();
        }
        @Override public void saveTable(TableMetadata m) {}
        @Override public Optional<TableMetadata> loadTable(String c, String s, String t) { return Optional.empty(); }
        @Override public void deleteTable(String c, String s, String t) {}
        @Override public List<QualifiedTableRef> searchTables(String pattern) { return List.of(); }
        @Override public Optional<Instant> getLastSyncTime(String catalogName) { return Optional.empty(); }
        @Override public void updateSyncTime(String cn, Instant st) {}
        @Override public void recordSyncHistory(SyncResult r) {}
        @Override public void saveSchemas(String cn, List<String> s) {}
    }
}
