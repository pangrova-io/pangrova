package org.pangrova.coordinator.query.task;

import org.pangrova.persistence.query.api.enums.WriteCommitState;


import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

import org.pangrova.coordinator.testinfra.QueryManagerTestContexts;

import org.pangrova.coordinator.query.task.write.NativeWriteCommitTracker;
import org.pangrova.persistence.query.memory.transaction.InMemoryWriteCommitJournal;
import org.pangrova.query.runtime.QueryManager;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.scheduler.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.scheduler.api.enums.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.strategies.placement.LoadBalancedStrategy;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.api.catalog.ConnectorHandleFactory;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.protocol.rest.data.WriteResultDescriptor;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.common.enums.MemoryPressure;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.connector.api.plugin.Connector;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.data.ConnectorPageSourceProvider;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.ConnectorOutputHandleCodec;
import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.api.commit.DistributedWriteCommitContext;
import org.pangrova.connector.api.metadata.ConnectorSplitManager;
import org.pangrova.connector.api.metadata.table.ColumnHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.pangrova.persistence.query.api.commit.WriteCommitEntry;
import org.pangrova.persistence.query.api.commit.WriteCommitFingerprint;
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
    private CatalogRegistry catalogRegistry;
    private TaskStatusReceiver receiver;

    private long sequenceCounter = 100;

    @BeforeEach
    void setUp() {
        tracker = new CoordinatorTaskTracker();
        WorkerManager workerManager = new WorkerManager(20_000, 60_000,
                new org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry());
        workerManager.registerWorker(workerInfo(WORKER_ID));
        writeCommitTracker = new NativeWriteCommitTracker();
        recordingMetadata = new RecordingConnectorMetadata();
        writeCommitJournal = new InMemoryWriteCommitJournal();

        catalogRegistry = buildCatalogRegistry(
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

        writeCommitTracker.registerWriteContext(
                QUERY_ID,
                CATALOG_NAME,
                new TestOutputTableHandle(),
                new TestConnectorSession(),
                binding(),
                org.pangrova.coordinator.testinfra.CoordinatorWriteReceiptFixtures.receipt(),
                new org.pangrova.coordinator.query.task.write.WriteAllocationIntent("INSERT", java.util.Map.of()), null, null);

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
        writeCommitJournal.recordIntent(entry(
                QUERY_ID, CATALOG_NAME, "test-table",
                WriteCommitState.PENDING, System.currentTimeMillis()));
        writeCommitJournal.markCommitted(QUERY_ID);

        writeCommitTracker.registerWriteContext(
                QUERY_ID,
                CATALOG_NAME,
                new TestOutputTableHandle(),
                new TestConnectorSession(),
                binding(),
                org.pangrova.coordinator.testinfra.CoordinatorWriteReceiptFixtures.receipt(),
                new org.pangrova.coordinator.query.task.write.WriteAllocationIntent("INSERT", java.util.Map.of()), null, null);

        registerSingleTask("t-1");
        receiver.onTaskStatus(finishedWithWriteResult("t-1", fragment));

        // finishDistributedWrite should NOT have been called (idempotency skip)
        assertThat(recordingMetadata.finishCalls).isEmpty();
    }

    @Test
    void failedQueryRetainsPendingRedoEvidenceForReconciliation() {
        writeCommitTracker.registerWriteContext(
                QUERY_ID,
                CATALOG_NAME,
                new TestOutputTableHandle(),
                new TestConnectorSession(),
                binding(),
                org.pangrova.coordinator.testinfra.CoordinatorWriteReceiptFixtures.receipt(),
                new org.pangrova.coordinator.query.task.write.WriteAllocationIntent("INSERT", java.util.Map.of()), null, null);

        // Manually record an intent
        writeCommitJournal.recordIntent(entry(
                QUERY_ID, CATALOG_NAME, "test-table",
                WriteCommitState.PENDING, System.currentTimeMillis()));

        tracker.registerTask(QUERY_ID, "t-1");
        registerCanonicalRequest("t-1");
        tracker.markScheduled(QUERY_ID, "t-1");

        receiver.onTaskStatus(failedReport("t-1"));

        // Task failure cannot prove that an external catalog did not publish the intent.
        assertThat(writeCommitJournal.getEntry(QUERY_ID)).get()
                .extracting(WriteCommitEntry::state).isEqualTo(WriteCommitState.PENDING);
        assertThat(writeCommitJournal.replayPendingEntries(Set.of(QUERY_ID)))
                .extracting(WriteCommitEntry::queryId).containsExactly(QUERY_ID);
        // Write state should also be cleaned up
        assertThat(writeCommitTracker.hasWriteContext(QUERY_ID)).isFalse();
    }

    @Test
    void journalReplayReturnsPendingEntries() {
        String queryId1 = "q-pending-1";
        String queryId2 = "q-pending-2";
        String queryId3 = "q-committed-3";

        writeCommitJournal.recordIntent(entry(
                queryId1, CATALOG_NAME, "table1",
                WriteCommitState.PENDING, 1000L));
        writeCommitJournal.recordIntent(entry(
                queryId2, CATALOG_NAME, "table2",
                WriteCommitState.PENDING, 2000L));
        writeCommitJournal.recordIntent(entry(
                queryId3, CATALOG_NAME, "table3",
                WriteCommitState.PENDING, 3000L));
        writeCommitJournal.markCommitted(queryId3);

        List<WriteCommitEntry> pending = writeCommitJournal.replayPendingEntries(
                Set.of(queryId1, queryId2, queryId3));
        assertThat(pending).hasSize(2);
        assertThat(pending.get(0).queryId()).isEqualTo(queryId1);
        assertThat(pending.get(1).queryId()).isEqualTo(queryId2);
    }

    @Test
    void replayableIntentPreservesFragmentsThroughCommitAndReplay() {
        // A-COMMIT-01: fragments (the replay payload) must survive recordIntent, be preserved by
        // markCommitted, and pending replay must exclude committed entries.
        java.util.List<byte[]> fragments = java.util.List.of(new byte[]{9, 8, 7});
        String fingerprint = WriteCommitFingerprint.compute(
                new org.pangrova.persistence.query.api.commit.WriteCommitFingerprint.Intent(
                        "q-replayable",
                        "tenant-test",
                        CATALOG_NAME,
                        "analytics.events",
                        "OVERWRITE",
                        11L,
                        new byte[]{42},
                        "alice",
                        org.pangrova.coordinator.testinfra.CoordinatorWriteReceiptFixtures.receipt(), null),
                fragments);
        writeCommitJournal.recordIntent(new WriteCommitEntry(
                "q-replayable",
                CATALOG_NAME,
                "analytics.events",
                WriteCommitState.PENDING,
                System.currentTimeMillis(),
                fragments,
                "OVERWRITE",
                fingerprint,
                "tenant-test",
                "alice",
                org.pangrova.coordinator.testinfra.CoordinatorWriteReceiptFixtures.receipt(),
                11L,
                new byte[]{42},
                fingerprint,
                "coord-A",
                "process-A",
                3L,
                null, null, null));

        WriteCommitEntry pendingEntry = writeCommitJournal.getEntry("q-replayable").orElseThrow();
        assertThat(pendingEntry.fragments().get(0)).isEqualTo(new byte[]{9, 8, 7});
        assertThat(pendingEntry.writeMode()).isEqualTo("OVERWRITE");

        writeCommitJournal.markCommitted("q-replayable");
        WriteCommitEntry committed = writeCommitJournal.getEntry("q-replayable").orElseThrow();
        assertThat(committed.state()).isEqualTo(WriteCommitState.COMMITTED);
        // Replay payload preserved across the state transition (not dropped).
        assertThat(committed.fragments().get(0)).isEqualTo(new byte[]{9, 8, 7});
        assertThat(committed.idempotencyKey()).isEqualTo(fingerprint);

        // Committed entries are excluded from pending replay.
        assertThat(writeCommitJournal.replayPendingEntries(Set.of("q-replayable")))
                .extracting(WriteCommitEntry::queryId)
                .doesNotContain("q-replayable");
    }

    @Test
    void inMemoryJournalRetainsIntentUntilCommittedAndAcknowledged() {
        InMemoryWriteCommitJournal journal = new InMemoryWriteCommitJournal();

        // Operations should not throw
        journal.recordIntent(entry(
                "q-1", "cat", "table",
                WriteCommitState.PENDING, System.currentTimeMillis()));
        assertThat(journal.isCommitted("q-1")).isFalse();

        journal.markCommitted("q-1");
        assertThat(journal.isCommitted("q-1")).isTrue();

        journal.acknowledgeNoDeferredWork(journal.getEntry("q-1").orElseThrow().commitFingerprint());
        journal.purge("q-1");
        assertThat(journal.isCommitted("q-1")).isTrue();
    }

    @Test
    void multiTaskWriteJournalsCorrectly() {
        byte[] fragment1 = {1, 2, 3};
        byte[] fragment2 = {4, 5, 6};

        writeCommitTracker.registerWriteContext(
                QUERY_ID,
                CATALOG_NAME,
                new TestOutputTableHandle(),
                new TestConnectorSession(),
                binding(),
                org.pangrova.coordinator.testinfra.CoordinatorWriteReceiptFixtures.receipt(),
                new org.pangrova.coordinator.query.task.write.WriteAllocationIntent("INSERT", java.util.Map.of()), null, null);

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

    private byte[] binding() {
        return CanonicalWriteCommitTestBinding.create(
                catalogRegistry.getCatalog(CATALOG_NAME), new TestOutputTableHandle());
    }

    private void registerSingleTask(String taskId) {
        tracker.registerTask(QUERY_ID, taskId);
        registerCanonicalRequest(taskId);
        tracker.markScheduled(QUERY_ID, taskId);
        tracker.updateTaskState(QUERY_ID, taskId, "RUNNING");
    }

    private static WriteCommitEntry entry(
            String queryId, String catalog, String table,
            WriteCommitState state, long timestampMs) {
        String mode = "INSERT";
        String fingerprint = WriteCommitFingerprint.compute(
                new org.pangrova.persistence.query.api.commit.WriteCommitFingerprint.Intent(
                        queryId,
                        "tenant-test",
                        catalog,
                        table,
                        mode,
                        -1L,
                        new byte[]{42},
                        "alice",
                        org.pangrova.coordinator.testinfra.CoordinatorWriteReceiptFixtures.receipt(), null),
                List.of());
        return new WriteCommitEntry(
                queryId,
                catalog,
                table,
                state,
                timestampMs,
                List.of(),
                mode,
                fingerprint,
                "tenant-test",
                "alice",
                org.pangrova.coordinator.testinfra.CoordinatorWriteReceiptFixtures.receipt(),
                -1L,
                new byte[]{42},
                fingerprint,
                null,
                null,
                0L,
                null, null, null);
    }

    private void registerTwoTasks() {
        for (String taskId : List.of("t-1", "t-2")) {
            tracker.registerTask(QUERY_ID, taskId);
            registerCanonicalRequest(taskId);
            tracker.markScheduled(QUERY_ID, taskId);
            tracker.updateTaskState(QUERY_ID, taskId, "RUNNING");
        }
    }

    private TaskStatusReport finishedWithWriteResult(String taskId, byte[] commitMetadata) {
        WriteResultDescriptor wr = new WriteResultDescriptor(
                List.of(), 100, 1024, commitMetadata, java.util.List.of());
        return new TaskStatusReport(
                taskId, QUERY_ID, 0, 0, "WRITE", WORKER_ID, "FINISHED",
                null, null, wr, sequenceCounter++, System.currentTimeMillis(),
                null, null, canonicalAttempt(taskId), canonicalReport(taskId, "FINISHED"));
    }

    private TaskStatusReport failedReport(String taskId) {
        return new TaskStatusReport(
                taskId, QUERY_ID, 0, 0, "WRITE", WORKER_ID, "FAILED",
                null, null, null, sequenceCounter++, System.currentTimeMillis(),
                null, null, canonicalAttempt(taskId), canonicalReport(taskId, "FAILED"));
    }

    private void registerCanonicalRequest(String taskId) {
        var request = org.pangrova.coordinator.CanonicalTaskRequestTestFixture.request(
                taskId, QUERY_ID, 0, 0,
                org.pangrova.protocol.v2.TaskKind.TASK_KIND_WRITE, new byte[]{1});
        tracker.registerTaskRequest(taskId, request);
        tracker.registerTaskAttempt(taskId,
                org.pangrova.protocol.rest.task.TaskAttemptDescriptor.from(
                        request.taskRequest().getAttempt()));
    }

    private org.pangrova.protocol.rest.task.TaskAttemptDescriptor canonicalAttempt(
            String taskId) {
        return org.pangrova.protocol.rest.task.TaskAttemptDescriptor.from(
                tracker.getTaskRequest(taskId).taskRequest().getAttempt());
    }

    private byte[] canonicalReport(String taskId, String state) {
        var attempt = tracker.getTaskRequest(taskId).taskRequest().getAttempt();
        var coverage = org.pangrova.protocol.v2.CoverageReport.newBuilder()
                .setExpectedWorkUnits(1);
        var report = org.pangrova.protocol.v2.TaskReport.newBuilder()
                .setAttempt(attempt)
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin
                        .TASK_REPORT_ORIGIN_NATIVE);
        if ("FINISHED".equals(state)) {
            coverage.setCompletedWorkUnits(1).setCoverageRatio(1.0)
                    .setCompletionState(org.pangrova.protocol.v2.CompletionState
                            .COMPLETION_STATE_COMPLETE);
            report.setState(org.pangrova.protocol.v2.TaskState.TASK_STATE_FINISHED);
        } else {
            coverage.addFailedWorkUnitIds(taskId)
                    .setCompletionState(org.pangrova.protocol.v2.CompletionState
                            .COMPLETION_STATE_PARTIAL);
            report.setState(org.pangrova.protocol.v2.TaskState.TASK_STATE_FAILED)
                    .setError(org.pangrova.protocol.v2.ExecutionError.newBuilder()
                            .setErrorClass(org.pangrova.protocol.v2.ErrorClass.ERROR_CLASS_INTERNAL)
                            .setCode("TEST_FAILURE").setMessage("failed"));
        }
        return report.setCoverage(coverage).build().toByteArray();
    }

    private static WorkerInfo workerInfo(String workerId) {
        return new WorkerInfo(workerId, "host-1", 8080, 8081, 0,
                HealthState.ACTIVE, MemoryPressure.NONE, 0.0, 0,
                1024L * 1024 * 1024, Instant.now(), 1.0, null, 2,
                null, List.of(), null,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.canonicalInventory(),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity(workerId));
    }

    private static QueryManager buildQueryManager(WorkerManager wm,
                                                   CoordinatorTaskTracker tracker) {
        TaskScheduler scheduler = new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty());
        RemoteTaskDispatcher td = new RemoteTaskDispatcher(
                CircuitBreakerPolicy.noOp(), new NoOpTracingProvider());
        QueryDispatcherContext qdDeps = QueryDispatcherContext.withDefaults(
                new SqlCompiler(), null, null, null,
                td, scheduler, wm, tracker,
                null, null, null, 0L);
        QueryDispatcher dispatcher = new QueryDispatcher(
                qdDeps, null, null);
        return new QueryManager(
                QueryManagerTestContexts.minimal(dispatcher, new NoOpTracingProvider()));
    }

    private static CatalogRegistry buildCatalogRegistry(String catalogName,
                                                         ConnectorMetadata metadata) {
        CatalogDescriptor descriptor = new CatalogDescriptor(
                catalogName, CatalogType.CONNECTOR, "test-provider",
                CatalogState.ACTIVE, Map.of(), "", Set.of());
        ConnectorCatalogHandle handle = new ConnectorCatalogHandle(
                descriptor, new TestConnector(metadata));
        ConnectorHandleFactory factory = desc -> handle;
        return new CatalogRegistry(memoryTableStores(),
                new TaskWriteCatalogDeployments(List.of(descriptor)), factory, null, null);
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

        @Override
        public void finishDistributedWrite(ConnectorSession session,
                                           ConnectorOutputTableHandle outputHandle,
                                           List<byte[]> fragments,
                                           DistributedWriteCommitContext context) {
            finishDistributedWrite(session, outputHandle, fragments);
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
        public ConnectorOutputHandleCodec getOutputHandleCodec() {
            return CanonicalWriteCommitTestBinding.codec();
        }

        @Override
        public ConnectorSplitManager getSplitManager() { return null; }

        @Override
        public ConnectorPageSourceProvider getPageSourceProvider() { return null; }
    }

    static final class TestOutputTableHandle implements ConnectorOutputTableHandle {
        @Override public Optional<SchemaTableName> targetTable() {
            return Optional.of(new SchemaTableName("default", "events"));
        }
        @Override public String writeMode() { return "INSERT"; }
    }

    static final class TestConnectorSession implements ConnectorSession {
        @Override
        public java.util.Optional<org.pangrova.connector.api.query.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

        @Override public String getUser() { return "test-user"; }
        @Override public Optional<String> getCatalog() { return Optional.of(CATALOG_NAME); }
        @Override public Optional<String> getSchema() { return Optional.of("default"); }
        @Override public Map<String, String> getProperties() {
            return Map.of("tenant_id", "tenant-test");
        }
    }
}
