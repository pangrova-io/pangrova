package org.pangrova.coordinator.query.task;

import org.pangrova.coordinator.query.task.write.NativeWriteCommitTracker;
import org.pangrova.coordinator.query.task.write.WriteCommitFencer;

import org.pangrova.coordinator.query.task.report.ArtifactWorkReportSink;

import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryManagerContext;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.dispatch.dag.StageCompletionCallback;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.core.catalog.CatalogRegistry;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.core.catalog.ConnectorCatalogHandle;
import org.pangrova.metadata.core.catalog.ConnectorHandleFactory;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.schema.TableMetadata;
import org.pangrova.metadata.core.store.MetadataStore;
import org.pangrova.metadata.core.store.SyncResult;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.protocol.rest.data.WriteResultDescriptor;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.resource.MemoryPressure;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.spi.connector.Connector;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorOutputHandleCodec;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.DistributedWriteCommitContext;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.pangrova.spi.coordination.ExecutionAnchor;
import org.pangrova.coordinator.coordination.inmemory.InMemoryWriteCommitJournal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


/**
 * Verifies TaskStatusReceiver correctly collects native write commitMetadata
 * from task status reports and calls finishDistributedWrite on query completion.
 *
 * <p>Uses hand-written test doubles (no mock frameworks) per pangrova-rules §12.
 * Follows wiring patterns from {@code SpeculativeExecutionWiringTest}.</p>
 */
class TaskStatusReceiverWriteCommitTest {

    private static final String QUERY_ID = "q-write-1";
    private static final String WORKER_ID = "w-1";
    private static final String CATALOG_NAME = "test-catalog";

    private CoordinatorTaskTracker tracker;
    private NativeWriteCommitTracker writeCommitTracker;
    private RecordingConnectorMetadata recordingMetadata;
    private InMemoryWriteCommitJournal writeCommitJournal;
    private CatalogRegistry catalogRegistry;
    private QueryManager queryManager;
    private WorkerManager workerManager;
    private TaskStatusReceiver receiver;

    private long sequenceCounter = 100;

    @BeforeEach
    void setUp() {
        tracker = new CoordinatorTaskTracker();
        workerManager = new WorkerManager(20_000, 60_000,
                new org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry());
        workerManager.registerWorker(workerInfo(WORKER_ID));
        writeCommitTracker = new NativeWriteCommitTracker();
        recordingMetadata = new RecordingConnectorMetadata();
        writeCommitJournal = new InMemoryWriteCommitJournal();

        catalogRegistry = buildCatalogRegistry(
                CATALOG_NAME, recordingMetadata);
        catalogRegistry.initialize();

        queryManager = buildQueryManager(workerManager, tracker);
        TaskStatusReceiverContext deps = new TaskStatusReceiverContext(
                queryManager, tracker, workerManager,
                null, null, null, null,
                catalogRegistry, writeCommitTracker, null, null, writeCommitJournal,
                ArtifactWorkReportSink.rejecting());
        receiver = new TaskStatusReceiver(deps);
    }

    @Test
    void deferredDagCompletionFinalizesOnTerminalEvent() {
        DeferredStageCompletion callback = new DeferredStageCompletion();
        receiver = receiverWithStageCallback(callback);
        registerRunningQuery();
        registerTwoTasks();

        receiver.onTaskStatus(finishedWithoutWriteResult("t-1"));
        receiver.onTaskStatus(finishedWithoutWriteResult("t-2"));

        assertThat(queryManager.getQueryState(QUERY_ID)).contains(QueryStateMachine.State.RUNNING);
        assertThat(callback.listener).isNotNull();
        callback.complete(true);
        assertThat(queryManager.getQueryState(QUERY_ID)).contains(QueryStateMachine.State.FINISHED);
    }

    @Test
    void failedDagTerminalEventCannotFinalizeQueryAsSuccess() {
        DeferredStageCompletion callback = new DeferredStageCompletion();
        receiver = receiverWithStageCallback(callback);
        registerRunningQuery();
        registerTwoTasks();

        receiver.onTaskStatus(finishedWithoutWriteResult("t-1"));
        receiver.onTaskStatus(finishedWithoutWriteResult("t-2"));
        callback.complete(false);

        assertThat(queryManager.getQueryState(QUERY_ID)).contains(QueryStateMachine.State.RUNNING);
    }

    @Test
    void terminalDagCallbackSeesWriteFragmentBeforeCatalogCommit() {
        byte[] fragment = {7, 8, 9};
        AtomicInteger fragmentsAtTerminal = new AtomicInteger(-1);
        AtomicBoolean dagComplete = new AtomicBoolean();
        AtomicReference<Consumer<Boolean>> terminalListener = new AtomicReference<>();
        receiver = receiverWithStageCallback(new StageCompletionCallback() {
            @Override public void onTaskTerminal(String queryId, String taskId, String state, ErrorDetail error) {
                if (!"t-2".equals(taskId)) return;
                var writeState = writeCommitTracker.peekWriteState(queryId);
                fragmentsAtTerminal.set(writeState == null ? -1 : writeState.fragments().size());
                dagComplete.set(true);
                Consumer<Boolean> listener = terminalListener.get();
                if (listener != null) listener.accept(true);
            }

            @Override public boolean isQueryComplete(String queryId) {
                return dagComplete.get();
            }

            @Override public void onQueryTerminal(String queryId, Consumer<Boolean> listener) {
                terminalListener.set(listener);
            }
        });
        writeCommitTracker.registerWriteContext(
                QUERY_ID, CATALOG_NAME, new TestOutputTableHandle(), new TestConnectorSession(),
                binding(), org.pangrova.coordinator.testinfra.CoordinatorWriteReceiptFixtures.receipt(),
                new org.pangrova.coordinator.query.task.write.WriteAllocationIntent("INSERT", Map.of()), null);
        registerRunningQuery();
        registerTwoTasks();

        receiver.onTaskStatus(finishedWithoutWriteResult("t-1"));
        receiver.onTaskStatus(finishedWithWriteResult("t-2", fragment));

        assertThat(fragmentsAtTerminal.get()).isEqualTo(1);
        assertThat(recordingMetadata.finishCalls).hasSize(1);
        assertThat(recordingMetadata.finishCalls.get(0).fragments()).containsExactly(fragment);
    }

    private TaskStatusReceiver receiverWithStageCallback(StageCompletionCallback callback) {
        return new TaskStatusReceiver(new TaskStatusReceiverContext(
                queryManager, tracker, workerManager,
                null, null, null, null,
                catalogRegistry, writeCommitTracker, callback, null, writeCommitJournal,
                ArtifactWorkReportSink.rejecting()));
    }

    private void registerRunningQuery() {
        QueryStateMachine state = new QueryStateMachine();
        state.tryTransition(QueryStateMachine.State.PLANNING);
        state.tryTransition(QueryStateMachine.State.SCHEDULING);
        state.tryTransition(QueryStateMachine.State.RUNNING);
        queryManager.registerStateMachine(QUERY_ID, state);
    }

    private static final class DeferredStageCompletion implements StageCompletionCallback {
        private Consumer<Boolean> listener;
        private boolean complete;

        @Override
        public void onTaskTerminal(String queryId, String taskId, String state, ErrorDetail error) {
            // The test controls the later DAG outcome explicitly.
        }

        @Override
        public boolean isQueryComplete(String queryId) {
            return complete;
        }

        @Override
        public void onQueryTerminal(String queryId, Consumer<Boolean> observer) {
            listener = observer;
        }

        private void complete(boolean successful) {
            complete = successful;
            listener.accept(successful);
        }
    }

    @Test
    void retryAfterFailedStateTransitionDoesNotConsumeReportSequence() {
        // Reproduces a status arriving after identity preparation but before
        // the old post-HTTP task-state registration. The same report must be
        // retryable after registration instead of being rejected as stale.
        tracker.openQueryIssuance(QUERY_ID);
        registerCanonicalRequest("t-early");
        TaskStatusReport running = new TaskStatusReport(
                "t-early", QUERY_ID, 0, 0, "WRITE", WORKER_ID, "RUNNING",
                null, null, null, 1, System.currentTimeMillis(),
                null, null, canonicalAttempt("t-early"));

        assertThatThrownBy(() -> receiver.onTaskStatus(running))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown queryId");
        assertThat(tracker.getRunningStageIds(QUERY_ID)).isEmpty();

        tracker.registerTask(QUERY_ID, "t-early");
        tracker.markScheduled(QUERY_ID, "t-early");
        assertThat(receiver.onTaskStatus(running)).isTrue();
        assertThat(tracker.getTaskState(QUERY_ID, "t-early"))
                .isEqualTo(org.pangrova.coordinator.task.TaskStateMachine.State.RUNNING);
        assertThat(tracker.getRunningStageIds(QUERY_ID)).containsExactly("0");
        assertThat(tracker.getTaskDurationSnapshots(QUERY_ID, "0", Set.of(), Instant.now()))
                .singleElement().extracting(org.pangrova.coordinator.task.TaskDuration::taskId)
                .isEqualTo("t-early");

        TaskStatusReport staleAttempt = new TaskStatusReport(
                "t-early", QUERY_ID, 1, 0, "WRITE", WORKER_ID, "RUNNING",
                null, null, null, 2, System.currentTimeMillis(),
                null, null, canonicalAttempt("t-early").nextAttempt());
        assertThat(receiver.onTaskStatus(staleAttempt)).isFalse();
        assertThat(tracker.getRunningStageIds(QUERY_ID)).containsExactly("0");
        assertThat(tracker.getTaskDurationSnapshots(QUERY_ID, "1", Set.of(), Instant.now()))
                .isEmpty();
    }

    @Test
    void runningObservationPrecedesConcurrentFinishTimestamp() {
        AtomicReference<Instant> finishObservedAt = new AtomicReference<>();
        AtomicReference<Instant> startObservedAt = new AtomicReference<>();
        tracker = new CoordinatorTaskTracker() {
            @Override public boolean updateTaskState(String queryId, String taskId, String workerState) {
                boolean accepted = super.updateTaskState(queryId, taskId, workerState);
                if (accepted && "RUNNING".equals(workerState)) {
                    super.updateTaskState(queryId, taskId, "FINISHED");
                    finishObservedAt.set(Instant.now());
                }
                return accepted;
            }

            @Override public void recordTaskStart(String taskId, String stageId, Instant observedAt) {
                startObservedAt.set(observedAt);
                super.recordTaskStart(taskId, stageId, observedAt);
            }
        };
        queryManager = buildQueryManager(workerManager, tracker);
        receiver = new TaskStatusReceiver(new TaskStatusReceiverContext(
                queryManager, tracker, workerManager,
                null, null, null, null,
                catalogRegistry, writeCommitTracker, null, null, writeCommitJournal,
                ArtifactWorkReportSink.rejecting()));
        tracker.openQueryIssuance(QUERY_ID);
        registerRunningQuery();
        tracker.registerTask(QUERY_ID, "t-race");
        registerCanonicalRequest("t-race");
        tracker.markScheduled(QUERY_ID, "t-race");
        tracker.registerTask(QUERY_ID, "pending-peer");
        tracker.markScheduled(QUERY_ID, "pending-peer");

        TaskStatusReport running = new TaskStatusReport(
                "t-race", QUERY_ID, 0, 0, "WRITE", WORKER_ID, "RUNNING",
                null, null, null, 1, System.currentTimeMillis(),
                null, null, canonicalAttempt("t-race"));
        assertThat(receiver.onTaskStatus(running)).isTrue();
        assertThat(startObservedAt.get()).isNotNull().isBeforeOrEqualTo(finishObservedAt.get());
        assertThat(tracker.getTaskDurationSnapshots(QUERY_ID, "0", Set.of(), Instant.now()))
                .singleElement().satisfies(duration -> {
                    assertThat(duration.completed()).isTrue();
                    assertThat(duration.durationMs()).isGreaterThanOrEqualTo(0L);
                });
    }

    @Test
    void writeResultNonNull_collectsCommitMetadataAndCallsFinishDistributedWrite() {
        byte[] fragment1 = {1, 2, 3};
        byte[] fragment2 = {4, 5, 6};

        writeCommitTracker.registerWriteContext(
                QUERY_ID,
                CATALOG_NAME,
                new TestOutputTableHandle(),
                new TestConnectorSession(),
                binding(),
                org.pangrova.coordinator.testinfra.CoordinatorWriteReceiptFixtures.receipt(),
                new org.pangrova.coordinator.query.task.write.WriteAllocationIntent("INSERT", java.util.Map.of()), null);

        registerTwoTasks();

        receiver.onTaskStatus(finishedWithWriteResult("t-1", fragment1));
        receiver.onTaskStatus(finishedWithWriteResult("t-2", fragment2));

        assertThat(recordingMetadata.finishCalls).hasSize(1);
        RecordingConnectorMetadata.FinishCall call = recordingMetadata.finishCalls.get(0);
        assertThat(call.fragments()).hasSize(2);
        assertThat(call.fragments().get(0)).isEqualTo(fragment1);
        assertThat(call.fragments().get(1)).isEqualTo(fragment2);
    }

    @Test
    void unconfiguredQualityEvaluationCannotPublishFinishedNativeFragments() {
        writeCommitTracker.registerWriteContext(
                QUERY_ID, CATALOG_NAME, new TestOutputTableHandle(), new TestConnectorSession(),
                binding(), org.pangrova.coordinator.testinfra.CoordinatorWriteReceiptFixtures.receipt(),
                new org.pangrova.coordinator.query.task.write.WriteAllocationIntent("INSERT", Map.of()), null);
        var rules = new org.pangrova.governance.quality.rule.InMemoryQualityRuleStore();
        rules.saveRule(new org.pangrova.governance.quality.rule.DataQualityRule(
                1, "required_positive", CATALOG_NAME, "default", "events", "amount > 0",
                org.pangrova.governance.quality.alert.Severity.ERROR, "msg", true, null,
                Instant.now(), Instant.now(),
                new org.pangrova.governance.quality.rule.QualityExecutionIdentity("writer", "tenant"), null));
        receiver.setWriteQualityGate(new org.pangrova.governance.quality.gate.RuleBasedWriteQualityGate(
                rules, new org.pangrova.governance.quality.rule.UnconfiguredQualityRuleExecutor()));
        registerRunningQuery();
        registerTwoTasks();
        receiver.onTaskStatus(finishedWithWriteResult("t-1", new byte[]{1}));
        receiver.onTaskStatus(finishedWithWriteResult("t-2", new byte[]{2}));

        assertThat(recordingMetadata.finishCalls).isEmpty();
        assertThat(writeCommitJournal.getEntry(QUERY_ID)).isEmpty();
        assertThat(queryManager.getQueryState(QUERY_ID)).contains(QueryStateMachine.State.FAILED);
        assertThat(rules.getRecentResults()).singleElement().satisfies(result -> {
            assertThat(result.passed()).isFalse();
            assertThat(result.errorMessage()).contains("was not evaluated");
        });
    }

    @Test
    void qualityGateBlocksWriteBeforeCatalogCommit() {
        writeCommitTracker.registerWriteContext(
                QUERY_ID, CATALOG_NAME, new TestOutputTableHandle(), new TestConnectorSession(),
                binding(), org.pangrova.coordinator.testinfra.CoordinatorWriteReceiptFixtures.receipt(),
                new org.pangrova.coordinator.query.task.write.WriteAllocationIntent("INSERT", Map.of()), null);
        receiver.setWriteQualityGate((catalog, schema, table) ->
                org.pangrova.governance.quality.gate.WriteValidationResult.blocked(
                        List.of(), "Write blocked by quality rules: 'not_null' failed"));

        registerRunningQuery();
        registerTwoTasks();
        receiver.onTaskStatus(finishedWithWriteResult("t-1", new byte[]{1}));
        receiver.onTaskStatus(finishedWithWriteResult("t-2", new byte[]{2}));

        assertThat(recordingMetadata.finishCalls)
                .as("a quality failure must prevent the catalog commit")
                .isEmpty();
        assertThat(writeCommitJournal.getEntry(QUERY_ID))
                .as("no durable write intent is recorded for a rejected write")
                .isEmpty();
        assertThat(queryManager.getQueryState(QUERY_ID)).contains(QueryStateMachine.State.FAILED);
    }

    @Test
    void qualityGateSeesTheExactTargetAndAllowsCommit() {
        writeCommitTracker.registerWriteContext(
                QUERY_ID, CATALOG_NAME, new TestOutputTableHandle(), new TestConnectorSession(),
                binding(), org.pangrova.coordinator.testinfra.CoordinatorWriteReceiptFixtures.receipt(),
                new org.pangrova.coordinator.query.task.write.WriteAllocationIntent("INSERT", Map.of()), null);
        AtomicReference<String> observed = new AtomicReference<>();
        receiver.setWriteQualityGate((catalog, schema, table) -> {
            observed.set(catalog + "." + schema + "." + table);
            return org.pangrova.governance.quality.gate.WriteValidationResult.allowed(List.of());
        });

        registerRunningQuery();
        registerTwoTasks();
        receiver.onTaskStatus(finishedWithWriteResult("t-1", new byte[]{1}));
        receiver.onTaskStatus(finishedWithWriteResult("t-2", new byte[]{2}));

        assertThat(observed.get()).isEqualTo(CATALOG_NAME + ".default.events");
        assertThat(recordingMetadata.finishCalls).hasSize(1);
    }

    @Test
    void qualityGateEvaluationFailureFailsClosed() {
        writeCommitTracker.registerWriteContext(
                QUERY_ID, CATALOG_NAME, new TestOutputTableHandle(), new TestConnectorSession(),
                binding(), org.pangrova.coordinator.testinfra.CoordinatorWriteReceiptFixtures.receipt(),
                new org.pangrova.coordinator.query.task.write.WriteAllocationIntent("INSERT", Map.of()), null);
        receiver.setWriteQualityGate((catalog, schema, table) -> {
            throw new IllegalStateException("rule store unavailable");
        });

        registerRunningQuery();
        registerTwoTasks();
        receiver.onTaskStatus(finishedWithWriteResult("t-1", new byte[]{1}));
        receiver.onTaskStatus(finishedWithWriteResult("t-2", new byte[]{2}));

        assertThat(recordingMetadata.finishCalls).isEmpty();
        assertThat(queryManager.getQueryState(QUERY_ID)).contains(QueryStateMachine.State.FAILED);
    }

    @Test
    void writeResultNull_finishDistributedWriteNotCalled() {
        // No write context registered — simulates a pure read/compute query
        registerTwoTasks();

        receiver.onTaskStatus(finishedWithoutWriteResult("t-1"));
        receiver.onTaskStatus(finishedWithoutWriteResult("t-2"));

        assertThat(recordingMetadata.finishCalls).isEmpty();
    }

    @Test
    void allTasksComplete_noCommitMetadataCollected_callsFinishWithEmptyFragments() {
        writeCommitTracker.registerWriteContext(
                QUERY_ID,
                CATALOG_NAME,
                new TestOutputTableHandle(),
                new TestConnectorSession(),
                binding(),
                org.pangrova.coordinator.testinfra.CoordinatorWriteReceiptFixtures.receipt(),
                new org.pangrova.coordinator.query.task.write.WriteAllocationIntent("INSERT", java.util.Map.of()), null);

        registerTwoTasks();

        // First task finishes without writeResult — no commitMetadata collected
        receiver.onTaskStatus(finishedWithoutWriteResult("t-1"));

        // Second task triggers allTasksFinished → commitDistributedWriteIfNeeded
        // → fragments empty → finishDistributedWrite called with empty list
        receiver.onTaskStatus(finishedWithoutWriteResult("t-2"));

        assertThat(recordingMetadata.finishCalls).hasSize(1);
        assertThat(recordingMetadata.finishCalls.get(0).fragments()).isEmpty();
    }

    @Test
    void queryFailure_cleansUpWriteState() {
        writeCommitTracker.registerWriteContext(
                QUERY_ID,
                CATALOG_NAME,
                new TestOutputTableHandle(),
                new TestConnectorSession(),
                binding(),
                org.pangrova.coordinator.testinfra.CoordinatorWriteReceiptFixtures.receipt(),
                new org.pangrova.coordinator.query.task.write.WriteAllocationIntent("INSERT", java.util.Map.of()), null);

        tracker.registerTask(QUERY_ID, "t-1");
        registerCanonicalRequest("t-1");
        tracker.markScheduled(QUERY_ID, "t-1");

        receiver.onTaskStatus(failedReport("t-1"));

        assertThat(writeCommitTracker.hasWriteContext(QUERY_ID)).isFalse();
        assertThat(recordingMetadata.finishCalls).isEmpty();
    }

    @Test
    void staleExecutionOwnerIsFencedBeforeCatalogCommit() {
        writeCommitTracker.registerWriteContext(
                QUERY_ID,
                CATALOG_NAME,
                new TestOutputTableHandle(),
                new TestConnectorSession(),
                binding(),
                org.pangrova.coordinator.testinfra.CoordinatorWriteReceiptFixtures.receipt(),
                new org.pangrova.coordinator.query.task.write.WriteAllocationIntent("INSERT", java.util.Map.of()), null);
        AtomicInteger fenceChecks = new AtomicInteger();
        receiver.setWriteCommitFencer(new WriteCommitFencer() {
            @Override public ExecutionAnchor currentAnchor(String executionId) {
                return anchor("stale-node", 7L);
            }
            @Override public boolean isCurrent(String executionId, long token) {
                // Ownership is valid at finalization entry, then changes after the durable
                // PENDING intent is recorded but before the catalog is touched.
                return fenceChecks.incrementAndGet() == 1;
            }
        });

        registerTwoTasks();
        receiver.onTaskStatus(finishedWithWriteResult("t-1", new byte[]{1}));
        receiver.onTaskStatus(finishedWithWriteResult("t-2", new byte[]{2}));

        assertThat(recordingMetadata.finishCalls)
                .as("stale execution owner must be fenced before finishDistributedWrite")
                .isEmpty();
        assertThat(writeCommitTracker.hasWriteContext(QUERY_ID))
                .as("the replacement owner still needs the write context or durable intent")
                .isTrue();
        assertThat(writeCommitJournal.getEntry(QUERY_ID)).get()
                .extracting(org.pangrova.spi.coordination.WriteCommitEntry::state)
                .isEqualTo(org.pangrova.spi.coordination.WriteCommitEntry.State.PENDING);
    }

    @Test
    void missingLocalExecutionAnchorRejectsStatusBeforeTaskMutation() {
        registerTwoTasks();
        receiver.setWriteCommitFencer(new WriteCommitFencer() {
            @Override public ExecutionAnchor currentAnchor(String executionId) {
                throw new IllegalStateException("execution owner moved");
            }
            @Override public boolean isCurrent(String executionId, long token) {
                return false;
            }
        });

        boolean accepted = receiver.onTaskStatus(
                finishedWithWriteResult("t-1", new byte[]{1}));

        assertThat(accepted).isFalse();
        assertThat(tracker.getTaskState(QUERY_ID, "t-1"))
                .isEqualTo(org.pangrova.coordinator.task.TaskStateMachine.State.RUNNING);
    }

    @Test
    void currentExecutionOwnerCommitProceeds() {
        byte[] fragment = {9};
        writeCommitTracker.registerWriteContext(
                QUERY_ID,
                CATALOG_NAME,
                new TestOutputTableHandle(),
                new TestConnectorSession(),
                binding(),
                org.pangrova.coordinator.testinfra.CoordinatorWriteReceiptFixtures.receipt(),
                new org.pangrova.coordinator.query.task.write.WriteAllocationIntent("INSERT", java.util.Map.of()), null);
        receiver.setWriteCommitFencer(new WriteCommitFencer() {
            @Override public ExecutionAnchor currentAnchor(String executionId) {
                return anchor("active-node", 7L);
            }
            @Override public boolean isCurrent(String executionId, long token) {
                return QUERY_ID.equals(executionId) && token == 7L;
            }
        });

        registerTwoTasks();
        receiver.onTaskStatus(finishedWithWriteResult("t-1", fragment));
        receiver.onTaskStatus(finishedWithoutWriteResult("t-2"));

        assertThat(recordingMetadata.finishCalls)
                .as("current execution owner commits normally").hasSize(1);
    }

    @Test
    void replacedCatalogIsRejectedBeforeFirstCommit() {
        writeCommitTracker.registerWriteContext(
                QUERY_ID,
                CATALOG_NAME,
                new TestOutputTableHandle(),
                new TestConnectorSession(),
                binding(),
                org.pangrova.coordinator.testinfra.CoordinatorWriteReceiptFixtures.receipt(),
                new org.pangrova.coordinator.query.task.write.WriteAllocationIntent("INSERT", java.util.Map.of()), null);
        catalogRegistry.unregister(CATALOG_NAME);
        catalogRegistry.register(new CatalogDescriptor(
                CATALOG_NAME, CatalogType.CONNECTOR, "test-provider",
                CatalogState.ACTIVE, Map.of(), "", Set.of()));

        registerTwoTasks();
        receiver.onTaskStatus(finishedWithWriteResult("t-1", new byte[]{1}));
        receiver.onTaskStatus(finishedWithWriteResult("t-2", new byte[]{2}));

        assertThat(recordingMetadata.finishCalls).isEmpty();
        assertThat(writeCommitJournal.getEntry(QUERY_ID)).get()
                .extracting(org.pangrova.spi.coordination.WriteCommitEntry::state)
                .isEqualTo(org.pangrova.spi.coordination.WriteCommitEntry.State.PENDING);
    }

    private static ExecutionAnchor anchor(String ownerId, long token) {
        Instant acquired = Instant.parse("2026-01-01T00:00:00Z");
        return new ExecutionAnchor(QUERY_ID, ownerId, ownerId + "-process",
                token, acquired, acquired.plusSeconds(30));
    }

    // --- Helpers ---

    private byte[] binding() {
        return CanonicalWriteCommitTestBinding.create(
                catalogRegistry.getCatalog(CATALOG_NAME), new TestOutputTableHandle());
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
                List.of(), 100, 1024, commitMetadata);
        return new TaskStatusReport(
                taskId, QUERY_ID, 0, 0, "WRITE", WORKER_ID, "FINISHED",
                null, null, wr, sequenceCounter++, System.currentTimeMillis(),
                null, null, canonicalAttempt(taskId), canonicalReport(taskId, "FINISHED"));
    }

    private TaskStatusReport finishedWithoutWriteResult(String taskId) {
        return new TaskStatusReport(
                taskId, QUERY_ID, 0, 0, "WRITE", WORKER_ID, "FINISHED",
                null, null, null, sequenceCounter++, System.currentTimeMillis(),
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
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin
                        .TASK_REPORT_ORIGIN_NATIVE)
                .setAttempt(attempt);
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

    private static QueryManager buildQueryManager(WorkerManager wm,
                                                   CoordinatorTaskTracker tracker) {
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer());
        RemoteTaskDispatcher td = new RemoteTaskDispatcher(
                CircuitBreakerPolicy.noOp(), new NoOpTracingProvider());
        QueryDispatcherContext qdDeps = QueryDispatcherContext.withDefaults(
                new SqlCompiler(), null, null, null,
                td, scheduler, wm, tracker,
                null, null, null, 0L);
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
        ConnectorHandleFactory factory = desc -> new ConnectorCatalogHandle(
                desc, new TestConnector(metadata));
        return new CatalogRegistry(
                new ReadOnlyMetadataStore(List.of(descriptor)), factory, null, null);
    }

    private static WorkerInfo workerInfo(String workerId) {
        return new WorkerInfo(workerId, "host-1", 8080, 8081, 0,
                HealthState.ACTIVE, MemoryPressure.NONE, 0.0, 0,
                1024L * 1024 * 1024, Instant.now(), 1.0, null, 2,
                null, List.of(), null,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.canonicalInventory(),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity(workerId));
    }

    // --- Test doubles (hand-written, no mock frameworks — pangrova-rules §12) ---

    /** Records finishDistributedWrite invocations for assertion. */
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
            return null; // not exercised in write-commit tests
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession s,
                                                        ConnectorTableHandle t) {
            return null; // not exercised in write-commit tests
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession s,
                                                           ConnectorTableHandle t) {
            return Map.of(); // not exercised in write-commit tests
        }

        @Override
        public TableStatistics getTableStatistics(ConnectorSession s,
                                                   ConnectorTableHandle t) {
            return null; // not exercised in write-commit tests
        }

        @Override
        public List<String> listSchemaNames(ConnectorSession s) {
            return List.of(); // not exercised in write-commit tests
        }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession s, Optional<String> schema) {
            return List.of(); // not exercised in write-commit tests
        }
    }

    /** Returns the recording metadata for any session. */
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
        public ConnectorSplitManager getSplitManager() {
            return null; // not exercised in write-commit tests
        }

        @Override
        public ConnectorPageSourceProvider getPageSourceProvider() {
            return null; // not exercised in write-commit tests
        }
    }

    /** Marker output table handle for test wiring. */
    static final class TestOutputTableHandle implements ConnectorOutputTableHandle {
        @Override public Optional<SchemaTableName> targetTable() {
            return Optional.of(new SchemaTableName("default", "events"));
        }
        @Override public String writeMode() { return "INSERT"; }
    }

    /** Minimal session for test wiring. */
    static final class TestConnectorSession implements ConnectorSession {
        @Override
        public java.util.Optional<org.pangrova.spi.connector.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }


        @Override
        public String getUser() {
            return "test-user";
        }

        @Override
        public Optional<String> getCatalog() {
            return Optional.of(CATALOG_NAME);
        }

        @Override
        public Optional<String> getSchema() {
            return Optional.of("default");
        }

        @Override
        public Map<String, String> getProperties() {
            return Map.of("tenant_id", "tenant-test");
        }
    }

    /**
     * Pre-populated read-only metadata store for CatalogRegistry initialization.
     * Write operations are intentional no-ops since the registry only reads
     * descriptors during initialize().
     */
    static final class ReadOnlyMetadataStore implements MetadataStore {

        private final List<CatalogDescriptor> descriptors;

        ReadOnlyMetadataStore(List<CatalogDescriptor> descriptors) {
            this.descriptors = List.copyOf(descriptors);
        }

        @Override
        public List<CatalogDescriptor> listAllCatalogDescriptors() {
            return descriptors;
        }

        @Override public void saveCatalogDescriptor(CatalogDescriptor d) { /* read-only store */ }
        @Override public void deleteCatalogDescriptor(String name) { /* read-only store */ }

        @Override
        public Optional<CatalogDescriptor> getCatalogDescriptor(String name) {
            return descriptors.stream().filter(d -> d.name().equals(name)).findFirst();
        }

        @Override public void saveTable(TableMetadata m) { /* read-only store */ }

        @Override
        public Optional<TableMetadata> loadTable(String c, String s, String t) {
            return Optional.empty();
        }

        @Override public void deleteTable(String c, String s, String t) { /* read-only store */ }

        @Override
        public List<QualifiedTableRef> searchTables(String pattern) {
            return List.of();
        }

        @Override
        public Optional<Instant> getLastSyncTime(String catalogName) {
            return Optional.empty();
        }

        @Override public void updateSyncTime(String cn, Instant st) { /* read-only store */ }
        @Override public void recordSyncHistory(SyncResult r) { /* read-only store */ }
        @Override public void saveSchemas(String cn, List<String> s) { /* read-only store */ }
    }
}
