package org.pangrova.coordinator.query.task;


import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

import org.pangrova.coordinator.testinfra.QueryManagerTestContexts;

import org.pangrova.query.api.enums.QueryState;

import org.pangrova.scheduler.api.speculation.TaskDuration;

import org.pangrova.coordinator.query.task.write.NativeWriteCommitTracker;
import org.pangrova.coordinator.query.task.write.WriteCommitFencer;
import org.pangrova.coordinator.query.task.report.GenerationWorkReportSink;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.query.lifecycle.QueryStateMachine;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.query.api.stage.StageCompletionCallback;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.scheduler.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.scheduler.api.enums.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.builtin.placement.LoadBalancedStrategy;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.api.catalog.ConnectorHandleFactory;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.protocol.rest.data.WriteResultDescriptor;
import org.pangrova.query.api.error.QueryFailure;
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
import org.pangrova.execution.api.ownership.ExecutionAnchor;
import org.pangrova.persistence.query.memory.transaction.InMemoryWriteCommitJournal;
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
                new org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry());
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
                GenerationWorkReportSink.rejecting());
        receiver = new TaskStatusReceiver(deps);
    }

    @Test
    void missingWriteJournalIsRejectedBeforeTaskStatusReceiverConstruction() {
        assertThatThrownBy(() -> TaskStatusReceiverContext.withoutSpeculation(
                queryManager, tracker, workerManager, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("writeCommitJournal must be selected explicitly");
    }

    @Test
    void deferredDagCompletionFinalizesOnTerminalEvent() {
        DeferredStageCompletion callback = new DeferredStageCompletion();
        receiver = receiverWithStageCallback(callback);
        registerRunningQuery();
        registerTwoTasks();

        receiver.onTaskStatus(finishedWithoutWriteResult("t-1"));
        receiver.onTaskStatus(finishedWithoutWriteResult("t-2"));

        assertThat(queryManager.getQueryState(QUERY_ID)).contains(QueryState.RUNNING);
        assertThat(callback.listener).isNotNull();
        callback.complete(true);
        assertThat(queryManager.getQueryState(QUERY_ID)).contains(QueryState.FINISHED);
    }

    @Test
    void failedPhysicalAttemptCannotOverrideSuccessfulLogicalDagCompletion() {
        var callback = new DeferredStageCompletion();
        receiver = receiverWithStageCallback(callback);
        registerRunningQuery();
        registerTwoTasks();
        assertThat(receiver.onTaskStatus(failedReport("t-1"))).isTrue();
        assertThat(tracker.hasFailedTask(QUERY_ID)).isTrue();
        assertThat(queryManager.getQueryState(QUERY_ID)).contains(QueryState.RUNNING);
        assertThat(receiver.onTaskStatus(finishedWithoutWriteResult("t-2"))).isTrue();
        callback.complete(true);
        assertThat(queryManager.getQueryState(QUERY_ID)).contains(QueryState.FINISHED);
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

        assertThat(queryManager.getQueryState(QUERY_ID)).contains(QueryState.RUNNING);
    }

    @Test
    void terminalDagCallbackSeesWriteFragmentBeforeCatalogCommit() {
        byte[] fragment = {7, 8, 9};
        AtomicInteger fragmentsAtTerminal = new AtomicInteger(-1);
        AtomicBoolean dagComplete = new AtomicBoolean();
        AtomicReference<Consumer<Boolean>> terminalListener = new AtomicReference<>();
        receiver = receiverWithStageCallback(new StageCompletionCallback() {
            @Override public void onTaskTerminal(String queryId, String taskId, String state, QueryFailure error) {
                if (!"t-2".equals(taskId)) return;
                var writeState = writeCommitTracker.peekWriteState(queryId);
                fragmentsAtTerminal.set(writeState == null ? -1 : writeState.fragments().size());
                dagComplete.set(true);
                Consumer<Boolean> listener = terminalListener.get();
                if (listener != null) listener.accept(true);
            }

            @Override public boolean ownsQueryCompletion(String queryId) { return true; }

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
                GenerationWorkReportSink.rejecting()));
    }

    private void registerRunningQuery() {
        QueryStateMachine state = new QueryStateMachine();
        state.tryTransition(QueryState.PLANNING);
        state.tryTransition(QueryState.SCHEDULING);
        state.tryTransition(QueryState.RUNNING);
        queryManager.registerStateMachine(QUERY_ID, state);
    }

    private static final class DeferredStageCompletion implements StageCompletionCallback {
        private Consumer<Boolean> listener;
        private boolean complete;

        @Override
        public void onTaskTerminal(String queryId, String taskId, String state, QueryFailure error) {
            // The test controls the later DAG outcome explicitly.
        }

        @Override public boolean ownsQueryCompletion(String queryId) { return true; }

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
                .isEqualTo(org.pangrova.coordinator.enums.ScheduledTaskState.RUNNING);
        assertThat(tracker.getRunningStageIds(QUERY_ID)).containsExactly("0");
        assertThat(tracker.getTaskDurationSnapshots(QUERY_ID, "0", Set.of(), Instant.now()))
                .singleElement().extracting(org.pangrova.scheduler.api.speculation.TaskDuration::taskId)
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
                GenerationWorkReportSink.rejecting()));
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
        var rules = new org.pangrova.governance.runtime.quality.InMemoryQualityRuleStore();
        rules.saveRule(new org.pangrova.governance.api.quality.DataQualityRule(
                1, "required_positive", CATALOG_NAME, "default", "events", "amount > 0",
                org.pangrova.governance.api.enums.Severity.ERROR, "msg", true, null,
                Instant.now(), Instant.now(),
                new org.pangrova.governance.api.quality.QualityExecutionIdentity("writer", "tenant"), null));
        receiver.setWriteQualityGate(new org.pangrova.governance.runtime.quality.RuleBasedWriteQualityGate(
                rules, new org.pangrova.governance.runtime.quality.UnconfiguredQualityRuleExecutor()));
        registerRunningQuery();
        registerTwoTasks();
        receiver.onTaskStatus(finishedWithWriteResult("t-1", new byte[]{1}));
        receiver.onTaskStatus(finishedWithWriteResult("t-2", new byte[]{2}));

        assertThat(recordingMetadata.finishCalls).isEmpty();
        assertThat(writeCommitJournal.getEntry(QUERY_ID)).isEmpty();
        assertThat(queryManager.getQueryState(QUERY_ID)).contains(QueryState.FAILED);
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
                org.pangrova.governance.api.quality.WriteValidationResult.blocked(
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
        assertThat(queryManager.getQueryState(QUERY_ID)).contains(QueryState.FAILED);
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
            return org.pangrova.governance.api.quality.WriteValidationResult.allowed(List.of());
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
        assertThat(queryManager.getQueryState(QUERY_ID)).contains(QueryState.FAILED);
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
                .extracting(org.pangrova.persistence.query.api.commit.WriteCommitEntry::state)
                .isEqualTo(org.pangrova.persistence.query.api.enums.WriteCommitState.PENDING);
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
                .isEqualTo(org.pangrova.coordinator.enums.ScheduledTaskState.RUNNING);
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
                .extracting(org.pangrova.persistence.query.api.commit.WriteCommitEntry::state)
                .isEqualTo(org.pangrova.persistence.query.api.enums.WriteCommitState.PENDING);
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
        ConnectorHandleFactory factory = desc -> new ConnectorCatalogHandle(
                desc, new TestConnector(metadata));
        return new CatalogRegistry(memoryTableStores(),
                new TaskWriteCatalogDeployments(List.of(descriptor)), factory, null, null);
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
        public java.util.Optional<org.pangrova.connector.api.query.ConnectorQueryAuthority> getQueryAuthority() {
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
}
