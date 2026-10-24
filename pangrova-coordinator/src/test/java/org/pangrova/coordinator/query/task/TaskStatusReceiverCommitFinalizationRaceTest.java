package org.pangrova.coordinator.query.task;

import org.pangrova.coordinator.query.task.write.NativeWriteCommitTracker;

import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryManagerContext;
import org.pangrova.coordinator.query.QueryStateMachine;
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
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.protocol.rest.data.WriteResultDescriptor;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.resource.MemoryPressure;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the query-finalization vs distributed-write-commit race
 * (spec task 187).
 *
 * <p>Multiple threads can independently observe {@code allTasksFinished} and try to
 * finalize a write query. The thread that wins {@code removeWriteState} performs the
 * distributed catalog commit; the others must NOT independently transition the query
 * to FINISHED. Before the fix, a non-winning thread reaches FINISHED before the
 * committing thread reports a commit failure, masking a failed commit as SUCCESS.</p>
 *
 * <p>The fix elects a single finalizer: only the commit-winner runs both the commit
 * and the terminal transition, so a failed commit deterministically ends FAILED.</p>
 *
 * <p>Uses hand-written test doubles (no mock frameworks) per pangrova-rules §12.</p>
 */
class TaskStatusReceiverCommitFinalizationRaceTest {

    private static final String QUERY_ID = "q-race-1";
    private static final String WORKER_ID = "w-1";
    private static final String CATALOG_NAME = "test-catalog";

    /**
     * Reproduces the concurrent finalization race: the commit-winner thread blocks
     * inside finishDistributedWrite while a non-winner thread tries to finalize, then
     * the commit fails. The query MUST end FAILED, never FINISHED.
     */
    @Test
    void commitFailureRace_queryEndsFailedNotFinished() throws Exception {
        CyclicBarrier finalizeBarrier = new CyclicBarrier(2);
        CountDownLatch commitEntered = new CountDownLatch(1);
        CountDownLatch releaseCommit = new CountDownLatch(1);
        FailingConnectorMetadata metadata =
                new FailingConnectorMetadata(commitEntered, releaseCommit);

        RaceTaskTracker tracker = new RaceTaskTracker(finalizeBarrier);
        Harness h = newHarness(tracker, metadata);

        QueryStateMachine sm = runningStateMachine();
        h.queryManager.registerStateMachine(QUERY_ID, sm);
        h.writeCommitTracker.registerWriteContext(
                QUERY_ID,
                CATALOG_NAME,
                new TaskStatusReceiverWriteCommitTest.TestOutputTableHandle(),
                new TaskStatusReceiverWriteCommitTest.TestConnectorSession(),
                binding(h),
                org.pangrova.coordinator.testinfra.CoordinatorWriteReceiptFixtures.receipt(),
                new org.pangrova.coordinator.query.task.write.WriteAllocationIntent("INSERT", java.util.Map.of()), null);
        registerTwoRunningTasks(tracker);

        tracker.armRace();

        Thread a = new Thread(() ->
                h.receiver.onTaskStatus(finishedWithWriteResult(tracker, "t-1", new byte[]{1})),
                "report-t-1");
        Thread b = new Thread(() ->
                h.receiver.onTaskStatus(finishedWithWriteResult(tracker, "t-2", new byte[]{2})),
                "report-t-2");
        a.start();
        b.start();

        // Wait until the commit-winner is inside finishDistributedWrite (blocked).
        assertThat(commitEntered.await(5, TimeUnit.SECONDS)).isTrue();

        // Give a non-winner the opportunity to (wrongly) drive the query to FINISHED
        // while the commit is still in flight. On buggy HEAD this happens within a few
        // ms; with the fix the non-winner defers and the query stays non-terminal.
        long deadline = System.currentTimeMillis() + 1_000;
        while (System.currentTimeMillis() < deadline
                && h.queryManager.getQueryState(QUERY_ID).orElse(null)
                        != QueryStateMachine.State.FINISHED) {
            Thread.sleep(5);
        }

        // Now let the commit fail.
        releaseCommit.countDown();

        a.join(5_000);
        b.join(5_000);

        assertThat(h.queryManager.getQueryState(QUERY_ID))
                .as("commit failed -> query must be FAILED, not masked as FINISHED")
                .contains(QueryStateMachine.State.FAILED);
        assertThat(metadata.finishCalls.get())
                .as("commit executed exactly once (single finalizer)")
                .isEqualTo(1);
    }

    /**
     * Happy path under the same concurrency: when the commit succeeds, exactly one
     * commit runs and the query transitions to FINISHED. Guards against the fix
     * breaking normal completion.
     */
    @Test
    void commitSuccessRace_queryEndsFinishedWithSingleCommit() throws Exception {
        CyclicBarrier finalizeBarrier = new CyclicBarrier(2);
        TaskStatusReceiverWriteCommitTest.RecordingConnectorMetadata metadata =
                new TaskStatusReceiverWriteCommitTest.RecordingConnectorMetadata();

        RaceTaskTracker tracker = new RaceTaskTracker(finalizeBarrier);
        Harness h = newHarness(tracker, metadata);

        QueryStateMachine sm = runningStateMachine();
        h.queryManager.registerStateMachine(QUERY_ID, sm);
        h.writeCommitTracker.registerWriteContext(
                QUERY_ID,
                CATALOG_NAME,
                new TaskStatusReceiverWriteCommitTest.TestOutputTableHandle(),
                new TaskStatusReceiverWriteCommitTest.TestConnectorSession(),
                binding(h),
                org.pangrova.coordinator.testinfra.CoordinatorWriteReceiptFixtures.receipt(),
                new org.pangrova.coordinator.query.task.write.WriteAllocationIntent("INSERT", java.util.Map.of()), null);
        registerTwoRunningTasks(tracker);

        tracker.armRace();

        Thread a = new Thread(() ->
                h.receiver.onTaskStatus(finishedWithWriteResult(tracker, "t-1", new byte[]{1})),
                "report-t-1");
        Thread b = new Thread(() ->
                h.receiver.onTaskStatus(finishedWithWriteResult(tracker, "t-2", new byte[]{2})),
                "report-t-2");
        a.start();
        b.start();
        a.join(5_000);
        b.join(5_000);

        assertThat(h.queryManager.getQueryState(QUERY_ID))
                .contains(QueryStateMachine.State.FINISHED);
        assertThat(metadata.finishCalls)
                .as("exactly one distributed commit despite concurrent finalizers")
                .hasSize(1);
    }

    // --- Harness wiring ---

    private record Harness(QueryManager queryManager,
                           NativeWriteCommitTracker writeCommitTracker,
                           CatalogRegistry catalogRegistry,
                           TaskStatusReceiver receiver) {}

    private static byte[] binding(Harness harness) {
        return CanonicalWriteCommitTestBinding.create(
                harness.catalogRegistry().getCatalog(CATALOG_NAME),
                new TaskStatusReceiverWriteCommitTest.TestOutputTableHandle());
    }

    private static Harness newHarness(CoordinatorTaskTracker tracker,
                                      ConnectorMetadata metadata) {
        WorkerManager workerManager = new WorkerManager(20_000, 60_000,
                new org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry());
        workerManager.registerWorker(workerInfo(WORKER_ID));
        NativeWriteCommitTracker writeCommitTracker = new NativeWriteCommitTracker();
        CatalogRegistry catalogRegistry = buildCatalogRegistry(CATALOG_NAME, metadata);
        catalogRegistry.initialize();

        QueryManager qm = buildQueryManager(workerManager, tracker);
        TaskStatusReceiverContext deps = new TaskStatusReceiverContext(
                qm, tracker, workerManager,
                null, null, null, null,
                catalogRegistry, writeCommitTracker, null, null, null);
        return new Harness(qm, writeCommitTracker, catalogRegistry,
                new TaskStatusReceiver(deps));
    }

    private static QueryStateMachine runningStateMachine() {
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryStateMachine.State.PLANNING);
        sm.tryTransition(QueryStateMachine.State.SCHEDULING);
        sm.tryTransition(QueryStateMachine.State.RUNNING);
        return sm;
    }

    private static void registerTwoRunningTasks(CoordinatorTaskTracker tracker) {
        for (String taskId : List.of("t-1", "t-2")) {
            tracker.registerTask(QUERY_ID, taskId);
            var request = org.pangrova.coordinator.CanonicalTaskRequestTestFixture.request(
                    taskId, QUERY_ID, 0, 0,
                    org.pangrova.protocol.v2.TaskKind.TASK_KIND_WRITE, new byte[]{1});
            tracker.registerTaskRequest(taskId, request);
            tracker.registerTaskAttempt(taskId,
                    org.pangrova.protocol.rest.task.TaskAttemptDescriptor.from(
                            request.taskRequest().getAttempt()));
            tracker.markScheduled(QUERY_ID, taskId);
            tracker.updateTaskState(QUERY_ID, taskId, "RUNNING");
        }
    }

    private long sequenceCounter = 100;

    private TaskStatusReport finishedWithWriteResult(
            CoordinatorTaskTracker tracker, String taskId, byte[] commitMetadata) {
        WriteResultDescriptor wr = new WriteResultDescriptor(
                List.of(), 100, 1024, commitMetadata);
        return new TaskStatusReport(
                taskId, QUERY_ID, 0, 0, "WRITE", WORKER_ID, "FINISHED",
                null, null, wr, sequenceCounter++, System.currentTimeMillis(),
                null, null, org.pangrova.protocol.rest.task.TaskAttemptDescriptor.from(
                        tracker.getTaskRequest(taskId).taskRequest().getAttempt()),
                canonicalReport(tracker, taskId));
    }

    private static byte[] canonicalReport(CoordinatorTaskTracker tracker, String taskId) {
        return org.pangrova.protocol.v2.TaskReport.newBuilder()
                .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin
                        .TASK_REPORT_ORIGIN_NATIVE)
                .setAttempt(tracker.getTaskRequest(taskId).taskRequest().getAttempt())
                .setState(org.pangrova.protocol.v2.TaskState.TASK_STATE_FINISHED)
                .setCoverage(org.pangrova.protocol.v2.CoverageReport.newBuilder()
                        .setExpectedWorkUnits(1).setCompletedWorkUnits(1)
                        .setCoverageRatio(1.0)
                        .setCompletionState(org.pangrova.protocol.v2.CompletionState
                                .COMPLETION_STATE_COMPLETE))
                .build().toByteArray();
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
        ConnectorCatalogHandle handle = new ConnectorCatalogHandle(
                descriptor, new TaskStatusReceiverWriteCommitTest.TestConnector(metadata));
        ConnectorHandleFactory factory = desc -> handle;
        return new CatalogRegistry(
                new TaskStatusReceiverWriteCommitTest.ReadOnlyMetadataStore(
                        List.of(descriptor)),
                factory, null, null);
    }

    private static WorkerInfo workerInfo(String workerId) {
        return new WorkerInfo(workerId, "host-1", 8080, 8081, 0,
                HealthState.ACTIVE, MemoryPressure.NONE, 0.0, 0,
                1024L * 1024 * 1024, Instant.now(), 1.0, null, 2,
                null, List.of(), null,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.canonicalInventory(),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity(workerId));
    }

    // --- Test doubles ---

    /**
     * Task tracker that, once armed, forces every {@code allTasksFinished} caller to
     * rendezvous on a barrier and then reports all tasks finished. This deterministically
     * drives two concurrent threads into the finalization path simultaneously.
     */
    static final class RaceTaskTracker extends CoordinatorTaskTracker {

        private final CyclicBarrier barrier;
        private volatile boolean armed;

        RaceTaskTracker(CyclicBarrier barrier) {
            this.barrier = barrier;
        }

        void armRace() {
            this.armed = true;
        }

        @Override
        public boolean allTasksFinished(String queryId) {
            if (armed) {
                try {
                    barrier.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                } catch (BrokenBarrierException | java.util.concurrent.TimeoutException e) {
                    throw new RuntimeException(e);
                }
                return true;
            }
            return super.allTasksFinished(queryId);
        }
    }

    /**
     * Connector metadata whose distributed commit blocks until released, then throws —
     * simulating a failed/incomplete catalog commit while a concurrent thread races to
     * finalize the query.
     */
    static final class FailingConnectorMetadata implements ConnectorMetadata {

        final AtomicInteger finishCalls = new AtomicInteger();
        private final CountDownLatch entered;
        private final CountDownLatch release;

        FailingConnectorMetadata(CountDownLatch entered, CountDownLatch release) {
            this.entered = entered;
            this.release = release;
        }

        @Override
        public void finishDistributedWrite(ConnectorSession session,
                                           ConnectorOutputTableHandle outputHandle,
                                           List<byte[]> fragments) {
            finishCalls.incrementAndGet();
            entered.countDown();
            try {
                release.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException("simulated distributed commit failure");
        }

        @Override
        public void finishDistributedWrite(ConnectorSession session,
                                           ConnectorOutputTableHandle outputHandle,
                                           List<byte[]> fragments,
                                           org.pangrova.spi.connector.metadata.DistributedWriteCommitContext context) {
            finishDistributedWrite(session, outputHandle, fragments);
        }

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
}
