package org.pangrova.coordinator.query;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.http.CoordinatorHttpServer;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.coordinator.query.task.TaskStatusReceiverContext;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.scheduler.failure.NoAvailableWorkerException;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.testinfra.SourceFreeNativeWorkerFixture;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.pangrova.protocol.rest.query.SubmitQueryResponse;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskAttemptDescriptor;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.pangrova.testing.TestObservability;
import org.pangrova.worker.task.RemoteTaskStatusReporter;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pangrova.coordinator.testinfra.SourceFreeQueryFixture.withPolicies;

/** Real HTTP/JNI/result/status component integration in one JVM; not independent-process E2E. */
@Tag("native-required")
class PipelineIntegrationTest {
    private static final String QUERY_ID = "q1";
    private static final String WORKER_ID = "worker-1";
    private static final long TASK_MEMORY_BYTES = 64L * 1024 * 1024;
    @TempDir Path nativeDirectory;
    private final List<RemoteTaskDispatcher> clients = new ArrayList<>();
    private SourceFreeNativeWorkerFixture worker;
    private QueryManager queryManager;
    private QueryDispatcher orchestrator;
    private CoordinatorTaskTracker taskTracker;
    private TaskStatusReceiver taskStatusReceiver;
    private WorkerManager workerManager;
    private RemoteTaskDispatcher dispatcher;
    private RemoteTaskStatusReporter reporter;
    private CoordinatorHttpServer coordinatorServer;

    @BeforeEach
    void setUp() throws Exception {
        worker = new SourceFreeNativeWorkerFixture(WORKER_ID, nativeDirectory);
        taskTracker = new CoordinatorTaskTracker();
        workerManager = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        WorkerInfo actual = worker.workerInfo();
        workerManager.registerWorker(new WorkerInfo(actual.workerId(), actual.host(), actual.httpPort(),
                actual.transportPort(), actual.resultServerPort(), actual.healthState(), actual.memoryPressure(),
                actual.cpuUtilization(), actual.runningTaskCount(), TASK_MEMORY_BYTES, actual.lastHeartbeat(),
                actual.performanceScore(), actual.resourcePoolId(), actual.protocolVersion(), actual.rackId(),
                actual.dataLocality(), actual.cacheSummary(), actual.capabilityInventory()));
        dispatcher = newDispatcher();
        orchestrator = buildOrchestrator(new SqlCompiler(), workerManager, dispatcher);
        queryManager = worker.connectAsync(orchestrator, taskTracker, workerManager);
        taskStatusReceiver = new TaskStatusReceiver(new TaskStatusReceiverContext(queryManager, taskTracker,
                workerManager, null, null, null, null, null, null, orchestrator.dagEngine(), null, null));
        coordinatorServer = new CoordinatorHttpServer(0, taskStatusReceiver, queryManager, null);
        coordinatorServer.start();
        reporter = new RemoteTaskStatusReporter("http://localhost:" + coordinatorServer.getPort(),
                TestObservability.TRACING, null);
        worker.deliverReportsTo(reporter::report);
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            if (worker != null) {
                if (reporter != null) worker.deliverReportsTo(reporter::report);
                worker.close();
            }
        } finally {
            try { if (reporter != null) reporter.close(); }
            finally {
                try { if (coordinatorServer != null) coordinatorServer.stop(); }
                finally { clients.forEach(RemoteTaskDispatcher::close); }
            }
        }
    }

    @Test
    void constantQueryReachesRunning() {
        SubmitQueryResponse response = submitTestQuery();
        assertThat(response.queryId()).isEqualTo(QUERY_ID);
        assertThat(response.resultEndpoint()).isNotNull();
        assertThat(queryState()).isEqualTo(QueryStateMachine.State.RUNNING);
        finish(response.resultEndpoint());
    }

    @Test
    void dispatchCreatesWorkerTask() {
        String taskId = dispatchAndGetTaskId();
        String localId = worker.taskManager().resolveWorkerTaskId(taskId);
        assertThat(localId).isNotNull();
        assertThat(worker.taskManager().getTaskState(localId)).isPresent();
        assertThat(queryState()).isEqualTo(QueryStateMachine.State.RUNNING);
        finish(endpoint());
    }

    @Test
    void fullPipelineReachesFinished() {
        dispatchAndGetTaskId();
        finish(endpoint());
        assertThat(queryState()).isEqualTo(QueryStateMachine.State.FINISHED);
        assertThat(taskTracker.allTasksFinished(QUERY_ID)).isTrue();
    }

    @Test
    void rejectedTerminalReportDoesNotConsumeSequenceOrFinishQuery() {
        var pending = new CopyOnWriteArrayList<TaskStatusReport>();
        worker.deliverReportsTo(report -> {
            if (report.state().equals("RUNNING")) reporter.report(report);
            else pending.add(report);
        });
        String taskId = dispatchAndGetTaskId();
        worker.consume(endpoint());
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> pending.stream()
                .anyMatch(report -> report.taskId().equals(taskId) && report.state().equals("FINISHED")));
        TaskStatusReport valid = pending.stream().filter(report -> report.taskId().equals(taskId)
                && report.state().equals("FINISHED")).findFirst().orElseThrow();
        var missing = new TaskStatusReport(valid.taskId(), valid.queryId(), valid.stageId(), valid.partitionId(),
                valid.taskType(), valid.workerId(), valid.state(), valid.stats(), valid.error(), valid.writeResult(),
                valid.reportSequence(), valid.timestampMs(), valid.errorCode(), valid.errorMetadata(), valid.attempt(), null);
        assertThat(taskStatusReceiver.onTaskStatus(missing)).isFalse();
        assertThat(queryState()).isEqualTo(QueryStateMachine.State.RUNNING);
        assertThat(taskTracker.allTasksFinished(QUERY_ID)).isFalse();
        assertThat(taskStatusReceiver.onTaskStatus(valid)).isTrue();
        assertThat(queryState()).isEqualTo(QueryStateMachine.State.FINISHED);
        assertThat(worker.rows(endpoint())).containsExactly(List.of(1));
    }

    @Test
    void stateMachineReachesTerminalAfterCompletion() {
        dispatchAndGetTaskId();
        QueryStateMachine state = queryManager.getStateMachine(QUERY_ID);
        assertThat(state.getState()).isEqualTo(QueryStateMachine.State.RUNNING);
        finish(endpoint());
        assertThat(state.getState()).isEqualTo(QueryStateMachine.State.FINISHED);
        assertThat(state.isTerminal()).isTrue();
    }

    @Test
    void trackerShowsFinishedAndNoFailure() {
        dispatchAndGetTaskId();
        finish(endpoint());
        assertThat(taskTracker.allTasksFinished(QUERY_ID)).isTrue();
        assertThat(taskTracker.hasFailedTask(QUERY_ID)).isFalse();
    }

    @Test
    void compileFailureReachesFailed() {
        QueryManager manager = new QueryManager(QueryManagerContext.minimal(
                buildOrchestrator(new SqlCompiler(), workerManager, newDispatcher()), TestObservability.TRACING));
        assertThatThrownBy(() -> manager.submitQuery(QUERY_ID, "THIS IS NOT VALID SQL !!!", testSession()))
                .isInstanceOf(org.pangrova.parser.ParseException.class);
        assertThat(manager.getQueryState(QUERY_ID)).contains(QueryStateMachine.State.FAILED);
    }

    @Test
    void noAvailableWorkerReachesFailed() {
        var empty = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        QueryManager manager = new QueryManager(QueryManagerContext.minimal(
                buildOrchestrator(new SqlCompiler(), empty, newDispatcher()), TestObservability.TRACING));
        assertThatThrownBy(() -> manager.submitQuery(QUERY_ID, "SELECT 1", testSession()))
                .isInstanceOf(NoAvailableWorkerException.class);
        assertThat(manager.getQueryState(QUERY_ID)).contains(QueryStateMachine.State.FAILED);
    }

    @Test
    void drainRejectsNewQueries() {
        queryManager.setAcceptingQueries(false);
        assertThatThrownBy(this::submitTestQuery).isInstanceOf(QueryRejectedException.class)
                .hasMessageContaining("draining");
    }

    @Test
    void multiTaskAllFinishedReachesQueryFinished() {
        String first = dispatchAndGetTaskId();
        ResultEndpoint second = dispatchAdditionalTask(first, "extra-task-2", false);
        worker.consume(endpoint());
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(taskTracker.getTaskState(QUERY_ID, first))
                        .isEqualTo(org.pangrova.coordinator.task.TaskStateMachine.State.FINISHED));
        assertThat(queryState()).isEqualTo(QueryStateMachine.State.RUNNING);
        finish(second);
        assertThat(worker.rows(endpoint())).containsExactly(List.of(1));
        assertThat(taskTracker.allTasksFinished(QUERY_ID)).isTrue();
        assertThat(taskTracker.hasFailedTask(QUERY_ID)).isFalse();
    }

    @Test
    void multiTaskPartialFailureReachesQueryFailed() {
        String first = dispatchAndGetTaskId();
        dispatchAdditionalTask(first, "extra-task-2", true);
        awaitState(QueryStateMachine.State.FAILED);
        assertThat(worker.reports()).anySatisfy(report -> {
            assertThat(report.taskId()).isEqualTo("extra-task-2");
            assertThat(report.state()).isEqualTo("FAILED");
            assertThat(report.error()).isNotNull();
            var canonical = org.pangrova.protocol.v2.TaskReport.parseFrom(report.canonicalTaskReport());
            assertThat(canonical.getAttempt().getQueryOrJobId()).isEqualTo(QUERY_ID);
            assertThat(canonical.getState()).isEqualTo(org.pangrova.protocol.v2.TaskState.TASK_STATE_FAILED);
        });
        assertThat(queryManager.awaitAllQueries(5_000)).isTrue();
        assertThat(taskTracker.getTaskIds(QUERY_ID)).isEmpty();
    }

    private QueryDispatcher buildOrchestrator(SqlCompiler compiler, WorkerManager workers,
                                               RemoteTaskDispatcher transport) {
        return new QueryDispatcher(withPolicies(QueryDispatcherContext.withDefaults(compiler,
                null, null, null, transport, new TaskScheduler(new WorkerPerformanceScorer()),
                workers, taskTracker, null, null, null, 30L)), null, null);
    }

    private RemoteTaskDispatcher newDispatcher() {
        var transport = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING);
        clients.add(transport);
        transport.setControlEpochSupplier(() -> 1L);
        transport.setExecutionOwnerResolver(ignored -> new org.pangrova.protocol.rest.task.ExecutionOwnerIdentity(
                "test-coordinator", "test-incarnation", 1L));
        return transport;
    }

    private static CoordinatorSessionContext testSession() {
        return org.pangrova.coordinator.testinfra.SourceFreeQueryFixture.session(QUERY_ID);
    }

    private SubmitQueryResponse submitTestQuery() {
        return queryManager.submitQuery(QUERY_ID, "SELECT 1", testSession());
    }

    private String dispatchAndGetTaskId() {
        var response = submitTestQuery();
        var ids = taskTracker.getTaskIds(QUERY_ID);
        assertThat(ids).hasSize(1);
        String taskId = ids.iterator().next();
        assertThat(response.resultEndpoint().ticket()).isEqualTo(
                worker.resultTicket(taskTracker.getTaskRequest(taskId).taskRequest()));
        return taskId;
    }

    private ResultEndpoint dispatchAdditionalTask(String first, String taskId, boolean invalidPlan) {
        var original = taskTracker.getTaskRequest(first).taskRequest();
        var attempt = original.getAttempt().toBuilder().setTaskId(taskId).setStageId(1).build();
        var builder = original.toBuilder().setAttempt(attempt);
        if (invalidPlan) {
            byte[] invalid = {0};
            builder.setSubstraitPlan(ByteString.copyFrom(invalid))
                    .setSubstraitPlanChecksum(CanonicalTaskRequests.sha256(invalid));
        }
        var request = dispatcher.bindControlIdentity(SubmitTaskRequest.from(builder.build()));
        taskTracker.registerTask(QUERY_ID, taskId);
        taskTracker.registerTaskRequest(taskId, request);
        taskTracker.registerTaskAttempt(taskId, TaskAttemptDescriptor.from(request.taskRequest().getAttempt()));
        taskTracker.assignWorker(taskId, WORKER_ID);
        taskTracker.markScheduled(QUERY_ID, taskId);
        var target = workerManager.getWorker(WORKER_ID);
        try (var operation = taskTracker.enterQueryOperation(QUERY_ID)) {
            taskTracker.bindDispatchTarget(request, target);
            taskTracker.enterTaskDispatch(request);
            return dispatcher.dispatch(request, target).resultEndpoint();
        }
    }

    private ResultEndpoint endpoint() { return queryManager.getResultEndpoint(QUERY_ID).orElseThrow(); }
    private QueryStateMachine.State queryState() { return queryManager.getQueryState(QUERY_ID).orElseThrow(); }

    private void finish(ResultEndpoint endpoint) {
        worker.consume(endpoint);
        awaitState(QueryStateMachine.State.FINISHED);
        assertThat(worker.rows(endpoint)).containsExactly(List.of(1));
    }

    private void awaitState(QueryStateMachine.State state) {
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(queryState()).isEqualTo(state));
    }
}
