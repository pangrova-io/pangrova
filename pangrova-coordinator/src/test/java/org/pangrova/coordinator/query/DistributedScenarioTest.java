package org.pangrova.coordinator.query;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pangrova.coordinator.testinfra.SourceFreeQueryFixture.withPolicies;

import com.google.protobuf.ByteString;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.http.CoordinatorHttpServer;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.coordinator.query.task.TaskStatusReceiverContext;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.testinfra.SourceFreeNativeWorkerFixture;
import org.pangrova.coordinator.worker.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.pangrova.protocol.rest.query.SubmitQueryResponse;
import org.pangrova.protocol.rest.task.ExecutionOwnerIdentity;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskAttemptDescriptor;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.pangrova.spi.security.UserIdentity;
import org.pangrova.testing.TestObservability;
import org.pangrova.worker.task.RemoteTaskStatusReporter;

/**
 * Single-JVM HTTP routing/JNI component scenarios, not multi-process E2E or independent capacity.
 * Two real TaskManagers share one Native runtime, result server and resource pool. Each routing
 * snapshot advertises a 64 MiB task budget; five concurrent queries fit the actual global pool.
 * Successful completion is reported only after the real Flight consumer reaches EOF.
 */
@Tag("native-required")
class DistributedScenarioTest {
    private static final String QUERY_ID = "dist-q1";
    private static final String WORKER_1_ID = "worker-1";
    private static final String WORKER_2_ID = "worker-2";
    private static final long ROUTING_MEMORY_BYTES = 64L * 1024 * 1024;
    private static final UserIdentity TEST_IDENTITY = new UserIdentity(
            "test-user", Set.of("public"), Map.of(), Instant.EPOCH);
    @TempDir Path nativeDirectory;
    private SourceFreeNativeWorkerFixture nativeWorker;
    private SourceFreeNativeWorkerFixture.WorkerNode worker2;
    private CoordinatorHttpServer coordinatorServer;
    private QueryManager queryManager;
    private QueryDispatcher orchestrator;
    private CoordinatorTaskTracker taskTracker;
    private WorkerManager workerManager;
    private RemoteTaskDispatcher remoteTaskDispatcher;
    private RemoteTaskStatusReporter statusReporter;

    @BeforeEach
    void setUp() throws Exception {
        nativeWorker = new SourceFreeNativeWorkerFixture(WORKER_1_ID, nativeDirectory);
        worker2 = nativeWorker.addWorker(WORKER_2_ID);
        taskTracker = new CoordinatorTaskTracker();
        remoteTaskDispatcher = testDispatcher();
        workerManager = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        workerManager.registerWorker(routingSnapshot(nativeWorker.workerInfo(), 1.0, Instant.now()));
        workerManager.registerWorker(routingSnapshot(nativeWorker.workerInfo(worker2), 1.0, Instant.now()));
        var deps = QueryDispatcherContext.withDefaults(new SqlCompiler(), null, null, null,
                remoteTaskDispatcher, new TaskScheduler(new WorkerPerformanceScorer()),
                workerManager, taskTracker, null, null, null, 30_000L);
        orchestrator = new QueryDispatcher(withPolicies(deps), null, null);
        queryManager = nativeWorker.connectAsync(orchestrator, taskTracker, workerManager);
        var receiver = new TaskStatusReceiver(new TaskStatusReceiverContext(queryManager,
                taskTracker, workerManager, null, null, null, null, null, null,
                orchestrator.dagEngine(), null, null));
        coordinatorServer = new CoordinatorHttpServer(0, receiver, queryManager, null);
        coordinatorServer.start();
        statusReporter = new RemoteTaskStatusReporter(
                "http://localhost:" + coordinatorServer.getPort(), TestObservability.TRACING, null);
        nativeWorker.deliverReportsTo(statusReporter::report);
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            if (nativeWorker != null) nativeWorker.close();
        } finally {
            try {
                if (statusReporter != null) statusReporter.close();
            } finally {
                if (coordinatorServer != null) coordinatorServer.stop();
                if (remoteTaskDispatcher != null) remoteTaskDispatcher.close();
            }
        }
    }

    @Test
    void submitQueryDispatchesTaskViaHttp() {
        String taskId = dispatchAndGetTaskId(QUERY_ID);
        assertThat(taskId).isNotNull();
        assertThat(queryState(QUERY_ID)).isEqualTo(QueryStateMachine.State.RUNNING);
        finishQuery(QUERY_ID);
    }

    @Test
    void dispatchedTaskExistsOnWorker() {
        String taskId = dispatchAndGetTaskId(QUERY_ID);
        var tasks = WORKER_1_ID.equals(taskTracker.getWorkerForTask(taskId))
                ? nativeWorker.taskManager() : worker2.taskManager();
        assertThat(tasks.resolveWorkerTaskId(taskId)).isNotNull();
        assertThat(tasks.getTaskState(tasks.resolveWorkerTaskId(taskId))).isPresent();
        assertThat(queryState(QUERY_ID)).isEqualTo(QueryStateMachine.State.RUNNING);
        finishQuery(QUERY_ID);
    }

    @Test
    void canonicalHttpTaskReportsFinishQuery() {
        String taskId = dispatchAndGetTaskId(QUERY_ID);
        finishQuery(QUERY_ID);
        assertThat(taskTracker.allTasksFinished(QUERY_ID)).isTrue();
        assertThat(nativeWorker.reports()).anySatisfy(report -> {
            assertThat(report.taskId()).isEqualTo(taskId);
            assertThat(report.state()).isEqualTo("FINISHED");
            assertThat(report.canonicalTaskReport()).isNotEmpty();
        });
    }

    @Test
    void schedulerSelectsHigherScoringWorker() {
        workerManager.registerWorker(routingSnapshot(nativeWorker.workerInfo(), 0.3, Instant.now()));
        workerManager.registerWorker(routingSnapshot(nativeWorker.workerInfo(worker2), 0.9, Instant.now()));
        String taskId = dispatchAndGetTaskId("score-q");
        assertThat(taskTracker.getWorkerForTask(taskId)).isEqualTo(WORKER_2_ID);
        assertThat(worker2.taskManager().resolveWorkerTaskId(taskId)).isNotNull();
        assertThat(nativeWorker.taskManager().resolveWorkerTaskId(taskId)).isNull();
        assertThat(endpoint("score-q").port()).isEqualTo(workerManager.getWorker(WORKER_2_ID).resultServerPort());
        finishQuery("score-q");
    }

    @Test
    void heartbeatTimeoutDetectsStaleWorker() {
        WorkerManager health = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        health.registerWorker(routingSnapshot(nativeWorker.workerInfo(), 1.0, Instant.now().minusSeconds(30)));
        assertThat(health.evaluateHealth(WORKER_1_ID, Instant.now())).isEqualTo(HealthState.SUSPECT);
    }

    @Test
    void drainRejectsNewQueriesButAllowsInFlightCompletion() {
        dispatchAndGetTaskId("drain-q1");
        queryManager.setAcceptingQueries(false);
        assertThatThrownBy(() -> submitTestQuery("drain-q2")).isInstanceOf(QueryRejectedException.class);
        finishQuery("drain-q1");
        assertThat(queryState("drain-q1")).isEqualTo(QueryStateMachine.State.FINISHED);
    }

    @Test
    void sourceFreeQueryWithWriteScenarioIdIsAdmitted() {
        dispatchAndGetTaskId("write-q1");
        assertThat(queryState("write-q1")).isEqualTo(QueryStateMachine.State.RUNNING);
        finishQuery("write-q1");
    }

    @Test
    void multipleAdmittedTasksFinishQueryAfterAllReports() {
        String task0 = dispatchAndGetTaskId("multi-q1");
        ResultEndpoint second = dispatchAdditionalTask(task0, "multi-q1-stage1-task", false);
        nativeWorker.consume(endpoint("multi-q1"));
        awaitTaskFinished(task0);
        assertThat(taskTracker.allTasksFinished("multi-q1")).isFalse();
        assertThat(queryState("multi-q1")).isEqualTo(QueryStateMachine.State.RUNNING);
        nativeWorker.consume(second);
        awaitQueryState("multi-q1", QueryStateMachine.State.FINISHED);
        assertOne(nativeWorker.rows(second));
        assertThat(taskTracker.allTasksFinished("multi-q1")).isTrue();
    }

    @Test
    void sourceFreeQueryWithShuffleScenarioIdIsAdmitted() {
        dispatchAndGetTaskId("shuffle-q1");
        assertThat(queryState("shuffle-q1")).isEqualTo(QueryStateMachine.State.RUNNING);
        finishQuery("shuffle-q1");
    }

    @Test
    void concurrentQuerySubmissionsAllReachRunning() throws Exception {
        var submitters = Executors.newFixedThreadPool(5);
        List<Future<SubmitQueryResponse>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < 5; i++) {
                String id = "concurrent-q" + i;
                futures.add(submitters.submit(() -> submitTestQuery(id)));
            }
            for (int i = 0; i < 5; i++) {
                var response = futures.get(i).get(30, TimeUnit.SECONDS);
                assertThat(response.queryId()).isEqualTo("concurrent-q" + i);
                assertThat(response.resultEndpoint()).isNotNull();
                assertThat(queryState(response.queryId())).isEqualTo(QueryStateMachine.State.RUNNING);
            }
            for (int i = 0; i < 5; i++) finishQuery("concurrent-q" + i);
        } finally {
            submitters.shutdownNow();
            assertThat(submitters.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void taskFailureDetectedByTracker() {
        String taskId = dispatchAndGetTaskId("resched-q1");
        dispatchAdditionalTask(taskId, "resched-q1-invalid-plan", true);
        awaitQueryState("resched-q1", QueryStateMachine.State.FAILED);
        assertThat(taskTracker.hasFailedTask("resched-q1")).isTrue();
        assertActualFailure("resched-q1-invalid-plan");
    }

    @Test
    void queryCancellationPropagates() throws Exception {
        String taskId = dispatchAndGetTaskId("cancel-q1");
        var expectedAttempt = taskTracker.getTaskRequest(taskId).taskRequest().getAttempt();
        assertThat(queryManager.cancelQuery("cancel-q1")).isTrue();
        assertThat(queryState("cancel-q1")).isEqualTo(QueryStateMachine.State.CANCELLED);
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(() ->
                nativeWorker.reports().stream().anyMatch(report -> report.taskId().equals(taskId)
                        && report.state().equals("CANCELLED")));
        var terminal = nativeWorker.reports().stream().filter(report -> report.taskId().equals(taskId)
                && report.state().equals("CANCELLED")).findFirst().orElseThrow();
        var canonical = org.pangrova.protocol.v2.TaskReport.parseFrom(terminal.canonicalTaskReport());
        assertThat(terminal.queryId()).isEqualTo("cancel-q1");
        assertThat(terminal.attempt()).isEqualTo(TaskAttemptDescriptor.from(expectedAttempt));
        assertThat(canonical.getAttempt()).isEqualTo(expectedAttempt);
        assertThat(canonical.getAttempt().getTaskId()).isEqualTo(taskId);
        assertThat(canonical.getState()).isEqualTo(org.pangrova.protocol.v2.TaskState.TASK_STATE_CANCELLED);
        nativeWorker.awaitDispatchesDrained();
    }

    @Test
    void haQueryOwnershipTransferIteratesActiveQueries() {
        dispatchAndGetTaskId("ha-q1");
        assertThat(queryManager.getActiveQueryIds()).contains("ha-q1");
        finishQuery("ha-q1");
    }

    @Test
    void partialTaskFailureCausesQueryFailure() {
        String task0 = dispatchAndGetTaskId("pwf-q1");
        ResultEndpoint second = dispatchAdditionalTask(task0, "pwf-q1-task2", false);
        nativeWorker.consume(endpoint("pwf-q1"));
        awaitTaskFinished(task0);
        dispatchAdditionalTask(task0, "pwf-q1-invalid-plan", true);
        awaitQueryState("pwf-q1", QueryStateMachine.State.FAILED);
        assertThat(second).isNotNull();
        assertThat(taskTracker.hasFailedTask("pwf-q1")).isTrue();
        assertActualFailure("pwf-q1-invalid-plan");
    }

    private static RemoteTaskDispatcher testDispatcher() {
        var dispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING);
        dispatcher.setControlEpochSupplier(() -> 1L);
        dispatcher.setExecutionOwnerResolver(ignored -> new ExecutionOwnerIdentity(
                "test-coordinator", "test-incarnation", 1L));
        return dispatcher;
    }

    private SubmitQueryResponse submitTestQuery(String queryId) {
        var session = new CoordinatorSessionContext(queryId, Map.of(), TEST_IDENTITY, "test-tenant");
        return queryManager.submitQuery(queryId, "SELECT 1", session);
    }

    private String dispatchAndGetTaskId(String queryId) {
        return submitTestQuery(queryId).resultEndpoint().ticket();
    }

    private ResultEndpoint dispatchAdditionalTask(String originalTaskId, String taskId, boolean invalidPlan) {
        var original = taskTracker.getTaskRequest(originalTaskId).taskRequest();
        var attempt = original.getAttempt().toBuilder().setTaskId(taskId).setStageId(1).build();
        var builder = original.toBuilder().setAttempt(attempt);
        if (invalidPlan) {
            // Protobuf tag zero is invalid. Native's parser rejects it, not a fabricated status.
            byte[] malformedPlan = {0};
            builder.setSubstraitPlan(ByteString.copyFrom(malformedPlan))
                    .setSubstraitPlanChecksum(CanonicalTaskRequests.sha256(malformedPlan));
        }
        var request = remoteTaskDispatcher.bindControlIdentity(SubmitTaskRequest.from(builder.build()));
        String queryId = attempt.getQueryOrJobId();
        taskTracker.registerTask(queryId, taskId);
        taskTracker.registerTaskRequest(taskId, request);
        taskTracker.registerTaskAttempt(taskId, TaskAttemptDescriptor.from(request.taskRequest().getAttempt()));
        taskTracker.assignWorker(taskId, WORKER_1_ID);
        taskTracker.markScheduled(queryId, taskId);
        var result = remoteTaskDispatcher.dispatch(request, workerManager.getWorker(WORKER_1_ID));
        assertThat(result.taskId()).isEqualTo(taskId);
        return result.resultEndpoint();
    }

    private void finishQuery(String queryId) {
        ResultEndpoint result = endpoint(queryId);
        nativeWorker.consume(result);
        awaitQueryState(queryId, QueryStateMachine.State.FINISHED);
        assertOne(nativeWorker.rows(result));
    }

    private static void assertOne(List<List<Object>> rows) {
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0)).hasSize(1);
        assertThat(((Number) rows.get(0).get(0)).intValue()).isEqualTo(1);
    }

    private void assertActualFailure(String taskId) {
        assertThat(nativeWorker.reports()).anySatisfy(report -> {
            assertThat(report.taskId()).isEqualTo(taskId);
            assertThat(report.state()).isEqualTo("FAILED");
            assertThat(report.error()).isNotNull();
            assertThat(report.canonicalTaskReport()).isNotEmpty();
        });
    }

    private ResultEndpoint endpoint(String queryId) {
        return queryManager.getResultEndpoint(queryId).orElseThrow();
    }

    private QueryStateMachine.State queryState(String queryId) {
        return queryManager.getQueryState(queryId).orElseThrow();
    }

    private void awaitTaskFinished(String taskId) {
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(() ->
                nativeWorker.reports().stream().anyMatch(report -> report.taskId().equals(taskId)
                        && report.state().equals("FINISHED")));
    }

    private void awaitQueryState(String queryId, QueryStateMachine.State expected) {
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(queryState(queryId)).isEqualTo(expected));
    }

    private static WorkerInfo routingSnapshot(WorkerInfo actual, double score, Instant heartbeat) {
        return new WorkerInfo(actual.workerId(), actual.host(), actual.httpPort(), actual.transportPort(),
                actual.resultServerPort(), actual.healthState(), actual.memoryPressure(), 1.0 - score,
                actual.runningTaskCount(), ROUTING_MEMORY_BYTES, heartbeat, score, actual.resourcePoolId(),
                actual.protocolVersion(), actual.rackId(), actual.dataLocality(), actual.cacheSummary(),
                actual.capabilityInventory());
    }
}
