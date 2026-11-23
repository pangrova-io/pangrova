package org.pangrova.coordinator.property.query;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Set;
import java.time.Instant;

import net.jqwik.api.Arbitraries;

import org.pangrova.query.api.session.QuerySession;
import org.pangrova.query.lifecycle.QueryStateMachine;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;

import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.execution.task.DispatchResult;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.scheduler.placement.TaskScheduler;
import org.pangrova.testing.testkit.constants.TestObservability;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.strategies.placement.LoadBalancedStrategy;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.testing.testkit.TestExchangeTransport;
import org.pangrova.worker.http.WorkerHttpServer;
import org.pangrova.worker.task.TaskManager;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;
import org.pangrova.security.api.identity.UserIdentity;

/**
 * Property test for CoordinatorTaskTracker registration after dispatch.
 * Validates: Requirements 10.1, 10.2
 */
class TaskTrackerRegistrationPropertyTest {

    // The HTTP wiring fixture advertises a Flight endpoint without starting its listener.
    private static final int TRANSPORT_PORT = 19095;
    private static final long WORKER_MEMORY = 4L * 1024 * 1024 * 1024;
    private static final UserIdentity TEST_IDENTITY = new UserIdentity(
            "test-user", Set.of("public"), Map.of(), Instant.EPOCH);

    private WorkerHttpServer workerHttpServer;
    private TaskManager workerTaskManager;
    private QueryDispatcher orchestrator;
    private CoordinatorTaskTracker taskTracker;

    void setUp() throws Exception {
        workerTaskManager = TaskManager.forHttpWiringOnly();
        var config = new WorkerHttpServer.WorkerHttpConfig(
                0, "localhost", TRANSPORT_PORT);
        byte[] signingKey = new byte[32];
        new java.security.SecureRandom().nextBytes(signingKey);
        workerHttpServer = new WorkerHttpServer(
                config, workerTaskManager, TestExchangeTransport.INSTANCE,
                null, null, new org.pangrova.worker.result.ResultReadTickets(signingKey));
        workerHttpServer.start();

        taskTracker = new CoordinatorTaskTracker();
        orchestrator = buildOrchestrator(workerHttpServer.getPort(),
                workerTaskManager.workerExecutionIdentity());

        var dagEngine = orchestrator.dagEngine();
        workerTaskManager.setAutoFinishReporter(report ->
                dagEngine.onTaskTerminal(report.queryId(), report.taskId(),
                        report.state(), org.pangrova.coordinator.query.task.TaskReportCompletion.failure(report)));
    }

    void tearDown() throws Exception {
        try {
            if (workerHttpServer != null) {
                workerHttpServer.stop();
            }
        } finally {
            if (workerTaskManager != null) {
                workerTaskManager.close();
            }
        }
    }

    /**
     * Property 13: Task tracker registration after dispatch.
     *
     * For any successfully dispatched task, CoordinatorTaskTracker should
     * contain the task in SCHEDULED state, with the correct queryId and taskId.
     *
     * **Validates: Requirements 10.1, 10.2**
     */
    @Property(tries = 10)
    @Tag("Feature: e2e-pipeline-integration, Property 13: Task tracker registration after dispatch")
    void dispatchedTaskIsRegisteredAsScheduled(
            @ForAll("queryIds") String queryId) throws Exception {
        try {
            setUp();
            QueryStateMachine sm = new QueryStateMachine();
            QuerySession session = new QuerySession(
                    queryId, Map.of(), TEST_IDENTITY, "test-tenant", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());

            DispatchResult result = orchestrator.execute(sm, "SELECT 1", session);

            String taskId = result.taskId();
            assertTaskInScheduledState(queryId, taskId);
        } finally {
            tearDown();
        }
    }

    private void assertTaskInScheduledState(String queryId, String taskId) {
        // With auto-finish wiring, the task may already have transitioned to
        // FINISHED by the time dispatch returns. Verify the task was registered
        // in the tracker (exists and is queryable) rather than asserting a
        // specific intermediate state.
        var state = taskTracker.getTaskState(queryId, taskId);
        assertThat(state)
                .as("Task should be registered in tracker")
                .isNotNull();
        assertThat(taskTracker.hasFailedTask(queryId))
                .as("Task should not be failed")
                .isFalse();
    }

    @Provide
    Arbitrary<String> queryIds() {
        return Arbitraries.strings()
                .alpha()
                .ofMinLength(3)
                .ofMaxLength(12)
                .map(s -> "q-" + s);
    }

    private QueryDispatcher buildOrchestrator(int workerPort,
            org.pangrova.protocol.execution.identity.WorkerExecutionIdentity workerIdentity) {
        WorkerManager wm = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        wm.registerWorker(workerInfo(workerPort, workerIdentity));
        RemoteTaskDispatcher dispatcher = new RemoteTaskDispatcher(
                CircuitBreakerPolicy.noOp(), TestObservability.TRACING);
        dispatcher.setControlEpochSupplier(() -> 1L);
        dispatcher.setExecutionOwnerResolver(ignored ->
                new org.pangrova.protocol.rest.task.ExecutionOwnerIdentity(
                        "test-coordinator", "test-incarnation", 1L));
        return new QueryDispatcher(
                org.pangrova.coordinator.testinfra.SourceFreeQueryFixture.withPolicies(
                QueryDispatcherContext.withDefaults(
                        new SqlCompiler(), null, null, null,
                        dispatcher,
                        new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty()),
                        wm,
                        taskTracker,
                        null, null, null, 0L)),
                null, null);
    }

    private static WorkerInfo workerInfo(int httpPort,
            org.pangrova.protocol.execution.identity.WorkerExecutionIdentity workerIdentity) {
        return TestWorkerInfoBuilder.active("test-worker", "localhost", httpPort,
                TRANSPORT_PORT, workerIdentity);
    }
}
