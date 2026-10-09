package org.pangrova.coordinator.property.query;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Set;
import java.time.Instant;

import net.jqwik.api.Arbitraries;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;
import net.jqwik.api.lifecycle.AfterTry;
import net.jqwik.api.lifecycle.BeforeTry;

import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.testing.TestObservability;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.coordinator.testinfra.SourceFreeNativeWorkerFixture;
import org.pangrova.coordinator.testinfra.SourceFreeQueryFixture;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.pangrova.spi.security.UserIdentity;

/**
 * Property test for CoordinatorTaskTracker registration after dispatch.
 * Validates: Requirements 10.1, 10.2
 */
@Tag("native-required")
@Tag("routing-integration")
class TaskTrackerRegistrationPropertyTest {

    private static final UserIdentity TEST_IDENTITY = new UserIdentity(
            "test-user", Set.of("public"), Map.of(), Instant.EPOCH);

    private SourceFreeNativeWorkerFixture worker;
    private RemoteTaskDispatcher remote;
    private QueryDispatcher orchestrator;
    private CoordinatorTaskTracker taskTracker;

    @BeforeTry
    void setUp() throws Exception {
        worker = new SourceFreeNativeWorkerFixture("test-worker",
                java.nio.file.Files.createTempDirectory("task-registration-native-"));
        taskTracker = new CoordinatorTaskTracker();
        orchestrator = buildOrchestrator();
    }

    @AfterTry
    void tearDown() throws Exception {
        try { if (worker != null) worker.close(); }
        finally { if (remote != null) remote.close(); }
    }

    /**
     * Property 13: Task tracker registration after dispatch.
     *
     * For any successfully dispatched task, CoordinatorTaskTracker should
     * retain the registered task through real Native completion, with its exact queryId and taskId.
     *
     * **Validates: Requirements 10.1, 10.2**
     */
    @Property(tries = 10)
    @Tag("Feature: e2e-pipeline-integration, Property 13: Task tracker registration after dispatch")
    void dispatchedTaskIsRegisteredAsScheduled(
            @ForAll("queryIds") String queryId) {
        QueryStateMachine sm = new QueryStateMachine();
        CoordinatorSessionContext session = new CoordinatorSessionContext(
                queryId, Map.of(), TEST_IDENTITY, "test-tenant");

        DispatchResult result = worker.execute(orchestrator, sm, "SELECT 1", session);
        assertThat(worker.rows(result)).singleElement().satisfies(row ->
                assertThat(((Number) row.get(0)).intValue()).isEqualTo(1));

        String taskId = result.taskId();
        assertTaskInScheduledState(queryId, taskId);
    }

    private void assertTaskInScheduledState(String queryId, String taskId) {
        // The real Native client consumed through EOF, so completion is no longer synthetic.
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

    private QueryDispatcher buildOrchestrator() {
        WorkerManager wm = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        wm.registerWorker(worker.workerInfo());
        remote = new RemoteTaskDispatcher(
                CircuitBreakerPolicy.noOp(), TestObservability.TRACING);
        remote.setControlEpochSupplier(() -> 1L);
        remote.setExecutionOwnerResolver(ignored ->
                new org.pangrova.protocol.rest.task.ExecutionOwnerIdentity(
                        "test-coordinator", "test-incarnation", 1L));
        var execution = new QueryDispatcher(
                SourceFreeQueryFixture.withPolicies(
                QueryDispatcherContext.withDefaults(
                        new SqlCompiler(), null, null, null,
                        remote,
                        new TaskScheduler(new WorkerPerformanceScorer()),
                        wm,
                        taskTracker,
                        null, null, null, 30L)),
                null, null);
        worker.connect(execution, taskTracker, wm);
        return execution;
    }
}
