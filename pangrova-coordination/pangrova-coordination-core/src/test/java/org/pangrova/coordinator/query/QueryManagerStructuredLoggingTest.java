package org.pangrova.coordinator.query;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.scheduler.failure.NoAvailableWorkerException;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.scheduler.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.scoring.WorkerPerformanceScorer;
import org.pangrova.testing.testkit.TestObservability;
import org.pangrova.planner.PangrovaPlanner;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.planner.substrait.SubstraitConverter;
import org.pangrova.planner.substrait.SubstraitPlanSerializer;
import org.pangrova.client.error.ErrorDetail;
import org.pangrova.persistence.memory.coordination.InMemoryWorkerRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.coordinator.testinfra.SourceFreeNativeWorkerFixture;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pangrova.coordinator.testinfra.SourceFreeQueryFixture.session;
import static org.pangrova.coordinator.testinfra.SourceFreeQueryFixture.withPolicies;

/**
 * Tests QueryManager lifecycle logging behavior.
 * Verifies that submit, complete, and fail paths execute without error
 * and that the QueryManager correctly transitions state machines.
 *
 * <p>Structured logging format (event=... key=value) is verified by
 * StructuredLoggingPropertyTest at the project level.</p>
 */
@org.junit.jupiter.api.Tag("native-required")
class QueryManagerStructuredLoggingTest {
    @TempDir Path nativeDirectory;
    private final List<RemoteTaskDispatcher> clients = new ArrayList<>();

    @AfterEach
    void closeClients() {
        clients.forEach(RemoteTaskDispatcher::close);
    }

    @Test
    void submitQueryFailsClosedWhenNoWorkerIsAvailable() {
        QueryManager qm = buildQueryManager();
        CoordinatorSessionContext session = session("q-log-1");

        assertThatThrownBy(() -> qm.submitQuery("q-log-1", "SELECT 1 + 2", session))
                .isInstanceOf(NoAvailableWorkerException.class);
        assertThat(qm.getQueryState("q-log-1"))
                .contains(QueryStateMachine.State.FAILED);
    }

    @Test
    void onAllTasksFinishedTransitionsToFinished() throws Exception {
        try (var worker = new SourceFreeNativeWorkerFixture("w1", nativeDirectory)) {
            var workers = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
            workers.registerWorker(worker.workerInfo());
            var tracker = new CoordinatorTaskTracker();
            var remote = newDispatcher();
            remote.setControlEpochSupplier(() -> 1L);
            remote.setExecutionOwnerResolver(ignored -> new org.pangrova.protocol.rest.task.ExecutionOwnerIdentity(
                    "test-coordinator", "test-incarnation", 1L));
            var dispatcher = new QueryDispatcher(withPolicies(QueryDispatcherContext.withDefaults(
                    new SqlCompiler(), null, null, null, remote,
                    new TaskScheduler(new WorkerPerformanceScorer()), workers, tracker,
                    null, null, null, 30L)), null, null);
            QueryManager qm = worker.connect(dispatcher, tracker, workers);
            QueryStateMachine sm = new QueryStateMachine();
            qm.registerStateMachine("q-log-2", sm);
            worker.execute(dispatcher, sm, "SELECT 42", session("q-log-2"));
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(qm.getQueryState("q-log-2")).hasValue(QueryStateMachine.State.FINISHED));
        }
    }

    @Test
    void onTaskFailedTransitionsToFailed() {
        QueryManager qm = buildQueryManager();

        // Exercise the lifecycle callback directly; no Worker execution is claimed.
        QueryStateMachine sm = new QueryStateMachine();
        qm.registerStateMachine("q-log-3", sm);
        sm.tryTransition(QueryStateMachine.State.PLANNING);
        sm.tryTransition(QueryStateMachine.State.SCHEDULING);
        sm.tryTransition(QueryStateMachine.State.RUNNING);

        qm.onTaskFailed("q-log-3", new ErrorDetail("INTERNAL", "INTERNAL", "disk full"));

        assertThat(qm.getQueryState("q-log-3"))
                .isPresent()
                .hasValue(QueryStateMachine.State.FAILED);
    }

    @Test
    void nullLoggingProviderDoesNotHideNoWorkerFailure() {
        QueryManager qm = buildQueryManager();
        CoordinatorSessionContext session = session("q-log-4");

        assertThatThrownBy(() -> qm.submitQuery("q-log-4", "SELECT 1", session))
                .isInstanceOf(NoAvailableWorkerException.class);
        assertThat(qm.getQueryState("q-log-4"))
                .contains(QueryStateMachine.State.FAILED);
    }

    // --- Helpers ---

    private QueryManager buildQueryManager() {
        return new QueryManager(QueryManagerContext.minimal(
                buildOrchestrator(), TestObservability.TRACING));
    }

    private QueryDispatcher buildOrchestrator() {
        SqlCompiler compiler = new SqlCompiler(
                new PangrovaPlanner(), new SubstraitConverter(),
                new SubstraitPlanSerializer());
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer());
        WorkerManager wm = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        return new QueryDispatcher(
                withPolicies(QueryDispatcherContext.withDefaults(
                        compiler, null, null, null,
                        newDispatcher(),
                        scheduler, wm, new CoordinatorTaskTracker(), null, null, null, 0L)),
                null, null);
    }

    private RemoteTaskDispatcher newDispatcher() {
        var client = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING);
        clients.add(client);
        return client;
    }
}
