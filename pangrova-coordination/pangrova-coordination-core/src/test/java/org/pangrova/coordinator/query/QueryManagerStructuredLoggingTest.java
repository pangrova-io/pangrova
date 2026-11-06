package org.pangrova.coordinator.query;

import org.pangrova.query.core.lifecycle.QueryStateMachine;

import org.pangrova.coordinator.testinfra.QueryManagerTestContexts;

import org.pangrova.query.api.lifecycle.QueryState;

import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.scheduler.api.error.NoAvailableWorkerException;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.scheduler.core.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.builtin.placement.LoadBalancedStrategy;
import org.pangrova.testing.testkit.TestObservability;
import org.pangrova.planner.core.analysis.PangrovaPlanner;
import org.pangrova.planner.core.analysis.SqlCompiler;
import org.pangrova.planner.core.substrait.SubstraitConverter;
import org.pangrova.planner.core.substrait.SubstraitPlanSerializer;
import org.pangrova.query.api.error.QueryFailure;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;
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
        QuerySession session = session("q-log-1");

        assertThatThrownBy(() -> qm.submitQuery("q-log-1", "SELECT 1 + 2", session))
                .isInstanceOf(NoAvailableWorkerException.class);
        assertThat(qm.getQueryState("q-log-1"))
                .contains(QueryState.FAILED);
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
                    new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty()), workers, tracker,
                    null, null, null, 30L)), null, null);
            QueryManager qm = worker.connect(dispatcher, tracker, workers);
            QueryStateMachine sm = new QueryStateMachine();
            qm.registerStateMachine("q-log-2", sm);
            worker.execute(dispatcher, sm, "SELECT 42", session("q-log-2"));
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(qm.getQueryState("q-log-2")).hasValue(QueryState.FINISHED));
        }
    }

    @Test
    void onTaskFailedTransitionsToFailed() {
        QueryManager qm = buildQueryManager();

        // Exercise the lifecycle callback directly; no Worker execution is claimed.
        QueryStateMachine sm = new QueryStateMachine();
        qm.registerStateMachine("q-log-3", sm);
        sm.tryTransition(QueryState.PLANNING);
        sm.tryTransition(QueryState.SCHEDULING);
        sm.tryTransition(QueryState.RUNNING);

        qm.onTaskFailed("q-log-3", new QueryFailure("INTERNAL", "INTERNAL", "disk full"));

        assertThat(qm.getQueryState("q-log-3"))
                .isPresent()
                .hasValue(QueryState.FAILED);
    }

    @Test
    void nullLoggingProviderDoesNotHideNoWorkerFailure() {
        QueryManager qm = buildQueryManager();
        QuerySession session = session("q-log-4");

        assertThatThrownBy(() -> qm.submitQuery("q-log-4", "SELECT 1", session))
                .isInstanceOf(NoAvailableWorkerException.class);
        assertThat(qm.getQueryState("q-log-4"))
                .contains(QueryState.FAILED);
    }

    // --- Helpers ---

    private QueryManager buildQueryManager() {
        return new QueryManager(QueryManagerTestContexts.minimal(
                buildOrchestrator(), TestObservability.TRACING));
    }

    private QueryDispatcher buildOrchestrator() {
        SqlCompiler compiler = new SqlCompiler(
                new PangrovaPlanner(), new SubstraitConverter(),
                new SubstraitPlanSerializer());
        TaskScheduler scheduler = new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty());
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
