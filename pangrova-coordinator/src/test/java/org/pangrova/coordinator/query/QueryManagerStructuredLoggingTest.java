package org.pangrova.coordinator.query;

import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.testing.TestObservability;
import org.pangrova.planner.PangrovaPlanner;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.planner.substrait.SubstraitConverter;
import org.pangrova.planner.substrait.SubstraitPlanSerializer;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * Tests QueryManager lifecycle logging behavior.
 * Verifies that submit, complete, and fail paths execute without error
 * and that the QueryManager correctly transitions state machines.
 *
 * <p>Structured logging format (event=... key=value) is verified by
 * StructuredLoggingPropertyTest at the project level.</p>
 */
class QueryManagerStructuredLoggingTest {

    @Test
    void submitQueryDoesNotThrow() {
        QueryManager qm = buildQueryManager();
        CoordinatorSessionContext session = new CoordinatorSessionContext("q-log-1", Map.of(), null, null);

        assertThatNoException().isThrownBy(() ->
                qm.submitQuery("q-log-1", "SELECT 1 + 2", session));
    }

    @Test
    void onAllTasksFinishedTransitionsToFinished() {
        QueryManager qm = buildQueryManager();
        CoordinatorSessionContext session = new CoordinatorSessionContext("q-log-2", Map.of(), null, null);

        qm.submitQuery("q-log-2", "SELECT 42", session);
        qm.onAllTasksFinished("q-log-2");

        assertThat(qm.getQueryState("q-log-2"))
                .isPresent()
                .hasValue(QueryStateMachine.State.FINISHED);
    }

    @Test
    void onTaskFailedTransitionsToFailed() {
        QueryManager qm = buildQueryManager();

        // Register state machine at RUNNING directly (submitQuery with constant
        // queries completes synchronously to FINISHED, blocking FAILED transition).
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
    void submitQueryWithNullLoggingProviderDoesNotThrow() {
        QueryManager qm = buildQueryManager();
        CoordinatorSessionContext session = new CoordinatorSessionContext("q-log-4", Map.of(), null, null);

        assertThatNoException().isThrownBy(() -> {
            qm.submitQuery("q-log-4", "SELECT 1", session);
            qm.onAllTasksFinished("q-log-4");
        });
    }

    // --- Helpers ---

    private static QueryManager buildQueryManager() {
        return new QueryManager(QueryManagerContext.minimal(
                buildOrchestrator(), TestObservability.TRACING));
    }

    private static QueryDispatcher buildOrchestrator() {
        SqlCompiler compiler = new SqlCompiler(
                new PangrovaPlanner(), new SubstraitConverter(),
                new SubstraitPlanSerializer());
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer());
        WorkerManager wm = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        return new QueryDispatcher(
                QueryDispatcherContext.withDefaults(
                        compiler, null, null, null,
                        new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING),
                        scheduler, wm, new CoordinatorTaskTracker(), null, null, null, null, 0L),
                null, null);
    }
}
