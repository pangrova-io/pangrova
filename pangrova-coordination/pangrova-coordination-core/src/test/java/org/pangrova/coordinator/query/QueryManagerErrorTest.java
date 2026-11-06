package org.pangrova.coordinator.query;

import org.pangrova.query.core.lifecycle.QueryStateMachine;

import org.pangrova.coordinator.testinfra.QueryManagerTestContexts;

import org.pangrova.query.api.lifecycle.QueryState;

import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.query.api.error.QueryFailure;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for QueryManager error storage via onTaskFailed().
 * Validates: Requirements 44.3, 44.4
 */
class QueryManagerErrorTest {

    private final QueryManager queryManager = buildQueryManager();

    @Test
    void onTaskFailedStoresQueryFailureBeforeTransition() {
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryState.PLANNING);
        sm.tryTransition(QueryState.SCHEDULING);
        sm.tryTransition(QueryState.RUNNING);
        queryManager.registerStateMachine("q-fail", sm);

        QueryFailure error = new QueryFailure("INTERNAL", "INTERNAL", "task crashed");
        queryManager.onTaskFailed("q-fail", error);

        assertThat(sm.getState()).isEqualTo(QueryState.FAILED);
        assertThat(sm.getError()).isPresent().hasValue(error);
    }

    @Test
    void getQueryErrorReturnsStoredError() {
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryState.PLANNING);
        sm.tryTransition(QueryState.SCHEDULING);
        sm.tryTransition(QueryState.RUNNING);
        queryManager.registerStateMachine("q-err", sm);

        QueryFailure error = new QueryFailure("TIMEOUT", "TIMEOUT", "execution timed out");
        queryManager.onTaskFailed("q-err", error);

        // failQuery keeps the SM in the map for client polling,
        // so getQueryError returns the stored error.
        assertThat(sm.getError()).isPresent().hasValue(error);
        assertThat(queryManager.getQueryError("q-err")).isPresent().hasValue(error);
    }

    @Test
    void getQueryErrorReturnsEmptyForUnknownQueryId() {
        Optional<QueryFailure> result = queryManager.getQueryError("nonexistent");
        assertThat(result).isEmpty();
    }

    @Test
    void getQueryErrorReturnsEmptyForQueryWithoutError() {
        QueryStateMachine sm = new QueryStateMachine();
        queryManager.registerStateMachine("q-ok", sm);

        Optional<QueryFailure> result = queryManager.getQueryError("q-ok");
        assertThat(result).isEmpty();
    }

    @Test
    void onTaskFailedIgnoresUnknownQueryId() {
        QueryFailure error = new QueryFailure("INTERNAL", "INTERNAL", "unknown query");
        queryManager.onTaskFailed("ghost", error);

        assertThat(queryManager.getQueryError("ghost")).isEmpty();
    }

    private static QueryManager buildQueryManager() {
        QueryDispatcher orchestrator = buildOrchestrator();
        return new QueryManager(QueryManagerTestContexts.minimal(orchestrator,
                new org.pangrova.observability.noop.NoOpTracingProvider()));
    }

    private static QueryDispatcher buildOrchestrator() {
        org.pangrova.planner.core.analysis.SqlCompiler compiler = new org.pangrova.planner.core.analysis.SqlCompiler();
        org.pangrova.scheduler.core.placement.TaskScheduler scheduler =
                new org.pangrova.scheduler.core.placement.TaskScheduler(
                        new org.pangrova.scheduler.builtin.placement.LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty());
        return new QueryDispatcher(
                QueryDispatcherContext.withDefaults(
                        compiler, null, null, null,
                        new org.pangrova.coordinator.scheduler.RemoteTaskDispatcher(
                                org.pangrova.common.resilience.CircuitBreakerPolicy.noOp(),
                                new org.pangrova.observability.noop.NoOpTracingProvider()),
                        scheduler,
                        new org.pangrova.coordinator.worker.WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry()),
                        new org.pangrova.coordinator.task.CoordinatorTaskTracker(),
                        null, null, null, 0L),
                null);
    }
}
