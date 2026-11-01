package org.pangrova.coordinator.query;

import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.client.error.ErrorDetail;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.persistence.memory.coordination.InMemoryWorkerRegistry;
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
    void onTaskFailedStoresErrorDetailBeforeTransition() {
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryStateMachine.State.PLANNING);
        sm.tryTransition(QueryStateMachine.State.SCHEDULING);
        sm.tryTransition(QueryStateMachine.State.RUNNING);
        queryManager.registerStateMachine("q-fail", sm);

        ErrorDetail error = new ErrorDetail("INTERNAL", "INTERNAL", "task crashed");
        queryManager.onTaskFailed("q-fail", error);

        assertThat(sm.getState()).isEqualTo(QueryStateMachine.State.FAILED);
        assertThat(sm.getError()).isPresent().hasValue(error);
    }

    @Test
    void getQueryErrorReturnsStoredError() {
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryStateMachine.State.PLANNING);
        sm.tryTransition(QueryStateMachine.State.SCHEDULING);
        sm.tryTransition(QueryStateMachine.State.RUNNING);
        queryManager.registerStateMachine("q-err", sm);

        ErrorDetail error = new ErrorDetail("TIMEOUT", "TIMEOUT", "execution timed out");
        queryManager.onTaskFailed("q-err", error);

        // failQuery keeps the SM in the map for client polling,
        // so getQueryError returns the stored error.
        assertThat(sm.getError()).isPresent().hasValue(error);
        assertThat(queryManager.getQueryError("q-err")).isPresent().hasValue(error);
    }

    @Test
    void getQueryErrorReturnsEmptyForUnknownQueryId() {
        Optional<ErrorDetail> result = queryManager.getQueryError("nonexistent");
        assertThat(result).isEmpty();
    }

    @Test
    void getQueryErrorReturnsEmptyForQueryWithoutError() {
        QueryStateMachine sm = new QueryStateMachine();
        queryManager.registerStateMachine("q-ok", sm);

        Optional<ErrorDetail> result = queryManager.getQueryError("q-ok");
        assertThat(result).isEmpty();
    }

    @Test
    void onTaskFailedIgnoresUnknownQueryId() {
        ErrorDetail error = new ErrorDetail("INTERNAL", "INTERNAL", "unknown query");
        queryManager.onTaskFailed("ghost", error);

        assertThat(queryManager.getQueryError("ghost")).isEmpty();
    }

    private static QueryManager buildQueryManager() {
        QueryDispatcher orchestrator = buildOrchestrator();
        return new QueryManager(QueryManagerContext.minimal(orchestrator,
                new org.pangrova.observability.sdk.noop.NoOpTracingProvider()));
    }

    private static QueryDispatcher buildOrchestrator() {
        org.pangrova.planner.SqlCompiler compiler = new org.pangrova.planner.SqlCompiler();
        org.pangrova.scheduler.placement.TaskScheduler scheduler =
                new org.pangrova.scheduler.placement.TaskScheduler(
                        new org.pangrova.scheduler.scoring.WorkerPerformanceScorer());
        return new QueryDispatcher(
                QueryDispatcherContext.withDefaults(
                        compiler, null, null, null,
                        new org.pangrova.coordinator.scheduler.RemoteTaskDispatcher(
                                org.pangrova.common.resilience.CircuitBreakerPolicy.noOp(),
                                new org.pangrova.observability.sdk.noop.NoOpTracingProvider()),
                        scheduler,
                        new org.pangrova.coordinator.worker.WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry()),
                        new org.pangrova.coordinator.task.CoordinatorTaskTracker(),
                        null, null, null, 0L),
                null);
    }
}
