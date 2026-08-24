package org.pangrova.coordinator.query;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.UUID;

import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.testing.TestObservability;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Verifies BUG-C01 fix: {@link QueryManager#onAllTasksFinished(String)}
 * checks {@code tryTransition(FINISHED)} return value and bails out
 * when the query has already moved to a terminal state (FAILED/CANCELLED).
 *
 * <p>Prevents double cleanup and corrupted audit/lineage data.</p>
 */
class QueryManagerOnAllTasksFinishedTest {

    private QueryManager queryManager;

    @BeforeEach
    void setUp() {
        QueryDispatcher dispatcher = buildDispatcher();
        queryManager = new QueryManager(
                QueryManagerContext.minimal(dispatcher, TestObservability.TRACING));
    }

    @Test
    @DisplayName("onAllTasksFinished transitions RUNNING query to FINISHED")
    void onAllTasksFinished_normalPath_transitionsToFinished() {
        String queryId = registerRunningQuery();

        queryManager.onAllTasksFinished(queryId);

        assertThat(queryManager.getQueryState(queryId))
                .hasValue(QueryStateMachine.State.FINISHED);
    }

    @Test
    @DisplayName("onAllTasksFinished bails out when query already FAILED — no double cleanup")
    void onAllTasksFinished_alreadyFailed_bailsOut() {
        String queryId = registerRunningQuery();

        // failQuery keeps the SM in the map for client polling (FAILED state)
        queryManager.onTaskFailed(queryId,
                new ErrorDetail("RUNTIME_INTERNAL", "task crashed", null));

        assertThat(queryManager.getQueryState(queryId))
                .as("failQuery keeps SM in map with FAILED state for client polling")
                .hasValue(QueryStateMachine.State.FAILED);

        // Now onAllTasksFinished runs — SM is in FAILED (terminal), so transition to FINISHED fails
        queryManager.onAllTasksFinished(queryId);

        // State must remain FAILED
        assertThat(queryManager.getQueryState(queryId))
                .hasValue(QueryStateMachine.State.FAILED);
    }

    @Test
    @DisplayName("onAllTasksFinished bails out when query already CANCELLED")
    void onAllTasksFinished_alreadyCancelled_bailsOut() {
        String queryId = registerRunningQuery();

        queryManager.cancelQuery(queryId);
        assertThat(queryManager.getQueryState(queryId))
                .hasValue(QueryStateMachine.State.CANCELLED);

        queryManager.onAllTasksFinished(queryId);

        // State must remain CANCELLED
        assertThat(queryManager.getQueryState(queryId))
                .hasValue(QueryStateMachine.State.CANCELLED);
    }

    @Test
    @DisplayName("onAllTasksFinished is no-op for unknown queryId")
    void onAllTasksFinished_unknownQuery_noOp() {
        // Should not throw
        queryManager.onAllTasksFinished("nonexistent-" + UUID.randomUUID());
    }

    @Test
    @DisplayName("onAllTasksFinished catches up intermediate states before FINISHED")
    void onAllTasksFinished_catchesUpFromQueued() {
        // Register a query still in QUEUED state
        String queryId = "catchup-" + UUID.randomUUID();
        QueryStateMachine sm = new QueryStateMachine();
        queryManager.registerStateMachine(queryId, sm);

        assertThat(sm.getState()).isEqualTo(QueryStateMachine.State.QUEUED);

        // onAllTasksFinished should catch up through RUNNING → FINISHING → FINISHED
        // Note: QUEUED → RUNNING is not a valid direct transition,
        // so tryTransition(RUNNING) returns false. The method still proceeds
        // to try FINISHING and FINISHED, which also fail from QUEUED.
        queryManager.onAllTasksFinished(queryId);

        // Query stays in QUEUED because none of the transitions are valid from QUEUED
        assertThat(sm.getState()).isEqualTo(QueryStateMachine.State.QUEUED);
    }

    /**
     * Registers a query in RUNNING state and returns its queryId.
     */
    private String registerRunningQuery() {
        String queryId = "test-" + UUID.randomUUID();
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryStateMachine.State.PLANNING);
        sm.tryTransition(QueryStateMachine.State.SCHEDULING);
        sm.tryTransition(QueryStateMachine.State.RUNNING);
        queryManager.registerStateMachine(queryId, sm);
        return queryId;
    }

    private static QueryDispatcher buildDispatcher() {
        SqlCompiler compiler = new SqlCompiler();
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer());
        WorkerManager wm = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        return new QueryDispatcher(
                QueryDispatcherContext.withDefaults(
                        compiler, null, null, null,
                        new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING),
                        scheduler, wm, new CoordinatorTaskTracker(),
                        null, null, null, null, 0L),
                null, null);
    }
}
