package org.pangrova.coordinator.testinfra;

import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryManagerContext;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.scheduler.core.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.core.scoring.WorkerPerformanceScorer;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.client.core.error.ErrorDetail;
import org.pangrova.coordination.inmemory.InMemoryWorkerRegistry;
import org.pangrova.testing.testkit.TestObservability;

/** Real control-plane collaborators for REST registry tests; no Native task or SQL result is simulated. */
public final class QueryControlPlaneFixture implements AutoCloseable {
    private final CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
    private final WorkerManager workers = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
    private final RemoteTaskDispatcher transport =
            new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING);
    private final QueryManager queries;

    public QueryControlPlaneFixture() {
        try {
            var context = QueryDispatcherContext.withDefaults(new SqlCompiler(), null, null, null,
                    transport, new TaskScheduler(new WorkerPerformanceScorer()), workers, tracker,
                    null, null, null, 0L);
            var dispatcher = new QueryDispatcher(SourceFreeQueryFixture.withPolicies(context), null, null);
            queries = new QueryManager(QueryManagerContext.minimal(dispatcher, TestObservability.TRACING));
        } catch (RuntimeException | Error failure) {
            try { transport.close(); }
            catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    public QueryManager queries() { return queries; }
    public WorkerManager workers() { return workers; }
    public CoordinatorTaskTracker tracker() { return tracker; }

    @Override
    public void close() {
        queries.setAcceptingQueries(false);
        for (String queryId : queries.getActiveQueryIds()) {
            var state = queries.getStateMachine(queryId);
            if (state.getState() == QueryStateMachine.State.FINISHED) {
                queries.onAllTasksFinished(queryId);
            } else if (state.getState() == QueryStateMachine.State.FAILED) {
                queries.onTaskFailed(queryId, state.getError().orElseGet(() ->
                        new ErrorDetail("FIXTURE_FAILED", "COMPONENT_TEST", "Registered component state failed")));
            } else {
                queries.cancelQuery(queryId);
            }
        }
        if (!queries.awaitAllQueries(5_000)) {
            throw new IllegalStateException("Control-plane fixture still owns query cleanup");
        }
        transport.close();
    }
}
