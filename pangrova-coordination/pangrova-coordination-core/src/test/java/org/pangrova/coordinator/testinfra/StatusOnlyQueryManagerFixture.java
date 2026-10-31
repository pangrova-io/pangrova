package org.pangrova.coordinator.testinfra;

import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryManagerContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.scheduler.core.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.core.scoring.WorkerPerformanceScorer;
import org.pangrova.observability.api.noop.NoOpTracingProvider;
import org.pangrova.planner.SqlCompiler;

/** Supplies complete deny-all query wiring for HTTP status-only component tests. */
public final class StatusOnlyQueryManagerFixture {
    private StatusOnlyQueryManagerFixture() {}

    public static QueryManager create(WorkerManager workers, CoordinatorTaskTracker tasks) {
        var tracing = new NoOpTracingProvider();
        var context = SourceFreeQueryFixture.withPolicies(QueryDispatcherContext.withDefaults(
                new SqlCompiler(), null, null, null,
                new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), tracing),
                new TaskScheduler(new WorkerPerformanceScorer()),
                workers, tasks, null, null, null, 0L));
        return new QueryManager(QueryManagerContext.minimal(
                new QueryDispatcher(context, null, tracing), tracing));
    }
}
