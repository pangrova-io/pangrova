package org.pangrova.coordinator.testinfra;

import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.persistence.query.api.state.QueryStateJournal;
import org.pangrova.persistence.query.memory.state.InMemoryQueryStateJournal;
import org.pangrova.query.runtime.QueryManager;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.scheduler.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.strategies.placement.LoadBalancedStrategy;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.planner.analysis.SqlCompiler;

/** Supplies complete deny-all query wiring for HTTP status-only component tests. */
public final class StatusOnlyQueryManagerFixture {
    private StatusOnlyQueryManagerFixture() {}

    public static QueryManager create(WorkerManager workers, CoordinatorTaskTracker tasks) {
        return create(workers, tasks, new InMemoryQueryStateJournal());
    }

    public static QueryManager create(WorkerManager workers, CoordinatorTaskTracker tasks, QueryStateJournal journal) {
        var tracing = new NoOpTracingProvider();
        var context = SourceFreeQueryFixture.withPolicies(QueryDispatcherContext.withDefaults(
                new SqlCompiler(), null, null, null,
                new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), tracing),
                new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty()),
                workers, tasks, null, null, null, 0L));
        return new QueryManager(QueryManagerTestContexts.minimal(
                new QueryDispatcher(context, null, tracing), tracing, journal));
    }
}
