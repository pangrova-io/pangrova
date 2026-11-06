package org.pangrova.testing.integration.e2e;

import org.pangrova.coordinator.testinfra.QueryManagerTestContexts;

import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.scheduler.core.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.builtin.placement.LoadBalancedStrategy;
import org.pangrova.planner.core.analysis.SqlCompiler;
import org.pangrova.testing.testkit.TestObservability;

/** Recovery-only manager with real task identity tracking and a closed dispatch boundary. */
public final class RecoveryQueryManagerFixture {
    private RecoveryQueryManagerFixture() {}

    public static QueryManager create(CoordinatorTaskTracker tracker) {
        TaskDispatcher closedTransport = (request, worker) -> {
            throw new IllegalStateException("Recovery fixture must not dispatch a new task");
        };
        var dispatcher = new QueryDispatcher(QueryDispatcherContext.withDefaults(
                new SqlCompiler(), null, null, null, closedTransport,
                new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty()), new WorkerManager(new org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry()), tracker,
                null, null, null, 0L), null);
        return new QueryManager(QueryManagerTestContexts.minimal(dispatcher, TestObservability.TRACING));
    }
}
