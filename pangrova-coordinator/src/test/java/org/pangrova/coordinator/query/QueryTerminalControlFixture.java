package org.pangrova.coordinator.query;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.spi.coordination.QueryStateJournal;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.pangrova.testing.TestObservability;
import static org.assertj.core.api.Assertions.assertThat;

/** Actual control-plane owners only: no task is issued or simulated by this fixture. */
final class QueryTerminalControlFixture implements AutoCloseable {
    final AtomicLong clock = new AtomicLong();
    final CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
    final QueryManager manager;
    private final RemoteTaskDispatcher transport =
            new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING);
    private final List<org.pangrova.coordinator.task.QueryIssuanceOperation> operations = new ArrayList<>();

    QueryTerminalControlFixture(QueryStateJournal journal) {
        var dispatcher = new QueryDispatcher(QueryDispatcherContext.withDefaults(new SqlCompiler(), null, null, null,
                transport, new TaskScheduler(new WorkerPerformanceScorer()),
                new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry()), tracker,
                null, null, null, 0L), null, null);
        manager = new QueryManager(new QueryManagerContext(dispatcher, TestObservability.TRACING,
                null, null, null, null, null, null, null, null, journal), clock::get);
    }

    String registerRunningQuery() {
        String id = "terminal-" + UUID.randomUUID();
        var state = new QueryStateMachine();
        state.tryTransition(QueryStateMachine.State.PLANNING);
        state.tryTransition(QueryStateMachine.State.SCHEDULING);
        state.tryTransition(QueryStateMachine.State.RUNNING);
        manager.getStateJournal().appendQueryEventSync(new org.pangrova.spi.coordination.QueryStateEvent(
                org.pangrova.spi.coordination.JournalEventContext.create(System.currentTimeMillis(), null),
                id, null, null, "RUNNING"));
        manager.registerStateMachine(id, state);
        return id;
    }

    org.pangrova.coordinator.task.QueryIssuanceOperation enter(String queryId) {
        var operation = tracker.enterQueryOperation(queryId);
        operations.add(operation);
        return operation;
    }

    void retry() {
        clock.addAndGet(TimeUnit.SECONDS.toNanos(31));
        manager.retryPendingTerminalCompletions();
    }

    @Override
    public void close() {
        operations.forEach(org.pangrova.coordinator.task.QueryIssuanceOperation::close);
        manager.getActiveQueryIds().forEach(manager::cancelQuery);
        retry();
        assertThat(manager.awaitAllQueries(1000)).isTrue();
        transport.close();
    }
}
