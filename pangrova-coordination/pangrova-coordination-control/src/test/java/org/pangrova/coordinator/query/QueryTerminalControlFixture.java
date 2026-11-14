package org.pangrova.coordinator.query;

import org.pangrova.query.lifecycle.QueryStateMachine;

import org.pangrova.query.api.enums.QueryState;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.scheduler.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.builtin.placement.LoadBalancedStrategy;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.persistence.query.api.state.QueryStateJournal;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;
import org.pangrova.testing.testkit.TestObservability;
import static org.assertj.core.api.Assertions.assertThat;

/** Actual control-plane owners only: no task is issued or simulated by this fixture. */
final class QueryTerminalControlFixture implements AutoCloseable {
    final AtomicLong clock = new AtomicLong();
    final CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
    final QueryManager manager;
    private final RemoteTaskDispatcher transport =
            new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING);
    private final List<org.pangrova.query.api.lifecycle.QueryOperation> operations = new ArrayList<>();

    QueryTerminalControlFixture(QueryStateJournal journal) {
        this(journal, java.util.Optional.empty());
    }

    QueryTerminalControlFixture(QueryStateJournal journal,
            java.util.Optional<org.pangrova.query.api.retention.ReaderPinLifecycle> readerPins) {
        var dispatcher = new QueryDispatcher(QueryDispatcherContext.withDefaults(new SqlCompiler(), null, null, null,
                transport, new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty()),
                new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry()), tracker,
                null, null, null, 0L), null, null);
        manager = new QueryManager(new QueryManagerContext(dispatcher, TestObservability.TRACING,
                null, null, null, null, null, null, null, null, journal, new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory()), readerPins, clock::get);
    }

    String registerRunningQuery() {
        String id = "terminal-" + UUID.randomUUID();
        var state = new QueryStateMachine();
        state.tryTransition(QueryState.PLANNING);
        state.tryTransition(QueryState.SCHEDULING);
        state.tryTransition(QueryState.RUNNING);
        manager.getStateJournal().appendQueryEventSync(new org.pangrova.query.api.journal.QueryStateEvent(
                org.pangrova.query.api.journal.JournalEventContext.create(System.currentTimeMillis(), null),
                id, null, null, "RUNNING"));
        manager.registerStateMachine(id, state);
        return id;
    }

    org.pangrova.query.api.lifecycle.QueryOperation enter(String queryId) {
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
        operations.forEach(org.pangrova.query.api.lifecycle.QueryOperation::close);
        manager.getActiveQueryIds().forEach(manager::cancelQuery);
        retry();
        assertThat(manager.awaitAllQueries(1000)).isTrue();
        manager.stopPeriodicCleanup();
        transport.close();
    }
}
