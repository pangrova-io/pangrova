package org.pangrova.coordinator.query;

import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for QueryManager terminal query cleanup.
 * Validates: Requirements 44.2
 */
class QueryManagerCleanupTest {

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "test-cleanup");
                t.setDaemon(true);
                return t;
            });

    private final QueryManager queryManager = buildQueryManager();

    @AfterEach
    void tearDown() {
        queryManager.stopPeriodicCleanup();
        scheduler.shutdownNow();
    }

    @Test
    void cleanupRemovesTerminalQueriesOlderThanRetention() {
        QueryStateMachine finished = new QueryStateMachine();
        transitionToFinished(finished);
        queryManager.registerStateMachine("q-old", finished);

        // Zero retention removes all terminal queries immediately
        int removed = queryManager.cleanupTerminalQueries(Duration.ZERO);

        assertThat(removed).isEqualTo(1);
        assertThat(queryManager.getQueryState("q-old")).isEmpty();
    }

    @Test
    void cleanupPreservesActiveQueries() {
        QueryStateMachine active = new QueryStateMachine();
        active.tryTransition(QueryStateMachine.State.PLANNING);
        queryManager.registerStateMachine("q-active", active);

        int removed = queryManager.cleanupTerminalQueries(Duration.ZERO);

        assertThat(removed).isEqualTo(0);
        assertThat(queryManager.getQueryState("q-active")).isPresent();
    }

    @Test
    void cleanupPreservesRecentTerminalQueries() {
        QueryStateMachine finished = new QueryStateMachine();
        transitionToFinished(finished);
        queryManager.registerStateMachine("q-recent", finished);

        // Large retention keeps recent terminal queries
        int removed = queryManager.cleanupTerminalQueries(Duration.ofHours(1));

        assertThat(removed).isEqualTo(0);
        assertThat(queryManager.getQueryState("q-recent")).isPresent();
    }

    @Test
    void cleanupHandlesFailedAndCancelledStates() {
        QueryStateMachine failed = new QueryStateMachine();
        failed.tryTransition(QueryStateMachine.State.FAILED);
        queryManager.registerStateMachine("q-failed", failed);

        QueryStateMachine cancelled = new QueryStateMachine();
        cancelled.tryTransition(QueryStateMachine.State.CANCELLED);
        queryManager.registerStateMachine("q-cancelled", cancelled);

        int removed = queryManager.cleanupTerminalQueries(Duration.ZERO);

        assertThat(removed).isEqualTo(2);
        assertThat(queryManager.getQueryState("q-failed")).isEmpty();
        assertThat(queryManager.getQueryState("q-cancelled")).isEmpty();
    }

    @Test
    void periodicCleanupRemovesExpiredEntries() throws InterruptedException {
        QueryStateMachine finished = new QueryStateMachine();
        transitionToFinished(finished);
        queryManager.registerStateMachine("q-periodic", finished);

        queryManager.startPeriodicCleanup(scheduler, Duration.ZERO, Duration.ofMillis(20));

        Thread.sleep(200);

        assertThat(queryManager.getQueryState("q-periodic")).isEmpty();
    }

    @Test
    void stopPeriodicCleanupPreventsSubsequentRuns() throws InterruptedException {
        queryManager.startPeriodicCleanup(scheduler, Duration.ZERO, Duration.ofMillis(20));
        queryManager.stopPeriodicCleanup();

        // Add a terminal query after stop — it should not be cleaned up
        QueryStateMachine finished = new QueryStateMachine();
        transitionToFinished(finished);
        queryManager.registerStateMachine("q-after-stop", finished);

        Thread.sleep(200);

        assertThat(queryManager.getQueryState("q-after-stop")).isPresent();
    }

    private static void transitionToFinished(QueryStateMachine sm) {
        sm.tryTransition(QueryStateMachine.State.PLANNING);
        sm.tryTransition(QueryStateMachine.State.SCHEDULING);
        sm.tryTransition(QueryStateMachine.State.RUNNING);
        sm.tryTransition(QueryStateMachine.State.FINISHING);
        sm.tryTransition(QueryStateMachine.State.FINISHED);
    }

    private static QueryManager buildQueryManager() {
        QueryDispatcher orchestrator = buildOrchestrator();
        return new QueryManager(QueryManagerContext.minimal(orchestrator,
                new org.pangrova.observability.noop.NoOpTracingProvider()));
    }

    private static QueryDispatcher buildOrchestrator() {
        org.pangrova.planner.SqlCompiler compiler = new org.pangrova.planner.SqlCompiler();
        org.pangrova.coordinator.scheduler.TaskScheduler scheduler =
                new org.pangrova.coordinator.scheduler.TaskScheduler(
                        new org.pangrova.coordinator.worker.WorkerPerformanceScorer());
        return new QueryDispatcher(
                QueryDispatcherContext.withDefaults(
                        compiler, null, null, null,
                        new org.pangrova.coordinator.scheduler.RemoteTaskDispatcher(
                                org.pangrova.common.resilience.CircuitBreakerPolicy.noOp(),
                                new org.pangrova.observability.noop.NoOpTracingProvider()),
                        scheduler,
                        new org.pangrova.coordinator.worker.WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry()),
                        new org.pangrova.coordinator.task.CoordinatorTaskTracker(),
                        null, null, null, null, 0L),
                null, null);
    }
}
