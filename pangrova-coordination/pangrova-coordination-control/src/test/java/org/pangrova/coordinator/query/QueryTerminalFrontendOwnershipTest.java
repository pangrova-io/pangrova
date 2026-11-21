package org.pangrova.coordinator.query;
import org.pangrova.query.runtime.QueryManager;

import org.pangrova.query.lifecycle.QueryStateMachine;

import org.pangrova.coordinator.testinfra.QueryManagerTestContexts;

import org.pangrova.query.api.enums.QueryState;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.scheduler.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.strategies.placement.LoadBalancedStrategy;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;
import org.pangrova.testing.testkit.constants.TestObservability;
import static org.assertj.core.api.Assertions.assertThat;

/** Real local frontend admission/exit ownership; no Worker task or Native exit is simulated. */
class QueryTerminalFrontendOwnershipTest {
    private final AtomicLong clock = new AtomicLong();
    private final List<org.pangrova.query.api.lifecycle.QueryOperation> operations = new ArrayList<>();
    private CoordinatorTaskTracker tracker;
    private QueryManager manager;
    private RemoteTaskDispatcher transport;

    @BeforeEach
    void createActualControlOwners() {
        tracker = new CoordinatorTaskTracker();
        transport = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING);
        var dispatcher = new QueryDispatcher(QueryDispatcherContext.withDefaults(new SqlCompiler(), null, null, null,
                transport, new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty()),
                new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry()), tracker,
                null, null, null, 0L), null, null);
        manager = new QueryManager(QueryManagerTestContexts.minimal(dispatcher, TestObservability.TRACING), clock::get);
    }

    @AfterEach
    void closeActualControlOwners() {
        operations.forEach(org.pangrova.query.api.lifecycle.QueryOperation::close);
        manager.getActiveQueryIds().forEach(manager::cancelQuery);
        clock.addAndGet(TimeUnit.SECONDS.toNanos(31));
        manager.retryPendingTerminalCompletions();
        assertThat(manager.awaitAllQueries(1000)).isTrue();
        transport.close();
    }

    @Test
    void pendingFinishedCleanupSurvivesRetention() {
        String id = registerRunningQuery();
        var operation = enter(id);
        manager.onAllTasksFinished(id);
        assertThat(manager.getActiveQueryCount()).isEqualTo(1);
        assertThat(manager.cleanupTerminalQueries(Duration.ZERO)).isZero();
        assertThat(manager.getQueryState(id)).contains(QueryState.FINISHED);
        assertThat(manager.awaitAllQueries(1)).isFalse();
        operation.close();
        advanceRetry();
        assertThat(manager.cleanupTerminalQueries(Duration.ZERO)).isEqualTo(1);
        assertThat(manager.getQueryState(id)).isEmpty();
    }

    @Test
    void terminalPublicationIsProtectedFromRetentionSweep() throws Exception {
        String id = registerRunningQuery();
        var published = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        manager.getStateMachine(id).addStateChangeListener((previous, current) -> {
            if (current == QueryState.FINISHED) {
                published.countDown();
                await(release);
            }
        });
        var executor = Executors.newSingleThreadExecutor();
        try {
            var completion = executor.submit(() -> manager.onAllTasksFinished(id));
            assertThat(published.await(3, TimeUnit.SECONDS)).isTrue();
            assertThat(manager.cleanupTerminalQueries(Duration.ZERO)).isZero();
            assertThat(manager.getQueryState(id)).contains(QueryState.FINISHED);
            release.countDown();
            completion.get(3, TimeUnit.SECONDS);
            assertThat(manager.cleanupTerminalQueries(Duration.ZERO)).isEqualTo(1);
        } finally {
            release.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(3, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void pendingTerminalQueriesStayOwnedUntilOperationsExit() {
        for (int i = 0; i < 10; i++) {
            String id = registerRunningQuery();
            enter(id);
            manager.onAllTasksFinished(id);
        }
        assertThat(manager.getActiveQueryCount()).isEqualTo(10);
        advanceRetry(); // Four actual attempts remain blocked on the still-owned frontend.
        assertThat(manager.getActiveQueryCount()).isEqualTo(10);
        manager.retryPendingTerminalCompletions();
        assertThat(manager.getActiveQueryCount()).isEqualTo(10);
        operations.forEach(org.pangrova.query.api.lifecycle.QueryOperation::close);
        drainRetries();
        assertThat(manager.getActiveQueryCount()).isZero();
    }

    @Test
    void staleRetryDoesNotAffectReregisteredQuery() {
        String id = registerRunningQuery();
        var operation = enter(id);
        manager.onAllTasksFinished(id);
        operation.close();
        manager.onAllTasksFinished(id); // Explicit real cleanup succeeds before the queued ticket is due.
        assertThat(manager.cleanupTerminalQueries(Duration.ZERO)).isEqualTo(1);
        var replacement = new QueryStateMachine();
        replacement.tryTransition(QueryState.PLANNING);
        manager.registerStateMachine(id, replacement);
        advanceRetry();
        assertThat(manager.getQueryState(id)).contains(QueryState.PLANNING);
        assertThat(manager.getActiveQueryCount()).isEqualTo(1);
    }

    @Test
    void concurrentTerminalQueriesReleaseOnExactOperationExit() throws Exception {
        var ids = new ArrayList<String>();
        for (int i = 0; i < 12; i++) {
            String id = registerRunningQuery();
            enter(id);
            ids.add(id);
        }
        var executor = Executors.newFixedThreadPool(4);
        var start = new CountDownLatch(1);
        try {
            var completions = ids.stream().map(id -> executor.submit(() -> {
                await(start);
                manager.onAllTasksFinished(id);
            })).toList();
            start.countDown();
            for (var completion : completions) completion.get(3, TimeUnit.SECONDS);
        } finally {
            start.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(3, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(manager.getActiveQueryCount()).isEqualTo(12);
        operations.forEach(org.pangrova.query.api.lifecycle.QueryOperation::close);
        drainRetries();
        assertThat(manager.getActiveQueryCount()).isZero();
    }

    @Test
    void stoppedSchedulerStillReleasesOnExplicitRetryAfterOperationExit() throws Exception {
        String id = registerRunningQuery();
        var operation = enter(id);
        manager.onAllTasksFinished(id);
        var scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            manager.startPeriodicCleanup(scheduler, Duration.ZERO, Duration.ofHours(1));
            manager.stopPeriodicCleanup();
            operation.close();
            scheduler.submit(() -> { }).get(3, TimeUnit.SECONDS);
            assertThat(manager.getActiveQueryCount()).isEqualTo(1);
            advanceRetry();
            assertThat(manager.getActiveQueryCount()).isZero();
        } finally {
            manager.stopPeriodicCleanup();
            scheduler.shutdownNow();
            assertThat(scheduler.awaitTermination(3, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void drainedFrontendReleasesAdmissionBeforePeriodicSweep() throws Exception {
        String id = registerRunningQuery();
        var operation = enter(id);
        var timer = Executors.newSingleThreadScheduledExecutor();
        var cleanupWorker = Executors.newSingleThreadExecutor();
        try {
            manager.startPeriodicCleanup(timer, cleanupWorker,
                    Duration.ZERO, Duration.ofHours(1));
            manager.onAllTasksFinished(id);
            assertThat(manager.getActiveQueryCount()).isEqualTo(1);
            operation.close();
            org.awaitility.Awaitility.await("drained frontend admission release")
                    .atMost(Duration.ofSeconds(3))
                    .untilAsserted(() -> assertThat(manager.getActiveQueryCount()).isZero());
        } finally {
            manager.stopPeriodicCleanup();
            timer.shutdownNow();
            cleanupWorker.shutdownNow();
            assertThat(timer.awaitTermination(3, TimeUnit.SECONDS)).isTrue();
            assertThat(cleanupWorker.awaitTermination(3, TimeUnit.SECONDS)).isTrue();
        }
    }

    private String registerRunningQuery() {
        String id = "frontend-" + UUID.randomUUID();
        var state = new QueryStateMachine();
        state.tryTransition(QueryState.PLANNING);
        state.tryTransition(QueryState.SCHEDULING);
        state.tryTransition(QueryState.RUNNING);
        manager.registerStateMachine(id, state);
        return id;
    }

    private org.pangrova.query.api.lifecycle.QueryOperation enter(String id) {
        var operation = tracker.enterQueryOperation(id);
        operations.add(operation);
        return operation;
    }

    private void advanceRetry() {
        clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(100));
        manager.retryPendingTerminalCompletions();
    }

    private void drainRetries() {
        clock.addAndGet(TimeUnit.SECONDS.toNanos(31));
        for (int i = 0; i < 4; i++) manager.retryPendingTerminalCompletions();
    }

    private static void await(CountDownLatch latch) {
        try { assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue(); }
        catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Frontend boundary wait interrupted", interrupted);
        }
    }
}
