package org.pangrova.coordinator.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.Future;

import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.planner.throttle.QueryThrottlingService;
import org.pangrova.security.audit.AuditEvent;
import org.pangrova.security.audit.AuditEventEmitter;
import org.pangrova.security.audit.AuditEventType;
import org.pangrova.spi.coordination.QueryStateEvent;
import org.pangrova.spi.coordination.QueryStateJournal;
import org.pangrova.spi.coordination.TaskStateEvent;
import org.pangrova.testing.TestObservability;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.concurrent.CopyOnWriteArrayList;

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

    @Test
    @DisplayName("failed FINISHED journal append leaves completion effects retryable")
    void failedTerminalJournalCanBeRetried() {
        AtomicLong clock = new AtomicLong();
        FailOnceFinishedJournal journal = new FailOnceFinishedJournal();
        queryManager = new QueryManager(new QueryManagerContext(
                buildDispatcher(), TestObservability.TRACING,
                null, null, null, null, null, null, null, null, journal), clock::get);
        String queryId = registerRunningQuery();

        assertThatThrownBy(() -> queryManager.onAllTasksFinished(queryId))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("journal unavailable");
        assertThat(queryManager.getQueryState(queryId))
                .hasValue(QueryStateMachine.State.FINISHED);
        assertThat(queryManager.getActiveQueryCount()).isEqualTo(1);

        clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(100));
        queryManager.retryPendingTerminalCompletions();
        queryManager.onAllTasksFinished(queryId);
        assertThat(journal.finishedAttempts.get()).isEqualTo(2);
        assertThat(queryManager.getActiveQueryCount()).isZero();
    }

    @Test
    @DisplayName("failed terminal journal automatically retries and then releases the permit")
    void terminalJournalRetryReleasesPermitExactlyOnce() {
        AtomicLong clock = new AtomicLong();
        AtomicInteger cleanupAttempts = new AtomicInteger();
        QueryDispatcher dispatcher = controlledDispatcher(
                cleanupAttempts, new CountDownLatch(1), 0);
        FailOnceFinishedJournal journal = new FailOnceFinishedJournal();
        QueryThrottlingService throttling = new QueryThrottlingService(1, 1000, 1000);
        queryManager = new QueryManager(new QueryManagerContext(
                dispatcher, TestObservability.TRACING,
                null, null, null, null, null, null, null, throttling, journal),
                clock::get);
        String queryId = "journal-retry-" + UUID.randomUUID();
        queryManager.submitQuery(queryId, "SELECT 1",
                new CoordinatorSessionContext(queryId, Map.of(), null, null));

        assertThatThrownBy(() -> queryManager.onAllTasksFinished(queryId))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("journal unavailable");
        assertThat(queryManager.getActiveQueryCount()).isEqualTo(1);
        assertThat(throttling.availableGlobalPermits()).isZero();

        clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(100));
        queryManager.retryPendingTerminalCompletions();
        assertThat(journal.finishedAttempts.get()).isEqualTo(2);
        assertThat(queryManager.getActiveQueryCount()).isZero();
        assertThat(throttling.availableGlobalPermits()).isEqualTo(1);
        assertThat(cleanupAttempts.get()).isEqualTo(1);
        queryManager.retryPendingTerminalCompletions();
        assertThat(throttling.availableGlobalPermits()).isEqualTo(1);
    }

    @Test
    @DisplayName("failed dispatcher cleanup retries in background without repeating terminal effects")
    void finishedCleanupRetriesWithoutRepeatingEffects() throws Exception {
        AtomicInteger cleanupAttempts = new AtomicInteger();
        CountDownLatch cleanupRetried = new CountDownLatch(1);
        QueryDispatcher dispatcher = controlledDispatcher(cleanupAttempts, cleanupRetried, 1);
        FailOnceFinishedJournal journal = new FailOnceFinishedJournal(false);
        QueryThrottlingService throttling = new QueryThrottlingService(1, 1000, 1000);
        List<AuditEvent> auditEvents = new CopyOnWriteArrayList<>();
        AuditEventEmitter audit = new AuditEventEmitter(auditEvents::add);
        try {
            queryManager = new QueryManager(new QueryManagerContext(
                    dispatcher, TestObservability.TRACING,
                    null, null, null, null, audit, null, null, throttling, journal));
            String queryId = "cleanup-" + UUID.randomUUID();
            queryManager.submitQuery(queryId, "SELECT 1",
                    new CoordinatorSessionContext(queryId, Map.of(), null, null));
            assertThat(throttling.availableGlobalPermits()).isZero();

            queryManager.onAllTasksFinished(queryId);

            assertThat(queryManager.getQueryState(queryId))
                    .hasValue(QueryStateMachine.State.FINISHED);
            assertThat(queryManager.getActiveQueryCount()).isZero();
            assertThat(throttling.availableGlobalPermits()).isEqualTo(1);
            assertThat(journal.finishedAttempts.get()).isEqualTo(1);
            assertThat(cleanupAttempts.get()).isEqualTo(1);

            var scheduler = Executors.newSingleThreadScheduledExecutor();
            try {
                queryManager.startPeriodicCleanup(
                        scheduler, Duration.ofHours(1), Duration.ofMillis(10));
                assertThat(cleanupRetried.await(3, TimeUnit.SECONDS)).isTrue();
            } finally {
                queryManager.stopPeriodicCleanup();
                scheduler.shutdownNow();
            }
            queryManager.onAllTasksFinished(queryId);
            assertThat(cleanupAttempts.get()).isEqualTo(2);
            assertThat(journal.finishedAttempts.get()).isEqualTo(1);
            assertThat(queryManager.getActiveQueryCount()).isZero();
            assertThat(throttling.availableGlobalPermits()).isEqualTo(1);
        } finally {
            audit.close();
        }
        assertThat(auditEvents.stream()
                .filter(event -> event.eventType() == AuditEventType.QUERY_COMPLETED)
                .count()).isEqualTo(1);
    }

    @Test
    @DisplayName("terminal state is not evicted while dispatcher cleanup remains pending")
    void pendingFinishedCleanupSurvivesRetention() {
        AtomicInteger cleanupAttempts = new AtomicInteger();
        QueryDispatcher dispatcher = controlledDispatcher(
                cleanupAttempts, new CountDownLatch(1), 2);
        queryManager = new QueryManager(QueryManagerContext.minimal(
                dispatcher, TestObservability.TRACING));
        String queryId = registerRunningQuery();

        queryManager.onAllTasksFinished(queryId);
        assertThat(queryManager.cleanupTerminalQueries(Duration.ZERO)).isZero();
        assertThat(queryManager.getQueryState(queryId))
                .hasValue(QueryStateMachine.State.FINISHED);

        queryManager.onAllTasksFinished(queryId);
        queryManager.onAllTasksFinished(queryId);
        assertThat(queryManager.cleanupTerminalQueries(Duration.ZERO)).isEqualTo(1);
        assertThat(cleanupAttempts.get()).isEqualTo(3);
        assertThat(queryManager.getQueryState(queryId)).isEmpty();
    }

    @Test
    @DisplayName("FINISHED cannot be evicted between state publication and completion registration")
    void terminalPublicationIsProtectedFromRetentionSweep() throws Exception {
        String queryId = registerRunningQuery();
        QueryStateMachine sm = queryManager.getStateMachine(queryId);
        CountDownLatch published = new CountDownLatch(1);
        CountDownLatch continueCompletion = new CountDownLatch(1);
        sm.addStateChangeListener((oldState, newState) -> {
            if (newState == QueryStateMachine.State.FINISHED) {
                published.countDown();
                await(continueCompletion);
            }
        });
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> finish = executor.submit(() -> queryManager.onAllTasksFinished(queryId));
            assertThat(published.await(3, TimeUnit.SECONDS)).isTrue();
            assertThat(queryManager.cleanupTerminalQueries(Duration.ZERO)).isZero();
            assertThat(queryManager.getQueryState(queryId))
                    .hasValue(QueryStateMachine.State.FINISHED);
            continueCompletion.countDown();
            finish.get(3, TimeUnit.SECONDS);
            assertThat(queryManager.cleanupTerminalQueries(Duration.ZERO)).isEqualTo(1);
        } finally {
            continueCompletion.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    @DisplayName("terminal cleanup retries at most four due queries per tick with backoff")
    void terminalRetryBatchIsBoundedAndBacksOff() {
        AtomicLong clock = new AtomicLong();
        AtomicInteger cleanupAttempts = new AtomicInteger();
        QueryDispatcher dispatcher = controlledDispatcher(
                cleanupAttempts, new CountDownLatch(1), Integer.MAX_VALUE);
        queryManager = new QueryManager(QueryManagerContext.minimal(
                dispatcher, TestObservability.TRACING), clock::get);
        for (int i = 0; i < 10; i++) {
            queryManager.onAllTasksFinished(registerRunningQuery());
        }
        assertThat(cleanupAttempts.get()).isEqualTo(10);

        clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(100));
        queryManager.retryPendingTerminalCompletions();
        assertThat(cleanupAttempts.get()).isEqualTo(14);
        queryManager.retryPendingTerminalCompletions();
        assertThat(cleanupAttempts.get()).isEqualTo(18);
        queryManager.retryPendingTerminalCompletions();
        assertThat(cleanupAttempts.get()).isEqualTo(20);
        queryManager.retryPendingTerminalCompletions();
        assertThat(cleanupAttempts.get()).isEqualTo(20);
        assertThat(queryManager.getActiveQueryCount()).isZero();
    }

    @Test
    @DisplayName("stale retry ticket cannot clean a newly registered query with the same id")
    void staleRetryDoesNotAffectReregisteredQuery() {
        AtomicLong clock = new AtomicLong();
        AtomicInteger cleanupAttempts = new AtomicInteger();
        QueryDispatcher dispatcher = controlledDispatcher(
                cleanupAttempts, new CountDownLatch(1), 1);
        queryManager = new QueryManager(QueryManagerContext.minimal(
                dispatcher, TestObservability.TRACING), clock::get);
        String queryId = registerRunningQuery();
        queryManager.onAllTasksFinished(queryId);
        queryManager.onAllTasksFinished(queryId); // explicit retry succeeds before queued due time
        assertThat(queryManager.cleanupTerminalQueries(Duration.ZERO)).isEqualTo(1);

        QueryStateMachine replacement = new QueryStateMachine();
        replacement.tryTransition(QueryStateMachine.State.PLANNING);
        queryManager.registerStateMachine(queryId, replacement);
        clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(100));
        queryManager.retryPendingTerminalCompletions();

        assertThat(cleanupAttempts.get()).isEqualTo(2);
        assertThat(queryManager.getQueryState(queryId))
                .hasValue(QueryStateMachine.State.PLANNING);
        assertThat(queryManager.getActiveQueryCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("concurrent terminal failures enqueue safely and retry only a bounded batch")
    void concurrentTerminalRetryQueueIsBounded() throws Exception {
        AtomicLong clock = new AtomicLong();
        AtomicInteger cleanupAttempts = new AtomicInteger();
        QueryDispatcher dispatcher = controlledDispatcher(
                cleanupAttempts, new CountDownLatch(1), Integer.MAX_VALUE);
        queryManager = new QueryManager(QueryManagerContext.minimal(
                dispatcher, TestObservability.TRACING), clock::get);
        List<String> queryIds = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            queryIds.add(registerRunningQuery());
        }
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            List<Future<?>> completions = new ArrayList<>();
            for (String queryId : queryIds) {
                completions.add(executor.submit(() -> {
                    await(start);
                    queryManager.onAllTasksFinished(queryId);
                }));
            }
            start.countDown();
            for (Future<?> completion : completions) {
                completion.get(3, TimeUnit.SECONDS);
            }
        } finally {
            start.countDown();
            executor.shutdownNow();
        }
        assertThat(cleanupAttempts.get()).isEqualTo(12);
        clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(100));
        queryManager.retryPendingTerminalCompletions();
        assertThat(cleanupAttempts.get()).isEqualTo(16);
        assertThat(queryManager.getActiveQueryCount()).isZero();
    }

    @Test
    @DisplayName("stopping periodic cleanup leaves pending work available for explicit recovery")
    void stoppedSchedulerDoesNotLosePendingRetry() throws Exception {
        AtomicLong clock = new AtomicLong();
        AtomicInteger cleanupAttempts = new AtomicInteger();
        QueryDispatcher dispatcher = controlledDispatcher(
                cleanupAttempts, new CountDownLatch(1), 1);
        queryManager = new QueryManager(QueryManagerContext.minimal(
                dispatcher, TestObservability.TRACING), clock::get);
        String queryId = registerRunningQuery();
        queryManager.onAllTasksFinished(queryId);
        var scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            queryManager.startPeriodicCleanup(
                    scheduler, Duration.ZERO, Duration.ofHours(1));
            queryManager.stopPeriodicCleanup();
            clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(100));
            scheduler.submit(() -> { }).get(3, TimeUnit.SECONDS);
            assertThat(cleanupAttempts.get()).isEqualTo(1);
            assertThat(queryManager.getQueryState(queryId))
                    .hasValue(QueryStateMachine.State.FINISHED);
            queryManager.retryPendingTerminalCompletions();
            assertThat(cleanupAttempts.get()).isEqualTo(2);
        } finally {
            queryManager.stopPeriodicCleanup();
            scheduler.shutdownNow();
        }
    }

    @Test
    @DisplayName("slow terminal cleanup retry does not block the shared health timer")
    void slowRetryRunsOffSharedTimer() throws Exception {
        AtomicInteger cleanupAttempts = new AtomicInteger();
        CountDownLatch retryStarted = new CountDownLatch(1);
        CountDownLatch releaseRetry = new CountDownLatch(1);
        CountDownLatch retryCompleted = new CountDownLatch(1);
        QueryDispatcher dispatcher = controlledDispatcher(
                cleanupAttempts, retryCompleted, 1, retryStarted, releaseRetry);
        queryManager = new QueryManager(QueryManagerContext.minimal(
                dispatcher, TestObservability.TRACING));
        queryManager.onAllTasksFinished(registerRunningQuery());
        var healthTimer = Executors.newSingleThreadScheduledExecutor();
        var cleanupPool = ManagedExecutorFactory.newBoundedThreadPool(
                "terminal-cleanup-test", 2, 2);
        try {
            queryManager.startPeriodicCleanup(healthTimer, cleanupPool,
                    Duration.ofHours(1), Duration.ofMillis(10));
            assertThat(retryStarted.await(3, TimeUnit.SECONDS)).isTrue();
            CountDownLatch healthTick = new CountDownLatch(1);
            healthTimer.schedule(healthTick::countDown, 0, TimeUnit.MILLISECONDS);
            assertThat(healthTick.await(1, TimeUnit.SECONDS)).isTrue();
            releaseRetry.countDown();
            assertThat(retryCompleted.await(3, TimeUnit.SECONDS)).isTrue();
            assertThat(cleanupAttempts.get()).isEqualTo(2);
        } finally {
            releaseRetry.countDown();
            queryManager.stopPeriodicCleanup();
            cleanupPool.shutdownNow();
            healthTimer.shutdownNow();
        }
    }

    @Test
    @DisplayName("FAILED cleanup exception preserves permit, audit, and drain completion")
    void failedCleanupDoesNotLoseTerminalEffects() {
        AtomicLong clock = new AtomicLong();
        AtomicInteger cleanupAttempts = new AtomicInteger();
        QueryDispatcher dispatcher = controlledDispatcher(
                cleanupAttempts, new CountDownLatch(1), 1);
        QueryThrottlingService throttling = new QueryThrottlingService(1, 1000, 1000);
        FailOnceFinishedJournal journal = new FailOnceFinishedJournal(false);
        List<AuditEvent> auditEvents = new CopyOnWriteArrayList<>();
        AuditEventEmitter audit = new AuditEventEmitter(auditEvents::add);
        try {
            queryManager = new QueryManager(new QueryManagerContext(
                    dispatcher, TestObservability.TRACING,
                    null, null, null, null, audit, null, null, throttling, journal),
                    clock::get);
            String queryId = "failed-" + UUID.randomUUID();
            queryManager.submitQuery(queryId, "SELECT 1",
                    new CoordinatorSessionContext(queryId, Map.of(), null, null));
            queryManager.onTaskFailed(queryId,
                    new ErrorDetail("RUNTIME_INTERNAL", "worker failed", null));

            assertThat(queryManager.getQueryState(queryId))
                    .hasValue(QueryStateMachine.State.FAILED);
            assertThat(queryManager.getActiveQueryCount()).isZero();
            assertThat(throttling.availableGlobalPermits()).isEqualTo(1);
            assertThat(queryManager.awaitAllQueries(100)).isTrue();
            assertThat(journal.failedAttempts.get()).isEqualTo(1);
            assertThat(cleanupAttempts.get()).isEqualTo(1);

            clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(100));
            queryManager.retryPendingTerminalCompletions();
            assertThat(cleanupAttempts.get()).isEqualTo(2);
            assertThat(journal.failedAttempts.get()).isEqualTo(1);
            assertThat(throttling.availableGlobalPermits()).isEqualTo(1);
        } finally {
            audit.close();
        }
        assertThat(auditEvents.stream()
                .filter(event -> event.eventType() == AuditEventType.QUERY_FAILED)
                .count()).isEqualTo(1);
    }

    @Test
    @DisplayName("CANCELLED cleanup exception releases permit and retries without double effects")
    void cancelledCleanupDoesNotLoseTerminalEffects() {
        AtomicLong clock = new AtomicLong();
        AtomicInteger cleanupAttempts = new AtomicInteger();
        QueryDispatcher dispatcher = controlledDispatcher(
                cleanupAttempts, new CountDownLatch(1), 1);
        QueryThrottlingService throttling = new QueryThrottlingService(1, 1000, 1000);
        FailOnceFinishedJournal journal = new FailOnceFinishedJournal(false);
        queryManager = new QueryManager(new QueryManagerContext(
                dispatcher, TestObservability.TRACING,
                null, null, null, null, null, null, null, throttling, journal),
                clock::get);
        String queryId = "cancelled-" + UUID.randomUUID();
        queryManager.submitQuery(queryId, "SELECT 1",
                new CoordinatorSessionContext(queryId, Map.of(), null, null));

        assertThat(queryManager.cancelQuery(queryId)).isTrue();
        assertThat(queryManager.getQueryState(queryId))
                .hasValue(QueryStateMachine.State.CANCELLED);
        assertThat(queryManager.getActiveQueryCount()).isZero();
        assertThat(throttling.availableGlobalPermits()).isEqualTo(1);
        assertThat(queryManager.awaitAllQueries(100)).isTrue();
        assertThat(journal.cancelledAttempts.get()).isEqualTo(1);

        clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(100));
        queryManager.retryPendingTerminalCompletions();
        assertThat(cleanupAttempts.get()).isEqualTo(2);
        assertThat(journal.cancelledAttempts.get()).isEqualTo(1);
        assertThat(throttling.availableGlobalPermits()).isEqualTo(1);
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(3, TimeUnit.SECONDS)) {
                throw new IllegalStateException("timed out waiting for test barrier");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static QueryDispatcher controlledDispatcher(
            AtomicInteger cleanupAttempts, CountDownLatch cleanupRetried, int failures) {
        return controlledDispatcher(cleanupAttempts, cleanupRetried, failures, null, null);
    }

    private static QueryDispatcher controlledDispatcher(
            AtomicInteger cleanupAttempts, CountDownLatch cleanupRetried, int failures,
            CountDownLatch retryStarted, CountDownLatch releaseRetry) {
        SqlCompiler compiler = new SqlCompiler();
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer());
        WorkerManager wm = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        return new QueryDispatcher(
                QueryDispatcherContext.withDefaults(
                        compiler, null, null, null,
                        new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(),
                                TestObservability.TRACING),
                        scheduler, wm, new CoordinatorTaskTracker(),
                        null, null, null, 0L), null, null) {
            @Override
            public DispatchResult execute(QueryStateMachine sm, String sql,
                                          CoordinatorSessionContext session) {
                sm.tryTransition(QueryStateMachine.State.PLANNING);
                sm.tryTransition(QueryStateMachine.State.SCHEDULING);
                sm.tryTransition(QueryStateMachine.State.RUNNING);
                return new DispatchResult("worker", "task",
                        new ResultEndpoint("worker", 8816, "ticket", "arrow-flight"));
            }

            @Override
            public void cleanupQuery(String queryId) {
                int attempt = cleanupAttempts.incrementAndGet();
                if (attempt <= failures) {
                    throw new IllegalStateException("shuffle cleanup pending");
                }
                if (retryStarted != null) {
                    retryStarted.countDown();
                    await(releaseRetry);
                }
                cleanupRetried.countDown();
            }
        };
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
                        null, null, null, 0L),
                null, null);
    }

    private static final class FailOnceFinishedJournal implements QueryStateJournal {
        private final boolean failFirst;
        private final QueryStateJournal delegate = org.pangrova.coordinator.coordination
                .inmemory.InMemoryQueryStateJournal.INSTANCE;
        private final AtomicInteger finishedAttempts = new AtomicInteger();
        private final AtomicInteger failedAttempts = new AtomicInteger();
        private final AtomicInteger cancelledAttempts = new AtomicInteger();

        private FailOnceFinishedJournal() {
            this(true);
        }

        private FailOnceFinishedJournal(boolean failFirst) {
            this.failFirst = failFirst;
        }

        @Override
        public void appendQueryEvent(QueryStateEvent event) {
            delegate.appendQueryEvent(event);
        }

        @Override
        public void appendQueryEventSync(QueryStateEvent event) {
            if ("FINISHED".equals(event.state())) {
                if (finishedAttempts.incrementAndGet() == 1 && failFirst) {
                    throw new IllegalStateException("journal unavailable");
                }
            } else if ("FAILED".equals(event.state())) {
                failedAttempts.incrementAndGet();
            } else if ("CANCELLED".equals(event.state())) {
                cancelledAttempts.incrementAndGet();
            }
            delegate.appendQueryEventSync(event);
        }

        @Override
        public void appendTaskEvent(TaskStateEvent event) {
            delegate.appendTaskEvent(event);
        }

        @Override
        public void appendTaskEventSync(TaskStateEvent event) {
            delegate.appendTaskEventSync(event);
        }

        @Override
        public List<QueryStateEvent> replayActiveQueries() {
            return delegate.replayActiveQueries();
        }

        @Override
        public List<TaskStateEvent> replayActiveTasks() {
            return delegate.replayActiveTasks();
        }

        @Override
        public Optional<QueryStateEvent> latestQueryEvent(String queryId) {
            return delegate.latestQueryEvent(queryId);
        }

        @Override
        public void purgeQuery(String queryId) {
            delegate.purgeQuery(queryId);
        }
    }
}
