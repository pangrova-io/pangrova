package org.pangrova.coordinator.query;

import org.pangrova.query.lifecycle.QueryStateMachine;

import org.pangrova.coordinator.testinfra.QueryManagerTestContexts;

import org.pangrova.query.api.enums.QueryState;

import org.pangrova.query.api.exception.QueryRejectedException;
import org.pangrova.query.api.session.QuerySession;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.ArrayList;

import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.execution.task.DispatchResult;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.scheduler.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.builtin.placement.LoadBalancedStrategy;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.query.api.error.QueryFailure;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.testing.testkit.TestObservability;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Validates that concurrent drain + query completion does not deadlock.
 * Verifies Requirement 5.7: ReentrantReadWriteLock replaces drainLock+CompletableFuture.
 */
class QueryManagerDrainConcurrencyTest {

    private QueryManager queryManager;
    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        QueryDispatcher dispatcher = buildDispatcher();
        queryManager = new QueryManager(
                QueryManagerTestContexts.minimal(dispatcher, TestObservability.TRACING));
        executor = Executors.newFixedThreadPool(4);
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    @DisplayName("drain blocks new submissions while allowing in-flight completion")
    void drainBlocksNewSubmissionsWhileInFlightComplete() {
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryState.PLANNING);
        sm.tryTransition(QueryState.SCHEDULING);
        sm.tryTransition(QueryState.RUNNING);
        queryManager.registerStateMachine("inflight-1", sm);

        queryManager.setAcceptingQueries(false);

        assertThatThrownBy(() -> queryManager.submitQuery(
                "rejected-1", "SELECT 1", testSession()))
                .isInstanceOf(QueryRejectedException.class);

        queryManager.onAllTasksFinished("inflight-1");
        assertThat(sm.getState()).isEqualTo(QueryState.FINISHED);
    }

    @Test
    @DisplayName("awaitAllQueries returns true when all queries complete within timeout")
    void awaitAllQueriesReturnsWhenQueriesComplete() throws Exception {
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryState.PLANNING);
        sm.tryTransition(QueryState.SCHEDULING);
        sm.tryTransition(QueryState.RUNNING);
        queryManager.registerStateMachine("await-1", sm);

        CountDownLatch drainEntered = new CountDownLatch(1);
        Future<Boolean> drainFuture = executor.submit(() -> {
            drainEntered.countDown();
            return queryManager.awaitAllQueries(5000);
        });
        assertThat(drainEntered.await(3, TimeUnit.SECONDS)).isTrue();
        queryManager.onAllTasksFinished("await-1");

        boolean drained = drainFuture.get(3, TimeUnit.SECONDS);
        assertThat(drained).isTrue();
    }

    @Test
    @DisplayName("awaitAllQueries returns false on timeout with active queries")
    void awaitAllQueriesTimesOutWithActiveQueries() {
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryState.PLANNING);
        queryManager.registerStateMachine("timeout-1", sm);

        boolean drained = queryManager.awaitAllQueries(100);
        assertThat(drained).isFalse();
    }

    @Test
    @DisplayName("dispatcher-finished queries decrement active count before drain")
    void dispatcherFinishedQueryDrainsWithoutTimeout() {
        QueryManager localQueryManager = new QueryManager(
                QueryManagerTestContexts.minimal(new FinishingDispatcher(), TestObservability.TRACING));

        localQueryManager.submitQuery("local-finished-1", "SELECT 1", testSession());

        assertThat(localQueryManager.getActiveQueryCount()).isZero();
        assertThat(localQueryManager.awaitAllQueries(100)).isTrue();
    }

    @Test
    @DisplayName("concurrent drain and query failure does not deadlock")
    void concurrentDrainAndFailureNoDeadlock() throws Exception {
        int queryCount = 10;
        for (int i = 0; i < queryCount; i++) {
            QueryStateMachine sm = new QueryStateMachine();
            sm.tryTransition(QueryState.PLANNING);
            sm.tryTransition(QueryState.SCHEDULING);
            sm.tryTransition(QueryState.RUNNING);
            queryManager.registerStateMachine("concurrent-" + i, sm);
        }

        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicBoolean drainResult = new AtomicBoolean(false);

        Future<?> drainThread = executor.submit(() -> {
            startLatch.countDown();
            drainResult.set(queryManager.awaitAllQueries(5000));
        });

        startLatch.await();

        for (int i = 0; i < queryCount; i++) {
            final int idx = i;
            executor.submit(() -> {
                if (idx % 2 == 0) {
                    queryManager.onAllTasksFinished("concurrent-" + idx);
                } else {
                    queryManager.onTaskFailed("concurrent-" + idx,
                            new QueryFailure("TEST", "TEST", "simulated failure"));
                }
            });
        }

        drainThread.get(5, TimeUnit.SECONDS);
        assertThat(drainResult.get()).isTrue();
        assertThat(queryManager.getActiveQueryCount()).isZero();
    }

    @Test
    @DisplayName("parallel protected submissions fail Worker admission without deadlocking")
    void concurrentSubmissionsFailWorkerAdmissionWithoutDeadlocking() throws Exception {
        int threadCount = 8;
        var submissions = new ArrayList<Future<?>>();

        for (int i = 0; i < threadCount; i++) {
            final int idx = i;
            submissions.add(executor.submit(() -> assertThatThrownBy(() -> queryManager.submitQuery(
                    "parallel-" + idx, "SELECT " + idx, testSession()))
                    .isInstanceOf(org.pangrova.scheduler.api.exception.NoAvailableWorkerException.class)));
        }
        for (Future<?> submission : submissions) submission.get(5, TimeUnit.SECONDS);
        assertThat(queryManager.getActiveQueryCount()).isZero();
        for (int i = 0; i < threadCount; i++) {
            assertThat(queryManager.getQueryState("parallel-" + i)).hasValue(QueryState.FAILED);
        }
    }

    @Test
    void cancellationWaitsForExecutionAnchorBeforeRecordingTerminalCleanup() throws Exception {
        BlockingOwnershipDispatcher dispatcher = new BlockingOwnershipDispatcher();
        QueryManager manager = new QueryManager(
                QueryManagerTestContexts.minimal(dispatcher, TestObservability.TRACING));
        CountDownLatch terminalRecorded = new CountDownLatch(1);
        manager.setShuffleCleanupPending(queryId -> {
            assertThat(dispatcher.ownerAcquired.get()).isTrue();
            terminalRecorded.countDown();
        });

        Future<?> submission = executor.submit(() -> manager.submitQuery(
                "anchor-race", "SELECT 1", testSession()));
        assertThat(dispatcher.acquireEntered.await(3, TimeUnit.SECONDS)).isTrue();
        CountDownLatch cancelEntered = new CountDownLatch(1);
        Future<Boolean> cancellation = executor.submit(() -> {
            cancelEntered.countDown();
            return manager.cancelQuery("anchor-race");
        });
        assertThat(cancelEntered.await(3, TimeUnit.SECONDS)).isTrue();
        assertThatThrownBy(() -> cancellation.get(100, TimeUnit.MILLISECONDS))
                .isInstanceOf(TimeoutException.class);
        assertThat(terminalRecorded.getCount()).isEqualTo(1);

        dispatcher.allowAcquire.countDown();
        assertThat(cancellation.get(3, TimeUnit.SECONDS)).isTrue();
        submission.get(3, TimeUnit.SECONDS);
        assertThat(manager.getQueryState("anchor-race"))
                .contains(QueryState.CANCELLED);
        assertThat(dispatcher.cleanupCount.get()).isEqualTo(1);
    }

    @Test
    void deniedOwnershipUnblocksCancellationWithoutCleanup() throws Exception {
        BlockingOwnershipDispatcher dispatcher = new BlockingOwnershipDispatcher(false);
        QueryManager manager = new QueryManager(
                QueryManagerTestContexts.minimal(dispatcher, TestObservability.TRACING));
        AtomicInteger terminalRecords = new AtomicInteger();
        manager.setShuffleCleanupPending(queryId -> terminalRecords.incrementAndGet());

        Future<?> submission = executor.submit(() -> manager.submitQuery(
                "denied-anchor", "SELECT 1", testSession()));
        assertThat(dispatcher.acquireEntered.await(3, TimeUnit.SECONDS)).isTrue();
        Future<Boolean> cancellation = executor.submit(() -> manager.cancelQuery("denied-anchor"));
        assertThatThrownBy(() -> cancellation.get(100, TimeUnit.MILLISECONDS))
                .isInstanceOf(TimeoutException.class);

        dispatcher.allowAcquire.countDown();
        assertThatThrownBy(() -> submission.get(3, TimeUnit.SECONDS))
                .hasCauseInstanceOf(QueryRejectedException.class);
        assertThat(cancellation.get(3, TimeUnit.SECONDS)).isFalse();
        assertThat(terminalRecords.get()).isZero();
        assertThat(dispatcher.cleanupCount.get()).isZero();
    }

    @Test
    void terminalIntentWriteDoesNotHoldStateMachineMonitor() throws Exception {
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryState.PLANNING);
        sm.tryTransition(QueryState.SCHEDULING);
        sm.tryTransition(QueryState.RUNNING);
        queryManager.registerStateMachine("slow-intent", sm);
        CountDownLatch recorderEntered = new CountDownLatch(1);
        CountDownLatch releaseRecorder = new CountDownLatch(1);
        AtomicBoolean monitorHeld = new AtomicBoolean();
        queryManager.setShuffleCleanupPending(queryId -> {
            monitorHeld.set(Thread.holdsLock(sm));
            recorderEntered.countDown();
            try {
                if (!releaseRecorder.await(3, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("intent write timed out");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(interrupted);
            }
        });

        Future<?> failed = executor.submit(() -> queryManager.onTaskFailed(
                "slow-intent", new QueryFailure("INTERNAL", "task failed", null)));
        Future<Boolean> cancelled = null;
        try {
            assertThat(recorderEntered.await(3, TimeUnit.SECONDS)).isTrue();
            // Wiring acquires the state-machine monitor. It must stay available
            // while the JDBC intent write is in progress.
            Future<?> rewired = executor.submit(
                    () -> queryManager.setShuffleCleanupPending(queryId -> { }));
            rewired.get(1, TimeUnit.SECONDS);
            assertThat(sm.getState()).isEqualTo(QueryState.RUNNING);
            assertThat(monitorHeld.get()).isFalse();
            cancelled = executor.submit(() -> queryManager.cancelQuery("slow-intent"));
            Future<Boolean> waitingCancellation = cancelled;
            assertThatThrownBy(() -> waitingCancellation.get(100, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
        } finally {
            releaseRecorder.countDown();
        }
        failed.get(3, TimeUnit.SECONDS);
        assertThat(cancelled.get(3, TimeUnit.SECONDS)).isFalse();
        assertThat(sm.getState()).isEqualTo(QueryState.FAILED);
    }

    @Test
    void losingFailureDoesNotPublishAnErrorOnFinishedQuery() throws Exception {
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryState.PLANNING);
        sm.tryTransition(QueryState.SCHEDULING);
        sm.tryTransition(QueryState.RUNNING);
        sm.tryTransition(QueryState.FINISHING);
        CountDownLatch recorderEntered = new CountDownLatch(1);
        CountDownLatch releaseRecorder = new CountDownLatch(1);
        sm.setBeforeTerminalTransition(() -> {
            recorderEntered.countDown();
            try {
                if (!releaseRecorder.await(3, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("intent write timed out");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(interrupted);
            }
        });
        Future<Boolean> finished = executor.submit(
                () -> sm.tryTransition(QueryState.FINISHED));
        try {
            assertThat(recorderEntered.await(3, TimeUnit.SECONDS)).isTrue();
            Future<Boolean> failed = executor.submit(() -> sm.tryTransitionToFailed(
                    new QueryFailure("INTERNAL", "losing failure", null)));
            assertThatThrownBy(() -> failed.get(100, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            assertThat(sm.getError()).isEmpty();
            releaseRecorder.countDown();
            assertThat(finished.get(3, TimeUnit.SECONDS)).isTrue();
            assertThat(failed.get(3, TimeUnit.SECONDS)).isFalse();
            assertThat(sm.getError()).isEmpty();
        } finally {
            releaseRecorder.countDown();
        }
    }

    @Test
    void admittedSessionCannotRetargetOrCreateAnotherQueryOwner() {
        var query = new org.pangrova.coordinator.task.TestConnectorQuery("existing-owner");
        try {
            var session = org.pangrova.coordinator.testinfra.SourceFreeQueryFixture.session("existing-owner")
                    .withQueryAuthority(query.authority());
            assertThatThrownBy(() -> queryManager.submitQuery("another-owner", "SELECT 1", session))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("cannot change its query identity");
            assertThat(queryManager.getQueryState("another-owner")).isEmpty();
            assertThat(queryManager.getActiveQueryCount()).isZero();
        } finally {
            query.seal();
            query.retire();
        }
    }

    private static QuerySession testSession() {
        return org.pangrova.coordinator.testinfra.SourceFreeQueryFixture.session("test-query");
    }

    private static QueryDispatcher buildDispatcher() {
        SqlCompiler compiler = new SqlCompiler();
        TaskScheduler scheduler = new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty());
        return new QueryDispatcher(
                org.pangrova.coordinator.testinfra.SourceFreeQueryFixture.withPolicies(
                QueryDispatcherContext.withDefaults(
                        compiler, null, null, null,
                        new RemoteTaskDispatcher(
                                CircuitBreakerPolicy.noOp(),
                                TestObservability.TRACING),
                        scheduler,
                        new WorkerManager(20_000, 60_000, new org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry()),
                        new CoordinatorTaskTracker(),
                        null, null, null, 0L)),
                null, null);
    }

    private static class FinishingDispatcher extends QueryDispatcher {

        FinishingDispatcher() {
            super(QueryDispatcherContext.withDefaults(
                    new SqlCompiler(), null, null, null,
                        new RemoteTaskDispatcher(
                                CircuitBreakerPolicy.noOp(),
                                TestObservability.TRACING),
                    new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty()),
                    new WorkerManager(20_000, 60_000,
                            new org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry()),
                    new CoordinatorTaskTracker(),
                    null, null, null, 0L),
                    null, null);
        }

        @Override
        public DispatchResult execute(QueryStateMachine sm, String sql,
                                      QuerySession session) {
            sm.toFinisher().run();
            return new DispatchResult(
                    "local", "task-local", new ResultEndpoint("local", 0, "/results/local", "http"));
        }
    }

    private static final class BlockingOwnershipDispatcher extends FinishingDispatcher {
        private final boolean grant;
        private final CountDownLatch acquireEntered = new CountDownLatch(1);
        private final CountDownLatch allowAcquire = new CountDownLatch(1);
        private final AtomicBoolean ownerAcquired = new AtomicBoolean();
        private final AtomicInteger cleanupCount = new AtomicInteger();

        private BlockingOwnershipDispatcher() {
            this(true);
        }

        private BlockingOwnershipDispatcher(boolean grant) {
            this.grant = grant;
        }

        @Override
        public boolean acquireQueryOwnership(String queryId) {
            acquireEntered.countDown();
            try {
                if (!allowAcquire.await(3, TimeUnit.SECONDS)) {
                    return false;
                }
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                return false;
            }
            ownerAcquired.set(grant);
            return grant;
        }

        @Override
        public DispatchResult execute(QueryStateMachine sm, String sql,
                                      QuerySession session) {
            return null;
        }

        @Override
        public void cleanupQuery(String queryId) {
            cleanupCount.incrementAndGet();
        }
    }
}
