package org.pangrova.coordinator.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.pangrova.testing.TestObservability;
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
                QueryManagerContext.minimal(dispatcher, TestObservability.TRACING));
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
        sm.tryTransition(QueryStateMachine.State.PLANNING);
        sm.tryTransition(QueryStateMachine.State.SCHEDULING);
        sm.tryTransition(QueryStateMachine.State.RUNNING);
        queryManager.registerStateMachine("inflight-1", sm);

        queryManager.setAcceptingQueries(false);

        assertThatThrownBy(() -> queryManager.submitQuery(
                "rejected-1", "SELECT 1", testSession()))
                .isInstanceOf(QueryRejectedException.class);

        queryManager.onAllTasksFinished("inflight-1");
        assertThat(sm.getState()).isEqualTo(QueryStateMachine.State.FINISHED);
    }

    @Test
    @DisplayName("awaitAllQueries returns true when all queries complete within timeout")
    void awaitAllQueriesReturnsWhenQueriesComplete() throws Exception {
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryStateMachine.State.PLANNING);
        sm.tryTransition(QueryStateMachine.State.SCHEDULING);
        sm.tryTransition(QueryStateMachine.State.RUNNING);
        queryManager.registerStateMachine("await-1", sm);

        Future<Boolean> drainFuture = executor.submit(
                () -> queryManager.awaitAllQueries(5000));

        Thread.sleep(50);
        queryManager.onAllTasksFinished("await-1");

        boolean drained = drainFuture.get(3, TimeUnit.SECONDS);
        assertThat(drained).isTrue();
    }

    @Test
    @DisplayName("awaitAllQueries returns false on timeout with active queries")
    void awaitAllQueriesTimesOutWithActiveQueries() {
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryStateMachine.State.PLANNING);
        queryManager.registerStateMachine("timeout-1", sm);

        boolean drained = queryManager.awaitAllQueries(100);
        assertThat(drained).isFalse();
    }

    @Test
    @DisplayName("dispatcher-finished queries decrement active count before drain")
    void dispatcherFinishedQueryDrainsWithoutTimeout() {
        QueryManager localQueryManager = new QueryManager(
                QueryManagerContext.minimal(new FinishingDispatcher(), TestObservability.TRACING));

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
            sm.tryTransition(QueryStateMachine.State.PLANNING);
            sm.tryTransition(QueryStateMachine.State.SCHEDULING);
            sm.tryTransition(QueryStateMachine.State.RUNNING);
            queryManager.registerStateMachine("concurrent-" + i, sm);
        }

        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicBoolean drainResult = new AtomicBoolean(false);

        Future<?> drainThread = executor.submit(() -> {
            startLatch.countDown();
            drainResult.set(queryManager.awaitAllQueries(5000));
        });

        startLatch.await();
        Thread.sleep(20);

        for (int i = 0; i < queryCount; i++) {
            final int idx = i;
            executor.submit(() -> {
                if (idx % 2 == 0) {
                    queryManager.onAllTasksFinished("concurrent-" + idx);
                } else {
                    queryManager.onTaskFailed("concurrent-" + idx,
                            new ErrorDetail("TEST", "TEST", "simulated failure"));
                }
            });
        }

        drainThread.get(5, TimeUnit.SECONDS);
        assertThat(drainResult.get()).isTrue();
        assertThat(queryManager.getActiveQueryCount()).isZero();
    }

    @Test
    @DisplayName("multiple concurrent submissions succeed under read lock")
    void concurrentSubmissionsDoNotBlock() throws Exception {
        int threadCount = 8;
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int idx = i;
            executor.submit(() -> {
                try {
                    queryManager.submitQuery(
                            "parallel-" + idx, "SELECT " + idx, testSession());
                } catch (Exception expected) {
                    // dispatch may fail but submission should not deadlock
                } finally {
                    latch.countDown();
                }
            });
        }

        boolean completed = latch.await(5, TimeUnit.SECONDS);
        assertThat(completed)
                .as("All submissions should complete within timeout (no deadlock)")
                .isTrue();
    }

    private static CoordinatorSessionContext testSession() {
        return new CoordinatorSessionContext("test-query", null, null, null);
    }

    private static QueryDispatcher buildDispatcher() {
        SqlCompiler compiler = new SqlCompiler();
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer());
        return new QueryDispatcher(
                QueryDispatcherContext.withDefaults(
                        compiler, null, null, null,
                        new RemoteTaskDispatcher(
                                CircuitBreakerPolicy.noOp(),
                                TestObservability.TRACING),
                        scheduler,
                        new WorkerManager(20_000, 60_000, new org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry()),
                        new CoordinatorTaskTracker(),
                        null, null, null, null, 0L),
                null, null);
    }

                private static final class FinishingDispatcher extends QueryDispatcher {

                FinishingDispatcher() {
                    super(QueryDispatcherContext.withDefaults(
                        new SqlCompiler(), null, null, null,
                        new RemoteTaskDispatcher(
                            CircuitBreakerPolicy.noOp(),
                            TestObservability.TRACING),
                        new TaskScheduler(new WorkerPerformanceScorer()),
                        new WorkerManager(20_000, 60_000,
                            new org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry()),
                        new CoordinatorTaskTracker(),
                        null, null, null, null, 0L),
                        null, null);
                }

                @Override
                public DispatchResult execute(QueryStateMachine sm, String sql,
                                  CoordinatorSessionContext session) {
                    sm.toFinisher().transitionToFinished();
                    return new DispatchResult(
                            "local", "task-local", new ResultEndpoint("local", 0, "/results/local", "http"));
                }
                }
}
