package org.pangrova.coordinator.query;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.scheduler.failure.NoAvailableWorkerException;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.protocol.rest.query.SubmitQueryResponse;
import org.pangrova.testing.TestObservability;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Regression test for Task #186: validates that async query submission returns
 * RUNNING before the query finishes executing. The submit response must be
 * decoupled from full DAG execution — submit timeout covers only compilation
 * + initial dispatch, not the full query lifecycle.
 */
class QueryManagerAsyncSubmitTest {

    private QueryManager queryManager;
    private ExecutorService asyncExecutor;
    private CountDownLatch executionBlockLatch;
    private CountDownLatch executionStartedLatch;

    @BeforeEach
    void setUp() {
        executionBlockLatch = new CountDownLatch(1);
        executionStartedLatch = new CountDownLatch(1);
        asyncExecutor = Executors.newFixedThreadPool(4);

        QueryDispatcher slowDispatcher = new SlowQueryDispatcher(
                executionStartedLatch, executionBlockLatch);
        AsyncSubmitConfig asyncConfig = AsyncSubmitConfig.defaultEnabled();
        queryManager = new QueryManager(
                QueryManagerContext.minimalAsync(
                        slowDispatcher, TestObservability.TRACING,
                        asyncConfig, asyncExecutor));
    }

    @AfterEach
    void tearDown() {
        executionBlockLatch.countDown(); // release any blocked threads
        asyncExecutor.shutdownNow();
    }

    @Test
    @DisplayName("async submit returns RUNNING before query execution completes")
    void asyncSubmitReturnsRunningBeforeExecutionCompletes() throws Exception {
        // Submit a query — the SlowQueryDispatcher will transition to RUNNING
        // quickly but then block for a long time simulating slow execution.
        long startMs = System.currentTimeMillis();
        SubmitQueryResponse response = queryManager.submitQuery(
                "async-test-1", "SELECT 1", testSession("async-test-1"));
        long elapsedMs = System.currentTimeMillis() - startMs;

        // Assert: submit returned within a bounded time (well before the 2s block)
        assertThat(elapsedMs)
                .as("Submit should return before execution completes (< 1000ms)")
                .isLessThan(1000L);

        // Assert: state is RUNNING (or beyond, if the fast dispatch completed)
        assertThat(response.state())
                .as("Submit response state should be RUNNING or terminal")
                .isIn("RUNNING", "FINISHING", "FINISHED");

        // Assert: queryId is propagated correctly
        assertThat(response.queryId()).isEqualTo("async-test-1");

        // Assert: resultEndpoint is available
        assertThat(response.resultEndpoint()).isNotNull();
        assertThat(response.resultEndpoint().ticket()).contains("async-test-1");

        // The query should still be active (blocked in background)
        assertThat(queryManager.getActiveQueryCount()).isGreaterThanOrEqualTo(1);
    }

    @Test
    @DisplayName("async submit: query eventually reaches FINISHED after background execution completes")
    void asyncSubmitQueryEventuallyCompletes() throws Exception {
        SubmitQueryResponse response = queryManager.submitQuery(
                "async-test-2", "SELECT 2", testSession("async-test-2"));

        // Submit returned with RUNNING
        assertThat(response.state()).isIn("RUNNING", "FINISHING", "FINISHED");

        // Release the execution block — simulate query completion
        executionBlockLatch.countDown();

        // Wait for the background execution to complete
        boolean completed = awaitQueryTerminal("async-test-2", 5000);
        assertThat(completed)
                .as("Query should reach terminal state after execution block is released")
                .isTrue();

        // State should now be FINISHED
        var finalState = queryManager.getQueryState("async-test-2");
        assertThat(finalState).isPresent();
        assertThat(finalState.get()).isEqualTo(QueryStateMachine.State.FINISHED);
    }

    @Test
    @DisplayName("async submit: submit timeout does not couple to execution timeout")
    void submitTimeoutIsDecoupledFromExecutionTimeout() throws Exception {
        // Create a manager with a very short submit timeout (200ms)
        // but execution that reaches RUNNING within 50ms
        ExecutorService shortTimeoutExecutor = Executors.newSingleThreadExecutor();
        try {
            CountDownLatch blockLatch = new CountDownLatch(1);
            CountDownLatch startLatch = new CountDownLatch(1);
            QueryDispatcher fastStartDispatcher = new SlowQueryDispatcher(
                    startLatch, blockLatch);
            AsyncSubmitConfig shortTimeout = new AsyncSubmitConfig(true, 200);
            QueryManager shortTimeoutManager = new QueryManager(
                    QueryManagerContext.minimalAsync(
                            fastStartDispatcher, TestObservability.TRACING,
                            shortTimeout, shortTimeoutExecutor));

            // Should succeed — RUNNING is reached within the 200ms timeout
            SubmitQueryResponse response = shortTimeoutManager.submitQuery(
                    "timeout-test-1", "SELECT 1", testSession("timeout-test-1"));
            assertThat(response.state()).isIn("RUNNING", "FINISHING", "FINISHED");

            blockLatch.countDown();
        } finally {
            shortTimeoutExecutor.shutdownNow();
        }
    }

    @Test
    @DisplayName("synchronous mode: submit blocks until full execution completes (backward compat)")
    void syncModeBlocksUntilCompletion() {
        // Build a manager with async DISABLED — legacy behavior
        CountDownLatch blockLatch = new CountDownLatch(1);
        CountDownLatch startLatch = new CountDownLatch(1);
        QueryDispatcher dispatcher = new SlowQueryDispatcher(startLatch, blockLatch);
        AsyncSubmitConfig syncConfig = AsyncSubmitConfig.disabled();
        ExecutorService syncExecutor = Executors.newSingleThreadExecutor();

        QueryManager syncManager = new QueryManager(
                QueryManagerContext.minimalAsync(
                        dispatcher, TestObservability.TRACING,
                        syncConfig, syncExecutor));

        // With sync mode (disabled async), submit should block.
        // Release the latch immediately so the query completes.
        blockLatch.countDown();

        SubmitQueryResponse response = syncManager.submitQuery(
                "sync-test-1", "SELECT 1", testSession("sync-test-1"));

        // In sync mode, response should show terminal state
        assertThat(response.state()).isEqualTo("FINISHED");

        syncExecutor.shutdownNow();
    }

    @Test
    @DisplayName("async submit: GET /state returns current state for in-flight query")
    void getStateReturnsRunningForInFlightQuery() throws Exception {
        queryManager.submitQuery(
                "state-test-1", "SELECT 1", testSession("state-test-1"));

        // Query is in-flight (blocked in background), state should be RUNNING
        var state = queryManager.getQueryState("state-test-1");
        assertThat(state).isPresent();
        assertThat(state.get()).isIn(
                QueryStateMachine.State.RUNNING,
                QueryStateMachine.State.FINISHING,
                QueryStateMachine.State.FINISHED);

        // ResultEndpoint should be available for polling clients
        var endpoint = queryManager.getResultEndpoint("state-test-1");
        assertThat(endpoint).isPresent();
    }

    @Test
    @DisplayName("publishing an endpoint before RUNNING does not return an intermediate state")
    void endpointAloneDoesNotCompleteAsyncSubmit() throws Exception {
        CountDownLatch endpointPublished = new CountDownLatch(1);
        CountDownLatch allowRunning = new CountDownLatch(1);
        QueryDispatcher dispatcher = new SlowQueryDispatcher(
                new CountDownLatch(1), new CountDownLatch(1)) {
            @Override
            public DispatchResult execute(QueryStateMachine sm, String sql,
                                          CoordinatorSessionContext session) {
                var endpoint = new ResultEndpoint(
                        "localhost", 9090, "ticket-early", "arrow-flight");
                sm.tryTransition(QueryStateMachine.State.PLANNING);
                sm.tryTransition(QueryStateMachine.State.SCHEDULING);
                sm.setResultEndpoint(endpoint);
                endpointPublished.countDown();
                await(allowRunning);
                sm.tryTransition(QueryStateMachine.State.RUNNING);
                return new DispatchResult("localhost", "task-early", endpoint);
            }
        };
        QueryManager manager = new QueryManager(QueryManagerContext.minimalAsync(
                dispatcher, TestObservability.TRACING,
                AsyncSubmitConfig.defaultEnabled(), asyncExecutor));
        ExecutorService submitter = Executors.newSingleThreadExecutor();
        try {
            Future<SubmitQueryResponse> submitted = submitter.submit(() ->
                    manager.submitQuery("endpoint-before-running", "SELECT 1",
                            testSession("endpoint-before-running")));
            assertThat(endpointPublished.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> submitted.get(200, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            allowRunning.countDown();
            SubmitQueryResponse response = submitted.get(5, TimeUnit.SECONDS);
            assertThat(response.state()).isEqualTo("RUNNING");
            assertThat(response.resultEndpoint()).isNotNull();
        } finally {
            allowRunning.countDown();
            submitter.shutdownNow();
        }
    }

    @Test
    @DisplayName("FINISHED before DispatchResult waits for the control-plane endpoint")
    void terminalBeforeEndpointDoesNotReturnNullEndpoint() throws Exception {
        CountDownLatch terminalReached = new CountDownLatch(1);
        CountDownLatch allowResult = new CountDownLatch(1);
        QueryDispatcher dispatcher = new SlowQueryDispatcher(
                new CountDownLatch(1), new CountDownLatch(1)) {
            @Override
            public DispatchResult execute(QueryStateMachine sm, String sql,
                                          CoordinatorSessionContext session) {
                sm.tryTransition(QueryStateMachine.State.PLANNING);
                sm.tryTransition(QueryStateMachine.State.SCHEDULING);
                sm.tryTransition(QueryStateMachine.State.RUNNING);
                sm.tryTransition(QueryStateMachine.State.FINISHING);
                sm.tryTransition(QueryStateMachine.State.FINISHED);
                terminalReached.countDown();
                await(allowResult);
                return new DispatchResult("localhost", "task-ddl",
                        new ResultEndpoint("localhost", 9090,
                                "ticket-ddl", "arrow-flight"));
            }
        };
        QueryManager manager = new QueryManager(QueryManagerContext.minimalAsync(
                dispatcher, TestObservability.TRACING,
                AsyncSubmitConfig.defaultEnabled(), asyncExecutor));
        ExecutorService submitter = Executors.newSingleThreadExecutor();
        try {
            Future<SubmitQueryResponse> submitted = submitter.submit(() ->
                    manager.submitQuery("ddl-before-endpoint", "SHOW TABLES",
                            testSession("ddl-before-endpoint")));
            assertThat(terminalReached.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> submitted.get(200, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            allowResult.countDown();
            SubmitQueryResponse response = submitted.get(5, TimeUnit.SECONDS);
            assertThat(response.state()).isEqualTo("FINISHED");
            assertThat(response.resultEndpoint()).isNotNull();
            assertThat(response.resultEndpoint().ticket()).isEqualTo("ticket-ddl");
        } finally {
            allowResult.countDown();
            submitter.shutdownNow();
        }
    }

    @Test
    @DisplayName("dispatch failure before endpoint is published propagates to submitter")
    void failedDispatchDoesNotReturnSuccessfulSubmit() {
        QueryDispatcher unavailable = new SlowQueryDispatcher(
                new CountDownLatch(1), new CountDownLatch(1)) {
            @Override
            public DispatchResult execute(QueryStateMachine sm, String sql,
                                          CoordinatorSessionContext session) {
                sm.tryTransition(QueryStateMachine.State.PLANNING);
                sm.tryTransition(QueryStateMachine.State.FAILED);
                throw new NoAvailableWorkerException();
            }
        };
        QueryManager manager = new QueryManager(QueryManagerContext.minimalAsync(
                unavailable, TestObservability.TRACING,
                AsyncSubmitConfig.defaultEnabled(), asyncExecutor));

        assertThatThrownBy(() -> manager.submitQuery(
                "no-worker", "SELECT 1", testSession("no-worker")))
                .isInstanceOf(NoAvailableWorkerException.class);
        assertThat(manager.getQueryState("no-worker"))
                .contains(QueryStateMachine.State.FAILED);
        assertThat(manager.getResultEndpoint("no-worker")).isEmpty();
    }

    @Test
    @DisplayName("terminal write failure wakes submit before endpoint timeout")
    void terminalFailurePreservesCommitErrorBeforeEndpointTimeout() throws Exception {
        CountDownLatch dispatched = new CountDownLatch(1);
        CountDownLatch blocked = new CountDownLatch(1);
        QueryDispatcher dispatcher = new SlowQueryDispatcher(dispatched, blocked) {
            @Override
            public DispatchResult execute(QueryStateMachine sm, String sql,
                                          CoordinatorSessionContext session) {
                sm.tryTransition(QueryStateMachine.State.PLANNING);
                sm.tryTransition(QueryStateMachine.State.SCHEDULING);
                sm.tryTransition(QueryStateMachine.State.RUNNING);
                dispatched.countDown();
                await(blocked);
                throw new IllegalStateException("dispatch should be interrupted");
            }
        };
        QueryManager manager = new QueryManager(QueryManagerContext.minimalAsync(
                dispatcher, TestObservability.TRACING,
                new AsyncSubmitConfig(true, 5_000), asyncExecutor));
        ExecutorService submitter = Executors.newSingleThreadExecutor();
        try {
            Future<SubmitQueryResponse> submitted = submitter.submit(() ->
                    manager.submitQuery("write-commit-failed", "INSERT INTO t VALUES (1)",
                            testSession("write-commit-failed")));
            assertThat(dispatched.await(2, TimeUnit.SECONDS)).isTrue();
            manager.onTaskFailed("write-commit-failed", new ErrorDetail(
                    "RUNTIME-0001", "CATALOG_COMMIT_FAILED", "catalog commit rejected"));
            assertThatThrownBy(() -> submitted.get(2, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasRootCauseMessage("catalog commit rejected");
        } finally {
            blocked.countDown();
            submitter.shutdownNow();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Test dispatch gate timed out");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Test dispatch interrupted", e);
        }
    }

    // ── Helpers ──

    private boolean awaitQueryTerminal(String queryId, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            var state = queryManager.getQueryState(queryId);
            if (state.isPresent() && isTerminal(state.get())) {
                return true;
            }
            Thread.sleep(20);
        }
        return false;
    }

    private static boolean isTerminal(QueryStateMachine.State state) {
        return state == QueryStateMachine.State.FINISHED
                || state == QueryStateMachine.State.FAILED
                || state == QueryStateMachine.State.CANCELLED;
    }

    private static CoordinatorSessionContext testSession(String queryId) {
        return new CoordinatorSessionContext(queryId, null, null, null);
    }

    /**
     * A test QueryDispatcher that simulates a slow query: it transitions the state
     * machine to RUNNING quickly (setting a resultEndpoint), then blocks on a latch
     * before completing. This allows testing that async submit returns before
     * execution finishes.
     */
    static class SlowQueryDispatcher extends QueryDispatcher {

        private final CountDownLatch executionStartedLatch;
        private final CountDownLatch executionBlockLatch;

        SlowQueryDispatcher(CountDownLatch executionStartedLatch,
                            CountDownLatch executionBlockLatch) {
            super(buildMinimalContext(), null, null);
            this.executionStartedLatch = executionStartedLatch;
            this.executionBlockLatch = executionBlockLatch;
        }

        @Override
        public DispatchResult execute(QueryStateMachine sm, String sql,
                                      CoordinatorSessionContext session) {
            // Set the result endpoint BEFORE transitioning to RUNNING — the async
            // listener fires on RUNNING and immediately reads the endpoint.
            ResultEndpoint endpoint = new ResultEndpoint(
                    "localhost", 9090,
                    "ticket-" + session.queryId(), "arrow-flight");
            sm.setResultEndpoint(endpoint);

            // Simulate compile → schedule → dispatch transitions quickly
            sm.tryTransition(QueryStateMachine.State.PLANNING);
            sm.tryTransition(QueryStateMachine.State.SCHEDULING);
            sm.tryTransition(QueryStateMachine.State.RUNNING);

            // Signal that execution has started (RUNNING reached)
            executionStartedLatch.countDown();

            // Block here — simulating slow execution (DML commit, large scan, etc.)
            try {
                executionBlockLatch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // Complete the query
            sm.tryTransition(QueryStateMachine.State.FINISHING);
            sm.tryTransition(QueryStateMachine.State.FINISHED);

            return new DispatchResult("localhost",
                    "task-" + session.queryId(), endpoint);
        }

        @Override
        public boolean acquireQueryOwnership(String queryId) {
            return true; // always succeed in test
        }

        @Override
        public List<String> removeColumnNames(String queryId) {
            return List.of("col1");
        }

        @Override
        public void cleanupQuery(String queryId) {
            // no-op in test
        }

        @Override
        public void cancelDispatchedTasks(String queryId) {
            // no-op in test
        }

        private static QueryDispatcherContext buildMinimalContext() {
            SqlCompiler compiler = new SqlCompiler();
            TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer());
            return QueryDispatcherContext.withDefaults(
                    compiler, null, null, null,
                    new RemoteTaskDispatcher(
                            CircuitBreakerPolicy.noOp(),
                            TestObservability.TRACING),
                    scheduler,
                    new WorkerManager(20_000, 60_000,
                            new org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry()),
                    new CoordinatorTaskTracker(),
                    null, null, null, 0L);
        }
    }
}
