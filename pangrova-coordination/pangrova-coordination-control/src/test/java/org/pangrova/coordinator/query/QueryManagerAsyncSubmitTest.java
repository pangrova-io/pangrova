package org.pangrova.coordinator.query;
import org.pangrova.query.runtime.QueryManager;

import org.pangrova.query.lifecycle.QueryStateMachine;

import org.pangrova.coordinator.testinfra.QueryManagerTestContexts;

import org.pangrova.query.api.enums.QueryState;

import org.pangrova.query.api.submission.AsyncSubmitConfig;
import org.pangrova.query.api.session.QuerySession;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
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
import org.pangrova.execution.task.DispatchResult;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.scheduler.api.exception.NoAvailableWorkerException;
import org.pangrova.scheduler.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.strategies.placement.LoadBalancedStrategy;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.query.api.error.QueryFailure;
import org.pangrova.query.api.submission.QuerySubmissionResult;
import org.pangrova.testing.testkit.constants.TestObservability;
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
                QueryManagerTestContexts.minimalAsync(
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
        QuerySubmissionResult response = queryManager.submitQuery(
                "async-test-1", "SELECT 1", testSession("async-test-1"));
        long elapsedMs = System.currentTimeMillis() - startMs;

        // Assert: submit returned within a bounded time (well before the 2s block)
        assertThat(elapsedMs)
                .as("Submit should return before execution completes (< 1000ms)")
                .isLessThan(1000L);

        // Assert: state is RUNNING (or beyond, if the fast dispatch completed)
        assertThat(response.state())
                .as("Submit response state should be RUNNING or terminal")
                .isIn(QueryState.RUNNING, QueryState.FINISHING, QueryState.FINISHED);

        // Assert: queryId is propagated correctly
        assertThat(response.queryId()).isEqualTo("async-test-1");

        // Assert: resultEndpoint is available
        assertThat(response.resultEndpoint()).isNotNull();
        assertThat(response.resultEndpoint().ticket()).contains("async-test-1");

        // The query should still be active (blocked in background)
        assertThat(queryManager.getActiveQueryCount()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void asyncReceiptReturnsWhilePlanningIsHeldAndObserverCancellationKeepsTheQuery() throws Exception {
        var entering = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var endpoint = new ResultEndpoint("localhost", 9090, "ticket-async-observer", "arrow-flight");
        var dispatcher = new SlowQueryDispatcher(entering, release) {
            @Override
            public CompletionStage<DispatchResult> executeAsync(QueryStateMachine sm, String sql,
                    QuerySession session) {
                sm.tryTransition(QueryState.PLANNING);
                entering.countDown();
                await(release);
                sm.tryTransition(QueryState.SCHEDULING);
                sm.tryTransition(QueryState.RUNNING);
                sm.setResultEndpoint(endpoint);
                return CompletableFuture.completedFuture(new DispatchResult("localhost", "task-observer", endpoint));
            }
        };
        var manager = new QueryManager(QueryManagerTestContexts.minimalAsync(
                dispatcher, TestObservability.TRACING, AsyncSubmitConfig.defaultEnabled(), asyncExecutor));
        var request = Executors.newSingleThreadExecutor();
        try {
            String queryId = "async-observer";
            var receipt = request.submit(() ->
                    manager.submitQueryAsync(queryId, "SELECT 1", testSession(queryId))).get(2, TimeUnit.SECONDS);
            assertThat(entering.await(5, TimeUnit.SECONDS)).isTrue();
            var disconnected = receipt.toCompletableFuture();
            assertThat(disconnected.cancel(false)).isTrue();
            var remaining = receipt.toCompletableFuture();
            assertThat(remaining).isNotDone();

            release.countDown();
            assertThat(remaining.get(5, TimeUnit.SECONDS).resultEndpoint()).isEqualTo(endpoint);
            assertThat(manager.getQueryState(queryId)).contains(QueryState.RUNNING);
        } finally {
            release.countDown();
            request.shutdownNow();
        }
    }

    @Test
    void resultlessWriteDispatchReturnsBeforeLongCommit() throws Exception {
        CountDownLatch commit = new CountDownLatch(1);
        QueryDispatcher dispatcher = new SlowQueryDispatcher(new CountDownLatch(1), commit) {
            @Override
            public CompletionStage<DispatchResult> executeAsync(QueryStateMachine sm, String sql,
                                          QuerySession session) {
                sm.tryTransition(QueryState.PLANNING);
                sm.tryTransition(QueryState.SCHEDULING);
                sm.tryTransition(QueryState.RUNNING);
                sm.markWriteDispatchReady();
                await(commit);
                sm.tryTransition(QueryState.FINISHING);
                sm.tryTransition(QueryState.FINISHED);
                return CompletableFuture.completedFuture(null);
            }
        };
        QueryManager manager = new QueryManager(QueryManagerTestContexts.minimalAsync(
                dispatcher, TestObservability.TRACING,
                new AsyncSubmitConfig(true, 200), asyncExecutor));

        try {
            QuerySubmissionResult response = manager.submitQuery(
                    "long-write", "INSERT INTO t VALUES (1)", testSession("long-write"));
            assertThat(response.state()).isEqualTo(org.pangrova.query.api.enums.QueryState.RUNNING);
            assertThat(response.resultEndpoint()).isNull();
            assertThat(manager.getQueryState("long-write")).contains(QueryState.RUNNING);
        } finally {
            commit.countDown();
        }
    }

    @Test
    @DisplayName("async submit: query eventually reaches FINISHED after background execution completes")
    void asyncSubmitQueryEventuallyCompletes() throws Exception {
        QuerySubmissionResult response = queryManager.submitQuery(
                "async-test-2", "SELECT 2", testSession("async-test-2"));

        // Submit returned with RUNNING
        assertThat(response.state()).isIn(org.pangrova.query.api.enums.QueryState.RUNNING, org.pangrova.query.api.enums.QueryState.FINISHING, org.pangrova.query.api.enums.QueryState.FINISHED);

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
        assertThat(finalState.get()).isEqualTo(QueryState.FINISHED);
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
                    QueryManagerTestContexts.minimalAsync(
                            fastStartDispatcher, TestObservability.TRACING,
                            shortTimeout, shortTimeoutExecutor));

            // Should succeed — RUNNING is reached within the 200ms timeout
            QuerySubmissionResult response = shortTimeoutManager.submitQuery(
                    "timeout-test-1", "SELECT 1", testSession("timeout-test-1"));
            assertThat(response.state()).isIn(org.pangrova.query.api.enums.QueryState.RUNNING, org.pangrova.query.api.enums.QueryState.FINISHING, org.pangrova.query.api.enums.QueryState.FINISHED);

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
                QueryManagerTestContexts.minimalAsync(
                        dispatcher, TestObservability.TRACING,
                        syncConfig, syncExecutor));

        // With sync mode (disabled async), submit should block.
        // Release the latch immediately so the query completes.
        blockLatch.countDown();

        QuerySubmissionResult response = syncManager.submitQuery(
                "sync-test-1", "SELECT 1", testSession("sync-test-1"));

        // In sync mode, response should show terminal state
        assertThat(response.state()).isEqualTo(org.pangrova.query.api.enums.QueryState.FINISHED);

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
                QueryState.RUNNING,
                QueryState.FINISHING,
                QueryState.FINISHED);

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
            public CompletionStage<DispatchResult> executeAsync(QueryStateMachine sm, String sql,
                                          QuerySession session) {
                var endpoint = new ResultEndpoint(
                        "localhost", 9090, "ticket-early", "arrow-flight");
                sm.tryTransition(QueryState.PLANNING);
                sm.tryTransition(QueryState.SCHEDULING);
                sm.setResultEndpoint(endpoint);
                endpointPublished.countDown();
                await(allowRunning);
                sm.tryTransition(QueryState.RUNNING);
                return CompletableFuture.completedFuture(new DispatchResult("localhost", "task-early", endpoint));
            }
        };
        QueryManager manager = new QueryManager(QueryManagerTestContexts.minimalAsync(
                dispatcher, TestObservability.TRACING,
                AsyncSubmitConfig.defaultEnabled(), asyncExecutor));
        ExecutorService submitter = Executors.newSingleThreadExecutor();
        try {
            Future<QuerySubmissionResult> submitted = submitter.submit(() ->
                    manager.submitQuery("endpoint-before-running", "SELECT 1",
                            testSession("endpoint-before-running")));
            assertThat(endpointPublished.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> submitted.get(200, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            allowRunning.countDown();
            QuerySubmissionResult response = submitted.get(5, TimeUnit.SECONDS);
            assertThat(response.state()).isEqualTo(org.pangrova.query.api.enums.QueryState.RUNNING);
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
            public CompletionStage<DispatchResult> executeAsync(QueryStateMachine sm, String sql,
                                          QuerySession session) {
                sm.tryTransition(QueryState.PLANNING);
                sm.tryTransition(QueryState.SCHEDULING);
                sm.tryTransition(QueryState.RUNNING);
                sm.tryTransition(QueryState.FINISHING);
                sm.tryTransition(QueryState.FINISHED);
                terminalReached.countDown();
                await(allowResult);
                return CompletableFuture.completedFuture(new DispatchResult("localhost", "task-ddl",
                        new ResultEndpoint("localhost", 9090,
                                "ticket-ddl", "arrow-flight")));
            }
        };
        QueryManager manager = new QueryManager(QueryManagerTestContexts.minimalAsync(
                dispatcher, TestObservability.TRACING,
                AsyncSubmitConfig.defaultEnabled(), asyncExecutor));
        ExecutorService submitter = Executors.newSingleThreadExecutor();
        try {
            Future<QuerySubmissionResult> submitted = submitter.submit(() ->
                    manager.submitQuery("ddl-before-endpoint", "SHOW TABLES",
                            testSession("ddl-before-endpoint")));
            assertThat(terminalReached.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> submitted.get(200, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            allowResult.countDown();
            QuerySubmissionResult response = submitted.get(5, TimeUnit.SECONDS);
            assertThat(response.state()).isEqualTo(org.pangrova.query.api.enums.QueryState.FINISHED);
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
            public CompletionStage<DispatchResult> executeAsync(QueryStateMachine sm, String sql,
                                          QuerySession session) {
                sm.tryTransition(QueryState.PLANNING);
                sm.tryTransition(QueryState.FAILED);
                throw new NoAvailableWorkerException();
            }
        };
        QueryManager manager = new QueryManager(QueryManagerTestContexts.minimalAsync(
                unavailable, TestObservability.TRACING,
                AsyncSubmitConfig.defaultEnabled(), asyncExecutor));

        assertThatThrownBy(() -> manager.submitQuery(
                "no-worker", "SELECT 1", testSession("no-worker")))
                .isInstanceOf(NoAvailableWorkerException.class);
        assertThat(manager.getQueryState("no-worker"))
                .contains(QueryState.FAILED);
        assertThat(manager.getResultEndpoint("no-worker")).isEmpty();
    }

    @Test
    void realDispatcherFailureRetainsAdmissionUntilFailedStateIsPublished() {
        var dependencies = SlowQueryDispatcher.buildMinimalContext();
        try (var remote = (RemoteTaskDispatcher) dependencies.dispatcher()) {
            var dispatcher = new QueryDispatcher(dependencies, null, null);
            var manager = new QueryManager(QueryManagerTestContexts.minimalAsync(
                    dispatcher, TestObservability.TRACING,
                    AsyncSubmitConfig.defaultEnabled(), asyncExecutor));
            String queryId = "real-dispatch-failure";

            assertThatThrownBy(() -> manager.submitQuery(queryId, "THIS IS NOT VALID SQL !!!",
                    new QuerySession(queryId, java.util.Map.of(),
                            new org.pangrova.security.api.identity.UserIdentity("test-user", java.util.Set.of("public"),
                                    java.util.Map.of(), java.time.Instant.EPOCH), "default", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture())))
                    .isInstanceOf(org.pangrova.parser.api.exception.ParseException.class);
            assertThat(manager.getQueryState(queryId)).contains(QueryState.FAILED);
            assertThat(manager.getQueryError(queryId)).isPresent();
            org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(5)).untilAsserted(() -> {
                // The minimal fixture has no production cleanup scheduler; drive its actual retry tick.
                manager.retryPendingTerminalCompletions();
                assertThat(manager.getActiveQueryCount()).isZero();
            });
        }
    }

    @Test
    void failureAfterWriteReadinessStillPublishesFailedState() {
        var failDispatch = new CountDownLatch(1);
        var executingState = new AtomicReference<QueryStateMachine>();
        var compiler = new SqlCompiler() {
            @Override
            public org.pangrova.planner.analysis.PlanResult compileForPolicyApplication(String sql) {
                var state = executingState.get();
                state.tryTransition(QueryState.SCHEDULING);
                state.tryTransition(QueryState.RUNNING);
                state.markWriteDispatchReady();
                await(failDispatch);
                throw new org.pangrova.security.api.exception.AccessDeniedException("publication permission revoked");
            }
        };
        var dependencies = SlowQueryDispatcher.buildMinimalContext(compiler);
        try (var remote = (RemoteTaskDispatcher) dependencies.dispatcher()) {
            var dispatcher = new QueryDispatcher(dependencies, null, null) {
                @Override
                public CompletionStage<DispatchResult> executeAsync(QueryStateMachine sm, String sql, QuerySession session) {
                    executingState.set(sm);
                    return super.executeAsync(sm, sql, session);
                }
            };
            var manager = new QueryManager(QueryManagerTestContexts.minimalAsync(
                    dispatcher, TestObservability.TRACING,
                    AsyncSubmitConfig.defaultEnabled(), asyncExecutor));
            String queryId = "failure-after-readiness";
            var submitted = manager.submitQuery(queryId, "SELECT 1",
                    org.pangrova.coordinator.testinfra.SourceFreeQueryFixture.session(queryId));
            assertThat(submitted.state()).isEqualTo(org.pangrova.query.api.enums.QueryState.RUNNING);
            failDispatch.countDown();
            org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(manager.getQueryState(queryId)).contains(QueryState.FAILED);
                assertThat(manager.getQueryError(queryId).orElseThrow().errorCode()).isEqualTo("SECURITY-0002");
                assertThat(manager.getActiveQueryCount()).isZero();
            });
        } finally {
            failDispatch.countDown();
        }
    }

    @Test
    @DisplayName("terminal write failure wakes submit before endpoint timeout")
    void terminalFailurePreservesCommitErrorBeforeEndpointTimeout() throws Exception {
        CountDownLatch dispatched = new CountDownLatch(1);
        CountDownLatch blocked = new CountDownLatch(1);
        QueryDispatcher dispatcher = new SlowQueryDispatcher(dispatched, blocked) {
            @Override
            public CompletionStage<DispatchResult> executeAsync(QueryStateMachine sm, String sql,
                                          QuerySession session) {
                sm.tryTransition(QueryState.PLANNING);
                sm.tryTransition(QueryState.SCHEDULING);
                sm.tryTransition(QueryState.RUNNING);
                dispatched.countDown();
                await(blocked);
                throw new IllegalStateException("dispatch should be interrupted");
            }
        };
        QueryManager manager = new QueryManager(QueryManagerTestContexts.minimalAsync(
                dispatcher, TestObservability.TRACING,
                new AsyncSubmitConfig(true, 5_000), asyncExecutor));
        ExecutorService submitter = Executors.newSingleThreadExecutor();
        try {
            Future<QuerySubmissionResult> submitted = submitter.submit(() ->
                    manager.submitQuery("write-commit-failed", "INSERT INTO t VALUES (1)",
                            testSession("write-commit-failed")));
            assertThat(dispatched.await(2, TimeUnit.SECONDS)).isTrue();
            manager.onTaskFailed("write-commit-failed", new QueryFailure(
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

    private static boolean isTerminal(QueryState state) {
        return state == QueryState.FINISHED
                || state == QueryState.FAILED
                || state == QueryState.CANCELLED;
    }

    private static QuerySession testSession(String queryId) {
        return new QuerySession(queryId, null, null, null, java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());
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
        public CompletionStage<DispatchResult> executeAsync(QueryStateMachine sm, String sql,
                                      QuerySession session) {
            // Set the result endpoint BEFORE transitioning to RUNNING — the async
            // listener fires on RUNNING and immediately reads the endpoint.
            ResultEndpoint endpoint = new ResultEndpoint(
                    "localhost", 9090,
                    "ticket-" + session.queryId(), "arrow-flight");
            sm.setResultEndpoint(endpoint);

            // Simulate compile → schedule → dispatch transitions quickly
            sm.tryTransition(QueryState.PLANNING);
            sm.tryTransition(QueryState.SCHEDULING);
            sm.tryTransition(QueryState.RUNNING);

            // Signal that execution has started (RUNNING reached)
            executionStartedLatch.countDown();

            // Block here — simulating slow execution (DML commit, large scan, etc.)
            try {
                executionBlockLatch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // Complete the query
            sm.tryTransition(QueryState.FINISHING);
            sm.tryTransition(QueryState.FINISHED);

            return CompletableFuture.completedFuture(new DispatchResult("localhost",
                    "task-" + session.queryId(), endpoint));
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
        public boolean tryCleanupQuery(String queryId) {
            return true;
        }

        @Override
        public void cancelDispatchedTasks(String queryId) {
            // no-op in test
        }

        private static QueryDispatcherContext buildMinimalContext() {
            return buildMinimalContext(new SqlCompiler());
        }

        private static QueryDispatcherContext buildMinimalContext(SqlCompiler compiler) {
            TaskScheduler scheduler = new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty());
            return QueryDispatcherContext.withDefaults(
                    compiler, null, null, null,
                    new RemoteTaskDispatcher(
                            CircuitBreakerPolicy.noOp(),
                            TestObservability.TRACING),
                    scheduler,
                    new WorkerManager(20_000, 60_000,
                            new org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry()),
                    new CoordinatorTaskTracker(),
                    null, null, null, 0L);
        }
    }
}
