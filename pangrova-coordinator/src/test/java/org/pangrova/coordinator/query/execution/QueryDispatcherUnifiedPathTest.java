package org.pangrova.coordinator.query.execution;

import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.testinfra.SourceFreeNativeWorkerFixture;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.spi.security.UserIdentity;
import org.pangrova.testing.TestObservability;
import org.pangrova.worker.http.WorkerHttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pangrova.coordinator.testinfra.SourceFreeQueryFixture.withPolicies;

/**
 * Verifies the unified execution path: every relational query is dispatched to
 * a Worker, including constant and zero-row plans.
 * Executes real source-free Native tasks, consumes their results, and accepts canonical terminal reports.
 */
@org.junit.jupiter.api.Tag("native-required")
class QueryDispatcherUnifiedPathTest {

    private static final UserIdentity TEST_IDENTITY = new UserIdentity(
            "test-user", Set.of("public"), Map.of(), Instant.EPOCH);
    private WorkerHttpServer workerHttpServer;
    private QueryDispatcher dispatcher;
    private RemoteTaskDispatcher httpDispatcher;
    private SourceFreeNativeWorkerFixture worker;
    @TempDir java.nio.file.Path nativeDirectory;

    @BeforeEach
    void setUp() throws Exception {
        // Real worker HTTP server for distributed dispatch
        worker = new SourceFreeNativeWorkerFixture("w1", nativeDirectory);
        workerHttpServer = worker.httpServer();

        WorkerManager workerManager = new WorkerManager(20_000, 60_000, new org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry());
        workerManager.registerWorker(worker.workerInfo());

        httpDispatcher =
                new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING);
        httpDispatcher.setControlEpochSupplier(() -> 1L);
        httpDispatcher.setExecutionOwnerResolver(ignored ->
                new org.pangrova.protocol.rest.task.ExecutionOwnerIdentity(
                        "test-coordinator", "test-incarnation", 1L));
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer());

        var tracker = new CoordinatorTaskTracker();
        var deps = QueryDispatcherContext.withDefaults(
                new SqlCompiler(), null, null, null, httpDispatcher, scheduler, workerManager,
                tracker, null, null, null, 30L);
        dispatcher = new QueryDispatcher(withPolicies(deps), null, null);
        worker.connect(dispatcher, tracker, workerManager);
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            if (worker != null) worker.close();
        } finally {
            if (httpDispatcher != null) httpDispatcher.close();
        }
    }

    // ── Constants and zero-row plans → Worker/Native dispatch ──

    @Test
    void constantQueryReturnsWorkerEndpoint() {
        QueryStateMachine sm = new QueryStateMachine();
        CoordinatorSessionContext session = new CoordinatorSessionContext("const-1", Map.of(), TEST_IDENTITY, "default");

        DispatchResult result = worker.execute(dispatcher, sm, "SELECT 1", session);

        assertThat(result).isNotNull();
        assertThat(result.workerHost()).isEqualTo("localhost");
        assertThat(result.resultEndpoint().host()).isEqualTo("localhost");
        assertThat(result.resultEndpoint().protocol()).isEqualTo("grpc");
        assertThat(result.taskId()).doesNotStartWith("local-");
        assertThat(sm.getState()).isEqualTo(QueryStateMachine.State.RUNNING);
        assertOneValue(result, 1L);
    }

    @Test
    void multiConstantExpressionUsesWorker() {
        QueryStateMachine sm = new QueryStateMachine();
        CoordinatorSessionContext session = new CoordinatorSessionContext("multi-const", Map.of(), TEST_IDENTITY, "default");

        DispatchResult result = worker.execute(dispatcher, sm, "SELECT 1+2, 'hello'", session);

        assertThat(result).isNotNull();
        assertThat(result.workerHost()).isEqualTo("localhost");
        assertThat(sm.getState()).isEqualTo(QueryStateMachine.State.RUNNING);
        assertThat(worker.rows(result)).singleElement().satisfies(row -> {
            assertThat(row).hasSize(2);
            assertThat(((Number) row.get(0)).longValue()).isEqualTo(3L);
            assertThat(row.get(1).toString()).isEqualTo("hello");
        });
    }

    @Test
    void limitZeroUsesWorker() {
        QueryStateMachine sm = new QueryStateMachine();
        CoordinatorSessionContext session = new CoordinatorSessionContext(
                "limit-zero", Map.of(), TEST_IDENTITY, "default");

        DispatchResult result = worker.execute(dispatcher, sm, "SELECT 1 LIMIT 0", session);

        assertThat(result).isNotNull();
        assertThat(result.workerHost()).isEqualTo("localhost");
        assertThat(result.taskId()).doesNotStartWith("local-");
        assertThat(worker.rows(result)).isEmpty();
    }

    // ── DispatchResult contract ──

    @Test
    void allPathsReturnNonNullResultEndpoint() {
        QueryStateMachine sm = new QueryStateMachine();
        CoordinatorSessionContext session = new CoordinatorSessionContext("endpoint-q", Map.of(), TEST_IDENTITY, "default");

        DispatchResult result = worker.execute(dispatcher, sm, "SELECT 42", session);

        assertThat(result.resultEndpoint()).isNotNull();
        assertThat(result.resultEndpoint().protocol()).isEqualTo("grpc");
        assertOneValue(result, 42L);
    }

    @Test
    void resultEndpointTicketMatchesWorkerTaskId() {
        String queryId = "ticket-match";
        QueryStateMachine sm = new QueryStateMachine();
        CoordinatorSessionContext session = new CoordinatorSessionContext(queryId, Map.of(), TEST_IDENTITY, "default");

        DispatchResult result = worker.execute(dispatcher, sm, "SELECT 1", session);

        assertThat(result.resultEndpoint().ticket()).isEqualTo(result.taskId());
        assertOneValue(result, 1L);
    }

    private void assertOneValue(DispatchResult result, long expected) {
        assertThat(worker.rows(result)).singleElement().satisfies(row -> {
            assertThat(row).hasSize(1);
            assertThat(((Number) row.get(0)).longValue()).isEqualTo(expected);
        });
    }

}
