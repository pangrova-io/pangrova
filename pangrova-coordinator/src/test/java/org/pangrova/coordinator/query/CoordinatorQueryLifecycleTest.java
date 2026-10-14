package org.pangrova.coordinator.query;

import org.pangrova.coordinator.query.lifecycle.CoordinatorQueryLifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.testing.TestObservability;
import org.pangrova.spi.execution.QueryId;
import org.pangrova.spi.execution.QueryState;
import org.pangrova.spi.execution.SessionContext;
import org.pangrova.spi.observability.TracingProvider;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.testing.TestExchangeTransport;
import org.pangrova.shuffle.exchange.ExchangeTransport;
import org.pangrova.worker.http.WorkerHttpServer;
import org.pangrova.worker.task.TaskManager;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CoordinatorQueryLifecycle} SPI state mapping.
 * Uses real SqlCompiler + RemoteTaskDispatcher + WorkerHttpServer — no stubs.
 */
class CoordinatorQueryLifecycleTest {

    private static final int TRANSPORT_PORT = 0;
    private static final TracingProvider TRACING = TestObservability.TRACING;

    private QueryManager queryManager;
    private CoordinatorQueryLifecycle lifecycle;
    private WorkerHttpServer workerHttpServer;

    @BeforeEach
    void setUp() throws Exception {
        TaskManager taskManager = TaskManager.forHttpWiringOnly();
        var config = new WorkerHttpServer.WorkerHttpConfig(0, "localhost", TRANSPORT_PORT);
        workerHttpServer = new WorkerHttpServer(config, taskManager, TestExchangeTransport.INSTANCE);
        workerHttpServer.start();

        WorkerManager workerManager = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        workerManager.registerWorker(activeWorker("w1", workerHttpServer.getPort()));

        var deps = QueryDispatcherContext.withDefaults(
                new SqlCompiler(), null, null, null,
                new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TRACING),
                new TaskScheduler(new WorkerPerformanceScorer()),
                workerManager,
                new CoordinatorTaskTracker(),
                null, null, null, 0L);
        QueryDispatcher orchestrator = new QueryDispatcher(
                deps, null, null);

        var dagEngine = orchestrator.dagEngine();
        taskManager.setAutoFinishReporter(report ->
                dagEngine.onTaskTerminal(report.queryId(), report.taskId(),
                        report.state(), report.error()));

        queryManager = new QueryManager(QueryManagerContext.minimal(orchestrator, TRACING));
        lifecycle = new CoordinatorQueryLifecycle(queryManager);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (workerHttpServer != null) {
            workerHttpServer.stop();
        }
    }

    @Test
    void createQueryRejectsUntrustedSpiSession() {
        assertThatThrownBy(() -> lifecycle.createQuery(testSession(Map.of()), "SELECT 1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("authenticated tenant and principal");
    }

    @Test
    void createQueryFailureDoesNotLeaveActiveQuery() {
        assertThatThrownBy(() -> lifecycle.createQuery(testSession(Map.of()), "SELECT 1"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(queryManager.getActiveQueryIds()).isEmpty();
        assertThat(queryManager.getActiveQueryCount()).isZero();
    }

    @Test
    void tenantPropertyCannotAuthenticateSpiSession() {
        // The SPI has no trusted identity carrier; caller-controlled properties
        // cannot become a principal or tenant for Worker task dispatch.
        assertThatThrownBy(() -> lifecycle.createQuery(
                testSession(Map.of("tenant_id", "spoofed-tenant", "user", "spoofed-user")),
                "SELECT 1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("authenticated tenant and principal");
    }

    @Test
    void getQueryStateMapsFinishing() {
        QueryId id = createAndForceToRunning();
        forceTransition(id, QueryStateMachine.State.FINISHING);
        assertThat(lifecycle.getQueryState(id)).isEqualTo(QueryState.FINISHING);
    }

    @Test
    void getQueryStateMapsFinished() {
        QueryId id = createAndForceToRunning();
        forceTransition(id, QueryStateMachine.State.FINISHING);
        forceTransition(id, QueryStateMachine.State.FINISHED);
        assertThat(lifecycle.getQueryState(id)).isEqualTo(QueryState.FINISHED);
    }

    @Test
    void getQueryStateMapsFailed() {
        QueryId id = createAndForceToRunning();
        forceTransition(id, QueryStateMachine.State.FAILED);
        assertThat(lifecycle.getQueryState(id)).isEqualTo(QueryState.FAILED);
    }

    @Test
    void getQueryStatePreservesCancelled() {
        QueryId id = createAndForceToRunning();
        forceTransition(id, QueryStateMachine.State.CANCELLED);
        assertThat(lifecycle.getQueryState(id)).isEqualTo(QueryState.CANCELLED);
    }

    @Test
    void getQueryStateReturnsFailedForUnknownId() {
        QueryId unknown = new QueryId("nonexistent");
        assertThat(lifecycle.getQueryState(unknown)).isEqualTo(QueryState.FAILED);
    }

    @Test
    void cancelQueryTransitionsToCancelled() {
        QueryId id = createAndForceToRunning();
        lifecycle.cancelQuery(id);
        assertThat(queryManager.getQueryState(id.id()))
                .hasValue(QueryStateMachine.State.CANCELLED);
    }

    @Test
    void allStateMachineStatesMappedToSpiState() {
        for (QueryStateMachine.State state : QueryStateMachine.State.values()) {
            QueryId id = createAndForceToRunning();
            forceState(id, state);
            QueryState spiState = lifecycle.getQueryState(id);
            assertThat(spiState).isNotNull();
        }
    }

    private void forceTransition(QueryId id, QueryStateMachine.State target) {
        QueryStateMachine sm = queryManager.getStateMachine(id.id());
        sm.tryTransition(target);
    }

    /**
     * Creates a query registered in queryManager with state machine forced to RUNNING.
     * Used by tests that need a non-terminal starting state for transition testing.
     */
    private QueryId createAndForceToRunning() {
        String id = UUID.randomUUID().toString();
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryStateMachine.State.PLANNING);
        sm.tryTransition(QueryStateMachine.State.SCHEDULING);
        sm.tryTransition(QueryStateMachine.State.RUNNING);
        queryManager.registerStateMachine(id, sm);
        return new QueryId(id);
    }

    private void forceState(QueryId id, QueryStateMachine.State target) {
        QueryStateMachine sm = queryManager.getStateMachine(id.id());
        for (QueryStateMachine.State step : pathFromRunning(target)) {
            sm.tryTransition(step);
        }
    }

    private static QueryStateMachine.State[] pathFromRunning(QueryStateMachine.State target) {
        return switch (target) {
            case QUEUED, PLANNING, SCHEDULING, RUNNING ->
                    new QueryStateMachine.State[]{};
            case FINISHING -> new QueryStateMachine.State[]{
                    QueryStateMachine.State.FINISHING};
            case FINISHED -> new QueryStateMachine.State[]{
                    QueryStateMachine.State.FINISHING, QueryStateMachine.State.FINISHED};
            case FAILED -> new QueryStateMachine.State[]{QueryStateMachine.State.FAILED};
            case CANCELLED -> new QueryStateMachine.State[]{QueryStateMachine.State.CANCELLED};
        };
    }

    private static SessionContext testSession(Map<String, String> properties) {
        return new SessionContext() {
            @Override
            public String getUser() { return "test-user"; }

            @Override
            public Optional<String> getCatalog() { return Optional.empty(); }

            @Override
            public Optional<String> getSchema() { return Optional.empty(); }

            @Override
            public Map<String, String> getProperties() { return properties; }
        };
    }

    private static WorkerInfo activeWorker(String workerId, int httpPort) {
        return TestWorkerInfoBuilder.active(workerId, "localhost", httpPort, TRANSPORT_PORT);
    }
}
