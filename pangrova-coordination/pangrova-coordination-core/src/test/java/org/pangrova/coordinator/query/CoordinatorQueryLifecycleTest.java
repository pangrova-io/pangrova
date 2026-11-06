package org.pangrova.coordinator.query;

import org.pangrova.query.core.lifecycle.QueryStateMachine;

import org.pangrova.coordinator.testinfra.QueryManagerTestContexts;

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
import org.pangrova.scheduler.core.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.builtin.placement.LoadBalancedStrategy;
import org.pangrova.testing.testkit.TestObservability;
import org.pangrova.query.api.identity.QueryId;
import org.pangrova.query.api.lifecycle.QueryState;
import org.pangrova.common.session.SessionContext;
import org.pangrova.observability.spi.tracing.TracingProvider;
import org.pangrova.planner.core.analysis.SqlCompiler;
import org.pangrova.testing.testkit.TestExchangeTransport;
import org.pangrova.worker.http.WorkerHttpServer;
import org.pangrova.worker.task.TaskManager;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;
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
                new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty()),
                workerManager,
                new CoordinatorTaskTracker(),
                null, null, null, 0L);
        QueryDispatcher orchestrator = new QueryDispatcher(
                deps, null, null);

        var dagEngine = orchestrator.dagEngine();
        taskManager.setAutoFinishReporter(report ->
                dagEngine.onTaskTerminal(report.queryId(), report.taskId(),
                        report.state(), org.pangrova.coordinator.query.task.TaskReportQueryAdapter.failure(report)));

        queryManager = new QueryManager(QueryManagerTestContexts.minimal(orchestrator, TRACING));
        lifecycle = new CoordinatorQueryLifecycle(queryManager, new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory());
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
        forceTransition(id, QueryState.FINISHING);
        assertThat(lifecycle.getQueryState(id)).isEqualTo(QueryState.FINISHING);
    }

    @Test
    void getQueryStateMapsFinished() {
        QueryId id = createAndForceToRunning();
        forceTransition(id, QueryState.FINISHING);
        forceTransition(id, QueryState.FINISHED);
        assertThat(lifecycle.getQueryState(id)).isEqualTo(QueryState.FINISHED);
    }

    @Test
    void getQueryStateMapsFailed() {
        QueryId id = createAndForceToRunning();
        forceTransition(id, QueryState.FAILED);
        assertThat(lifecycle.getQueryState(id)).isEqualTo(QueryState.FAILED);
    }

    @Test
    void getQueryStatePreservesCancelled() {
        QueryId id = createAndForceToRunning();
        forceTransition(id, QueryState.CANCELLED);
        assertThat(lifecycle.getQueryState(id)).isEqualTo(QueryState.CANCELLED);
    }

    @Test
    void getQueryStateDistinguishesUnknownFromFailed() {
        QueryId unknown = new QueryId("nonexistent");
        assertThatThrownBy(() -> lifecycle.getQueryState(unknown))
                .isInstanceOf(java.util.NoSuchElementException.class)
                .hasMessageContaining("nonexistent");
    }

    @Test
    void cancelQueryTransitionsToCancelled() {
        QueryId id = createAndForceToRunning();
        lifecycle.cancelQuery(id);
        assertThat(queryManager.getQueryState(id.id()))
                .hasValue(QueryState.CANCELLED);
    }

    @Test
    void allAuthoritativeStatesAreReturnedWithoutLoss() {
        for (QueryState state : QueryState.values()) {
            var sm = new QueryStateMachine();
            String id = UUID.randomUUID().toString();
            queryManager.registerStateMachine(id, sm);
            if (state == QueryState.FAILED || state == QueryState.CANCELLED) {
                sm.tryTransition(state);
            } else {
                for (QueryState step : new QueryState[]{QueryState.PLANNING, QueryState.SCHEDULING,
                        QueryState.RUNNING, QueryState.FINISHING, QueryState.FINISHED}) {
                    if (sm.getState() == state) break;
                    assertThat(sm.tryTransition(step)).isTrue();
                }
            }
            assertThat(lifecycle.getQueryState(new QueryId(id))).isSameAs(state);
        }
    }

    private void forceTransition(QueryId id, QueryState target) {
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
        sm.tryTransition(QueryState.PLANNING);
        sm.tryTransition(QueryState.SCHEDULING);
        sm.tryTransition(QueryState.RUNNING);
        queryManager.registerStateMachine(id, sm);
        return new QueryId(id);
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
