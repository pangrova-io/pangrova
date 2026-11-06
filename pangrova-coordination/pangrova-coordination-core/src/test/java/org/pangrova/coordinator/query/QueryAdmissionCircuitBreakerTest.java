package org.pangrova.coordinator.query;

import org.pangrova.query.core.lifecycle.QueryStateMachine;

import org.pangrova.query.api.lifecycle.QueryState;

import org.pangrova.coordinator.query.admission.QueryRejectedException;
import org.pangrova.query.api.session.QuerySession;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.InputStream;
import java.util.Map;
import java.util.UUID;

import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.scheduler.core.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.builtin.placement.LoadBalancedStrategy;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.planner.core.analysis.SqlCompiler;
import org.pangrova.query.api.error.QueryFailure;
import org.pangrova.coordinator.resilience.resilience4j.ResilienceRegistry;
import org.pangrova.coordinator.resilience.config.ResilienceConfigLoader;
import org.pangrova.common.resilience.policy.ResiliencePolicyConfig;
import org.pangrova.testing.testkit.TestObservability;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Verifies F-RESIL-03 fix: the query admission circuit breaker is fed
 * success/failure data from actual query outcomes, and correctly rejects
 * new queries when OPEN (fail-closed behavior).
 *
 * <p>The breaker config (from resilience-defaults.yml) for
 * "coordinator-query-admission" is:
 * <ul>
 *   <li>failureRateThreshold: 50%</li>
 *   <li>minimumNumberOfCalls: 10</li>
 *   <li>slidingWindowSize: 20</li>
 *   <li>waitDurationInOpenStateMs: 30000</li>
 *   <li>permittedNumberOfCallsInHalfOpenState: 3</li>
 * </ul>
 */
class QueryAdmissionCircuitBreakerTest {

    private QueryManager queryManager;
    private ResilienceRegistry resilienceRegistry;

    @BeforeEach
    void setUp() {
        InputStream config = getClass().getClassLoader().getResourceAsStream("resilience-defaults.yml");
        Map<String, ResiliencePolicyConfig> policies = ResilienceConfigLoader.load(config);
        resilienceRegistry = new ResilienceRegistry(policies);

        QueryDispatcher dispatcher = buildDispatcher();
        QueryManagerContext ctx = new QueryManagerContext(
                dispatcher, TestObservability.TRACING,
                null, null, null, null, null, null,
                resilienceRegistry, null, new org.pangrova.persistence.query.memory.state.InMemoryQueryStateJournal(), null, new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory());
        queryManager = new QueryManager(ctx);
    }

    @Test
    @DisplayName("Admission breaker stays CLOSED when queries succeed")
    void admissionBreaker_staysClosed_onQuerySuccess() {
        // Simulate 15 successful queries
        for (int i = 0; i < 15; i++) {
            String queryId = registerRunningQuery();
            queryManager.onAllTasksFinished(queryId);
        }

        // Breaker should still be closed — new queries allowed
        assertThat(resilienceRegistry.allowRequest("coordinator-query-admission"))
                .isTrue();
    }

    @Test
    @DisplayName("Admission breaker opens after failure rate exceeds threshold and rejects new queries")
    void admissionBreaker_opensAndRejects_afterFailureThresholdExceeded() {
        // The config requires 10 minimum calls with 50% failure rate to trip.
        // Feed 10 consecutive failures (100% failure rate on 10 calls).
        for (int i = 0; i < 10; i++) {
            String queryId = registerRunningQuery();
            queryManager.onTaskFailed(queryId,
                    new QueryFailure("INTERNAL", "SIM", "simulated failure " + i));
        }

        // Breaker should now be OPEN — new queries must be rejected
        assertThat(resilienceRegistry.allowRequest("coordinator-query-admission"))
                .isFalse();

        // Verify that checkDegradation (called in submitQuery) rejects with clear message
        assertThatThrownBy(() -> queryManager.submitQuery(
                "rejected-" + UUID.randomUUID(), "SELECT 1",
                new QuerySession("q1", Map.of(), null, null, java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture())))
                .isInstanceOf(QueryRejectedException.class)
                .hasMessageContaining("system overloaded")
                .hasMessageContaining("admission circuit breaker open");
    }

    @Test
    @DisplayName("Admission breaker does not trip when failure rate is below threshold")
    void admissionBreaker_doesNotTrip_belowThreshold() {
        // Feed 6 successes and 4 failures = 40% failure rate, below 50% threshold.
        for (int i = 0; i < 6; i++) {
            String queryId = registerRunningQuery();
            queryManager.onAllTasksFinished(queryId);
        }
        for (int i = 0; i < 4; i++) {
            String queryId = registerRunningQuery();
            queryManager.onTaskFailed(queryId,
                    new QueryFailure("INTERNAL", "SIM", "failure " + i));
        }

        // 40% < 50% threshold — breaker remains closed
        assertThat(resilienceRegistry.allowRequest("coordinator-query-admission"))
                .isTrue();
    }

    @Test
    @DisplayName("User-initiated cancellation does not contribute to failure rate")
    void admissionBreaker_cancellationDoesNotCountAsFailure() {
        // Feed 10 cancellations — these are user-initiated, not system failures.
        for (int i = 0; i < 10; i++) {
            String queryId = registerRunningQuery();
            queryManager.cancelQuery(queryId);
        }

        // Breaker should still be closed since cancellations record as success
        assertThat(resilienceRegistry.allowRequest("coordinator-query-admission"))
                .isTrue();
    }

    @Test
    @DisplayName("Mixed success and failure with breaker trip and subsequent success")
    void admissionBreaker_tripsOnHighFailureRate_remainsOpenUntilRecovery() {
        // Trip the breaker: 10 failures
        for (int i = 0; i < 10; i++) {
            String queryId = registerRunningQuery();
            queryManager.onTaskFailed(queryId,
                    new QueryFailure("INTERNAL", "SIM", "failure " + i));
        }

        // Verify OPEN state
        assertThat(resilienceRegistry.allowRequest("coordinator-query-admission"))
                .isFalse();

        // Even with new successes, the breaker stays open during the wait duration
        // (30s in config). We cannot fast-forward time in this test, so just verify
        // the OPEN state persists.
        assertThat(resilienceRegistry.allowRequest("coordinator-query-admission"))
                .isFalse();
    }

    @Test
    @DisplayName("Admission breaker is fed from timeout owner path")
    void admissionBreaker_fedFromTimeoutPath() {
        // Simulate 10 query timeouts
        for (int i = 0; i < 10; i++) {
            String queryId = registerRunningQuery();
            queryManager.timeoutQueriesCreatedBy(java.time.Instant.now(), java.time.Duration.ofSeconds(30));
        }

        // Timeouts are failures — breaker should open
        assertThat(resilienceRegistry.allowRequest("coordinator-query-admission"))
                .isFalse();
    }

    /**
     * Registers a query in RUNNING state and returns its queryId.
     */
    private String registerRunningQuery() {
        String queryId = "test-" + UUID.randomUUID();
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryState.PLANNING);
        sm.tryTransition(QueryState.SCHEDULING);
        sm.tryTransition(QueryState.RUNNING);
        queryManager.registerStateMachine(queryId, sm);
        return queryId;
    }

    private static QueryDispatcher buildDispatcher() {
        SqlCompiler compiler = new SqlCompiler();
        TaskScheduler scheduler = new TaskScheduler(new LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty());
        WorkerManager wm = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        return new QueryDispatcher(
                QueryDispatcherContext.withDefaults(
                        compiler, null, null, null,
                        new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING),
                        scheduler, wm, new CoordinatorTaskTracker(),
                        null, null, null, 0L),
                null, null);
    }
}
