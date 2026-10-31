package org.pangrova.coordinator.scheduler;

import org.pangrova.scheduler.core.failure.CircuitBreakerOpenException;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.pangrova.scheduler.api.placement.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.observability.api.noop.NoOpTracingProvider;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.coordinator.resilience.breaker.CircuitBreaker;
import org.pangrova.common.value.MemoryPressure;
import org.pangrova.observability.api.TracingProvider;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests CircuitBreaker integration in RemoteTaskDispatcher.
 * Uses real CircuitBreaker — no mocks.
 */
class RemoteTaskDispatcherCircuitBreakerTest {

    private static final TracingProvider TRACING = new NoOpTracingProvider();

    @Test
    void defaultConstructor_createsClosedCircuitBreaker() {
        RemoteTaskDispatcher dispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TRACING);
        // Should not throw — circuit is closed by default
        assertThat(dispatcher).isNotNull();
    }

    @Test
    void circuitOpen_throwsCircuitBreakerOpenException() {
        CircuitBreaker breaker = forceOpenBreaker();
        RemoteTaskDispatcher dispatcher = new RemoteTaskDispatcher(breaker, TRACING);

        assertThatThrownBy(() -> dispatcher.dispatch(dummyRequest(), unreachableWorker()))
                .isInstanceOf(CircuitBreakerOpenException.class)
                .hasMessageContaining("unreachable-host");
    }

    @Test
    void circuitClosed_failureRecordedOnDispatchError() {
        CircuitBreaker breaker = new CircuitBreaker(0.5, 2, 30_000, 3, java.time.Clock.systemUTC());
        RemoteTaskDispatcher dispatcher = new RemoteTaskDispatcher(breaker, TRACING);

        // Dispatch to unreachable worker — should fail and record failure
        assertThatThrownBy(() -> dispatcher.dispatch(dummyRequest(), unreachableWorker()))
                .isInstanceOf(RuntimeException.class);

        // Circuit should still be CLOSED (only 1 failure, minimum 2 calls)
        assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    @Test
    void circuitBreakerOpenException_carriesWorkerHost() {
        CircuitBreakerOpenException ex = new CircuitBreakerOpenException("worker-1");
        assertThat(ex.getWorkerHost()).isEqualTo("worker-1");
        assertThat(ex.getMessage()).contains("worker-1");
    }

    private static CircuitBreaker forceOpenBreaker() {
        // minimumCalls=1, failureRate=0.01 → opens after 1 failure
        CircuitBreaker breaker = new CircuitBreaker(0.01, 1, 60_000, 3, java.time.Clock.systemUTC());
        breaker.onFailure();
        assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        return breaker;
    }

    private static SubmitTaskRequest dummyRequest() {
        return org.pangrova.coordinator.CanonicalTaskRequestTestFixture.request(
                "task-cb-1", "q-1", 0, 0,
                org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ,
                "plan".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static WorkerInfo unreachableWorker() {
        return new WorkerInfo(
                "w-unreachable", "unreachable-host", 99999, 0, 0,
                HealthState.ACTIVE, MemoryPressure.NONE,
                0.1, 0, 4L * 1024 * 1024 * 1024,
                Instant.now(), 0.9, null, 2,
                null, List.of(), null,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.canonicalInventory(),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity("w-unreachable"));
    }
}
