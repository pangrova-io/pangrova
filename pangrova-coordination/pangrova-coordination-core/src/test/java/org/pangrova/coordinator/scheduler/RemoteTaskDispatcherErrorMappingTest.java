package org.pangrova.coordinator.scheduler;

import org.pangrova.scheduler.core.failure.CircuitBreakerOpenException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.http.HttpClient;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.observability.api.TracingProvider;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Verifies that RemoteTaskDispatcher maps all errors to unified PangrovaErrorCode
 * at the coordinator-worker module boundary.
 *
 * Validates: Requirements 5.8
 */
@DisplayName("RemoteTaskDispatcher Error Mapping")
class RemoteTaskDispatcherErrorMappingTest {

    @Test
    @DisplayName("Circuit breaker open throws PangrovaException with COORDINATOR_CIRCUIT_BREAKER_OPEN")
    void circuitBreakerOpenThrowsPangrovaException() {
        CircuitBreakerPolicy alwaysOpen = new AlwaysOpenCircuitBreaker();
        TracingProvider noopTracing = noopTracingProvider();
        RemoteTaskDispatcher dispatcher = new RemoteTaskDispatcher(
                HttpClient.newHttpClient(), alwaysOpen, noopTracing);

        WorkerInfo worker = TestWorkerInfoBuilder.active("w1", "host1", 8080, 9090);
        SubmitTaskRequest request = buildRequest();

        assertThatThrownBy(() -> dispatcher.dispatch(request, worker))
                .isInstanceOf(CircuitBreakerOpenException.class)
                .isInstanceOf(PangrovaException.class)
                .hasFieldOrPropertyWithValue("errorCode", PangrovaErrorCode.COORDINATOR_CIRCUIT_BREAKER_OPEN);
    }

    @Test
    @DisplayName("Dispatch to unreachable worker throws PangrovaException with COORDINATOR_DISPATCH_FAILED")
    void unreachableWorkerThrowsPangrovaException() {
        CircuitBreakerPolicy allowAll = new AllowAllCircuitBreaker();
        TracingProvider noopTracing = noopTracingProvider();
        RemoteTaskDispatcher dispatcher = new RemoteTaskDispatcher(
                HttpClient.newHttpClient(), allowAll, noopTracing);

        WorkerInfo worker = TestWorkerInfoBuilder.active(
                "w1", "unreachable-host-99999.invalid", 1, 1);
        SubmitTaskRequest request = buildRequest();

        assertThatThrownBy(() -> dispatcher.dispatch(request, worker))
                .isInstanceOf(PangrovaException.class)
                .hasFieldOrPropertyWithValue("errorCode", PangrovaErrorCode.COORDINATOR_DISPATCH_FAILED);
    }

    @Test
    @DisplayName("CircuitBreakerOpenException extends PangrovaException")
    void circuitBreakerOpenExceptionExtendsPangrovaException() {
        CircuitBreakerOpenException ex = new CircuitBreakerOpenException("worker-host");
        assertThat(ex).isInstanceOf(PangrovaException.class);
        assertThat(ex.getErrorCode()).isEqualTo(PangrovaErrorCode.COORDINATOR_CIRCUIT_BREAKER_OPEN);
        assertThat(ex.getWorkerHost()).isEqualTo("worker-host");
    }

    private static SubmitTaskRequest buildRequest() {
        return org.pangrova.coordinator.CanonicalTaskRequestTestFixture.request(
                "task-1", "query-1", 0, 0,
                org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ,
                "plan".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static TracingProvider noopTracingProvider() {
        return new org.pangrova.observability.api.noop.NoOpTracingProvider();
    }

    /** Circuit breaker that always rejects (OPEN state). */
    private static final class AlwaysOpenCircuitBreaker implements CircuitBreakerPolicy {
        @Override
        public boolean allowRequest(String target) {
            return false;
        }

        @Override
        public void recordSuccess(String target) {}

        @Override
        public void recordFailure(String target, Throwable error) {}
    }

    /** Circuit breaker that always allows (CLOSED state). */
    private static final class AllowAllCircuitBreaker implements CircuitBreakerPolicy {
        @Override
        public boolean allowRequest(String target) {
            return true;
        }

        @Override
        public void recordSuccess(String target) {}

        @Override
        public void recordFailure(String target, Throwable error) {}
    }
}
