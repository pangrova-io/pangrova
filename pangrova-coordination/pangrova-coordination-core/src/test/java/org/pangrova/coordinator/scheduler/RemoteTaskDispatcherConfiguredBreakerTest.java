package org.pangrova.coordinator.scheduler;

import org.pangrova.scheduler.core.failure.CircuitBreakerOpenException;
import org.pangrova.scheduler.core.failure.TaskDispatchException;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.scheduler.api.placement.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.observability.api.noop.NoOpTracingProvider;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.coordinator.resilience.provider.resilience4j.ResilienceRegistry;
import org.pangrova.coordinator.resilience.config.ResilienceConfigLoader;
import org.pangrova.common.resilience.policy.ResiliencePolicyConfig;
import org.pangrova.common.value.MemoryPressure;
import org.pangrova.observability.api.TracingProvider;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for F-RESIL-01: the per-service circuit-breaker configuration
 * ({@code coordinator-to-worker} template in resilience-defaults.yml) must actually
 * govern the breaker that the dispatcher drives for a given worker, instead of
 * silently falling back to Resilience4j defaults.
 *
 * <p>The {@code coordinator-to-worker} template sets minimumNumberOfCalls=5 and
 * failureRateThreshold=60, so five consecutive failures must OPEN the circuit.
 * The Resilience4j default (minimumNumberOfCalls=100) would NOT open after five.</p>
 */
class RemoteTaskDispatcherConfiguredBreakerTest {

    private static final TracingProvider TRACING = new NoOpTracingProvider();

    @Test
    void configuredCoordinatorToWorkerThreshold_opensBreakerAfterFiveFailures() {
        Map<String, ResiliencePolicyConfig> policies = loadDefaults();
        assertThat(policies).containsKey("coordinator-to-worker");
        ResilienceRegistry registry = new ResilienceRegistry(policies);
        RemoteTaskDispatcher dispatcher = new RemoteTaskDispatcher(registry, TRACING);

        WorkerInfo worker = closedPortWorker();

        // First five dispatches fail at the network layer (connect refused) and must
        // be recorded as breaker failures. With the configured threshold (min 5, 60%),
        // the circuit OPENS once five failures accumulate.
        for (int i = 0; i < 5; i++) {
            assertThatThrownBy(() -> dispatcher.dispatch(dummyRequest(), worker))
                    .isInstanceOf(TaskDispatchException.class);
        }

        // The sixth dispatch must be short-circuited by the now-OPEN breaker.
        assertThatThrownBy(() -> dispatcher.dispatch(dummyRequest(), worker))
                .isInstanceOf(CircuitBreakerOpenException.class);
    }

    @Test
    void defaultThreshold_doesNotOpenAfterFiveFailures() {
        // Empty config => no coordinator-to-worker template => Resilience4j defaults
        // (minimumNumberOfCalls=100) => five failures must NOT open the circuit.
        ResilienceRegistry registry = new ResilienceRegistry(Map.of());
        RemoteTaskDispatcher dispatcher = new RemoteTaskDispatcher(registry, TRACING);

        WorkerInfo worker = closedPortWorker();

        for (int i = 0; i < 6; i++) {
            assertThatThrownBy(() -> dispatcher.dispatch(dummyRequest(), worker))
                    .as("with default config the breaker stays closed; all calls reach the network")
                    .isInstanceOf(TaskDispatchException.class)
                    .isNotInstanceOf(CircuitBreakerOpenException.class);
        }
    }

    // ── Helpers ──

    private static Map<String, ResiliencePolicyConfig> loadDefaults() {
        try (var in = CircuitBreakerPolicy.class.getClassLoader()
                .getResourceAsStream("resilience-defaults.yml")) {
            return ResilienceConfigLoader.load(in);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static WorkerInfo closedPortWorker() {
        return new WorkerInfo(
                "w-cfg", "127.0.0.1", closedPort(), 0, 0,
                HealthState.ACTIVE, MemoryPressure.NONE,
                0.1, 0, 4L * 1024 * 1024 * 1024,
                Instant.now(), 0.9, null, 2,
                null, List.of(), null,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.canonicalInventory(),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity("w-cfg"));
    }

    private static int closedPort() {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static SubmitTaskRequest dummyRequest() {
        return org.pangrova.coordinator.CanonicalTaskRequestTestFixture.request(
                "task-cfg-1", "q-cfg", 0, 0,
                org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ,
                "plan".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
}
