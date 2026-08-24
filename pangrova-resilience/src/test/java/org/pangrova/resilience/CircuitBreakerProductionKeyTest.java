package org.pangrova.resilience;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.resilience.config.ResilienceConfigLoader;
import org.pangrova.resilience.config.ResiliencePolicyConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for F-RESIL-07: verifies the production circuit breaker
 * ({@link ResilienceRegistry} backed by Resilience4j) works correctly with
 * real production keys — the per-host service instance keys used by
 * {@code RemoteTaskDispatcher} (e.g., {@code "coordinator-to-worker#10.0.0.5"}).
 *
 * <p>This test exercises the SAME code path that production uses: the
 * {@link CircuitBreakerPolicy} SPI implemented by {@link ResilienceRegistry},
 * with per-host target keys built via
 * {@link ResilienceRegistry#serviceInstance(String, String)}.</p>
 */
class CircuitBreakerProductionKeyTest {

    /** The logical service name used by RemoteTaskDispatcher in production. */
    private static final String WORKER_SERVICE = "coordinator-to-worker";

    private ResilienceRegistry registry;

    @BeforeEach
    void setUp() {
        InputStream config = getClass().getClassLoader().getResourceAsStream("resilience-defaults.yml");
        Map<String, ResiliencePolicyConfig> policies = ResilienceConfigLoader.load(config);
        registry = new ResilienceRegistry(policies);
    }

    @Nested
    @DisplayName("Per-host circuit breaker isolation")
    class PerHostIsolation {

        @Test
        @DisplayName("different hosts get independent breaker instances")
        void differentHostsGetIndependentBreakers() {
            String host1 = ResilienceRegistry.serviceInstance(WORKER_SERVICE, "10.0.0.1");
            String host2 = ResilienceRegistry.serviceInstance(WORKER_SERVICE, "10.0.0.2");

            CircuitBreaker cb1 = registry.getCircuitBreaker(host1);
            CircuitBreaker cb2 = registry.getCircuitBreaker(host2);

            assertThat(cb1).isNotSameAs(cb2);
            assertThat(cb1.getName()).isEqualTo(host1);
            assertThat(cb2.getName()).isEqualTo(host2);
        }

        @Test
        @DisplayName("tripping one host does not affect another")
        void trippingOneHostDoesNotAffectAnother() {
            String healthyHost = ResilienceRegistry.serviceInstance(WORKER_SERVICE, "10.0.0.1");
            String failingHost = ResilienceRegistry.serviceInstance(WORKER_SERVICE, "10.0.0.2");

            // Trip the failing host's breaker (coordinator-to-worker: minimumNumberOfCalls=5, failureRate=60%)
            for (int i = 0; i < 10; i++) {
                registry.recordFailure(failingHost, new RuntimeException("host unreachable"));
            }

            // Failing host should be open
            assertThat(registry.allowRequest(failingHost)).isFalse();
            // Healthy host should remain closed
            assertThat(registry.allowRequest(healthyHost)).isTrue();
        }

        @Test
        @DisplayName("same host returns same breaker instance (idempotent)")
        void sameHostReturnsSameBreakerInstance() {
            String host = ResilienceRegistry.serviceInstance(WORKER_SERVICE, "10.0.0.3");

            CircuitBreaker first = registry.getCircuitBreaker(host);
            CircuitBreaker second = registry.getCircuitBreaker(host);

            assertThat(first).isSameAs(second);
        }
    }

    @Nested
    @DisplayName("Configuration inheritance from service template")
    class ConfigInheritance {

        @Test
        @DisplayName("per-host breaker inherits failureRateThreshold from service template")
        void perHostBreakerInheritsFailureRateThreshold() {
            // coordinator-to-worker config: failureRateThreshold=60%, minimumNumberOfCalls=5
            String host = ResilienceRegistry.serviceInstance(WORKER_SERVICE, "10.0.0.4");
            CircuitBreaker cb = registry.getCircuitBreaker(host);

            assertThat(cb.getCircuitBreakerConfig().getFailureRateThreshold()).isEqualTo(60.0f);
            assertThat(cb.getCircuitBreakerConfig().getMinimumNumberOfCalls()).isEqualTo(5);
        }

        @Test
        @DisplayName("per-host breaker inherits slidingWindowSize from service template")
        void perHostBreakerInheritsSlidingWindowSize() {
            // coordinator-to-worker config: slidingWindowSize=50
            String host = ResilienceRegistry.serviceInstance(WORKER_SERVICE, "10.0.0.5");
            CircuitBreaker cb = registry.getCircuitBreaker(host);

            assertThat(cb.getCircuitBreakerConfig().getSlidingWindowSize()).isEqualTo(50);
        }

        @Test
        @DisplayName("per-host breaker inherits permittedNumberOfCallsInHalfOpenState")
        void perHostBreakerInheritsHalfOpenCalls() {
            // coordinator-to-worker config: permittedNumberOfCallsInHalfOpenState=5
            String host = ResilienceRegistry.serviceInstance(WORKER_SERVICE, "10.0.0.6");
            CircuitBreaker cb = registry.getCircuitBreaker(host);

            assertThat(cb.getCircuitBreakerConfig().getPermittedNumberOfCallsInHalfOpenState()).isEqualTo(5);
        }
    }

    @Nested
    @DisplayName("State transitions via SPI with production keys")
    class StateTransitionsViaSpi {

        @Test
        @DisplayName("CLOSED -> OPEN when failure rate exceeds threshold via SPI")
        void closedToOpenOnFailureRate() {
            String host = ResilienceRegistry.serviceInstance(WORKER_SERVICE, "10.0.0.10");

            // Initially closed
            assertThat(registry.allowRequest(host)).isTrue();

            // Record failures beyond threshold (60% of at least 5 calls)
            for (int i = 0; i < 5; i++) {
                registry.recordFailure(host, new RuntimeException("connection refused"));
            }

            // Should now be OPEN
            assertThat(registry.allowRequest(host)).isFalse();

            CircuitBreaker cb = registry.getCircuitBreaker(host);
            assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        }

        @Test
        @DisplayName("recordSuccess keeps breaker CLOSED under threshold")
        void recordSuccessKeepsClosed() {
            String host = ResilienceRegistry.serviceInstance(WORKER_SERVICE, "10.0.0.11");

            // Record many successes
            for (int i = 0; i < 20; i++) {
                registry.recordSuccess(host);
            }

            assertThat(registry.allowRequest(host)).isTrue();
            CircuitBreaker cb = registry.getCircuitBreaker(host);
            assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        }

        @Test
        @DisplayName("mixed successes and failures stay CLOSED when below threshold")
        void mixedResultsBelowThresholdStaysClosed() {
            String host = ResilienceRegistry.serviceInstance(WORKER_SERVICE, "10.0.0.12");

            // 60% threshold means <60% failures keeps it CLOSED
            // Record 3 successes, 2 failures (40% failure rate) — should stay CLOSED
            for (int i = 0; i < 3; i++) {
                registry.recordSuccess(host);
            }
            for (int i = 0; i < 2; i++) {
                registry.recordFailure(host, new RuntimeException("timeout"));
            }

            assertThat(registry.allowRequest(host)).isTrue();
            CircuitBreaker cb = registry.getCircuitBreaker(host);
            assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        }

        @Test
        @DisplayName("mixed results above threshold trips breaker OPEN")
        void mixedResultsAboveThresholdTripsOpen() {
            String host = ResilienceRegistry.serviceInstance(WORKER_SERVICE, "10.0.0.13");

            // 60% threshold: 2 successes + 4 failures = 66.7% failure rate → OPEN
            // Need at least 5 calls (minimumNumberOfCalls=5)
            registry.recordSuccess(host);
            registry.recordSuccess(host);
            registry.recordFailure(host, new RuntimeException("error 1"));
            registry.recordFailure(host, new RuntimeException("error 2"));
            registry.recordFailure(host, new RuntimeException("error 3"));
            registry.recordFailure(host, new RuntimeException("error 4"));

            CircuitBreaker cb = registry.getCircuitBreaker(host);
            assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.OPEN);
            assertThat(registry.allowRequest(host)).isFalse();
        }
    }

    @Nested
    @DisplayName("HALF_OPEN recovery with production keys")
    class HalfOpenRecovery {

        @Test
        @DisplayName("OPEN -> HALF_OPEN -> CLOSED on successful probes")
        void openToHalfOpenToClosedOnSuccess() {
            String host = ResilienceRegistry.serviceInstance(WORKER_SERVICE, "10.0.0.20");
            CircuitBreaker cb = registry.getCircuitBreaker(host);

            // Trip to OPEN
            for (int i = 0; i < 5; i++) {
                cb.onError(0, TimeUnit.MILLISECONDS, new RuntimeException("fail"));
            }
            assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.OPEN);

            // Manually transition to HALF_OPEN (simulates wait duration elapsed)
            cb.transitionToHalfOpenState();
            assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.HALF_OPEN);

            // Successful probes (permittedNumberOfCallsInHalfOpenState=5 for coordinator-to-worker)
            for (int i = 0; i < 5; i++) {
                assertThat(cb.tryAcquirePermission()).isTrue();
                cb.onSuccess(10, TimeUnit.MILLISECONDS);
            }

            // Should transition back to CLOSED
            assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        }

        @Test
        @DisplayName("HALF_OPEN -> OPEN when probe failures exceed threshold")
        void halfOpenToOpenOnProbeFailures() {
            String host = ResilienceRegistry.serviceInstance(WORKER_SERVICE, "10.0.0.21");
            CircuitBreaker cb = registry.getCircuitBreaker(host);

            // Trip to OPEN
            for (int i = 0; i < 5; i++) {
                cb.onError(0, TimeUnit.MILLISECONDS, new RuntimeException("fail"));
            }
            assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.OPEN);

            // Transition to HALF_OPEN
            cb.transitionToHalfOpenState();
            assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.HALF_OPEN);

            // Resilience4j evaluates HALF_OPEN after all permitted probes complete.
            // coordinator-to-worker: permittedNumberOfCallsInHalfOpenState=5, failureRateThreshold=60%
            // Record 5 failures (100% failure rate > 60%) → should trip back to OPEN
            for (int i = 0; i < 5; i++) {
                cb.onError(0, TimeUnit.MILLISECONDS, new RuntimeException("still failing"));
            }
            assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        }

        @Test
        @DisplayName("HALF_OPEN limits probes to configured permittedNumberOfCalls")
        void halfOpenLimitsProbes() {
            String host = ResilienceRegistry.serviceInstance(WORKER_SERVICE, "10.0.0.22");
            CircuitBreaker cb = registry.getCircuitBreaker(host);

            // Trip to OPEN and then HALF_OPEN
            for (int i = 0; i < 5; i++) {
                cb.onError(0, TimeUnit.MILLISECONDS, new RuntimeException("fail"));
            }
            cb.transitionToHalfOpenState();

            // Should allow exactly 5 probes (permittedNumberOfCallsInHalfOpenState=5)
            for (int i = 0; i < 5; i++) {
                assertThat(cb.tryAcquirePermission()).isTrue();
            }
            // 6th should be rejected
            assertThat(cb.tryAcquirePermission()).isFalse();
        }
    }

    @Nested
    @DisplayName("SPI contract with coordinator-to-metadata service")
    class MetadataServiceBreaker {

        private static final String METADATA_SERVICE = "coordinator-to-metadata";

        @Test
        @DisplayName("metadata service breaker trips at 50% failure rate with minimum 10 calls")
        void metadataServiceTripsAtConfiguredThreshold() {
            // coordinator-to-metadata: failureRateThreshold=50%, minimumNumberOfCalls=10
            for (int i = 0; i < 10; i++) {
                registry.recordFailure(METADATA_SERVICE, new RuntimeException("metadata unavailable"));
            }
            assertThat(registry.allowRequest(METADATA_SERVICE)).isFalse();
        }

        @Test
        @DisplayName("metadata service does not trip below minimum calls")
        void metadataServiceDoesNotTripBelowMinCalls() {
            // minimumNumberOfCalls=10, so 9 failures should not trip
            for (int i = 0; i < 9; i++) {
                registry.recordFailure(METADATA_SERVICE, new RuntimeException("fail"));
            }
            assertThat(registry.allowRequest(METADATA_SERVICE)).isTrue();
        }
    }

    @Nested
    @DisplayName("Worker-to-shuffle service breaker with production keys")
    class ShuffleServiceBreaker {

        private static final String SHUFFLE_SERVICE = "worker-to-shuffle";

        @Test
        @DisplayName("shuffle service breaker trips at 70% failure rate")
        void shuffleServiceTripsAtConfiguredThreshold() {
            // worker-to-shuffle: failureRateThreshold=70%, minimumNumberOfCalls=10
            String target = ResilienceRegistry.serviceInstance(SHUFFLE_SERVICE, "10.0.1.5:9090");

            // 10 calls at 70%+ failure rate: 7 failures + 3 successes = 70% exactly
            for (int i = 0; i < 3; i++) {
                registry.recordSuccess(target);
            }
            for (int i = 0; i < 7; i++) {
                registry.recordFailure(target, new RuntimeException("shuffle partition unavailable"));
            }

            assertThat(registry.allowRequest(target)).isFalse();
        }

        @Test
        @DisplayName("shuffle service stays closed just below threshold")
        void shuffleServiceStaysClosedBelowThreshold() {
            // 69% failure rate should keep it closed (threshold is 70%)
            // 10 calls: 4 successes + 6 failures = 60% failure rate
            String target = ResilienceRegistry.serviceInstance(SHUFFLE_SERVICE, "10.0.1.6:9090");

            for (int i = 0; i < 4; i++) {
                registry.recordSuccess(target);
            }
            for (int i = 0; i < 6; i++) {
                registry.recordFailure(target, new RuntimeException("transient error"));
            }

            assertThat(registry.allowRequest(target)).isTrue();
        }
    }

    @Nested
    @DisplayName("serviceInstance key format")
    class ServiceInstanceKeyFormat {

        @Test
        @DisplayName("serviceInstance builds correct key format")
        void serviceInstanceBuildsCorrectKey() {
            String key = ResilienceRegistry.serviceInstance("coordinator-to-worker", "10.0.0.5");
            assertThat(key).isEqualTo("coordinator-to-worker#10.0.0.5");
        }

        @Test
        @DisplayName("serviceInstance key with port")
        void serviceInstanceKeyWithPort() {
            String key = ResilienceRegistry.serviceInstance("worker-to-shuffle", "10.0.1.5:9090");
            assertThat(key).isEqualTo("worker-to-shuffle#10.0.1.5:9090");
        }

        @Test
        @DisplayName("unrecognized service falls back to Resilience4j defaults")
        void unrecognizedServiceFallsToDefaults() {
            String unknownHost = ResilienceRegistry.serviceInstance("unknown-service", "10.0.0.99");
            // Should still work (default config applies)
            assertThat(registry.allowRequest(unknownHost)).isTrue();
            registry.recordSuccess(unknownHost);
            assertThat(registry.allowRequest(unknownHost)).isTrue();
        }
    }
}
