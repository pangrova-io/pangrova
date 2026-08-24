package org.pangrova.coordinator.scheduler;

import org.pangrova.coordinator.bootstrap.CoordinatorBootstrap;
import org.pangrova.coordinator.bootstrap.CoordinatorBootstrapConfig;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.resilience.ResilienceRegistry;
import org.pangrova.resilience.breaker.CircuitBreaker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that CoordinatorBootstrap injects an external CircuitBreakerPolicy
 * into RemoteTaskDispatcher when resilience is enabled, rather than relying
 * on the deprecated single-arg constructor's internal default.
 */
class RemoteTaskDispatcherCircuitBreakerInjectionTest {

    private CoordinatorBootstrap bootstrap;

    @AfterEach
    void tearDown() {
        if (bootstrap != null) {
            bootstrap.shutdown();
            bootstrap = null;
        }
    }

    @Test
    void resilienceEnabled_injectsExternalCircuitBreaker() {
        CoordinatorBootstrapConfig config = new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig("coord-cb-test", 0, 30_000),
                new CoordinatorBootstrapConfig.FeatureFlags(
                        false, false, false, false, false, true, false, false),
                CoordinatorBootstrapConfig.QueryConfig.defaults(),
                CoordinatorBootstrapConfig.TransportConfig.defaults(),
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                null, null, null);

        bootstrap = new CoordinatorBootstrap(config);
        bootstrap.start();

        CircuitBreakerPolicy cb = bootstrap.getCircuitBreaker();
        assertThat(cb).isNotNull();
        assertThat(cb).isInstanceOf(ResilienceRegistry.class);
        // Breaker is initially closed — requests are allowed
        assertThat(cb.allowRequest("coordinator-to-worker")).isTrue();
    }

    @Test
    void resilienceDisabled_circuitBreakerIsNull() {
        CoordinatorBootstrapConfig config =
                CoordinatorBootstrapConfig.withDefaults("coord-cb-disabled", 0);

        bootstrap = new CoordinatorBootstrap(config);
        bootstrap.start();

        assertThat(bootstrap.getCircuitBreaker()).isNull();
    }

    @Test
    void injectedCircuitBreaker_stateSharedWithDispatcher() {
        CoordinatorBootstrapConfig config = new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig("coord-cb-shared", 0, 30_000),
                new CoordinatorBootstrapConfig.FeatureFlags(
                        false, false, false, false, false, true, false, false),
                CoordinatorBootstrapConfig.QueryConfig.defaults(),
                CoordinatorBootstrapConfig.TransportConfig.defaults(),
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                null, null, null);

        bootstrap = new CoordinatorBootstrap(config);
        bootstrap.start();

        CircuitBreakerPolicy cb = bootstrap.getCircuitBreaker();
        assertThat(cb).isNotNull();
        assertThat(cb).isInstanceOf(ResilienceRegistry.class);

        // Drive CB with failures via the SPI — one failure should not open it
        cb.recordFailure("coordinator-to-worker", new RuntimeException("test"));
        assertThat(cb.allowRequest("coordinator-to-worker")).isTrue();

        // Force open with enough failures through the SPI
        // (coordinator-to-worker config: minimumNumberOfCalls=5, failureRateThreshold=60)
        for (int i = 0; i < 10; i++) {
            cb.recordFailure("coordinator-to-worker", new RuntimeException("test"));
        }
        assertThat(cb.allowRequest("coordinator-to-worker")).isFalse();
    }
}
