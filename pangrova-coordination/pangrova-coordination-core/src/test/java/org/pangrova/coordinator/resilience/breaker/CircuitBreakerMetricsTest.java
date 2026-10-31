package org.pangrova.coordinator.resilience.breaker;

import org.pangrova.coordinator.resilience.provider.resilience4j.ResilienceRegistry;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.pangrova.coordinator.resilience.config.ResilienceConfigLoader;
import org.pangrova.common.resilience.policy.ResiliencePolicyConfig;
import org.pangrova.observability.api.DoubleHistogram;
import org.pangrova.observability.api.LongCounter;
import org.pangrova.observability.api.LongUpDownCounter;
import org.pangrova.observability.api.MetricAttributes;
import org.pangrova.observability.api.MetricsProvider;
import org.pangrova.observability.api.ObservableDoubleGauge;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that circuit breaker state transitions and call outcomes produce
 * metrics events through the MetricsProvider — including per-host breakers
 * that are created dynamically (F-RESIL-04 fix).
 */
class CircuitBreakerMetricsTest {

    private SpyMetricsProvider metricsProvider;
    private ResilienceRegistry registry;

    @BeforeEach
    void setUp() {
        metricsProvider = new SpyMetricsProvider();
        InputStream config = getClass().getClassLoader().getResourceAsStream("resilience-defaults.yml");
        Map<String, ResiliencePolicyConfig> policies = ResilienceConfigLoader.load(config);
        registry = new ResilienceRegistry(policies, metricsProvider);
    }

    @Test
    void preConfiguredServiceRegistersGaugeMetrics() {
        // Pre-configured services should have gauge metrics registered during construction
        assertThat(metricsProvider.gaugeNames)
                .contains("resilience.circuitbreaker.state",
                          "resilience.circuitbreaker.failure_rate",
                          "resilience.circuitbreaker.buffered_calls");
    }

    @Test
    void perHostBreakerRegistersMetricsOnFirstAccess() {
        // Clear metrics accumulated during setUp (pre-configured services)
        metricsProvider.counterNames.clear();
        metricsProvider.gaugeNames.clear();

        // Access a per-host breaker (dynamically created via SERVICE_INSTANCE_DELIMITER)
        String perHostTarget = ResilienceRegistry.serviceInstance("coordinator-to-worker", "10.0.0.5");
        registry.getCircuitBreaker(perHostTarget);

        // Verify gauge metrics were registered for the per-host breaker
        assertThat(metricsProvider.gaugeNames)
                .contains("resilience.circuitbreaker.state",
                          "resilience.circuitbreaker.failure_rate",
                          "resilience.circuitbreaker.buffered_calls");

        // Verify counter metrics were registered for state transitions and call outcomes
        assertThat(metricsProvider.counterNames)
                .contains("resilience.circuitbreaker.state_transitions",
                          "resilience.circuitbreaker.calls");
    }

    @Test
    void stateTransitionEmitsMetricEvent() {
        String perHostTarget = ResilienceRegistry.serviceInstance("coordinator-to-worker", "10.0.0.7");
        CircuitBreaker cb = registry.getCircuitBreaker(perHostTarget);

        // coordinator-to-worker has minimumNumberOfCalls=5, failureRateThreshold=60%
        // Trigger enough failures to trip the breaker
        for (int i = 0; i < 10; i++) {
            cb.onError(0, TimeUnit.MILLISECONDS, new RuntimeException("test"));
        }

        // The breaker should be OPEN
        assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.OPEN);

        // Verify state transition counter was incremented
        assertThat(metricsProvider.stateTransitionCounter.recordings)
                .anySatisfy(recording -> {
                    assertThat(recording.attributes.asMap()).containsEntry("name", perHostTarget);
                    assertThat(recording.attributes.asMap().get("transition"))
                            .containsIgnoringCase("CLOSED").containsIgnoringCase("OPEN");
                });
    }

    @Test
    void successAndErrorCallsEmitMetricEvents() {
        String perHostTarget = ResilienceRegistry.serviceInstance("coordinator-to-worker", "10.0.0.8");
        CircuitBreaker cb = registry.getCircuitBreaker(perHostTarget);

        // Record some successes and errors
        cb.onSuccess(10, TimeUnit.MILLISECONDS);
        cb.onSuccess(20, TimeUnit.MILLISECONDS);
        cb.onError(5, TimeUnit.MILLISECONDS, new RuntimeException("fail"));

        // Verify success call result counter was incremented
        long successCount = metricsProvider.callResultCounter.recordings.stream()
                .filter(r -> "success".equals(r.attributes.asMap().get("outcome"))
                        && perHostTarget.equals(r.attributes.asMap().get("name")))
                .mapToLong(r -> r.value)
                .sum();
        assertThat(successCount).isEqualTo(2);

        // Verify error call result counter was incremented
        long errorCount = metricsProvider.callResultCounter.recordings.stream()
                .filter(r -> "error".equals(r.attributes.asMap().get("outcome"))
                        && perHostTarget.equals(r.attributes.asMap().get("name")))
                .mapToLong(r -> r.value)
                .sum();
        assertThat(errorCount).isEqualTo(1);
    }

    @Test
    void callNotPermittedEmitsMetricEvent() {
        String perHostTarget = ResilienceRegistry.serviceInstance("coordinator-to-worker", "10.0.0.9");
        CircuitBreaker cb = registry.getCircuitBreaker(perHostTarget);

        // Trip the breaker open
        for (int i = 0; i < 10; i++) {
            cb.onError(0, TimeUnit.MILLISECONDS, new RuntimeException("test"));
        }
        assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.OPEN);

        // Attempt a call that should be rejected
        cb.tryAcquirePermission(); // returns false, fires not_permitted event

        // Verify not_permitted counter was incremented
        long notPermittedCount = metricsProvider.callResultCounter.recordings.stream()
                .filter(r -> "not_permitted".equals(r.attributes.asMap().get("outcome"))
                        && perHostTarget.equals(r.attributes.asMap().get("name")))
                .mapToLong(r -> r.value)
                .sum();
        assertThat(notPermittedCount).isEqualTo(1);
    }

    @Test
    void spiRecordFailureOnPerHostTargetEmitsMetrics() {
        // Use the CircuitBreakerPolicy SPI methods (as the coordinator would)
        String perHostTarget = ResilienceRegistry.serviceInstance("coordinator-to-worker", "10.0.0.11");

        for (int i = 0; i < 10; i++) {
            registry.recordFailure(perHostTarget, new RuntimeException("host down"));
        }

        // The breaker should be OPEN via SPI
        assertThat(registry.allowRequest(perHostTarget)).isFalse();

        // Verify error counters were emitted
        long errorCount = metricsProvider.callResultCounter.recordings.stream()
                .filter(r -> "error".equals(r.attributes.asMap().get("outcome"))
                        && perHostTarget.equals(r.attributes.asMap().get("name")))
                .mapToLong(r -> r.value)
                .sum();
        assertThat(errorCount).isEqualTo(10);

        // Verify state transition was emitted
        assertThat(metricsProvider.stateTransitionCounter.recordings)
                .anySatisfy(recording -> {
                    assertThat(recording.attributes.asMap()).containsEntry("name", perHostTarget);
                    assertThat(recording.attributes.asMap().get("transition"))
                            .containsIgnoringCase("CLOSED").containsIgnoringCase("OPEN");
                });
    }

    // -- Spy MetricsProvider implementation --

    /**
     * A test spy MetricsProvider that records all counter increments and gauge registrations,
     * allowing assertions on metrics emission.
     */
    static class SpyMetricsProvider implements MetricsProvider {

        final List<String> gaugeNames = new CopyOnWriteArrayList<>();
        final List<String> counterNames = new CopyOnWriteArrayList<>();
        // Dedicated counters for tracking state transitions and call results.
        // The first counter created with name "state_transitions" is captured here;
        // the first with "calls" is captured in callResultCounter.
        final SpyLongCounter stateTransitionCounter = new SpyLongCounter();
        final SpyLongCounter callResultCounter = new SpyLongCounter();

        @Override
        public LongCounter createCounter(String name, String description, String unit) {
            counterNames.add(name);
            if (name.contains("state_transitions")) {
                return stateTransitionCounter;
            }
            if (name.contains("calls")) {
                return callResultCounter;
            }
            return new SpyLongCounter();
        }

        @Override
        public ObservableDoubleGauge createGauge(String name, String description,
                                                  String unit, Supplier<Double> supplier) {
            gaugeNames.add(name);
            return new ObservableDoubleGauge() {};
        }

        @Override
        public DoubleHistogram createHistogram(String name, String description,
                                               String unit, List<Double> boundaries) {
            return new DoubleHistogram() {
                @Override
                public void record(double value) {}

                @Override
                public void record(double value, MetricAttributes attributes) {}
            };
        }

        @Override
        public LongUpDownCounter createUpDownCounter(String name, String description, String unit) {
            return new LongUpDownCounter() {
                @Override
                public void add(long value) {}

                @Override
                public void add(long value, MetricAttributes attributes) {}
            };
        }
    }

    /**
     * A spy counter that records all add() invocations with their attributes.
     */
    static class SpyLongCounter implements LongCounter {

        record Recording(long value, MetricAttributes attributes) {}

        final List<Recording> recordings = new CopyOnWriteArrayList<>();

        @Override
        public void add(long value) {
            recordings.add(new Recording(value, MetricAttributes.empty()));
        }

        @Override
        public void add(long value, MetricAttributes attributes) {
            recordings.add(new Recording(value, attributes));
        }
    }
}
