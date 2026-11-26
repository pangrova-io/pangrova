package org.pangrova.coordinator.indexing.lifecycle;

import org.junit.jupiter.api.Test;
import org.pangrova.observability.api.metrics.MetricAttributes;
import org.pangrova.observability.api.metrics.ObservableDoubleGauge;
import org.pangrova.observability.noop.NoOpMetricsProvider;
import org.pangrova.persistence.generation.api.enums.SnapshotPinState;
import org.pangrova.persistence.generation.api.job.SnapshotPinPressure;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class NativeSnapshotPinPressureMetricsTest {
    private static final Instant NOW = Instant.parse("2026-11-25T00:00:00Z");
    private static final String PREFIX = "pangrova.index.snapshot_pin.native.";

    @Test
    void samplesAreBoundedDimensionalAndOmittedAfterFailureOrStaleness() {
        var provider = new CapturingMetrics();
        try (var metrics = new NativeSnapshotPinPressureMetrics(provider, Clock.fixed(NOW, ZoneOffset.UTC))) {
            assertThat(provider.value("observed_count_lower_bound", "pinned")).isEmpty();
            metrics.beginEpoch(1);
            metrics.record(1, Optional.of(sample(NOW.minusSeconds(10))));
            assertThat(provider.value("observed_count_lower_bound", "intended")).hasValue(0);
            assertThat(provider.value("oldest_intent_age_seconds", "intended")).isEmpty();
            assertThat(provider.value("observed_count_lower_bound", "pinned")).hasValue(1000);
            assertThat(provider.value("truncated", "pinned")).hasValue(1);
            assertThat(provider.value("oldest_intent_age_seconds", "pinned")).hasValue(240);
            assertThat(provider.value("sample_age_seconds", "")).hasValue(10);
            metrics.record(1, Optional.of(sample(NOW.minus(Duration.ofMinutes(16)))));
            assertThat(provider.value("observed_count_lower_bound", "pinned")).isEmpty();
            assertThat(provider.value("sample_age_seconds", "")).hasValue(960);
            metrics.record(1, Optional.empty());
            assertThat(provider.value("sample_age_seconds", "")).isEmpty();
            metrics.endEpoch();
            metrics.beginEpoch(2);
            metrics.record(1, Optional.of(sample(NOW)));
            assertThat(provider.value("sample_age_seconds", "")).isEmpty();
            metrics.record(2, Optional.of(sample(NOW)));
            assertThat(provider.value("observed_count_lower_bound", "pinned")).hasValue(1000);
        }
        assertThat(provider.samples).isEmpty();
    }

    private static SnapshotPinPressure sample(Instant observedAt) {
        return new SnapshotPinPressure(observedAt, List.of(
                new SnapshotPinPressure.StateCount(SnapshotPinState.INTENDED, 0, false, Optional.empty()),
                new SnapshotPinPressure.StateCount(SnapshotPinState.PINNED, 1000, true,
                        Optional.of(NOW.minus(Duration.ofMinutes(4)))),
                new SnapshotPinPressure.StateCount(SnapshotPinState.RELEASING, 1, false,
                        Optional.of(NOW.minusSeconds(30)))));
    }

    private static final class CapturingMetrics extends NoOpMetricsProvider {
        private final Map<String, Supplier<OptionalDouble>> samples = new HashMap<>();

        @Override
        public ObservableDoubleGauge createObservedGauge(String name, String description, String unit,
                MetricAttributes attributes, Supplier<OptionalDouble> sample) {
            String key = name + ":" + attributes.asMap().getOrDefault("state", "");
            samples.put(key, sample);
            return () -> samples.remove(key);
        }

        OptionalDouble value(String metric, String state) {
            return samples.get(PREFIX + metric + ":" + state).get();
        }
    }
}
