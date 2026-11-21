package org.pangrova.observability.sdk.metrics.otel;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import org.junit.jupiter.api.Test;
import org.pangrova.observability.api.metrics.DoubleHistogram;
import org.pangrova.observability.api.metrics.LongCounter;
import org.pangrova.observability.api.metrics.LongUpDownCounter;
import org.pangrova.observability.api.metrics.ObservableDoubleGauge;
import org.pangrova.observability.metrics.MetricsScope;
import org.pangrova.observability.sdk.metrics.runtime.NativeMetricsPublisher;
import org.pangrova.observability.sdk.metrics.runtime.NativeMetricsPuller;
import org.pangrova.observability.api.metrics.MetricsProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GaugeLifecycleTest {
    @Test
    void individualCloseUnregistersCallbackWithoutClosingSharedSdk() {
        var reader = InMemoryMetricReader.create();
        try (var sdk = SdkMeterProvider.builder().registerMetricReader(reader).build()) {
            var provider = new OtelMetricsProvider(sdk.get("lifecycle"));
            var calls = new AtomicInteger();
            var gauge = provider.createGauge("owned", "", "1", () -> (double) calls.incrementAndGet());
            try (var other = provider.createGauge("borrowed", "", "1", () -> 42.0)) {
                assertThat(reader.collectAllMetrics()).extracting(m -> m.getName()).contains("owned", "borrowed");
                assertThat(calls).hasValue(1);
                gauge.close();
                gauge.close();
                assertThat(reader.collectAllMetrics()).extracting(m -> m.getName()).containsExactly("borrowed");
                assertThat(calls).hasValue(1);
            }
        }
    }

    @Test
    void scopeReleasesDiscardedHandlesAndLeavesOtherOwnersActive() {
        var reader = InMemoryMetricReader.create();
        try (var sdk = SdkMeterProvider.builder().registerMetricReader(reader).build()) {
            var provider = new OtelMetricsProvider(sdk.get("lifecycle"));
            try (var scope = new MetricsScope(provider);
                 var unrelated = provider.createGauge("unrelated", "", "1", () -> 7.0)) {
                scope.createGauge("discarded", "", "1", () -> 1.0);
                assertThat(reader.collectAllMetrics()).hasSize(2);
                scope.close();
                assertThat(reader.collectAllMetrics()).extracting(m -> m.getName()).containsExactly("unrelated");
                assertThatThrownBy(() -> scope.createGauge("late", "", "1", () -> 1.0))
                        .isInstanceOf(IllegalStateException.class);
            }
        }
    }

    @Test
    void nativePublisherStopUnregistersOnlyItsOwnFourGauges() {
        var reader = InMemoryMetricReader.create();
        try (var sdk = SdkMeterProvider.builder().registerMetricReader(reader).build();
             var root = new MetricsScope(new OtelMetricsProvider(sdk.get("lifecycle")))) {
            root.createGauge("shared", "", "1", () -> 7.0);
            var pulls = new AtomicInteger();
            var publisher = new NativeMetricsPublisher(new NativeMetricsPuller(timeout -> { pulls.incrementAndGet(); return new byte[0]; }), root);
            publisher.pullAndPublish();
            assertThat(reader.collectAllMetrics()).hasSize(5);
            publisher.stop();
            publisher.stop();
            publisher.pullAndPublish();
            assertThat(pulls).hasValue(1);
            assertThat(reader.collectAllMetrics()).extracting(m -> m.getName()).containsExactly("shared");
        }
    }

    @Test
    void failedPublisherConstructionReleasesAlreadyRegisteredCallbacks() {
        var reader = InMemoryMetricReader.create();
        try (var sdk = SdkMeterProvider.builder().registerMetricReader(reader).build()) {
            var delegate = new OtelMetricsProvider(sdk.get("lifecycle"));
            var created = new AtomicInteger();
            MetricsProvider failing = new MetricsProvider() {
                @Override public ObservableDoubleGauge createGauge(String n, String d, String u, Supplier<Double> s) {
                    if (created.incrementAndGet() == 3) throw new IllegalStateException("third registration failed");
                    return delegate.createGauge(n, d, u, s);
                }
                @Override public LongCounter createCounter(String n, String d, String u) { return delegate.createCounter(n, d, u); }
                @Override public LongUpDownCounter createUpDownCounter(String n, String d, String u) { return delegate.createUpDownCounter(n, d, u); }
                @Override public DoubleHistogram createHistogram(String n, String d, String u, List<Double> b) { return delegate.createHistogram(n, d, u, b); }
            };
            assertThatThrownBy(() -> new NativeMetricsPublisher(new NativeMetricsPuller(timeout -> new byte[0]), failing))
                    .isInstanceOf(IllegalStateException.class).hasMessage("third registration failed");
            assertThat(reader.collectAllMetrics()).isEmpty();
        }
    }
}
