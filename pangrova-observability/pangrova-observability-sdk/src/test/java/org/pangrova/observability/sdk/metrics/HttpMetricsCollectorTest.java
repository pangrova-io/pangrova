package org.pangrova.observability.sdk.metrics;

import org.pangrova.observability.api.metrics.DoubleHistogram;
import org.pangrova.observability.api.metrics.MetricAttributes;
import org.pangrova.observability.api.metrics.LongCounter;
import org.pangrova.observability.api.metrics.LongUpDownCounter;
import org.pangrova.observability.api.metrics.MetricsProvider;
import org.pangrova.observability.api.metrics.ObservableDoubleGauge;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link HttpMetricsCollector}.
 *
 * <p>Verifies request counting, latency recording, and error classification.
 */
class HttpMetricsCollectorTest {

    @Test
    void registersExpectedMetrics() {
        var capturing = new CapturingMetricsProvider();
        new HttpMetricsCollector(capturing);

        assertThat(capturing.counterNames).containsExactlyInAnyOrder(
                "http.server.request.total",
                "http.server.error.total");
        assertThat(capturing.histogramNames).containsExactly(
                "http.server.request.duration");
    }

    @Test
    void recordRequestIncrementsCounter() {
        var capturing = new CapturingMetricsProvider();
        var collector = new HttpMetricsCollector(capturing);

        collector.recordRequest(200, 10.0);
        collector.recordRequest(201, 5.0);

        assertThat(capturing.counterValues.get("http.server.request.total")).isEqualTo(2L);
    }

    @Test
    void recordRequestRecordsLatency() {
        var capturing = new CapturingMetricsProvider();
        var collector = new HttpMetricsCollector(capturing);

        collector.recordRequest(200, 42.5);

        assertThat(capturing.histogramValues.get("http.server.request.duration"))
                .containsExactly(42.5);
    }

    @Test
    void successfulRequestDoesNotIncrementErrorCount() {
        var capturing = new CapturingMetricsProvider();
        var collector = new HttpMetricsCollector(capturing);

        collector.recordRequest(200, 10.0);
        collector.recordRequest(301, 5.0);

        assertThat(capturing.counterValues.getOrDefault("http.server.error.total", 0L)).isEqualTo(0L);
    }

    @Test
    void clientErrorIncrementsErrorCount() {
        var capturing = new CapturingMetricsProvider();
        var collector = new HttpMetricsCollector(capturing);

        collector.recordRequest(400, 10.0);
        collector.recordRequest(404, 5.0);

        assertThat(capturing.counterValues.get("http.server.error.total")).isEqualTo(2L);
    }

    @Test
    void serverErrorIncrementsErrorCount() {
        var capturing = new CapturingMetricsProvider();
        var collector = new HttpMetricsCollector(capturing);

        collector.recordRequest(500, 10.0);
        collector.recordRequest(503, 5.0);

        assertThat(capturing.counterValues.get("http.server.error.total")).isEqualTo(2L);
    }

    @Test
    void mixedStatusCodesCountErrorsCorrectly() {
        var capturing = new CapturingMetricsProvider();
        var collector = new HttpMetricsCollector(capturing);

        collector.recordRequest(200, 10.0);
        collector.recordRequest(404, 5.0);
        collector.recordRequest(500, 15.0);
        collector.recordRequest(201, 3.0);

        assertThat(capturing.counterValues.get("http.server.request.total")).isEqualTo(4L);
        assertThat(capturing.counterValues.get("http.server.error.total")).isEqualTo(2L);
    }

    @Test
    void status399IsNotAnError() {
        var capturing = new CapturingMetricsProvider();
        var collector = new HttpMetricsCollector(capturing);

        collector.recordRequest(399, 10.0);

        assertThat(capturing.counterValues.getOrDefault("http.server.error.total", 0L)).isEqualTo(0L);
    }

    @Test
    void status400IsAnError() {
        var capturing = new CapturingMetricsProvider();
        var collector = new HttpMetricsCollector(capturing);

        collector.recordRequest(400, 10.0);

        assertThat(capturing.counterValues.get("http.server.error.total")).isEqualTo(1L);
    }

    @Test
    void statusDimensionsRemainSeparateInCountsAndLatency() {
        var capturing = new CapturingMetricsProvider();
        var collector = new HttpMetricsCollector(capturing);
        collector.recordRequest(200, 1.0);
        collector.recordRequest(503, 2.0);
        var success = MetricAttributes.of("http.status_class", "2xx");
        var failure = MetricAttributes.of("http.status_class", "5xx");
        assertThat(capturing.dimensionalCounts.get("http.server.request.total"))
                .containsEntry(success, 1L).containsEntry(failure, 1L).hasSize(2);
        assertThat(capturing.dimensionalCounts.get("http.server.error.total"))
                .containsOnlyKeys(failure);
        assertThat(capturing.dimensionalValues.get("http.server.request.duration"))
                .containsEntry(success, List.of(1.0)).containsEntry(failure, List.of(2.0)).hasSize(2);
    }

    /**
     * Capturing provider that tracks counter increments and histogram recordings.
     */
    static final class CapturingMetricsProvider implements MetricsProvider {
        final List<String> counterNames = new ArrayList<>();
        final List<String> histogramNames = new ArrayList<>();
        final java.util.Map<String, Long> counterValues = new java.util.HashMap<>();
        final java.util.Map<String, java.util.Map<MetricAttributes, Long>> dimensionalCounts = new java.util.HashMap<>();
        final java.util.Map<String, java.util.Map<MetricAttributes, List<Double>>> dimensionalValues = new java.util.HashMap<>();
        final java.util.Map<String, List<Double>> histogramValues = new java.util.HashMap<>();

        @Override
        public LongCounter createCounter(String name, String description, String unit) {
            counterNames.add(name);
            counterValues.put(name, 0L);
            return new LongCounter() {
                @Override public void add(long value) { add(value, MetricAttributes.empty()); }
                @Override public void add(long value, MetricAttributes attributes) {
                    dimensionalCounts.computeIfAbsent(name, ignored -> new java.util.HashMap<>())
                            .merge(attributes, value, Long::sum);
                    counterValues.merge(name, value, Long::sum);
                }
            };
        }

        @Override
        public ObservableDoubleGauge createGauge(String name, String description,
                                                  String unit, Supplier<Double> supplier) {
            return () -> { /* This recording fixture retains no callback. */ };
        }

        @Override
        public DoubleHistogram createHistogram(String name, String description,
                                                String unit, List<Double> boundaries) {
            histogramNames.add(name);
            histogramValues.put(name, new ArrayList<>());
            return new DoubleHistogram() {
                @Override public void record(double value) { record(value, MetricAttributes.empty()); }
                @Override public void record(double value, MetricAttributes attributes) {
                    dimensionalValues.computeIfAbsent(name, ignored -> new java.util.HashMap<>())
                            .computeIfAbsent(attributes, ignored -> new ArrayList<>()).add(value);
                    histogramValues.get(name).add(value);
                }
            };
        }

        @Override
        public LongUpDownCounter createUpDownCounter(String name, String description, String unit) {
            return value -> {};
        }
    }
}
