package org.pangrova.observability.sdk.metrics;

import org.pangrova.observability.api.DoubleHistogram;
import org.pangrova.observability.api.LongCounter;
import org.pangrova.observability.api.LongUpDownCounter;
import org.pangrova.observability.api.MetricsProvider;
import org.pangrova.observability.api.ObservableDoubleGauge;
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

    /**
     * Capturing provider that tracks counter increments and histogram recordings.
     */
    private static final class CapturingMetricsProvider implements MetricsProvider {
        final List<String> counterNames = new ArrayList<>();
        final List<String> histogramNames = new ArrayList<>();
        final java.util.Map<String, Long> counterValues = new java.util.HashMap<>();
        final java.util.Map<String, List<Double>> histogramValues = new java.util.HashMap<>();

        @Override
        public LongCounter createCounter(String name, String description, String unit) {
            counterNames.add(name);
            counterValues.put(name, 0L);
            return value -> counterValues.merge(name, value, Long::sum);
        }

        @Override
        public ObservableDoubleGauge createGauge(String name, String description,
                                                  String unit, Supplier<Double> supplier) {
            return new ObservableDoubleGauge() {};
        }

        @Override
        public DoubleHistogram createHistogram(String name, String description,
                                                String unit, List<Double> boundaries) {
            histogramNames.add(name);
            histogramValues.put(name, new ArrayList<>());
            return value -> histogramValues.get(name).add(value);
        }

        @Override
        public LongUpDownCounter createUpDownCounter(String name, String description, String unit) {
            return value -> {};
        }
    }
}
