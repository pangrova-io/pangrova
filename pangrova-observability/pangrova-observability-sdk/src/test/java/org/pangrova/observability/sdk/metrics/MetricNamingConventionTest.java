package org.pangrova.observability.sdk.metrics;

import org.pangrova.observability.api.metrics.DoubleHistogram;
import org.pangrova.observability.api.metrics.LongCounter;
import org.pangrova.observability.api.metrics.LongUpDownCounter;
import org.pangrova.observability.spi.metrics.MetricsProvider;
import org.pangrova.observability.api.metrics.ObservableDoubleGauge;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that metric names registered by CoordinatorMetrics, WorkerMetrics,
 * and HttpMetricsCollector follow the conventions defined in
 * {@code docs/metrics-conventions.md}.
 *
 * <p>Key rules verified:
 * <ul>
 *   <li>Component segment is present (e.g., {@code pangrova.coordinator.*}, {@code pangrova.worker.*})</li>
 *   <li>Histograms use "duration" not "latency"</li>
 *   <li>Duration histograms use seconds ("s") as unit, not milliseconds</li>
 *   <li>Metric names use dot-separated lowercase segments</li>
 * </ul>
 */
class MetricNamingConventionTest {

    @Test
    void coordinatorMetricsContainComponentSegment() {
        var recorder = new MetricNameRecorder();
        new CoordinatorMetrics(recorder, () -> 0.0);

        for (String name : recorder.allNames) {
            assertThat(name)
                    .as("Metric '%s' must contain 'coordinator' component segment", name)
                    .contains(".coordinator.");
        }
    }

    @Test
    void workerMetricsContainComponentSegment() {
        var recorder = new MetricNameRecorder();
        new WorkerMetrics(recorder, () -> 0.0, () -> 0.0);

        for (String name : recorder.allNames) {
            assertThat(name)
                    .as("Metric '%s' must contain 'worker' component segment", name)
                    .contains(".worker.");
        }
    }

    @Test
    void coordinatorHistogramsUseDurationNotLatency() {
        var recorder = new MetricNameRecorder();
        new CoordinatorMetrics(recorder, () -> 0.0);

        for (String name : recorder.histogramNames) {
            assertThat(name)
                    .as("Histogram '%s' must use 'duration' not 'latency'", name)
                    .contains("duration")
                    .doesNotContain("latency");
        }
    }

    @Test
    void workerHistogramsUseDurationNotLatency() {
        var recorder = new MetricNameRecorder();
        new WorkerMetrics(recorder, () -> 0.0, () -> 0.0);

        for (String name : recorder.histogramNames) {
            assertThat(name)
                    .as("Histogram '%s' must use 'duration' not 'latency'", name)
                    .contains("duration")
                    .doesNotContain("latency");
        }
    }

    @Test
    void httpHistogramUsesDurationNotLatency() {
        var recorder = new MetricNameRecorder();
        new HttpMetricsCollector(recorder);

        for (String name : recorder.histogramNames) {
            assertThat(name)
                    .as("Histogram '%s' must use 'duration' not 'latency'", name)
                    .contains("duration")
                    .doesNotContain("latency");
        }
    }

    @Test
    void coordinatorDurationHistogramsUseSecondsUnit() {
        var recorder = new MetricNameRecorder();
        new CoordinatorMetrics(recorder, () -> 0.0);

        for (int i = 0; i < recorder.histogramNames.size(); i++) {
            String name = recorder.histogramNames.get(i);
            String unit = recorder.histogramUnits.get(i);
            assertThat(unit)
                    .as("Histogram '%s' must use 's' (seconds) unit, not 'ms'", name)
                    .isEqualTo("s");
        }
    }

    @Test
    void workerDurationHistogramUsesSecondsUnit() {
        var recorder = new MetricNameRecorder();
        new WorkerMetrics(recorder, () -> 0.0, () -> 0.0);

        for (int i = 0; i < recorder.histogramNames.size(); i++) {
            String name = recorder.histogramNames.get(i);
            String unit = recorder.histogramUnits.get(i);
            assertThat(unit)
                    .as("Histogram '%s' must use 's' (seconds) unit, not 'ms'", name)
                    .isEqualTo("s");
        }
    }

    @Test
    void httpDurationHistogramUsesSecondsUnit() {
        var recorder = new MetricNameRecorder();
        new HttpMetricsCollector(recorder);

        for (int i = 0; i < recorder.histogramNames.size(); i++) {
            String name = recorder.histogramNames.get(i);
            String unit = recorder.histogramUnits.get(i);
            assertThat(unit)
                    .as("Histogram '%s' must use 's' (seconds) unit, not 'ms'", name)
                    .isEqualTo("s");
        }
    }

    @Test
    void allMetricNamesUseDotSeparatedLowercase() {
        var recorder = new MetricNameRecorder();
        new CoordinatorMetrics(recorder, () -> 0.0);
        new WorkerMetrics(recorder, () -> 0.0, () -> 0.0);
        new HttpMetricsCollector(recorder);
        new ShuffleMetrics(recorder);

        for (String name : recorder.allNames) {
            assertThat(name)
                    .as("Metric '%s' must be dot-separated lowercase", name)
                    .matches("[a-z][a-z0-9]+(\\.[a-z][a-z0-9]*)+");
        }
    }

    @Test
    void shuffleMetricsContainComponentSegment() {
        var recorder = new MetricNameRecorder();
        new ShuffleMetrics(recorder);

        for (String name : recorder.allNames) {
            assertThat(name)
                    .as("Metric '%s' must contain 'shuffle' component segment", name)
                    .contains(".shuffle.");
        }
    }

    @Test
    void shuffleMetricsStartWithPangrovaPrefix() {
        var recorder = new MetricNameRecorder();
        new ShuffleMetrics(recorder);

        for (String name : recorder.allNames) {
            assertThat(name)
                    .as("Metric '%s' must start with 'pangrova.'", name)
                    .startsWith("pangrova.");
        }
    }

    @Test
    void shuffleHistogramsUseDurationNotLatency() {
        var recorder = new MetricNameRecorder();
        new ShuffleMetrics(recorder);

        for (String name : recorder.histogramNames) {
            assertThat(name)
                    .as("Histogram '%s' must use 'duration' not 'latency'", name)
                    .doesNotContain("latency");
        }
    }

    @Test
    void noMetricNameContainsUnitSuffix() {
        var recorder = new MetricNameRecorder();
        new CoordinatorMetrics(recorder, () -> 0.0);
        new WorkerMetrics(recorder, () -> 0.0, () -> 0.0);
        new HttpMetricsCollector(recorder);
        new ShuffleMetrics(recorder);

        for (String name : recorder.allNames) {
            // OTel convention: unit via parameter, never in metric name
            // (exception: "bytes" in "partition.size.bytes" is entity not unit)
            assertThat(name)
                    .as("Metric '%s' must not embed unit suffix '_ms' or '_seconds'", name)
                    .doesNotContain("_ms")
                    .doesNotContain("_seconds")
                    .doesNotContain("_bytes_total");
        }
    }

    @Test
    void coordinatorMetricsStartWithPangrovaPrefix() {
        var recorder = new MetricNameRecorder();
        new CoordinatorMetrics(recorder, () -> 0.0);

        for (String name : recorder.allNames) {
            assertThat(name)
                    .as("Metric '%s' must start with 'pangrova.'", name)
                    .startsWith("pangrova.");
        }
    }

    @Test
    void workerMetricsStartWithPangrovaPrefix() {
        var recorder = new MetricNameRecorder();
        new WorkerMetrics(recorder, () -> 0.0, () -> 0.0);

        for (String name : recorder.allNames) {
            assertThat(name)
                    .as("Metric '%s' must start with 'pangrova.'", name)
                    .startsWith("pangrova.");
        }
    }

    /**
     * Recorder that captures metric names and units for convention assertions.
     */
    private static final class MetricNameRecorder implements MetricsProvider {
        final List<String> allNames = new ArrayList<>();
        final List<String> histogramNames = new ArrayList<>();
        final List<String> histogramUnits = new ArrayList<>();

        @Override
        public LongCounter createCounter(String name, String description, String unit) {
            allNames.add(name);
            return value -> {};
        }

        @Override
        public ObservableDoubleGauge createGauge(String name, String description,
                                                  String unit, Supplier<Double> supplier) {
            allNames.add(name);
            return () -> { /* This recording fixture retains no callback. */ };
        }

        @Override
        public DoubleHistogram createHistogram(String name, String description,
                                                String unit, List<Double> boundaries) {
            allNames.add(name);
            histogramNames.add(name);
            histogramUnits.add(unit);
            return value -> {};
        }

        @Override
        public LongUpDownCounter createUpDownCounter(String name, String description, String unit) {
            allNames.add(name);
            return value -> {};
        }
    }
}
