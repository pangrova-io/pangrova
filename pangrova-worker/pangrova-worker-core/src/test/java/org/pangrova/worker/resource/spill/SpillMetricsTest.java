package org.pangrova.worker.resource.spill;

import org.pangrova.spi.observability.DoubleHistogram;
import org.pangrova.spi.observability.LongCounter;
import org.pangrova.spi.observability.LongUpDownCounter;
import org.pangrova.spi.observability.MetricAttributes;
import org.pangrova.spi.observability.MetricsProvider;
import org.pangrova.spi.observability.ObservableDoubleGauge;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link SpillMetrics}.
 *
 * <p>Verifies that the spill subsystem emits all six required instruments
 * with correct names, units, and dimensional attributes.
 */
class SpillMetricsTest {

    @Test
    void registersAllSixInstrumentsAtConstruction() {
        var capturing = new CapturingMetricsProvider();
        new SpillMetrics(capturing);

        assertThat(capturing.counterNames).containsExactly(
                "pangrova.resource.spill.bytes",
                "pangrova.resource.spill.files",
                "pangrova.resource.spill.reclaim.attempts",
                "pangrova.resource.spill.reclaim.failures");
        assertThat(capturing.histogramNames).containsExactly(
                "pangrova.resource.spill.duration");
        // Directory gauge is registered lazily on first updateDirectoryBytes() call.
        assertThat(capturing.gaugeNames).isEmpty();
    }

    @Test
    void recordSpillIncrementsBytesAndFilesAndDuration() {
        var capturing = new CapturingMetricsProvider();
        var metrics = new SpillMetrics(capturing);

        metrics.recordSpill(SpillMetrics.DOMAIN_JAVA, SpillMetrics.REASON_PRESSURE,
                "stage-2", "tenant-a", 4096, 3, 0.250);

        assertThat(capturing.counterValue("pangrova.resource.spill.bytes")).isEqualTo(4096L);
        assertThat(capturing.counterValue("pangrova.resource.spill.files")).isEqualTo(3L);
        assertThat(capturing.histogramValues.get("pangrova.resource.spill.duration"))
                .containsExactly(0.250);
    }

    @Test
    void recordSpillTagsBytesWithDomainReasonStageTenant() {
        var capturing = new CapturingMetricsProvider();
        var metrics = new SpillMetrics(capturing);

        metrics.recordSpill(SpillMetrics.DOMAIN_NATIVE, SpillMetrics.REASON_NATIVE_RECLAIM,
                "stage-7", "tenant-b", 1024, 1, 0.05);

        Map<String, String> attrs = capturing.lastAttrsFor("pangrova.resource.spill.bytes");
        assertThat(attrs)
                .containsEntry("domain", "native")
                .containsEntry("reason", "native_reclaim")
                .containsEntry("stage.id", "stage-7")
                .containsEntry("tenant.id", "tenant-b");
    }

    @Test
    void recordSpillBlankAttributesFallbackToUnknown() {
        var capturing = new CapturingMetricsProvider();
        var metrics = new SpillMetrics(capturing);

        metrics.recordSpill(null, null, "", "  ", 100, 0, 0);

        Map<String, String> attrs = capturing.lastAttrsFor("pangrova.resource.spill.bytes");
        assertThat(attrs)
                .containsEntry("domain", "unknown")
                .containsEntry("reason", "unknown")
                .containsEntry("stage.id", "unknown")
                .containsEntry("tenant.id", "unknown");
    }

    @Test
    void recordSpillSkipsFileCounterWhenZeroFiles() {
        var capturing = new CapturingMetricsProvider();
        var metrics = new SpillMetrics(capturing);

        metrics.recordSpill(SpillMetrics.DOMAIN_NATIVE, SpillMetrics.REASON_OOM,
                "stage-1", "tenant-c", 0, 0, 0);

        assertThat(capturing.counterValue("pangrova.resource.spill.files")).isEqualTo(0L);
        assertThat(capturing.histogramValues.get("pangrova.resource.spill.duration")).isEmpty();
    }

    @Test
    void recordSpillRejectsNegativeBytes() {
        var capturing = new CapturingMetricsProvider();
        var metrics = new SpillMetrics(capturing);
        assertThatIllegalArgumentException().isThrownBy(() ->
                metrics.recordSpill("d", "r", "s", "t", -1, 0, 0));
    }

    @Test
    void recordSpillRejectsNegativeFiles() {
        var capturing = new CapturingMetricsProvider();
        var metrics = new SpillMetrics(capturing);
        assertThatIllegalArgumentException().isThrownBy(() ->
                metrics.recordSpill("d", "r", "s", "t", 0, -1, 0));
    }

    @Test
    void recordReclaimAttemptIncrementsCounter() {
        var capturing = new CapturingMetricsProvider();
        var metrics = new SpillMetrics(capturing);

        metrics.recordReclaimAttempt("HIGH", "stage-2");
        metrics.recordReclaimAttempt("CRITICAL", "stage-3");

        assertThat(capturing.counterValue("pangrova.resource.spill.reclaim.attempts")).isEqualTo(2L);
        Map<String, String> attrs = capturing.lastAttrsFor("pangrova.resource.spill.reclaim.attempts");
        assertThat(attrs).containsEntry("pressure", "CRITICAL").containsEntry("stage.id", "stage-3");
    }

    @Test
    void recordReclaimFailureIncrementsCounter() {
        var capturing = new CapturingMetricsProvider();
        var metrics = new SpillMetrics(capturing);

        metrics.recordReclaimFailure("HIGH", SpillMetrics.REASON_NO_CANDIDATES);
        metrics.recordReclaimFailure("CRITICAL", SpillMetrics.REASON_RECLAIM_NO_PROGRESS);

        assertThat(capturing.counterValue("pangrova.resource.spill.reclaim.failures")).isEqualTo(2L);
    }

    @Test
    void updateDirectoryBytesRegistersGaugeOnce() {
        var capturing = new CapturingMetricsProvider();
        var metrics = new SpillMetrics(capturing);

        metrics.updateDirectoryBytes("/var/spill/a", 1024);
        metrics.updateDirectoryBytes("/var/spill/a", 2048);
        metrics.updateDirectoryBytes("/var/spill/b", 4096);

        // One gauge registration per unique directory.
        assertThat(capturing.gaugeNames)
                .containsOnly("pangrova.resource.spill.directory.bytes");
        assertThat(capturing.gaugeNames).hasSize(2);

        assertThat(metrics.getDirectoryBytes("/var/spill/a")).isEqualTo(2048L);
        assertThat(metrics.getDirectoryBytes("/var/spill/b")).isEqualTo(4096L);

        // Gauge supplier reflects the current ledger value.
        Supplier<Double> dirA = capturing.gaugeSuppliers.get(0);
        assertThat(dirA.get()).isEqualTo(2048.0);
    }

    @Test
    void updateDirectoryBytesRejectsNegativeBytes() {
        var capturing = new CapturingMetricsProvider();
        var metrics = new SpillMetrics(capturing);
        assertThatIllegalArgumentException().isThrownBy(() ->
                metrics.updateDirectoryBytes("/x", -1));
    }

    /**
     * Capturing provider that records counter increments, histogram observations,
     * and gauge registrations for assertion. Captures attributes on the most
     * recent emission.
     */
    private static final class CapturingMetricsProvider implements MetricsProvider {
        final List<String> counterNames = new ArrayList<>();
        final List<String> histogramNames = new ArrayList<>();
        final List<String> gaugeNames = new ArrayList<>();
        final List<Supplier<Double>> gaugeSuppliers = new ArrayList<>();
        final Map<String, Long> counterValues = new HashMap<>();
        final Map<String, List<Double>> histogramValues = new HashMap<>();
        final Map<String, Map<String, String>> lastAttrs = new HashMap<>();

        long counterValue(String name) {
            return counterValues.getOrDefault(name, 0L);
        }

        Map<String, String> lastAttrsFor(String name) {
            return lastAttrs.getOrDefault(name, Map.of());
        }

        @Override
        public LongCounter createCounter(String name, String description, String unit) {
            counterNames.add(name);
            counterValues.put(name, 0L);
            return new LongCounter() {
                @Override
                public void add(long value) {
                    counterValues.merge(name, value, Long::sum);
                }
                @Override
                public void add(long value, MetricAttributes attributes) {
                    counterValues.merge(name, value, Long::sum);
                    lastAttrs.put(name, new LinkedHashMap<>(attributes.asMap()));
                }
            };
        }

        @Override
        public ObservableDoubleGauge createGauge(String name, String description, String unit,
                                                  Supplier<Double> supplier) {
            gaugeNames.add(name);
            gaugeSuppliers.add(supplier);
            return new ObservableDoubleGauge() {};
        }

        @Override
        public DoubleHistogram createHistogram(String name, String description, String unit,
                                                List<Double> boundaries) {
            histogramNames.add(name);
            histogramValues.put(name, new ArrayList<>());
            return new DoubleHistogram() {
                @Override
                public void record(double value) {
                    histogramValues.get(name).add(value);
                }
                @Override
                public void record(double value, MetricAttributes attributes) {
                    histogramValues.get(name).add(value);
                    lastAttrs.put(name, new LinkedHashMap<>(attributes.asMap()));
                }
            };
        }

        @Override
        public LongUpDownCounter createUpDownCounter(String name, String description, String unit) {
            return value -> {};
        }
    }
}
