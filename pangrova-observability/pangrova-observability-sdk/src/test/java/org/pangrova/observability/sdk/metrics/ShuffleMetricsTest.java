package org.pangrova.observability.sdk.metrics;

import org.pangrova.observability.api.metrics.DoubleHistogram;
import org.pangrova.observability.api.metrics.LongCounter;
import org.pangrova.observability.api.metrics.LongUpDownCounter;
import org.pangrova.observability.api.metrics.MetricAttributes;
import org.pangrova.observability.spi.metrics.MetricsProvider;
import org.pangrova.observability.api.metrics.ObservableDoubleGauge;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the {@link ShuffleMetrics} surface for F-CHAIN-SHUFFLE-08 — the
 * end-to-end shuffle observability that AQE and operators need from the
 * native Flight data plane.
 */
class ShuffleMetricsTest {

    @Test
    void registersAllShuffleInstrumentsWithExpectedNamesAndUnits() {
        var recorder = new RecordingProvider();
        new ShuffleMetrics(recorder);

        assertThat(recorder.counterNames).containsExactlyInAnyOrder(
                "pangrova.shuffle.bytes.sent",
                "pangrova.shuffle.bytes.received",
                "pangrova.shuffle.partitions.served",
                "pangrova.shuffle.requests",
                "pangrova.shuffle.backpressure.events");
        assertThat(recorder.histogramNames).containsExactlyInAnyOrder(
                "pangrova.shuffle.send.duration",
                "pangrova.shuffle.partition.size.bytes",
                "pangrova.shuffle.request.duration");
        assertThat(recorder.upDownCounterNames).containsExactly(
                "pangrova.shuffle.active.connections");

        assertThat(recorder.unitFor("pangrova.shuffle.bytes.sent")).isEqualTo("By");
        assertThat(recorder.unitFor("pangrova.shuffle.bytes.received")).isEqualTo("By");
        assertThat(recorder.unitFor("pangrova.shuffle.partitions.served")).isEqualTo("{partition}");
        assertThat(recorder.unitFor("pangrova.shuffle.send.duration")).isEqualTo("s");
        assertThat(recorder.unitFor("pangrova.shuffle.partition.size.bytes")).isEqualTo("By");
        assertThat(recorder.unitFor("pangrova.shuffle.requests")).isEqualTo("{request}");
        assertThat(recorder.unitFor("pangrova.shuffle.request.duration")).isEqualTo("s");
        assertThat(recorder.unitFor("pangrova.shuffle.backpressure.events")).isEqualTo("{event}");
        assertThat(recorder.unitFor("pangrova.shuffle.active.connections")).isEqualTo("{connection}");
    }

    @Test
    void allShuffleMetricNamesUseDotSeparatedLowercaseAndPangrovaPrefix() {
        var recorder = new RecordingProvider();
        new ShuffleMetrics(recorder);

        for (String name : recorder.allNames()) {
            assertThat(name).startsWith("pangrova.shuffle.");
            assertThat(name).matches("[a-z][a-z0-9]+(\\.[a-z][a-z0-9]*)+");
        }
    }

    @Test
    void shuffleHistogramsUseDurationOrSizeNotLatency() {
        var recorder = new RecordingProvider();
        new ShuffleMetrics(recorder);

        for (String name : recorder.histogramNames) {
            assertThat(name).doesNotContain("latency");
            assertThat(name).matches(".*(duration|size).*");
        }
    }

    @Test
    void recordSendCompletedEmitsBytesSentAndPartitionsServed() {
        var recorder = new RecordingProvider();
        var metrics = new ShuffleMetrics(recorder);

        Map<Integer, Long> partitionBytes = new LinkedHashMap<>();
        partitionBytes.put(0, 1024L);
        partitionBytes.put(1, 2048L);
        partitionBytes.put(2, 4096L);

        metrics.recordSendCompleted("stage-7", 7168L, partitionBytes, 1.5);

        long bytesSent = recorder.counterTotal("pangrova.shuffle.bytes.sent");
        long partitionsServed = recorder.counterTotal("pangrova.shuffle.partitions.served");
        long bytesReceived = recorder.counterTotal("pangrova.shuffle.bytes.received");

        assertThat(bytesSent).isEqualTo(7168L);
        assertThat(partitionsServed).isEqualTo(3L);
        assertThat(bytesReceived).isZero();

        assertThat(recorder.histogramObservations("pangrova.shuffle.send.duration"))
                .containsExactly(1.5);
        assertThat(recorder.histogramObservations("pangrova.shuffle.partition.size.bytes"))
                .containsExactly(1024.0, 2048.0, 4096.0);
    }

    @Test
    void recordSendCompletedAttachesStageIdAttribute() {
        var recorder = new RecordingProvider();
        var metrics = new ShuffleMetrics(recorder);

        metrics.recordSendCompleted("stage-99", 100L, Map.of(0, 100L), 0.5);

        MetricAttributes lastAttrs = recorder.lastAttributesFor("pangrova.shuffle.bytes.sent");
        assertThat(lastAttrs).isNotNull();
        assertThat(lastAttrs.asMap()).containsEntry("stage.id", "stage-99");
    }

    @Test
    void recordSendCompletedSkipsHistogramWhenDurationIsZero() {
        var recorder = new RecordingProvider();
        var metrics = new ShuffleMetrics(recorder);

        metrics.recordSendCompleted("stage-1", 64L, Map.of(0, 64L), 0.0);

        assertThat(recorder.histogramObservations("pangrova.shuffle.send.duration"))
                .isEmpty();
        assertThat(recorder.histogramObservations("pangrova.shuffle.partition.size.bytes"))
                .containsExactly(64.0);
    }

    @Test
    void recordReceiveCompletedEmitsOnlyBytesReceived() {
        var recorder = new RecordingProvider();
        var metrics = new ShuffleMetrics(recorder);

        metrics.recordReceiveCompleted("stage-3", 9999L);

        assertThat(recorder.counterTotal("pangrova.shuffle.bytes.received")).isEqualTo(9999L);
        assertThat(recorder.counterTotal("pangrova.shuffle.bytes.sent")).isZero();
        assertThat(recorder.counterTotal("pangrova.shuffle.partitions.served")).isZero();
        assertThat(recorder.histogramObservations("pangrova.shuffle.send.duration")).isEmpty();
    }

    @Test
    void recordReceiveCompletedIsNoOpWhenZeroBytes() {
        var recorder = new RecordingProvider();
        var metrics = new ShuffleMetrics(recorder);

        metrics.recordReceiveCompleted("stage-3", 0L);

        assertThat(recorder.counterTotal("pangrova.shuffle.bytes.received")).isZero();
    }

    @Test
    void recordSendCompletedHandlesEmptyPartitionMap() {
        var recorder = new RecordingProvider();
        var metrics = new ShuffleMetrics(recorder);

        metrics.recordSendCompleted("stage-2", 0L, Map.of(), 0.0);

        assertThat(recorder.counterTotal("pangrova.shuffle.bytes.sent")).isZero();
        assertThat(recorder.counterTotal("pangrova.shuffle.partitions.served")).isZero();
        assertThat(recorder.histogramObservations("pangrova.shuffle.partition.size.bytes")).isEmpty();
    }

    // -- Per-request / live data-plane metrics (F-SHUFFLE-07) --

    @Test
    void recordRequestIncrementsRequestCounterAndDuration() {
        var recorder = new RecordingProvider();
        var metrics = new ShuffleMetrics(recorder);

        metrics.recordRequest("readPartition", 0.25, 4096);

        assertThat(recorder.counterTotal("pangrova.shuffle.requests")).isEqualTo(1);
        assertThat(recorder.histogramObservations("pangrova.shuffle.request.duration"))
                .containsExactly(0.25);
        // bytes also recorded via shuffleBytesSent
        assertThat(recorder.counterTotal("pangrova.shuffle.bytes.sent")).isEqualTo(4096);
    }

    @Test
    void recordRequestSkipsDurationWhenZero() {
        var recorder = new RecordingProvider();
        var metrics = new ShuffleMetrics(recorder);

        metrics.recordRequest("writeBatch", 0.0, 0);

        assertThat(recorder.counterTotal("pangrova.shuffle.requests")).isEqualTo(1);
        assertThat(recorder.histogramObservations("pangrova.shuffle.request.duration")).isEmpty();
        assertThat(recorder.counterTotal("pangrova.shuffle.bytes.sent")).isZero();
    }

    @Test
    void recordRequestAttachesRpcMethodAttribute() {
        var recorder = new RecordingProvider();
        var metrics = new ShuffleMetrics(recorder);

        metrics.recordRequest("readPartition", 0.1, 512);

        MetricAttributes attrs = recorder.lastAttributesFor("pangrova.shuffle.requests");
        assertThat(attrs).isNotNull();
        assertThat(attrs.asMap()).containsEntry("rpc.method", "readPartition");
    }

    @Test
    void recordBackpressureEventIncrementsCounter() {
        var recorder = new RecordingProvider();
        var metrics = new ShuffleMetrics(recorder);

        metrics.recordBackpressureEvent("writeBatch");
        metrics.recordBackpressureEvent("writeBatch");

        assertThat(recorder.counterTotal("pangrova.shuffle.backpressure.events")).isEqualTo(2);
    }

    @Test
    void connectionOpenedAndClosedTracksActiveConnections() {
        var recorder = new RecordingProvider();
        var metrics = new ShuffleMetrics(recorder);

        metrics.connectionOpened();
        metrics.connectionOpened();
        assertThat(recorder.upDownCounterTotal("pangrova.shuffle.active.connections")).isEqualTo(2);

        metrics.connectionClosed();
        assertThat(recorder.upDownCounterTotal("pangrova.shuffle.active.connections")).isEqualTo(1);

        metrics.connectionClosed();
        assertThat(recorder.upDownCounterTotal("pangrova.shuffle.active.connections")).isZero();
    }

    @Test
    void multipleRequestsAccumulateCorrectly() {
        var recorder = new RecordingProvider();
        var metrics = new ShuffleMetrics(recorder);

        metrics.recordRequest("readPartition", 0.1, 1024);
        metrics.recordRequest("readPartition", 0.2, 2048);
        metrics.recordRequest("writeBatch", 0.05, 512);

        assertThat(recorder.counterTotal("pangrova.shuffle.requests")).isEqualTo(3);
        assertThat(recorder.counterTotal("pangrova.shuffle.bytes.sent")).isEqualTo(3584);
        assertThat(recorder.histogramObservations("pangrova.shuffle.request.duration"))
                .containsExactly(0.1, 0.2, 0.05);
    }

    /**
     * In-memory test recorder that captures recorded values and the
     * {@link MetricAttributes} they were tagged with.
     */
    static final class RecordingProvider implements MetricsProvider {
        final List<String> counterNames = new ArrayList<>();
        final List<String> histogramNames = new ArrayList<>();
        final List<String> upDownCounterNames = new ArrayList<>();
        private final Map<String, RecordingCounter> counters = new LinkedHashMap<>();
        private final Map<String, RecordingHistogram> histograms = new LinkedHashMap<>();
        private final Map<String, RecordingUpDownCounter> upDownCounters = new LinkedHashMap<>();
        private final Map<String, String> units = new LinkedHashMap<>();

        @Override
        public LongCounter createCounter(String name, String description, String unit) {
            counterNames.add(name);
            units.put(name, unit);
            RecordingCounter counter = new RecordingCounter();
            counters.put(name, counter);
            return counter;
        }

        @Override
        public ObservableDoubleGauge createGauge(String name, String description,
                                                  String unit, Supplier<Double> supplier) {
            units.put(name, unit);
            return () -> { /* This recording fixture retains no callback. */ };
        }

        @Override
        public DoubleHistogram createHistogram(String name, String description,
                                                String unit, List<Double> boundaries) {
            histogramNames.add(name);
            units.put(name, unit);
            RecordingHistogram histogram = new RecordingHistogram();
            histograms.put(name, histogram);
            return histogram;
        }

        @Override
        public LongUpDownCounter createUpDownCounter(String name, String description, String unit) {
            upDownCounterNames.add(name);
            units.put(name, unit);
            RecordingUpDownCounter udc = new RecordingUpDownCounter();
            upDownCounters.put(name, udc);
            return udc;
        }

        List<String> allNames() {
            List<String> all = new ArrayList<>(counterNames);
            all.addAll(histogramNames);
            all.addAll(upDownCounterNames);
            return all;
        }

        String unitFor(String name) {
            return units.get(name);
        }

        long counterTotal(String name) {
            RecordingCounter c = counters.get(name);
            return c == null ? 0 : c.total;
        }

        long upDownCounterTotal(String name) {
            RecordingUpDownCounter c = upDownCounters.get(name);
            return c == null ? 0 : c.value.get();
        }

        List<Double> histogramObservations(String name) {
            RecordingHistogram h = histograms.get(name);
            return h == null ? List.of() : h.observations;
        }

        MetricAttributes lastAttributesFor(String name) {
            RecordingCounter c = counters.get(name);
            return c == null ? null : c.lastAttributes;
        }
    }

    static final class RecordingCounter implements LongCounter {
        long total = 0;
        MetricAttributes lastAttributes;

        @Override
        public void add(long value) {
            total += value;
        }

        @Override
        public void add(long value, MetricAttributes attributes) {
            total += value;
            lastAttributes = attributes;
        }
    }

    static final class RecordingHistogram implements DoubleHistogram {
        final List<Double> observations = new ArrayList<>();

        @Override
        public void record(double value) {
            observations.add(value);
        }

        @Override
        public void record(double value, MetricAttributes attributes) {
            observations.add(value);
        }
    }

    static final class RecordingUpDownCounter implements LongUpDownCounter {
        final AtomicLong value = new AtomicLong(0);

        @Override
        public void add(long delta) {
            value.addAndGet(delta);
        }

        @Override
        public void add(long delta, MetricAttributes attributes) {
            value.addAndGet(delta);
        }
    }
}
