package org.pangrova.worker.write;

import org.pangrova.observability.noop.NoOpMetricsProvider;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.observability.api.metrics.DoubleHistogram;
import org.pangrova.observability.api.metrics.LongCounter;
import org.pangrova.observability.api.metrics.MetricAttributes;
import org.pangrova.observability.api.metrics.MetricsProvider;
import org.pangrova.observability.api.metrics.LongUpDownCounter;
import org.pangrova.observability.api.metrics.ObservableDoubleGauge;
import org.pangrova.observability.api.tracing.Span;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link WriteMetrics} observability instrumentation.
 * Verifies that metrics are recorded correctly for batch appends,
 * commits, and failures.
 */
class WriteMetricsTest {

    @Test
    void recordBatchAppendIncrementsBothCounterAndHistogram() {
        var provider = new RecordingMetricsProvider();
        var metrics = new WriteMetrics(provider, new NoOpTracingProvider());

        metrics.recordBatchAppend(50_000_000L, 1000, 65536, "iceberg");

        assertThat(provider.counterValue("pangrova.worker.write.rows")).isEqualTo(1000);
        assertThat(provider.counterValue("pangrova.worker.write.bytes")).isEqualTo(65536);
        assertThat(provider.histogramRecordCount("pangrova.worker.write.batch.duration")).isEqualTo(1);
    }

    @Test
    void recordBatchAppendAccumulatesAcrossCalls() {
        var provider = new RecordingMetricsProvider();
        var metrics = new WriteMetrics(provider, new NoOpTracingProvider());

        metrics.recordBatchAppend(10_000_000L, 500, 32768, "paimon");
        metrics.recordBatchAppend(20_000_000L, 300, 16384, "paimon");

        assertThat(provider.counterValue("pangrova.worker.write.rows")).isEqualTo(800);
        assertThat(provider.counterValue("pangrova.worker.write.bytes")).isEqualTo(49152);
        assertThat(provider.histogramRecordCount("pangrova.worker.write.batch.duration")).isEqualTo(2);
    }

    @Test
    void recordCommitSuccessRecordsHistogramButNotFailureCounter() {
        var provider = new RecordingMetricsProvider();
        var metrics = new WriteMetrics(provider, new NoOpTracingProvider());

        metrics.recordCommit(100_000_000L, "iceberg", true);

        assertThat(provider.histogramRecordCount("pangrova.worker.write.commit.duration")).isEqualTo(1);
        assertThat(provider.counterValue("pangrova.worker.write.sessions.failed")).isZero();
    }

    @Test
    void recordCommitFailureRecordsBothHistogramAndFailureCounter() {
        var provider = new RecordingMetricsProvider();
        var metrics = new WriteMetrics(provider, new NoOpTracingProvider());

        metrics.recordCommit(200_000_000L, "iceberg", false);

        assertThat(provider.histogramRecordCount("pangrova.worker.write.commit.duration")).isEqualTo(1);
        assertThat(provider.counterValue("pangrova.worker.write.sessions.failed")).isEqualTo(1);
    }

    @Test
    void recordSessionFailedIncrementsFailureCounter() {
        var provider = new RecordingMetricsProvider();
        var metrics = new WriteMetrics(provider, new NoOpTracingProvider());

        metrics.recordSessionFailed("iceberg");
        metrics.recordSessionFailed("paimon");

        assertThat(provider.counterValue("pangrova.worker.write.sessions.failed")).isEqualTo(2);
    }

    @Test
    void recordFilesWrittenIncrementsFileCounter() {
        var provider = new RecordingMetricsProvider();
        var metrics = new WriteMetrics(provider, new NoOpTracingProvider());

        metrics.recordFilesWritten(3, "iceberg");
        metrics.recordFilesWritten(2, "paimon");

        assertThat(provider.counterValue("pangrova.worker.write.files")).isEqualTo(5);
    }

    @Test
    void recordFilesWrittenSkipsZeroCount() {
        var provider = new RecordingMetricsProvider();
        var metrics = new WriteMetrics(provider, new NoOpTracingProvider());

        metrics.recordFilesWritten(0, "iceberg");

        assertThat(provider.counterValue("pangrova.worker.write.files")).isZero();
    }

    @Test
    void recordCommitRetriesIncrementsRetryCounter() {
        var provider = new RecordingMetricsProvider();
        var metrics = new WriteMetrics(provider, new NoOpTracingProvider());

        metrics.recordCommitRetries(2, "iceberg");
        metrics.recordCommitRetries(1, "paimon");

        assertThat(provider.counterValue("pangrova.worker.write.commit.retries")).isEqualTo(3);
    }

    @Test
    void recordCommitRetriesSkipsZeroCount() {
        var provider = new RecordingMetricsProvider();
        var metrics = new WriteMetrics(provider, new NoOpTracingProvider());

        metrics.recordCommitRetries(0, "iceberg");

        assertThat(provider.counterValue("pangrova.worker.write.commit.retries")).isZero();
    }

    @Test
    void registersAllSevenInstrumentsAtConstruction() {
        var provider = new RecordingMetricsProvider();
        new WriteMetrics(provider, new NoOpTracingProvider());

        assertThat(provider.counters.keySet()).containsExactlyInAnyOrder(
                "pangrova.worker.write.rows",
                "pangrova.worker.write.bytes",
                "pangrova.worker.write.sessions.failed",
                "pangrova.worker.write.files",
                "pangrova.worker.write.commit.retries");
        assertThat(provider.histograms.keySet()).containsExactlyInAnyOrder(
                "pangrova.worker.write.batch.duration",
                "pangrova.worker.write.commit.duration");
    }

    @Test
    void startBatchSpanReturnsNonNullSpan() {
        var metrics = new WriteMetrics(new NoOpMetricsProvider(), new NoOpTracingProvider());

        Span span = metrics.startBatchSpan(1L, "iceberg");
        assertThat(span).isNotNull();
        span.end(); // must not throw
    }

    @Test
    void startCommitSpanReturnsNonNullSpan() {
        var metrics = new WriteMetrics(new NoOpMetricsProvider(), new NoOpTracingProvider());

        Span span = metrics.startCommitSpan(2L, "paimon");
        assertThat(span).isNotNull();
        span.end(); // must not throw
    }

    @Test
    void noOpMetricsProviderDoesNotThrow() {
        // Verify that WriteMetrics works safely with NoOp implementations
        var metrics = new WriteMetrics(new NoOpMetricsProvider(), new NoOpTracingProvider());

        metrics.recordBatchAppend(1_000_000L, 100, 4096, "test");
        metrics.recordCommit(5_000_000L, "test", true);
        metrics.recordCommit(5_000_000L, "test", false);
        metrics.recordSessionFailed("test");
        // No exception = success
    }

    // --- Test infrastructure: recording metrics provider ---

    /**
     * A simple metrics provider that records counter increments and histogram
     * record counts for verification in tests.
     */
    private static final class RecordingMetricsProvider implements MetricsProvider {

        private final java.util.Map<String, RecordingCounter> counters = new java.util.HashMap<>();
        private final java.util.Map<String, RecordingHistogram> histograms = new java.util.HashMap<>();

        long counterValue(String name) {
            RecordingCounter counter = counters.get(name);
            return counter != null ? counter.total.get() : 0;
        }

        long histogramRecordCount(String name) {
            RecordingHistogram hist = histograms.get(name);
            return hist != null ? hist.recordCount.get() : 0;
        }

        @Override
        public LongCounter createCounter(String name, String description, String unit) {
            var counter = new RecordingCounter();
            counters.put(name, counter);
            return counter;
        }

        @Override
        public ObservableDoubleGauge createGauge(String name, String description,
                                                  String unit, Supplier<Double> supplier) {
            return () -> { /* This recording fixture retains no callback. */ };
        }

        @Override
        public DoubleHistogram createHistogram(String name, String description,
                                               String unit, List<Double> boundaries) {
            var hist = new RecordingHistogram();
            histograms.put(name, hist);
            return hist;
        }

        @Override
        public LongUpDownCounter createUpDownCounter(String name, String description, String unit) {
            return new LongUpDownCounter() {
                @Override public void add(long value) {}
                @Override public void add(long value, MetricAttributes attributes) {}
            };
        }
    }

    private static final class RecordingCounter implements LongCounter {
        final AtomicLong total = new AtomicLong();

        @Override
        public void add(long value) {
            total.addAndGet(value);
        }

        @Override
        public void add(long value, MetricAttributes attributes) {
            total.addAndGet(value);
        }
    }

    private static final class RecordingHistogram implements DoubleHistogram {
        final AtomicLong recordCount = new AtomicLong();

        @Override
        public void record(double value) {
            recordCount.incrementAndGet();
        }

        @Override
        public void record(double value, MetricAttributes attributes) {
            recordCount.incrementAndGet();
        }
    }
}
