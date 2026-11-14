package org.pangrova.streaming.cdc;

import org.pangrova.observability.api.metrics.DoubleHistogram;
import org.pangrova.observability.api.metrics.LongCounter;
import org.pangrova.observability.api.metrics.MetricAttributes;
import org.pangrova.observability.api.metrics.MetricsProvider;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Metrics instrumentation for the CDC pipeline hot path.
 *
 * <p>Tracks records/bytes consumed, write duration, batches processed, failures,
 * consumer lag, commit latency, source poll duration, and pipeline uptime.
 * Each metric is tagged with the stream name for dimensional filtering.
 *
 * <p>When no {@link MetricsProvider} is available (e.g., in tests), use the
 * {@link #noop()} factory which returns a zero-overhead instance where all
 * recording methods are no-ops.
 *
 * <h3>Metric Inventory</h3>
 * <ul>
 *   <li>{@code pangrova.streaming.cdc.records.consumed} — Counter: total records polled</li>
 *   <li>{@code pangrova.streaming.cdc.bytes.consumed} — Counter: estimated bytes polled</li>
 *   <li>{@code pangrova.streaming.cdc.batches.processed} — Counter: batches written + committed</li>
 *   <li>{@code pangrova.streaming.cdc.write.duration} — Histogram: batch write time (seconds)</li>
 *   <li>{@code pangrova.streaming.cdc.failures} — Counter: pipeline cycle failures</li>
 *   <li>{@code pangrova.streaming.cdc.consumer.lag} — Gauge: sum of per-partition consumer lag</li>
 *   <li>{@code pangrova.streaming.cdc.commit.duration} — Histogram: offset commit time (seconds)</li>
 *   <li>{@code pangrova.streaming.cdc.source.poll.duration} — Histogram: source poll time (seconds)</li>
 *   <li>{@code pangrova.streaming.cdc.pipeline.uptime} — Gauge: seconds since pipeline start</li>
 * </ul>
 *
 * @see CdcPipelineLoop
 * @see MetricsProvider
 */
public final class CdcPipelineMetrics {

    private static final CdcPipelineMetrics NOOP = new CdcPipelineMetrics();

    private static final double NANOS_PER_SECOND = 1_000_000_000.0;

    private static final List<Double> WRITE_DURATION_BOUNDARIES_S = List.of(
        0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0, 10.0);

    private static final List<Double> COMMIT_DURATION_BOUNDARIES_S = List.of(
        0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0);

    private static final List<Double> POLL_DURATION_BOUNDARIES_S = List.of(
        0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0, 10.0);

    private final LongCounter recordsConsumed;
    private final LongCounter bytesConsumed;
    private final LongCounter batchesProcessed;
    private final DoubleHistogram writeDuration;
    private final LongCounter failures;
    private final DoubleHistogram commitDuration;
    private final DoubleHistogram pollDuration;
    private final MetricAttributes streamAttributes;

    // Mutable state for gauge callbacks — written by pipeline thread, read by metrics collector.
    private final AtomicLong consumerLag = new AtomicLong(0);
    private final long startTimeNanos;

    /**
     * Creates a no-op metrics instance where all recording methods are no-ops.
     * Use this in tests or when no MetricsProvider is configured.
     */
    public static CdcPipelineMetrics noop() {
        return NOOP;
    }

    /**
     * Creates a metrics instance backed by the given provider.
     *
     * @param provider   the metrics provider for creating instruments
     * @param streamName the CDC stream name, used as a dimension tag
     */
    public CdcPipelineMetrics(MetricsProvider provider, String streamName) {
        this.startTimeNanos = System.nanoTime();
        this.streamAttributes = MetricAttributes.of("stream", streamName);
        this.recordsConsumed = provider.createCounter(
            "pangrova.streaming.cdc.records.consumed",
            "Total CDC records consumed from the source",
            "{record}");
        this.bytesConsumed = provider.createCounter(
            "pangrova.streaming.cdc.bytes.consumed",
            "Estimated bytes consumed from the source",
            "By");
        this.batchesProcessed = provider.createCounter(
            "pangrova.streaming.cdc.batches.processed",
            "Total batches successfully processed (written and committed)",
            "{batch}");
        this.writeDuration = provider.createHistogram(
            "pangrova.streaming.cdc.write.duration",
            "Time taken to write a batch to the lake table",
            "s",
            WRITE_DURATION_BOUNDARIES_S);
        this.failures = provider.createCounter(
            "pangrova.streaming.cdc.failures",
            "Total pipeline cycle failures",
            "{error}");
        this.commitDuration = provider.createHistogram(
            "pangrova.streaming.cdc.commit.duration",
            "Time taken to commit source offsets after a successful write",
            "s",
            COMMIT_DURATION_BOUNDARIES_S);
        this.pollDuration = provider.createHistogram(
            "pangrova.streaming.cdc.source.poll.duration",
            "Time taken by the source reader to poll a batch of records",
            "s",
            POLL_DURATION_BOUNDARIES_S);

        // Consumer lag gauge — reports the latest lag value set by the pipeline thread.
        provider.createGauge(
            "pangrova.streaming.cdc.consumer.lag",
            "Sum of per-partition consumer lag (end-of-log offset minus consumed offset)",
            "{record}",
            () -> (double) consumerLag.get());

        // Pipeline uptime gauge — reports seconds since pipeline start.
        provider.createGauge(
            "pangrova.streaming.cdc.pipeline.uptime",
            "Seconds since the CDC pipeline started",
            "s",
            () -> (System.nanoTime() - startTimeNanos) / NANOS_PER_SECOND);
    }

    /** No-op constructor for the singleton NOOP instance. */
    private CdcPipelineMetrics() {
        this.startTimeNanos = System.nanoTime();
        this.streamAttributes = MetricAttributes.empty();
        this.recordsConsumed = NoOpLongCounter.INSTANCE;
        this.bytesConsumed = NoOpLongCounter.INSTANCE;
        this.batchesProcessed = NoOpLongCounter.INSTANCE;
        this.writeDuration = NoOpDoubleHistogram.INSTANCE;
        this.failures = NoOpLongCounter.INSTANCE;
        this.commitDuration = NoOpDoubleHistogram.INSTANCE;
        this.pollDuration = NoOpDoubleHistogram.INSTANCE;
    }

    /**
     * Records that a batch of records has been consumed from the source.
     *
     * @param records number of records polled
     * @param bytes   estimated byte size of the batch
     */
    public void onBatchConsumed(int records, long bytes) {
        recordsConsumed.add(records, streamAttributes);
        bytesConsumed.add(bytes, streamAttributes);
    }

    /**
     * Records the write duration for a single batch write operation.
     *
     * @param durationMs time in milliseconds to write the batch (converted to seconds internally)
     */
    public void recordWriteDuration(long durationMs) {
        writeDuration.record(durationMs / 1000.0, streamAttributes);
    }

    /**
     * Records that a batch has been fully processed (written + offsets committed).
     */
    public void onBatchProcessed() {
        batchesProcessed.add(1, streamAttributes);
    }

    /**
     * Records a pipeline cycle failure.
     */
    public void onFailure() {
        failures.add(1, streamAttributes);
    }

    /**
     * Records the duration of a source poll operation.
     *
     * @param durationMs time in milliseconds for the poll call
     */
    public void recordPollDuration(long durationMs) {
        pollDuration.record(durationMs / 1000.0, streamAttributes);
    }

    /**
     * Records the duration of an offset commit operation.
     *
     * @param durationMs time in milliseconds for the commit call
     */
    public void recordCommitDuration(long durationMs) {
        commitDuration.record(durationMs / 1000.0, streamAttributes);
    }

    /**
     * Updates the consumer lag gauge with the latest aggregate lag value.
     *
     * <p>The lag is the sum of (end-of-log offset - current consumed offset)
     * across all assigned partitions. Called by the pipeline loop after each
     * successful poll cycle.</p>
     *
     * @param lag total consumer lag in records across all partitions
     */
    public void recordConsumerLag(long lag) {
        consumerLag.set(lag);
    }

    // --- Inline no-op implementations to avoid dependency on pangrova-observability ---

    private enum NoOpLongCounter implements LongCounter {
        INSTANCE;

        @Override
        public void add(long value) {
            // no-op
        }

        @Override
        public void add(long value, MetricAttributes attributes) {
            // no-op
        }
    }

    private enum NoOpDoubleHistogram implements DoubleHistogram {
        INSTANCE;

        @Override
        public void record(double value) {
            // no-op
        }

        @Override
        public void record(double value, MetricAttributes attributes) {
            // no-op
        }
    }
}
