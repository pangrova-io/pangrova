package org.pangrova.streaming.cdc;

import org.pangrova.common.lifecycle.ResourceCloser;
import org.pangrova.observability.api.metrics.DoubleHistogram;
import org.pangrova.observability.api.metrics.LongCounter;
import org.pangrova.observability.api.metrics.MetricAttributes;
import org.pangrova.observability.api.metrics.MetricsProvider;
import org.pangrova.observability.api.metrics.ObservableDoubleGauge;
import org.pangrova.streaming.api.source.SourceRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.function.Supplier;

/**
 * Metrics instrumentation for the CDC pipeline hot path.
 *
 * <p>Tracks records/bytes consumed, write duration, batches processed, failures,
 * consumer lag, commit latency, source poll duration, and pipeline uptime.
 * Each metric is tagged with the stream name for dimensional filtering.
 *
 * <h3>Metric Inventory</h3>
 * <ul>
 *   <li>{@code pangrova.streaming.cdc.records.consumed} — Counter: total records polled</li>
 *   <li>{@code pangrova.streaming.cdc.bytes.consumed} — Counter: key and value bytes polled</li>
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
final class CdcPipelineMetrics implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CdcPipelineMetrics.class);
    // Observations are written only by the polling thread. Report the first degraded sample per attempt.
    private boolean reportedFailure;
    private final ObservableDoubleGauge uptime;
    private final ObservableDoubleGauge lag;
    private volatile OptionalLong lastLag = OptionalLong.empty();

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

    /**
     * Creates a metrics instance backed by the given provider.
     *
     * @param provider   the metrics provider for creating instruments
     * @param streamName the CDC stream name, used as a dimension tag
     */
    CdcPipelineMetrics(MetricsProvider provider, String streamName) {
        long startTimeNanos = System.nanoTime();
        this.streamAttributes = MetricAttributes.of("stream", streamName);
        this.recordsConsumed = provider.createCounter(
            "pangrova.streaming.cdc.records.consumed",
            "Total CDC records consumed from the source",
            "{record}");
        this.bytesConsumed = provider.createCounter(
            "pangrova.streaming.cdc.bytes.consumed",
            "Bytes in the keys and values returned by source polls",
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
            "Total recovery and pipeline cycle failures",
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

        this.uptime = provider.createObservedGauge("pangrova.streaming.cdc.pipeline.uptime",
                "Time since pipeline execution started", "s", streamAttributes,
                () -> OptionalDouble.of((System.nanoTime() - startTimeNanos) / NANOS_PER_SECOND));
        try {
            this.lag = provider.createObservedGauge("pangrova.streaming.cdc.consumer.lag",
                    "Last known sum of unconsumed offset distances", "{offset}", streamAttributes, () -> {
                        OptionalLong sample = lastLag;
                        return sample.isPresent() ? OptionalDouble.of(sample.getAsLong()) : OptionalDouble.empty();
                    });
        } catch (RuntimeException | Error failure) {
            ResourceCloser.closeAll(failure, uptime::close);
            throw failure;
        }
    }

    @Override
    public void close() {
        ResourceCloser.closeAll(null, lag::close, uptime::close);
    }

    void recordWriteDuration(long durationNanos) {
        observe(() -> writeDuration.record(durationNanos / NANOS_PER_SECOND, streamAttributes));
    }

    void onBatchProcessed() {
        observe(() -> batchesProcessed.add(1, streamAttributes));
    }

    void onFailure() {
        observe(() -> failures.add(1, streamAttributes));
    }

    void recordPollDuration(long durationNanos) {
        observe(() -> pollDuration.record(durationNanos / NANOS_PER_SECOND, streamAttributes));
    }

    void recordCommitDuration(long durationNanos) {
        observe(() -> commitDuration.record(durationNanos / NANOS_PER_SECOND, streamAttributes));
    }

    void onPolled(List<SourceRecord> batch, Supplier<OptionalLong> currentLag) {
        lastLag = OptionalLong.empty();
        observe(() -> lastLag = currentLag.get());
        if (!batch.isEmpty()) {
            long bytes = 0;
            for (SourceRecord record : batch) {
                if (record.key() != null) bytes += record.key().length;
                if (record.value() != null) bytes += record.value().length;
            }
            long payloadBytes = bytes;
            observe(() -> recordsConsumed.add(batch.size(), streamAttributes));
            observe(() -> bytesConsumed.add(payloadBytes, streamAttributes));
        }
    }

    private void observe(Runnable observation) {
        try {
            observation.run();
        } catch (RuntimeException failure) {
            // Retrying an acknowledged batch or aborting a staged write because an exporter failed is unsafe.
            if (!reportedFailure) {
                reportedFailure = true;
                LOG.warn("event=cdc_metric_observation_failed dimensions={}", streamAttributes.asMap(), failure);
            }
        }
    }
}
