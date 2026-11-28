package org.pangrova.worker.write;

import org.pangrova.observability.api.metrics.DoubleHistogram;
import org.pangrova.observability.api.metrics.LongCounter;
import org.pangrova.observability.api.metrics.MetricAttributes;
import org.pangrova.observability.api.metrics.MetricsProvider;
import org.pangrova.observability.api.tracing.TracingProvider;
import org.pangrova.observability.api.tracing.Span;
import org.pangrova.observability.api.enums.SpanKind;

import java.util.List;

import static org.pangrova.common.validation.Preconditions.checkNotNull;

/**
 * Observability instrumentation for the native write path.
 *
 * <p>Provides metrics (counters and histograms) and tracing spans for write
 * operations crossing the JNI boundary. All metric names follow the
 * conventions defined in {@code docs/metrics-conventions.md}.
 *
 * <p>Metrics emitted:
 * <ul>
 *   <li>{@code pangrova.worker.write.batch.duration} — histogram of per-batch append latency (seconds)</li>
 *   <li>{@code pangrova.worker.write.rows} — counter of total rows written</li>
 *   <li>{@code pangrova.worker.write.bytes} — counter of total bytes written (serialized columnar IPC size)</li>
 *   <li>{@code pangrova.worker.write.commit.duration} — histogram of commit latency (seconds)</li>
 *   <li>{@code pangrova.worker.write.sessions.failed} — counter of failed write sessions (abort or error)</li>
 *   <li>{@code pangrova.worker.write.files} — counter of data files written</li>
 *   <li>{@code pangrova.worker.write.commit.retries} — counter of commit retry attempts due to OCC conflicts</li>
 * </ul>
 *
 * <p>Tracing spans:
 * <ul>
 *   <li>{@code pangrova.worker.write.batch} — wraps each appendSerializedBatch call</li>
 *   <li>{@code pangrova.worker.write.commit} — wraps each commitWrite call</li>
 * </ul>
 *
 * <p>Thread safety: all instruments are immutable after construction. Individual
 * recordings are thread-safe per the OTel/SPI contract.
 */
public final class WriteMetrics {

    // Histogram boundaries for fast operations (batch appends typically < 1s)
    private static final List<Double> BATCH_DURATION_BOUNDARIES_S =
            List.of(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0);

    // Histogram boundaries for commit operations (can be slower, up to minutes for large writes)
    private static final List<Double> COMMIT_DURATION_BOUNDARIES_S =
            List.of(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0);

    private final DoubleHistogram batchDuration;
    private final LongCounter rowsWritten;
    private final LongCounter bytesWritten;
    private final DoubleHistogram commitDuration;
    private final LongCounter sessionsFailed;
    private final LongCounter filesWritten;
    private final LongCounter commitRetries;
    private final TracingProvider tracingProvider;

    /**
     * Creates write metrics instruments registered with the given providers.
     *
     * @param metricsProvider  the metrics provider for creating instruments
     * @param tracingProvider  the tracing provider for creating spans
     */
    public WriteMetrics(MetricsProvider metricsProvider, TracingProvider tracingProvider) {
        checkNotNull(metricsProvider, "metricsProvider");
        this.tracingProvider = checkNotNull(tracingProvider, "tracingProvider");

        this.batchDuration = metricsProvider.createHistogram(
                "pangrova.worker.write.batch.duration",
                "Duration of each write batch append operation",
                "s",
                BATCH_DURATION_BOUNDARIES_S);

        this.rowsWritten = metricsProvider.createCounter(
                "pangrova.worker.write.rows",
                "Total rows written through the native write path",
                "{row}");

        this.bytesWritten = metricsProvider.createCounter(
                "pangrova.worker.write.bytes",
                "Total bytes written (serialized columnar IPC batch size)",
                "By");

        this.commitDuration = metricsProvider.createHistogram(
                "pangrova.worker.write.commit.duration",
                "Duration of write commit operations",
                "s",
                COMMIT_DURATION_BOUNDARIES_S);

        this.sessionsFailed = metricsProvider.createCounter(
                "pangrova.worker.write.sessions.failed",
                "Total write sessions that failed (aborted or errored)",
                "{session}");

        this.filesWritten = metricsProvider.createCounter(
                "pangrova.worker.write.files",
                "Total data files written through the native write path",
                "{file}");

        this.commitRetries = metricsProvider.createCounter(
                "pangrova.worker.write.commit.retries",
                "Total commit retry attempts due to OCC conflicts",
                "{retry}");
    }

    /** Nanoseconds per second, for duration conversion. */
    private static final double NANOS_PER_SECOND = 1_000_000_000.0;

    /**
     * Records the duration of a batch append operation.
     *
     * @param durationNanos elapsed time in nanoseconds
     * @param rowCount      number of rows in the batch
     * @param byteCount     serialized byte size of the batch
     * @param catalogName   the catalog being written to
     */
    public void recordBatchAppend(long durationNanos, int rowCount, int byteCount, String catalogName) {
        double durationSeconds = durationNanos / NANOS_PER_SECOND;
        MetricAttributes attrs = MetricAttributes.of("connector.type", catalogName);
        batchDuration.record(durationSeconds, attrs);
        rowsWritten.add(rowCount, attrs);
        bytesWritten.add(byteCount, attrs);
    }

    /**
     * Records the duration of a commit operation.
     *
     * @param durationNanos elapsed time in nanoseconds
     * @param catalogName   the catalog being written to
     * @param success       whether the commit succeeded
     */
    public void recordCommit(long durationNanos, String catalogName, boolean success) {
        double durationSeconds = durationNanos / NANOS_PER_SECOND;
        MetricAttributes attrs = MetricAttributes.of("connector.type", catalogName);
        commitDuration.record(durationSeconds, attrs);
        if (!success) {
            sessionsFailed.add(1, attrs);
        }
    }

    /**
     * Records a write session failure (abort or error).
     *
     * @param catalogName the catalog that was being written to
     */
    public void recordSessionFailed(String catalogName) {
        MetricAttributes attrs = MetricAttributes.of("connector.type", catalogName);
        sessionsFailed.add(1, attrs);
    }

    /**
     * Records the number of data files written during a write session.
     *
     * @param fileCount   number of files written
     * @param catalogName the catalog being written to
     */
    public void recordFilesWritten(int fileCount, String catalogName) {
        if (fileCount > 0) {
            MetricAttributes attrs = MetricAttributes.of("connector.type", catalogName);
            filesWritten.add(fileCount, attrs);
        }
    }

    /**
     * Records commit retry attempts due to OCC conflicts.
     *
     * @param retryCount  number of retry attempts
     * @param catalogName the catalog being committed to
     */
    public void recordCommitRetries(int retryCount, String catalogName) {
        if (retryCount > 0) {
            MetricAttributes attrs = MetricAttributes.of("connector.type", catalogName);
            commitRetries.add(retryCount, attrs);
        }
    }

    /**
     * Starts a tracing span for a batch append operation.
     *
     * @param writeHandle the write session handle
     * @param catalogName the target catalog
     * @return a new span (caller must call {@link Span#end()})
     */
    public Span startBatchSpan(long writeHandle, String catalogName) {
        Span span = tracingProvider.startSpan("pangrova.worker.write.batch", SpanKind.INTERNAL);
        span.setAttribute("write.handle", writeHandle);
        span.setAttribute("connector.type", catalogName);
        return span;
    }

    /**
     * Starts a tracing span for a commit operation.
     *
     * @param writeHandle the write session handle
     * @param catalogName the target catalog
     * @return a new span (caller must call {@link Span#end()})
     */
    public Span startCommitSpan(long writeHandle, String catalogName) {
        Span span = tracingProvider.startSpan("pangrova.worker.write.commit", SpanKind.INTERNAL);
        span.setAttribute("write.handle", writeHandle);
        span.setAttribute("connector.type", catalogName);
        return span;
    }

    // Package-private accessors for testing

    DoubleHistogram getBatchDuration() {
        return batchDuration;
    }

    LongCounter getRowsWritten() {
        return rowsWritten;
    }

    LongCounter getBytesWritten() {
        return bytesWritten;
    }

    DoubleHistogram getCommitDuration() {
        return commitDuration;
    }

    LongCounter getSessionsFailed() {
        return sessionsFailed;
    }

    LongCounter getFilesWritten() {
        return filesWritten;
    }

    LongCounter getCommitRetries() {
        return commitRetries;
    }

    TracingProvider getTracingProvider() {
        return tracingProvider;
    }
}
