package org.pangrova.query.runtime;

import org.pangrova.observability.api.metrics.DoubleHistogram;
import org.pangrova.observability.api.metrics.LongCounter;
import org.pangrova.observability.api.metrics.MetricAttributes;
import org.pangrova.observability.api.metrics.MetricsProvider;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Measures exact local lifecycle boundaries without implying remote retirement. */
final class QueryLifecycleMetrics {
    private static final Logger LOG = LoggerFactory.getLogger(QueryLifecycleMetrics.class);
    private static final List<Double> LATENCY_BUCKETS_MS =
            List.of(0.1, 1.0, 10.0, 50.0, 100.0, 500.0, 1_000.0, 5_000.0, 30_000.0, 120_000.0);
    private final AtomicBoolean timeoutMetricFailed = new AtomicBoolean();
    private final AtomicBoolean releaseMetricFailed = new AtomicBoolean();
    private final AtomicBoolean admissionMetricFailed = new AtomicBoolean();
    private final AtomicBoolean dispatchMetricFailed = new AtomicBoolean();
    private final LongCounter timedOut;
    private final DoubleHistogram localRelease;
    private final LongCounter admissionRejections;
    private final DoubleHistogram dispatchQueue;
    private final LongSupplier clock;

    QueryLifecycleMetrics(MetricsProvider provider, LongSupplier clock) {
        this.clock = clock;
        timedOut = provider == null ? null : provider.createCounter("pangrova.coordinator.queries.timed_out",
                "Queries whose timeout won the terminal transition", "queries");
        localRelease = provider == null ? null : provider.createHistogram(
                "pangrova.coordinator.queries.terminal_cleanup.local_release_ms",
                "Time from terminal cleanup entry until local ownership release", "ms",
                LATENCY_BUCKETS_MS);
        admissionRejections = provider == null ? null : provider.createCounter(
                "pangrova.coordinator.queries.admission.rejections",
                "Requests rejected by query admission or dispatch enqueue", "queries");
        dispatchQueue = provider == null ? null : provider.createHistogram(
                "pangrova.coordinator.queries.dispatch.queue_ms",
                "Time before the accepted dispatch executor callback starts", "ms", LATENCY_BUCKETS_MS);
    }

    void recordTimeout() {
        if (timedOut == null) return;
        try {
            timedOut.add(1);
        } catch (RuntimeException failure) {
            warnOnce(timeoutMetricFailed, failure);
        }
    }

    void recordLocalRelease(long nanos) {
        if (localRelease == null) return;
        try {
            localRelease.record((double) nanos / TimeUnit.MILLISECONDS.toNanos(1));
        } catch (RuntimeException failure) {
            warnOnce(releaseMetricFailed, failure);
        }
    }

    void recordRejection(AdmissionRejection reason) {
        if (admissionRejections == null) return;
        try {
            admissionRejections.add(1, reason.attributes);
        } catch (RuntimeException failure) {
            warnOnce(admissionMetricFailed, failure);
        }
    }

    long dispatchEnqueuedAt() {
        return dispatchQueue == null ? 0 : clock.getAsLong();
    }

    void recordDispatchStarted(long queuedAt) {
        if (dispatchQueue == null) return;
        try {
            dispatchQueue.record((double) (clock.getAsLong() - queuedAt) / TimeUnit.MILLISECONDS.toNanos(1));
        } catch (RuntimeException failure) {
            warnOnce(dispatchMetricFailed, failure);
        }
    }

    enum AdmissionRejection {
        ADMISSION_CLOSED,
        TENANT_LIMIT,
        THROTTLED,
        HEALTH_PROBE,
        EXECUTION_OWNERSHIP_UNAVAILABLE,
        QUEUE_FULL,
        EXECUTOR_STOPPING;

        private final MetricAttributes attributes = MetricAttributes.of("reason", name());
    }

    private static void warnOnce(AtomicBoolean reported, RuntimeException failure) {
        if (reported.compareAndSet(false, true)) {
            LOG.warn("event=query_lifecycle_metric_recording_failed", failure);
        }
    }
}
