package org.pangrova.coordinator.query.task;

import org.pangrova.observability.api.metrics.DoubleHistogram;
import org.pangrova.observability.api.metrics.LongCounter;
import org.pangrova.observability.api.metrics.MetricAttributes;
import org.pangrova.observability.api.metrics.MetricsProvider;
import org.pangrova.query.profiling.OperatorStatsSource.DownstreamPreparation;
import org.pangrova.query.profiling.OperatorStatsSource.StageDispatchWork;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Process-wide aggregation of the same intervals observed by the per-query profile. */
final class QueryStageMetrics {
    private static final Logger LOG = LoggerFactory.getLogger(QueryStageMetrics.class);
    private static final List<Double> LATENCY_BUCKETS_MS =
            List.of(0.1, 0.5, 1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1_000.0, 5_000.0, 30_000.0);
    private final AtomicBoolean recordingFailureReported = new AtomicBoolean();
    private final DoubleHistogram dispatchLatency;
    private final DoubleHistogram preparationQueue;
    private final DoubleHistogram preparationRun;
    private final LongCounter preparationOutcomes;

    QueryStageMetrics(MetricsProvider provider) {
        dispatchLatency = provider.createHistogram("pangrova.coordinator.stage.dispatch.latency_ms",
                "Measured stage dispatch phase latency", "ms", LATENCY_BUCKETS_MS);
        preparationQueue = provider.createHistogram("pangrova.coordinator.stage.preparation.queue_ms",
                "Time from downstream preparation submission to start", "ms", LATENCY_BUCKETS_MS);
        preparationRun = provider.createHistogram("pangrova.coordinator.stage.preparation.run_ms",
                "Time from downstream preparation start to finish", "ms", LATENCY_BUCKETS_MS);
        preparationOutcomes = provider.createCounter("pangrova.coordinator.stage.preparation.outcomes",
                "Completed, skipped, failed or rejected downstream preparations", "preparations");
    }

    void recordDispatch(StageDispatchWork work) {
        try {
            dispatchLatency.record(millis(work.finishedNanos() - work.startedNanos()),
                    MetricAttributes.of("phase", work.phase().name(), "outcome",
                            work.successful() ? "SUCCESS" : "FAILED"));
        } catch (RuntimeException failure) {
            reportFailure(failure);
        }
    }

    void recordPreparation(DownstreamPreparation work) {
        try {
            var attributes = MetricAttributes.of("outcome", work.outcome().name());
            preparationOutcomes.add(1, attributes);
            // Rejected submissions never entered an executor. Do not invent queue or run intervals.
            if (work.startedNanos() != null) {
                preparationQueue.record(millis(work.startedNanos() - work.submittedNanos()), attributes);
                preparationRun.record(millis(work.finishedNanos() - work.startedNanos()), attributes);
            }
        } catch (RuntimeException failure) {
            reportFailure(failure);
        }
    }

    private void reportFailure(RuntimeException failure) {
        // Telemetry must not change the outcome of a scheduler operation, or
        // prevent release of its issuance ownership on rejection.
        if (recordingFailureReported.compareAndSet(false, true)) {
            LOG.warn("event=query_stage_metrics_recording_failed", failure);
        }
    }

    private static double millis(long nanos) {
        return (double) nanos / TimeUnit.MILLISECONDS.toNanos(1);
    }
}
