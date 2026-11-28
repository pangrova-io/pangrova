package org.pangrova.coordinator.query.task;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.pangrova.common.concurrency.ThreadSafe;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.observability.api.metrics.MetricsProvider;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.v2.NativeObjectReadStats;
import org.pangrova.protocol.v2.NativeResultDeliveryStats;
import org.pangrova.protocol.v2.NativeTaskStats;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.query.profiling.OperatorStatsSource;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/** Retains the latest measured counters for each exact issued attempt. */
@ThreadSafe
public final class QueryOperatorStatsRegistry implements OperatorStatsSource {

    private static final int MAX_QUERIES = 10_000;
    private static final Duration RETENTION = Duration.ofMinutes(15);
    private static final Comparator<Attempt> ATTEMPT_ORDER = Comparator.comparingInt(Attempt::stageId)
            .thenComparingInt(Attempt::partitionId).thenComparingLong(Attempt::planEpoch)
            .thenComparingInt(Attempt::attemptId).thenComparing(Attempt::taskId);

    private final QueryStageMetrics stageMetrics;

    public QueryOperatorStatsRegistry() {
        this(null);
    }

    /** Registers process-wide metrics once, when a provider is supplied. */
    public QueryOperatorStatsRegistry(MetricsProvider metricsProvider) {
        stageMetrics = metricsProvider == null ? null : new QueryStageMetrics(metricsProvider);
    }

    private final Cache<String, QueryTelemetry> byQuery = Caffeine.newBuilder()
            .maximumSize(MAX_QUERIES)
            .expireAfterAccess(RETENTION)
            .build();

    /** Borrows the immutable counters of an accepted Worker report. */
    public void record(TaskAttemptId attempt, TaskStatusReport report, TaskReport canonical) {
        NativeTaskStats stats = canonical != null && canonical.hasNativeTaskStats()
                ? canonical.getNativeTaskStats() : report.supplementalStats();
        var reads = canonical != null && canonical.hasNativeObjectReads() ? canonical.getNativeObjectReads() : null;
        var delivery = canonical != null && canonical.hasNativeResultDelivery()
                ? canonical.getNativeResultDelivery() : null;
        if (reads == null && delivery == null && (stats == null || stats.getOperatorStatsCount() == 0)) return;
        var sampleStats = stats == null ? NativeTaskStats.getDefaultInstance() : stats;
        var attempts = byQuery.get(attempt.getQueryOrJobId(), ignored -> new QueryTelemetry()).attempts;
        long sequence = report.reportSequence();
        attempts.compute(attempt, (ignored, previous) -> previous == null || sequence > previous.sequence()
                ? new Sample(report.workerId(), report.state(), sequence, sampleStats, reads, delivery) : previous);
    }

    @Override
    public List<Attempt> attemptStats(String queryId) {
        if (queryId == null) return List.of();
        var telemetry = byQuery.getIfPresent(queryId);
        if (telemetry == null) return List.of();
        return telemetry.attempts.entrySet().stream()
                .map(entry -> entry.getValue().toAttempt(entry.getKey()))
                .sorted(ATTEMPT_ORDER)
                .toList();
    }

    /** Copies only the public execution coordinates before the first stage can dispatch. */
    public QueryStageObservations beginExecution(String queryId, StageDAG dag) {
        var observation = new QueryStageObservations(dag, stageMetrics);
        byQuery.get(queryId, ignored -> new QueryTelemetry()).observations = observation;
        return observation;
    }

    @Override
    public ExecutionGraph executionGraph(String queryId) {
        var telemetry = byQuery.getIfPresent(queryId);
        var observations = telemetry == null ? null : telemetry.observations;
        return observations == null ? null : observations.snapshot();
    }

    private static final class QueryTelemetry {
        private final ConcurrentHashMap<TaskAttemptId, Sample> attempts = new ConcurrentHashMap<>();
        private volatile QueryStageObservations observations;
    }

    private record Sample(String workerId, String state, long sequence, NativeTaskStats stats,
            NativeObjectReadStats reads, NativeResultDeliveryStats delivery) {
        Attempt toAttempt(TaskAttemptId attempt) {
            var operators = stats.getOperatorStatsList().stream()
                    .map(op -> new Metric(op.getOperatorType(), op.getPlanNodeId(), op.getInputRows(),
                            op.getOutputRows(), op.getInputBytes(), op.getOutputBytes(), op.getCpuNanos(),
                            op.getWallNanos(), op.getPeakMemoryBytes(), op.getSpillBytes(), op.getBlockedNanos(),
                            op.getWaitForProducerNanos(), op.getWaitForConsumerNanos(), op.getWaitForSplitNanos(),
                            op.getWaitForConnectorNanos()))
                    .toList();
            return new Attempt(attempt.getStageId(), attempt.getPartitionId(), attempt.getPlanEpoch(),
                    attempt.getAttemptId(), attempt.getTaskId(), workerId, state, sequence,
                    stats.getElapsedTimeMs(), stats.getCpuTimeMs(), stats.getBackpressureWaitMs(),
                    operators, objectReads(), resultDelivery());
        }

        private ResultDelivery resultDelivery() {
            return delivery == null ? null : new ResultDelivery(delivery.getPayloadBatches(),
                    delivery.hasFirstPayloadNanos() ? delivery.getFirstPayloadNanos() : null,
                    delivery.getProducerEofNanos(), delivery.getTransportCompleteNanos());
        }

        private ObjectReads objectReads() {
            return reads == null ? null : new ObjectReads(reads.getRequestedBytes(), reads.getReturnedBytes(),
                    reads.getPayloadCalls(), reads.getMetadataCalls(), reads.getFailedCalls(),
                    reads.getS3SdkAttempts(), reads.getS3SdkRetries(), reads.getS3ResponseBodyBytes());
        }
    }
}
