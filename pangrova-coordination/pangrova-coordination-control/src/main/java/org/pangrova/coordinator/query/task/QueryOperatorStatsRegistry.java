package org.pangrova.coordinator.query.task;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.protocol.rest.task.TaskStats;
import org.pangrova.protocol.rest.task.TaskStatusReport;
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

    private final Cache<String, QueryTelemetry> byQuery = Caffeine.newBuilder()
            .maximumSize(MAX_QUERIES)
            .expireAfterAccess(RETENTION)
            .build();

    /** Borrows the immutable counters of an accepted Worker report. */
    public void record(TaskAttemptId attempt, TaskStatusReport report, TaskReport canonical) {
        NativeTaskStats nativeStats = canonical != null && canonical.hasNativeTaskStats()
                ? canonical.getNativeTaskStats() : null;
        TaskStats supplemental = report.supplementalStats();
        if (nativeStats == null
                ? supplemental == null || supplemental.operatorStats().isEmpty()
                : nativeStats.getOperatorStatsCount() == 0) {
            return;
        }
        var attempts = byQuery.get(attempt.getQueryOrJobId(), ignored -> new QueryTelemetry()).attempts;
        long sequence = report.reportSequence();
        attempts.compute(attempt, (ignored, previous) -> previous == null || sequence > previous.sequence()
                ? new Sample(report.workerId(), report.state(), sequence, nativeStats, supplemental) : previous);
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
        var observation = new QueryStageObservations(dag);
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

    private record Sample(String workerId, String state, long sequence, NativeTaskStats nativeStats,
            TaskStats supplemental) {
        Attempt toAttempt(TaskAttemptId attempt) {
            if (nativeStats != null) {
                var operators = nativeStats.getOperatorStatsList().stream()
                        .map(op -> new Metric(op.getOperatorType(), op.getPlanNodeId(), op.getInputRows(),
                                op.getOutputRows(), op.getInputBytes(), op.getOutputBytes(), op.getCpuNanos(),
                                op.getWallNanos(), op.getPeakMemoryBytes(), op.getSpillBytes()))
                        .toList();
                return toAttempt(attempt, nativeStats.getElapsedTimeMs(), nativeStats.getCpuTimeMs(),
                        nativeStats.getBackpressureWaitMs(), operators);
            }
            var operators = supplemental.operatorStats().stream()
                    .map(op -> new Metric(op.operatorType(), op.planNodeId(), op.inputRows(), op.outputRows(),
                            op.inputBytes(), op.outputBytes(), op.cpuNanos(), op.wallNanos(),
                            op.peakMemoryBytes(), op.spillBytes()))
                    .toList();
            return toAttempt(attempt, supplemental.elapsedTimeMs(), supplemental.cpuTimeMs(),
                    supplemental.backpressureWaitMs(), operators);
        }

        private Attempt toAttempt(TaskAttemptId attempt, long elapsedMs, long cpuMs, long backpressureMs,
                List<Metric> operators) {
            return new Attempt(attempt.getStageId(), attempt.getPartitionId(), attempt.getPlanEpoch(),
                    attempt.getAttemptId(), attempt.getTaskId(), workerId, state, sequence, elapsedMs,
                    cpuMs, backpressureMs, operators);
        }
    }
}
