package org.pangrova.coordinator.query.task;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.protocol.rest.task.TaskStats;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.v2.TaskAttemptId;
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

    private final Cache<String, ConcurrentHashMap<TaskAttemptId, Sample>> byQuery = Caffeine.newBuilder()
            .maximumSize(MAX_QUERIES)
            .expireAfterAccess(RETENTION)
            .build();

    /** Borrows the immutable counters of an accepted Worker report. */
    public void record(TaskAttemptId attempt, TaskStatusReport report) {
        TaskStats stats = report.stats();
        if (stats == null || stats.operatorStats().isEmpty()) return;
        var attempts = byQuery.get(attempt.getQueryOrJobId(), ignored -> new ConcurrentHashMap<>());
        long sequence = report.reportSequence();
        attempts.compute(attempt, (ignored, previous) -> previous == null || sequence > previous.sequence()
                ? new Sample(report.workerId(), report.state(), sequence, stats) : previous);
    }

    @Override
    public List<Attempt> attemptStats(String queryId) {
        if (queryId == null) return List.of();
        var attempts = byQuery.getIfPresent(queryId);
        if (attempts == null) return List.of();
        return attempts.entrySet().stream()
                .map(entry -> entry.getValue().toAttempt(entry.getKey()))
                .sorted(ATTEMPT_ORDER)
                .toList();
    }

    private record Sample(String workerId, String state, long sequence, TaskStats stats) {
        Attempt toAttempt(TaskAttemptId attempt) {
            var operators = stats.operatorStats().stream()
                    .map(op -> new Metric(op.operatorType(), op.planNodeId(), op.inputRows(), op.outputRows(),
                            op.inputBytes(), op.outputBytes(), op.cpuNanos(), op.wallNanos(),
                            op.peakMemoryBytes(), op.spillBytes()))
                    .toList();
            return new Attempt(attempt.getStageId(), attempt.getPartitionId(), attempt.getPlanEpoch(),
                    attempt.getAttemptId(), attempt.getTaskId(), workerId, state, sequence, stats.elapsedTimeMs(),
                    stats.cpuTimeMs(), stats.backpressureWaitMs(), operators);
        }
    }
}
