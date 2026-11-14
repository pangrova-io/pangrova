package org.pangrova.coordinator.query.task;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.protocol.rest.task.OperatorStats;
import org.pangrova.query.profiling.OperatorStatsSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Retains the latest cumulative operator counters per task and plan node so a
 * query profile can aggregate real telemetry instead of dividing totals.
 *
 * <p>Task stats are cumulative per task, so a later report for the same
 * {@code (taskId, planNodeId)} replaces the earlier one; aggregation sums across
 * tasks and takes the maximum peak memory. Entries expire so a lost completion
 * notification cannot grow the Coordinator without bound.</p>
 */
@ThreadSafe
public final class QueryOperatorStatsRegistry implements OperatorStatsSource {

    private static final int MAX_QUERIES = 10_000;
    private static final Duration RETENTION = Duration.ofMinutes(15);

    private final Cache<String, ConcurrentHashMap<String, OperatorStats>> byQuery;

    public QueryOperatorStatsRegistry() {
        this.byQuery = Caffeine.newBuilder()
                .maximumSize(MAX_QUERIES)
                .expireAfterWrite(RETENTION)
                .build();
    }

    /** Records one accepted status report; empty telemetry is ignored. */
    public void record(String queryId, String taskId, List<OperatorStats> stats) {
        if (queryId == null || taskId == null || stats == null || stats.isEmpty()) {
            return;
        }
        var perTask = byQuery.get(queryId, ignored -> new ConcurrentHashMap<>());
        for (OperatorStats stat : stats) {
            perTask.put(taskId + '\u0000' + nodeKey(stat), stat);
        }
    }

    @Override
    public List<Metric> operatorStats(String queryId) {
        if (queryId == null) {
            return List.of();
        }
        var perTask = byQuery.getIfPresent(queryId);
        if (perTask == null || perTask.isEmpty()) {
            return List.of();
        }
        Map<String, Aggregate> aggregated = new LinkedHashMap<>();
        for (OperatorStats stat : perTask.values()) {
            aggregated.computeIfAbsent(nodeKey(stat), ignored -> new Aggregate(stat))
                    .add(stat);
        }
        List<Metric> result = new ArrayList<>(aggregated.size());
        for (var entry : aggregated.entrySet()) {
            result.add(entry.getValue().toMetric(entry.getKey()));
        }
        return List.copyOf(result);
    }

    private static String nodeKey(OperatorStats stat) {
        String node = stat.planNodeId();
        return node == null || node.isBlank() ? stat.operatorType() : node;
    }

    private static final class Aggregate {
        private final String operatorType;
        private long inputRows;
        private long outputRows;
        private long inputBytes;
        private long outputBytes;
        private long cpuNanos;
        private long wallNanos;
        private long peakMemoryBytes;
        private long spillBytes;

        private Aggregate(OperatorStats first) {
            this.operatorType = first.operatorType();
        }

        private void add(OperatorStats stat) {
            inputRows += stat.inputRows();
            outputRows += stat.outputRows();
            inputBytes += stat.inputBytes();
            outputBytes += stat.outputBytes();
            cpuNanos += stat.cpuNanos();
            wallNanos += stat.wallNanos();
            peakMemoryBytes = Math.max(peakMemoryBytes, stat.peakMemoryBytes());
            spillBytes += stat.spillBytes();
        }

        private Metric toMetric(String planNodeId) {
            return new Metric(operatorType, planNodeId, inputRows, outputRows,
                    inputBytes, outputBytes, cpuNanos, wallNanos, peakMemoryBytes, spillBytes);
        }
    }
}
