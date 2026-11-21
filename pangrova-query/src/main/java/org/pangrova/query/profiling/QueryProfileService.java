package org.pangrova.query.profiling;

import org.pangrova.query.api.audit.QueryAuditRecord;
import org.pangrova.query.audit.QueryAuditService;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Produces structured query profile data including operator tree,
 * timing breakdown, and memory usage from audit records.
 */
public final class QueryProfileService {

    private final QueryAuditService auditService;
    private final OperatorStatsSource operatorStats;

    public QueryProfileService(QueryAuditService auditService) {
        this(auditService, null);
    }

    /** Creates a profile service backed by real Coordinator operator telemetry. */
    public QueryProfileService(QueryAuditService auditService, OperatorStatsSource operatorStats) {
        this.auditService = auditService;
        this.operatorStats = operatorStats;
    }

    /**
     * Retrieves a structured query profile for the given query ID.
     *
     * @return profile data if the query exists in the audit store, empty otherwise
     */
    public Optional<QueryProfile> getProfile(String queryId) {
        Optional<QueryAuditRecord> record = auditService.findByQueryId(queryId);
        if (record.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(buildProfile(record.get()));
    }

    private QueryProfile buildProfile(QueryAuditRecord record) {
        List<OperatorStatsSource.Metric> metrics = operatorStats == null
                ? List.of() : operatorStats.operatorStats(record.queryId());
        if (metrics.isEmpty()) {
            // No retained telemetry: report that explicitly instead of dividing
            // query totals into invented Stage-i / Task-i nodes.
            return new QueryProfile(
                    record.queryId(), record.sql(), record.success(),
                    List.of(), buildTimingBreakdown(record), buildMemoryUsage(record),
                    List.of(), false);
        }
        List<OperatorNode> operators = new ArrayList<>(metrics.size());
        for (OperatorStatsSource.Metric metric : metrics) {
            operators.add(new OperatorNode(
                    metric.operatorType() + "[" + metric.planNodeId() + "]", "operator",
                    nanosToMillis(metric.wallNanos()), nanosToMillis(metric.cpuNanos()),
                    metric.outputRows(), metric.peakMemoryBytes(), List.of()));
        }
        OperatorNode root = new OperatorNode("QueryRoot", "root", record.wallTimeMs(),
                record.cpuTimeMs(), record.outputRows(), record.peakMemoryBytes(), operators);
        List<OperatorNode> tree = List.of(root);
        return new QueryProfile(record.queryId(), record.sql(), record.success(),
                tree, buildTimingBreakdown(record), buildMemoryUsage(record),
                flameFrames(tree), true);
    }

    private static long nanosToMillis(long nanos) {
        return java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(nanos);
    }

    private static List<FlameGraphFrame> flameFrames(List<OperatorNode> tree) {
        List<FlameGraphFrame> frames = new ArrayList<>();
        collectFrames(tree, "", frames);
        return frames;
    }

    private static void collectFrames(List<OperatorNode> nodes, String prefix,
                                      List<FlameGraphFrame> frames) {
        for (OperatorNode node : nodes) {
            String path = prefix.isEmpty() ? node.name() : prefix + ";" + node.name();
            frames.add(new FlameGraphFrame(path, node.cpuTimeMs()));
            collectFrames(node.children(), path, frames);
        }
    }

    private TimingBreakdown buildTimingBreakdown(QueryAuditRecord record) {
        long cpuMs = record.cpuTimeMs();
        long wallMs = record.wallTimeMs();
        long ioMs = Math.max(0, wallMs - cpuMs);
        return new TimingBreakdown(wallMs, cpuMs, ioMs,
                record.inputRows(), record.outputRows());
    }

    private MemoryUsage buildMemoryUsage(QueryAuditRecord record) {
        return new MemoryUsage(
                record.peakMemoryBytes(),
                record.spilledBytes(),
                record.inputBytes(),
                record.outputBytes(),
                record.shuffleReadBytes(),
                record.shuffleWriteBytes());
    }

    /**
     * Structured query profile containing operator tree, timing, and memory data.
     */
    public record QueryProfile(
            String queryId,
            String sql,
            boolean success,
            List<OperatorNode> operatorTree,
            TimingBreakdown timing,
            MemoryUsage memory,
            List<FlameGraphFrame> flameGraph,
            boolean telemetryAvailable) {
        public QueryProfile {
            operatorTree = List.copyOf(operatorTree);
            flameGraph = List.copyOf(flameGraph);
        }
    }

    /**
     * A node in the operator execution tree.
     */
    public record OperatorNode(
            String name,
            String type,
            long wallTimeMs,
            long cpuTimeMs,
            long outputRows,
            long peakMemoryBytes,
            List<OperatorNode> children) {
        public OperatorNode {
            children = List.copyOf(children);
        }
    }

    /**
     * Timing breakdown for the query execution.
     */
    public record TimingBreakdown(
            long totalWallMs,
            long totalCpuMs,
            long ioWaitMs,
            long inputRows,
            long outputRows) {}

    /**
     * Memory usage statistics for the query.
     */
    public record MemoryUsage(
            long peakMemoryBytes,
            long spilledBytes,
            long inputBytes,
            long outputBytes,
            long shuffleReadBytes,
            long shuffleWriteBytes) {}

    /**
     * A single frame in the flame graph representation.
     */
    public record FlameGraphFrame(
            String stackPath,
            long durationMs) {}
}
