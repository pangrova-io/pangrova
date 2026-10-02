package org.pangrova.observability.profiling;

import org.pangrova.observability.audit.QueryAuditRecord;
import org.pangrova.observability.audit.QueryAuditService;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Produces structured query profile data including operator tree,
 * timing breakdown, and memory usage from audit records.
 */
public final class QueryProfileService {

    private final QueryAuditService auditService;

    public QueryProfileService(QueryAuditService auditService) {
        this.auditService = auditService;
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
        List<OperatorNode> operatorTree = buildOperatorTree(record);
        TimingBreakdown timing = buildTimingBreakdown(record);
        MemoryUsage memory = buildMemoryUsage(record);
        List<FlameGraphFrame> flameGraphFrames = buildFlameGraphFrames(operatorTree);
        return new QueryProfile(
                record.queryId(), record.sql(), record.success(),
                operatorTree, timing, memory, flameGraphFrames);
    }

    private List<OperatorNode> buildOperatorTree(QueryAuditRecord record) {
        long totalWall = record.wallTimeMs();
        long totalCpu = record.cpuTimeMs();
        int stageCount = Math.max(1, record.stageCount());
        int taskCount = Math.max(1, record.taskCount());
        long wallPerStage = totalWall / stageCount;
        int tasksPerStage = Math.max(1, taskCount / stageCount);

        List<OperatorNode> stageNodes = new ArrayList<>(stageCount);
        for (int i = 0; i < stageCount; i++) {
            long stageCpu = totalCpu / stageCount;
            long stageRows = record.inputRows() / stageCount;
            long stageMemory = record.peakMemoryBytes() / stageCount;

            List<OperatorNode> taskNodes = new ArrayList<>(tasksPerStage);
            for (int t = 0; t < tasksPerStage; t++) {
                taskNodes.add(new OperatorNode(
                        "Task-" + i + "-" + t, "task",
                        wallPerStage / tasksPerStage,
                        stageCpu / tasksPerStage,
                        stageRows / tasksPerStage,
                        stageMemory / tasksPerStage,
                        List.of()));
            }

            stageNodes.add(new OperatorNode(
                    "Stage-" + i, "stage", wallPerStage, stageCpu,
                    stageRows, stageMemory, taskNodes));
        }

        return List.of(new OperatorNode(
                "QueryRoot", "root", totalWall, totalCpu,
                record.outputRows(), record.peakMemoryBytes(), stageNodes));
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

    private List<FlameGraphFrame> buildFlameGraphFrames(List<OperatorNode> operatorTree) {
        List<FlameGraphFrame> frames = new ArrayList<>();
        flattenToFrames(operatorTree, "", frames);
        return frames;
    }

    private void flattenToFrames(List<OperatorNode> nodes, String prefix,
                                  List<FlameGraphFrame> frames) {
        for (OperatorNode node : nodes) {
            String path = prefix.isEmpty() ? node.name() : prefix + ";" + node.name();
            frames.add(new FlameGraphFrame(path, node.cpuTimeMs()));
            flattenToFrames(node.children(), path, frames);
        }
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
            List<FlameGraphFrame> flameGraph) {
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
