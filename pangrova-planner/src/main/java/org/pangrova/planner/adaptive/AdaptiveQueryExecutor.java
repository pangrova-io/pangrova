package org.pangrova.planner.adaptive;

import org.pangrova.planner.api.enums.JoinStrategy;
import org.pangrova.planner.api.adaptive.ReOptimizationResult;

import org.pangrova.connector.api.metadata.table.QualifiedTableName;

import java.util.Optional;

import org.pangrova.planner.statistics.StatisticsCollector;

/**
 * Adaptive Query Execution framework that adjusts the physical plan at runtime
 * based on actual data statistics collected after shuffle stages complete.
 *
 * <p>Re-optimization is triggered when actual data size differs from the estimate
 * by more than 2× or less than 0.5×. Join strategy may be switched from
 * sort-merge to broadcast hash join when the build side fits within the
 * broadcast threshold.</p>
 *
 * <p>When a {@link StatisticsCollector} is provided, runtime cardinality feedback
 * is recorded to improve future cost estimates for the same tables.</p>
 */
public class AdaptiveQueryExecutor {

    /** Default broadcast threshold: 10 MB. Used when no config is provided. */
    public static final long BROADCAST_THRESHOLD = 10_485_760L;

    private final StatisticsCollector statisticsCollector;

    public AdaptiveQueryExecutor(StatisticsCollector statisticsCollector) {
        this.statisticsCollector = statisticsCollector;
    }

    public AdaptiveQueryExecutor() {
        this(null);
    }

    /**
     * Determine whether re-optimization should be triggered based on the
     * ratio of actual to estimated data size.
     *
     * <p>Strictly greater than 2× or strictly less than 0.5× triggers
     * re-optimization. When estimated is zero, any non-zero actual triggers.</p>
     */
    public boolean shouldReOptimize(ShuffleStageStats stats) {
        if (stats.estimatedDataSize() == 0) {
            return stats.actualDataSize() != 0;
        }
        double ratio = (double) stats.actualDataSize() / stats.estimatedDataSize();
        return ratio > 2.0 || ratio < 0.5;
    }

    /**
     * Evaluate whether the join strategy should be switched based on the
     * actual build-side size, using the default broadcast threshold.
     *
     * @return BROADCAST_HASH_JOIN if build side fits, otherwise current strategy
     */
    public JoinStrategy evaluateJoinStrategy(long buildSideSize, JoinStrategy current) {
        return evaluateJoinStrategy(buildSideSize, current, BROADCAST_THRESHOLD);
    }

    /**
     * Evaluate whether the join strategy should be switched based on the
     * actual build-side size and a configurable broadcast threshold.
     *
     * @param broadcastThreshold maximum build-side bytes for broadcast switch
     * @return BROADCAST_HASH_JOIN if build side fits, otherwise current strategy
     */
    public JoinStrategy evaluateJoinStrategy(long buildSideSize, JoinStrategy current,
                                             long broadcastThreshold) {
        if (buildSideSize < broadcastThreshold) {
            return JoinStrategy.BROADCAST_HASH_JOIN;
        }
        return current;
    }

    /**
     * Process a completed shuffle stage using default broadcast threshold.
     */
    public ReOptimizationResult processStageCompletion(
            ShuffleStageStats stats, JoinStrategy currentJoinStrategy) {
        return processStageCompletion(stats, currentJoinStrategy, BROADCAST_THRESHOLD);
    }

    /**
     * Process a completed shuffle stage with configurable broadcast threshold.
     */
    public ReOptimizationResult processStageCompletion(
            ShuffleStageStats stats, JoinStrategy currentJoinStrategy,
            long broadcastThreshold) {

        if (!shouldReOptimize(stats)) {
            return ReOptimizationResult.none();
        }

        String reason = buildReason(stats);
        JoinStrategy newStrategy = evaluateJoinStrategy(
                stats.actualDataSize(), currentJoinStrategy, broadcastThreshold);
        Optional<JoinStrategy> strategyChange = newStrategy != currentJoinStrategy
                ? Optional.of(newStrategy)
                : Optional.empty();

        return new ReOptimizationResult(true, reason, strategyChange);
    }

    /**
     * Process a completed shuffle stage and record cardinality feedback for the
     * given table. Cardinality feedback is recorded independently of whether
     * re-optimization is triggered — dataSize divergence drives join strategy
     * changes, while rowCount divergence drives statistics correction.
     *
     * @param stats                actual vs estimated statistics from the completed stage
     * @param currentJoinStrategy  current join strategy for potential switching
     * @param broadcastThreshold   maximum build-side bytes for broadcast switch
     * @param tableName            table name for cardinality feedback; null skips feedback
     * @return re-optimization result with optional strategy change
     */
    public ReOptimizationResult processStageCompletion(
            ShuffleStageStats stats, JoinStrategy currentJoinStrategy,
            long broadcastThreshold, QualifiedTableName tableName) {

        ReOptimizationResult result = processStageCompletion(
                stats, currentJoinStrategy, broadcastThreshold);

        if (statisticsCollector != null && tableName != null
                && stats.estimatedRowCount() > 0 && stats.actualRowCount() > 0) {
            statisticsCollector.recordRuntimeFeedback(
                    tableName, stats.estimatedRowCount(), stats.actualRowCount());
        }

        return result;
    }

    private String buildReason(ShuffleStageStats stats) {
        if (stats.estimatedDataSize() == 0) {
            return "estimated size was 0 but actual is " + stats.actualDataSize();
        }
        double ratio = (double) stats.actualDataSize() / stats.estimatedDataSize();
        return String.format("actual/estimated ratio %.2f outside [0.5, 2.0]", ratio);
    }

    /** Target task size for dynamic parallelism: 256 MB. */
    private static final long TARGET_TASK_SIZE = 256L * 1024 * 1024;
    /** Maximum parallelism cap. */
    private static final int MAX_PARALLELISM = 1024;

    /**
     * Evaluates optimal parallelism based on actual data size after shuffle.
     * Returns the recommended task count, capped at MAX_PARALLELISM.
     */
    public int evaluateParallelism(ShuffleStageStats stats, int plannedParallelism) {
        long actualSize = stats.actualDataSize();
        if (actualSize <= 0) {
            return plannedParallelism;
        }
        long optimalTasks = Math.max(1L, actualSize / TARGET_TASK_SIZE);
        return (int) Math.min(optimalTasks, MAX_PARALLELISM);
    }
}
