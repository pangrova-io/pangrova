package org.pangrova.planner.api.adaptive;

import org.pangrova.common.api.annotation.Internal;

import java.util.Objects;
import java.util.Optional;

/**
 * Decision produced by {@code StageReplanCoordinator} after processing a completed
 * shuffle stage. Encapsulates all AQE decisions (coalescing, join strategy switch,
 * parallelism adjustment) into a single immutable result that downstream stages
 * consume to re-optimize before dispatching.
 *
 * @param needsReplan            whether any downstream re-optimization is required
 * @param coalesceResult         partition coalescing result (groups, original/new count)
 * @param newJoinStrategy        new join strategy if a switch is recommended; empty otherwise
 * @param reOptimizationTriggered whether the actual-vs-estimated divergence triggered re-optimization
 * @param reOptimizationReason   human-readable reason for re-optimization (empty when not triggered)
 * @param recommendedParallelism recommended task parallelism based on actual data size
 */
@Internal
public record StageReplanDecision(
        boolean needsReplan,
        CoalesceResult coalesceResult,
        Optional<JoinStrategy> newJoinStrategy,
        boolean reOptimizationTriggered,
        String reOptimizationReason,
        int recommendedParallelism) {

    /** No replan needed — returned when AQE is disabled or statistics are within expected range. */
    public static final StageReplanDecision NO_REPLAN = new StageReplanDecision(
            false, CoalesceResult.NO_OP, Optional.empty(), false, "", 0);

    public StageReplanDecision {
        Objects.requireNonNull(coalesceResult, "coalesceResult");
        Objects.requireNonNull(newJoinStrategy, "newJoinStrategy");
        Objects.requireNonNull(reOptimizationReason, "reOptimizationReason");
        if (recommendedParallelism < 0) {
            throw new IllegalArgumentException(
                    "recommendedParallelism must be non-negative, got: " + recommendedParallelism);
        }
    }
}
