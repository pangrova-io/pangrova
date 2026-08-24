package org.pangrova.coordinator.query.execution.dispatch.adaptive;

import java.util.Optional;

import org.pangrova.planner.adaptive.CoalesceResult;
import org.pangrova.planner.adaptive.JoinStrategy;
import org.pangrova.planner.adaptive.SkewResult;

/**
 * Aggregated AQE decision produced after a stage completes.
 * Stored on the finished Stage so the DAG dispatch engine can
 * apply adjustments when scheduling downstream stages.
 *
 * @param reOptimizationTriggered whether re-optimization was triggered
 * @param newJoinStrategy         recommended join strategy change, if any
 * @param coalesceResult          partition coalescing result
 * @param skewResult              skew detection result
 * @param adjustedParallelism     parallelism recommended by AQE (may equal planned)
 * @param plannedParallelism      original planned parallelism for comparison
 */
public record AdaptiveDecision(
        boolean reOptimizationTriggered,
        Optional<JoinStrategy> newJoinStrategy,
        CoalesceResult coalesceResult,
        SkewResult skewResult,
        int adjustedParallelism,
        int plannedParallelism) {

    /** No adaptive adjustments. */
    public static final AdaptiveDecision NONE = new AdaptiveDecision(
            false, Optional.empty(), CoalesceResult.NO_OP, SkewResult.NO_SKEW, 0, 0);

    /** Returns true if any adaptive adjustment was recommended. */
    public boolean hasAdjustments() {
        return reOptimizationTriggered
                || coalesceResult.isCoalesced()
                || skewResult.hasSkew()
                || adjustedParallelism != plannedParallelism;
    }

    /** Returns true if parallelism was adjusted. */
    public boolean parallelismChanged() {
        return adjustedParallelism != plannedParallelism && adjustedParallelism > 0;
    }
}
