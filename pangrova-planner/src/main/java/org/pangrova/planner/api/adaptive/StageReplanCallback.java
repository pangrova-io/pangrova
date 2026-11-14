package org.pangrova.planner.api.adaptive;

import org.pangrova.common.api.annotation.Internal;

import org.pangrova.planner.api.adaptive.StageReplanDecision;

/**
 * Callback interface invoked by {@code StageReplanCoordinator} after producing a
 * {@link StageReplanDecision} that indicates downstream sub-plans need re-optimization.
 *
 * <p>This extension has algorithm-level callers and tests. The production DAG
 * currently uses its AdaptiveQueryEvaluator path and does not assemble this
 * callback pipeline. Declaring this port does not enable adaptive execution.</p>
 *
 * <p>Typical flow:
 * <ol>
 *   <li>Shuffle stage completes, statistics are collected</li>
 *   <li>{@code StageReplanCoordinator} produces a decision</li>
 *   <li>If {@link StageReplanDecision#needsReplan()} is true, this callback is invoked</li>
 *   <li>The implementor re-optimizes downstream sub-plans using the decision context</li>
 * </ol>
 *
 * <p>Implementations must be thread-safe as this may be called from the
 * execution driver thread after each shuffle stage completes.</p>
 *
 * <p>This is a synchronous advisory notification, not a durable execution command.
 * The caller does not promise exactly-once delivery or retry failed callbacks.
 * Reoptimization must preserve the admitted query's authorization and selected
 * input versions. A callback neither owns nor closes the caller's planner.</p>
 */
@FunctionalInterface
@Internal
public interface StageReplanCallback {

    /**
     * Called when a stage replan decision indicates that downstream sub-plans
     * should be re-optimized.
     *
     * <p>The decision carries all context needed for re-optimization:
     * <ul>
     *   <li>Coalesce result (partition merging)</li>
     *   <li>Join strategy change recommendation</li>
     *   <li>Re-optimization reason (e.g., skew detected, cardinality divergence)</li>
     *   <li>Recommended parallelism based on actual data size</li>
     * </ul>
     *
     * @param stageId  the identifier of the completed shuffle stage that triggered replan
     * @param decision the replan decision containing runtime statistics and recommendations
     */
    void onReplanDecision(String stageId, StageReplanDecision decision);
}
