package org.pangrova.planner.adaptive;

/**
 * Callback interface invoked by {@link StageReplanCoordinator} after producing a
 * {@link StageReplanDecision} that indicates downstream sub-plans need re-optimization.
 *
 * <p>The Coordinator (pangrova-coordinator module) implements this callback to trigger
 * planner reoptimization of downstream sub-plans using actual runtime statistics
 * collected from completed shuffle stages.</p>
 *
 * <p>Typical flow:
 * <ol>
 *   <li>Shuffle stage completes, statistics are collected</li>
 *   <li>{@link StageReplanCoordinator#onShuffleStageCompleted} produces a decision</li>
 *   <li>If {@link StageReplanDecision#needsReplan()} is true, this callback is invoked</li>
 *   <li>The implementor re-optimizes downstream sub-plans using the decision context</li>
 * </ol>
 *
 * <p>Implementations must be thread-safe as this may be called from the
 * execution driver thread after each shuffle stage completes.</p>
 */
@FunctionalInterface
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
