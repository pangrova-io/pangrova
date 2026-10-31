package org.pangrova.coordinator.query.execution.dispatch.adaptive;

import java.util.Objects;

import org.pangrova.planner.SubPlanReoptimizer;
import org.pangrova.planner.adaptive.StageReplanCallback;
import org.pangrova.planner.adaptive.StageReplanDecision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinator-side implementation of {@link StageReplanCallback} that bridges
 * the planner's {@link org.pangrova.planner.adaptive.StageReplanCoordinator} decision
 * to actual sub-plan re-optimization via the coordinator's {@link SubPlanReoptimizer}.
 *
 * <p>When the {@code StageReplanCoordinator} detects data skew or cardinality
 * divergence and produces a decision with {@code needsReplan=true}, this callback
 * logs the decision and makes it available for the DAG dispatch engine to apply
 * during downstream stage scheduling (via {@code DAGDispatchEngine.applyAdaptiveDecisions}).
 *
 * <p>The actual re-optimization is performed by the {@link SubPlanReoptimizer}
 * (backed by {@code PangrovaPlanner.reoptimize}) when the DAG engine schedules
 * the next downstream stage.</p>
 *
 * <p>Thread-safety: this class is thread-safe. All fields are final and the
 * callback method performs no mutation beyond logging.</p>
 */
public final class CoordinatorReplanCallback implements StageReplanCallback {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorReplanCallback.class);

    private final SubPlanReoptimizer reoptimizer;

    /**
     * Creates a callback backed by the given sub-plan reoptimizer.
     *
     * @param reoptimizer the sub-plan reoptimizer (typically {@code PangrovaPlanner::reoptimize})
     */
    public CoordinatorReplanCallback(SubPlanReoptimizer reoptimizer) {
        this.reoptimizer = Objects.requireNonNull(reoptimizer, "reoptimizer");
    }

    @Override
    public void onReplanDecision(String stageId, StageReplanDecision decision) {
        Objects.requireNonNull(stageId, "stageId");
        Objects.requireNonNull(decision, "decision");

        if (!decision.needsReplan()) {
            LOG.debug("event=replan_callback_noop stageId={}", stageId);
            return;
        }

        LOG.info("event=replan_callback_triggered stageId={} reOptTriggered={} "
                + "newJoinStrategy={} coalesced={} recommendedParallelism={}",
                stageId,
                decision.reOptimizationTriggered(),
                decision.newJoinStrategy().map(Enum::name).orElse("none"),
                decision.coalesceResult().isCoalesced(),
                decision.recommendedParallelism());
    }

    /**
     * Returns the underlying sub-plan reoptimizer for use by the DAG dispatch engine
     * when applying adaptive decisions to downstream stages.
     */
    public SubPlanReoptimizer reoptimizer() {
        return reoptimizer;
    }
}
