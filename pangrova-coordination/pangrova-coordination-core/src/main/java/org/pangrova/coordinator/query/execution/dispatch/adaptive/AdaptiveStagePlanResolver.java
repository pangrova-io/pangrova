package org.pangrova.coordinator.query.execution.dispatch.adaptive;

import java.util.List;

import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.planner.PlanFragment;
import org.pangrova.planner.SubPlanReoptimizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Applies completed upstream adaptive decisions to a downstream stage plan. */
public final class AdaptiveStagePlanResolver {
    private static final Logger LOG =
            LoggerFactory.getLogger(AdaptiveStagePlanResolver.class);
    private static final int MINIMUM_SKEW_PARALLELISM_CAP = 1024;

    private final SubPlanReoptimizer subPlanReoptimizer;

    /** Creates the resolver with an optional sub-plan reoptimizer. */
    public AdaptiveStagePlanResolver(SubPlanReoptimizer subPlanReoptimizer) {
        this.subPlanReoptimizer = subPlanReoptimizer;
    }

    /** Returns the plan after applying every available upstream decision. */
    public StageNode apply(StageNode initialPlan, StageDAG dag) {
        if (initialPlan.stagePlan().mode() == org.pangrova.planner.StagePlanAccess.Mode.DURABLE_REPLAY) {
            return initialPlan;
        }
        StageNode plan = initialPlan;
        List<Integer> upstreamIds = dag.edges()
                .getOrDefault(plan.stageId(), List.of());
        for (int upstreamId : upstreamIds) {
            Stage upstream = dag.findStage(upstreamId);
            if (upstream != null && hasAdjustments(upstream)) {
                plan = applyDecision(plan, upstreamId, upstream.adaptiveDecision());
            }
        }
        return plan;
    }

    /** Production AQE obtains its optimizer from the immutable query scope. */
    public StageNode apply(StageNode initialPlan, StageDAG dag,
                           org.pangrova.planner.SqlCompiler compiler) {
        if (initialPlan.stagePlan().mode() == org.pangrova.planner.StagePlanAccess.Mode.DURABLE_REPLAY) {
            return initialPlan;
        }
        SubPlanReoptimizer optimizer = subPlanReoptimizer != null
                ? subPlanReoptimizer : compiler.subPlanReoptimizer();
        return new AdaptiveStagePlanResolver(optimizer).apply(initialPlan, dag);
    }

    private static boolean hasAdjustments(Stage upstream) {
        return upstream.adaptiveDecision() != null
                && upstream.adaptiveDecision().hasAdjustments();
    }

    private StageNode applyDecision(
            StageNode plan, int upstreamId, AdaptiveDecision decision) {
        StageNode adjusted = applyParallelism(plan, upstreamId, decision);
        adjusted = applySkew(adjusted, upstreamId, decision);
        return applyReoptimization(adjusted, upstreamId, decision);
    }

    private static StageNode applyParallelism(
            StageNode plan, int upstreamId, AdaptiveDecision decision) {
        StageNode adjusted = plan;
        if (decision.coalesceResult().isCoalesced()) {
            int coalesced = decision.coalesceResult().coalescedCount();
            LOG.info("event=aqe_coalesce_applied stageId={} upstreamStageId={}"
                            + " originalParallelism={} coalescedParallelism={}",
                    adjusted.stageId(), upstreamId, adjusted.parallelism(), coalesced);
            adjusted = adjusted.withParallelism(coalesced);
        }
        if (decision.parallelismChanged()) {
            int parallelism = decision.adjustedParallelism();
            LOG.info("event=aqe_parallelism_applied stageId={} upstreamStageId={}"
                            + " originalParallelism={} adjustedParallelism={}",
                    adjusted.stageId(), upstreamId,
                    adjusted.parallelism(), parallelism);
            adjusted = adjusted.withParallelism(parallelism);
        }
        return adjusted;
    }

    private static StageNode applySkew(
            StageNode plan, int upstreamId, AdaptiveDecision decision) {
        if (!decision.skewResult().hasSkew()) {
            return plan;
        }
        int skewedCount = decision.skewResult().skewedPartitions().size();
        int boosted = Math.min(
                plan.parallelism() + skewedCount,
                Math.max(MINIMUM_SKEW_PARALLELISM_CAP, plan.parallelism() * 2));
        LOG.info("event=aqe_skew_mitigated stageId={} upstreamStageId={}"
                        + " skewedPartitions={} originalParallelism={}"
                        + " adjustedParallelism={}",
                plan.stageId(), upstreamId, skewedCount,
                plan.parallelism(), boosted);
        return plan.withParallelism(boosted);
    }

    private StageNode applyReoptimization(
            StageNode plan, int upstreamId, AdaptiveDecision decision) {
        if (!decision.reOptimizationTriggered()) {
            return plan;
        }
        if (subPlanReoptimizer == null) {
            LOG.warn("event=aqe_reopt_detected_no_reoptimizer stageId={}"
                            + " upstreamStageId={} newJoinStrategy={}",
                    plan.stageId(), upstreamId,
                    decision.newJoinStrategy().map(Enum::name).orElse("none"));
            return plan;
        }
        PlanFragment reoptimized = reoptimize(plan, decision);
        if (reoptimized == plan.stagePlan()) {
            return plan;
        }
        LOG.info("event=aqe_reopt_applied stageId={} upstreamStageId={}"
                        + " newJoinStrategy={}",
                plan.stageId(), upstreamId,
                decision.newJoinStrategy().map(Enum::name).orElse("none"));
        return plan.withStagePlan(reoptimized);
    }

    private PlanFragment reoptimize(
            StageNode plan, AdaptiveDecision decision) {
        return decision.newJoinStrategy().isPresent()
                ? subPlanReoptimizer.reoptimize(
                        plan.stagePlan().requireLiveFragment(), decision.newJoinStrategy().orElseThrow())
                : subPlanReoptimizer.reoptimize(plan.stagePlan().requireLiveFragment());
    }
}
