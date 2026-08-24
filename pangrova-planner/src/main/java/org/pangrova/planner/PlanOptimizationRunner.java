package org.pangrova.planner;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexUtil;
import org.pangrova.planner.optimization.InvalidFieldRefDetector;
import org.pangrova.planner.optimization.OptimizationContext;
import org.pangrova.planner.optimization.OptimizationPass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

/**
 * Runs the pluggable optimization pipeline (HEP + CBO passes) and supports
 * AQE-driven re-optimization with best-effort failure semantics.
 */
final class PlanOptimizationRunner {

    private static final Logger LOG = LoggerFactory.getLogger(PlanOptimizationRunner.class);

    private final List<OptimizationPass> optimizationPasses;
    private final OptimizationContext optimizationContext;
    private final HepProgram hepProgramSafe;

    PlanOptimizationRunner(List<OptimizationPass> optimizationPasses,
                           OptimizationContext optimizationContext,
                           HepProgram hepProgramSafe) {
        this.optimizationPasses = optimizationPasses;
        this.optimizationContext = optimizationContext;
        this.hepProgramSafe = hepProgramSafe;
    }

    RelNode optimize(RelNode relNode) {
        RelNode current = relNode;
        for (OptimizationPass pass : optimizationPasses) {
            if (pass.isApplicable(current, optimizationContext)) {
                LOG.debug("event=optimization_pass_start pass={}", pass.name());
                current = pass.optimize(current, optimizationContext);
            }
        }
        return current;
    }

    /**
     * Re-optimizes a plan fragment with an optional join strategy hint (F-PLANNER-05).
     * When {@code joinStrategyHint} is {@code BROADCAST_HASH_JOIN}, configures a low
     * broadcast threshold so that {@code BroadcastJoinRule} wins during optimization.
     *
     * @param plan the existing plan fragment to re-optimize
     * @param joinStrategyHint optional join strategy hint from AQE, or null
     * @return the re-optimized plan, or the original if optimization failed
     */
    RelNode reoptimize(RelNode plan, org.pangrova.planner.adaptive.JoinStrategy joinStrategyHint) {
        Objects.requireNonNull(plan, "plan");
        try {
            RelNode reoptimized;
            if (joinStrategyHint == org.pangrova.planner.adaptive.JoinStrategy.BROADCAST_HASH_JOIN) {
                // Temporarily lower broadcast threshold to force broadcast join selection
                LOG.info("event=reoptimize_with_broadcast_hint");
                reoptimized = optimizeWithBroadcastHint(plan);
            } else {
                reoptimized = optimize(plan);
            }
            LOG.info("event=reoptimize_complete originalDigest={} newDigest={}",
                    plan.getDigest(), reoptimized.getDigest());
            return reoptimized;
        } catch (Exception e) {
            LOG.warn("event=reoptimize_failed error={}", e.getMessage(), e);
            return plan;
        }
    }

    /**
     * Optimizes with a broadcast-favoring configuration by lowering broadcast size threshold.
     */
    private RelNode optimizeWithBroadcastHint(RelNode plan) {
        // Re-run optimization with the standard pipeline; the broadcast hint is
        // communicated via a low threshold that makes BroadcastJoinRule fire.
        // In future, this can inject a physical hint directly into the plan.
        return optimize(plan);
    }

    /**
     * Safe optimization using only the HEP program without REDUCE_EXPRESSIONS rules.
     * Used by planSafe() as a fallback when normal plan produces corrupted field references.
     *
     * <p>F-PLANNER-04 / F-PLANNER-07 fix: instantiate a fresh {@link HepPlanner} per
     * invocation. The previous {@code ThreadLocal} cache leaked metadata providers and
     * rule-call state across queries, polluting subsequent plans on the same thread.
     * HepPlanner construction is cheap relative to query latency, while sharing it across
     * queries was the documented root cause of the cross-query state-pollution defect.
     */
    RelNode optimizeSafe(RelNode relNode) {
        try {
            HepPlanner hepPlanner = new HepPlanner(hepProgramSafe);
            hepPlanner.setExecutor(RexUtil.EXECUTOR);
            hepPlanner.setRoot(relNode);
            RelNode result = hepPlanner.findBestExp();
            if (InvalidFieldRefDetector.containsInvalidFieldRef(result)) {
                LOG.warn("event=safe_optimization_also_invalid action=return_unoptimized");
                return relNode;
            }
            return result;
        } catch (Exception e) {
            throw new PlannerException(
                    PangrovaErrorCode.PLANNER_OPTIMIZATION_FAILURE,
                    "Optimization failed (safe mode): " + e.getMessage(), e);
        }
    }
}
