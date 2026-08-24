package org.pangrova.planner.optimization;

import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Heuristic (rule-based) optimization pass using Calcite's HepPlanner.
 * Applies constant folding, filter pushdown, subquery decorrelation, and
 * window decomposition rules. Falls back to a safe program (without
 * REDUCE_EXPRESSIONS rules) when the primary program produces invalid
 * field references — a known Calcite bug with SUBSTRING/LIKE inside
 * CASE/aggregate expressions.
 *
 * <p>F-PLANNER-07: This class previously cached {@link HepPlanner} per-thread
 * via {@link ThreadLocal}, which retained {@code RelOptCluster} state across
 * unrelated queries on the same dispatcher thread and could leak rule
 * registrations or cost-model state. {@code HepPlanner} construction is
 * cheap (<1 ms with a pre-built program), so we now create a fresh planner
 * per invocation. This matches {@code PangrovaPlanner.optimizeSafe()}'s
 * approach.
 */
public final class HepOptimizationPass implements OptimizationPass {

    private static final Logger LOG = LoggerFactory.getLogger(HepOptimizationPass.class);

    private final HepProgram hepProgram;
    private final HepProgram hepProgramSafe;

    /**
     * Creates a pass applying the given Hep programs.
     *
     * @param hepProgram primary optimization program
     * @param hepProgramSafe fallback program used when the primary program fails
     */
    public HepOptimizationPass(HepProgram hepProgram, HepProgram hepProgramSafe) {
        if (hepProgram == null) {
            throw new NullPointerException("hepProgram");
        }
        if (hepProgramSafe == null) {
            throw new NullPointerException("hepProgramSafe");
        }
        this.hepProgram = hepProgram;
        this.hepProgramSafe = hepProgramSafe;
    }

    @Override
    public RelNode optimize(RelNode relNode, OptimizationContext context) {
        try {
            RelNode result = runWithProgram(hepProgram, relNode);
            if (InvalidFieldRefDetector.containsInvalidFieldRef(result)) {
                LOG.warn("event=reduce_expressions_produced_invalid_ref action=retry_without_reduce");
                return runSafe(relNode);
            }
            return result;
        } catch (Exception e) {
            LOG.warn("event=optimization_failed action=retry_without_reduce", e);
            return runSafe(relNode);
        }
    }

    @Override
    public String name() {
        return "HepRbo";
    }

    private RelNode runSafe(RelNode relNode) {
        try {
            RelNode result = runWithProgram(hepProgramSafe, relNode);
            if (InvalidFieldRefDetector.containsInvalidFieldRef(result)) {
                LOG.warn("event=safe_optimization_also_invalid action=return_unoptimized");
                return relNode;
            }
            return result;
        } catch (Exception e) {
            LOG.warn("event=safe_optimization_failed action=return_unoptimized", e);
            return relNode;
        }
    }

    private static RelNode runWithProgram(HepProgram program, RelNode input) {
        HepPlanner planner = new HepPlanner(program);
        planner.setExecutor(RexUtil.EXECUTOR);
        planner.setRoot(input);
        return planner.findBestExp();
    }
}
