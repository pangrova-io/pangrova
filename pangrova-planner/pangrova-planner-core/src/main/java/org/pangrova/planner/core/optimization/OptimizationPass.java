package org.pangrova.planner.core.optimization;

import org.apache.calcite.rel.RelNode;

/**
 * A pluggable optimization stage in the query planner pipeline.
 * Each pass transforms a {@link RelNode} plan tree, potentially improving
 * it via rule-based or cost-based optimization.
 *
 * <p>Passes are executed in registration order by {@code PangrovaPlanner.optimize()}.
 * A pass may declare itself non-applicable for a given input via
 * {@link #isApplicable(RelNode, OptimizationContext)}, in which case the
 * pipeline skips it and passes the input unchanged to the next pass.</p>
 */
public interface OptimizationPass {

    /**
     * Transforms the input plan tree. The returned node replaces the input
     * for subsequent passes in the pipeline.
     *
     * @param relNode the current plan tree
     * @param context optimization configuration and cost estimation infrastructure
     * @return the optimized plan tree (may be the same instance if no improvement found)
     */
    RelNode optimize(RelNode relNode, OptimizationContext context);

    /**
     * Returns a human-readable name for this pass, used in logging and diagnostics.
     */
    String name();

    /**
     * Returns whether this pass should run for the given input and context.
     * Default implementation returns {@code true} (always applicable).
     *
     * @param relNode the current plan tree
     * @param context optimization configuration and cost estimation infrastructure
     * @return true if this pass should execute, false to skip
     */
    default boolean isApplicable(RelNode relNode, OptimizationContext context) {
        return true;
    }
}
