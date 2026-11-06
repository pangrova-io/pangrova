package org.pangrova.planner.core.cost;

/**
 * Controls Cascades CBO search budget: join count thresholds, plan limits, and timeouts.
 *
 * @param volcanoJoinThreshold max joins for Volcano search (greedy fallback above this)
 * @param stage1PlanBudget     max plans explored in Stage 1 (quick search)
 * @param stage2PlanBudget     max plans explored in Stage 2 (bushy search)
 * @param stage1TimeoutMs      Stage 1 timeout in milliseconds
 * @param stage2TimeoutMs      Stage 2 timeout in milliseconds
 * @param stage2CostThreshold  trigger Stage 2 if Stage 1 best cost exceeds this
 */
public record CboConfig(
        int volcanoJoinThreshold,
        int stage1PlanBudget,
        int stage2PlanBudget,
        long stage1TimeoutMs,
        long stage2TimeoutMs,
        double stage2CostThreshold
) {

    public static final CboConfig DEFAULT = new CboConfig(10, 5000, 50000, 2000, 5000, 1e8);

    /** CBO disabled — all queries use HEP only. */
    public static final CboConfig DISABLED = new CboConfig(0, 0, 0, 0, 0, Double.MAX_VALUE);

    public boolean enabled() {
        return volcanoJoinThreshold > 0;
    }
}
