package org.pangrova.planner.cost.join;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalJoin;

/**
 * Converts a logical join to a physical sort-merge join.
 * Applicable to joins with equality keys regardless of input size.
 * Preferred when inputs are pre-sorted on join keys
 * or when both sides are too large for hash join.
 */
public final class SortMergeJoinRule extends RelOptRule {

    public SortMergeJoinRule() {
        super(operand(LogicalJoin.class, any()), "SortMergeJoinRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);
        return !join.analyzeCondition().leftKeys.isEmpty();
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);
        PangrovaSortMergeJoin sortMergeJoin = new PangrovaSortMergeJoin(
                new SortMergeJoinConfig(
                        join.getCluster(),
                        join.getTraitSet(),
                        join.getLeft(),
                        join.getRight(),
                        join.getCondition(),
                        join.getVariablesSet(),
                        join.getJoinType()));
        call.transformTo(sortMergeJoin);
    }
}
