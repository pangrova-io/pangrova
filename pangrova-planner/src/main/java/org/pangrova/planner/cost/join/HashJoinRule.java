package org.pangrova.planner.cost.join;

import java.util.List;

import org.pangrova.planner.cost.CostModelConfig;
import org.pangrova.planner.cost.distribution.PangrovaDistribution;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableIntList;

/**
 * Converts a logical join to a physical hash join when the build side
 * (smaller input) fits within the per-task memory budget.
 *
 * <p>Declares required HASH distribution on both child inputs using the
 * equi-join key columns. This enables the Calcite Volcano enforcer to
 * automatically insert {@link org.pangrova.planner.cost.distribution.PangrovaExchange}
 * nodes when the child's current distribution does not satisfy the requirement.</p>
 *
 * <p>Row size estimation uses statistics-based average row size from
 * {@link RelMetadataQuery#getAverageRowSize(RelNode)} when available,
 * falling back to a schema-based default of 128 bytes/row.</p>
 */
public final class HashJoinRule extends RelOptRule {

    private final CostModelConfig costConfig;

    public HashJoinRule(CostModelConfig costConfig) {
        super(operand(LogicalJoin.class, any()), "HashJoinRule");
        this.costConfig = costConfig;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Join join = call.rel(0);
        RelMetadataQuery mq = call.getMetadataQuery();
        double rightRows = join.getRight().estimateRowCount(mq);
        double leftRows = join.getLeft().estimateRowCount(mq);

        RelNode buildSide = (leftRows <= rightRows) ? join.getLeft() : join.getRight();
        double buildRows = Math.min(leftRows, rightRows);
        double buildRowBytes = BroadcastJoinRule.estimateRowBytes(buildSide, mq);

        double estimatedBuildBytes = buildRows * buildRowBytes;
        return estimatedBuildBytes <= costConfig.availableMemoryPerTask();
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);
        JoinInfo joinInfo = join.analyzeCondition();

        RelNode left = join.getLeft();
        RelNode right = join.getRight();
        RelTraitSet joinTraitSet = join.getTraitSet();

        ImmutableIntList leftKeys = joinInfo.leftKeys;
        ImmutableIntList rightKeys = joinInfo.rightKeys;

        // When equi-join keys are available, declare required HASH distribution
        // on child inputs so the enforcer inserts PangrovaExchange when needed.
        if (!leftKeys.isEmpty()) {
            PangrovaDistribution leftDist = PangrovaDistribution.hash(List.copyOf(leftKeys));
            PangrovaDistribution rightDist = PangrovaDistribution.hash(List.copyOf(rightKeys));

            RelTraitSet leftTraitSet = left.getTraitSet().plus(leftDist);
            RelTraitSet rightTraitSet = right.getTraitSet().plus(rightDist);

            left = convert(left, leftTraitSet);
            right = convert(right, rightTraitSet);

            // Propagate HASH distribution on the join's own traitSet
            joinTraitSet = joinTraitSet.plus(leftDist);
        }

        PangrovaHashJoin hashJoin = new PangrovaHashJoin(
                new HashJoinConfig(
                        join.getCluster(),
                        joinTraitSet,
                        left,
                        right,
                        join.getCondition(),
                        join.getVariablesSet(),
                        join.getJoinType()));
        call.transformTo(hashJoin);
    }
}
