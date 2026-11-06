package org.pangrova.planner.core.cost.join;

import org.pangrova.planner.core.cost.PangrovaCostFactory;
import org.pangrova.planner.core.cost.convention.PangrovaRel;
import org.pangrova.planner.core.cost.distribution.PangrovaDistribution;
import org.pangrova.planner.core.cost.distribution.PangrovaDistributionTraitDef;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

/**
 * Physical nested loop join operator for the Pangrova CBO.
 * Used for non-equi (theta) joins where hash or merge strategies are not applicable.
 * The native engine evaluates the join condition row-by-row across all input pairs.
 *
 * <p>CPU cost: O(n × m) — every left row compared against every right row.
 * Memory cost: minimal — only one row from each side in memory at a time.
 * Network cost: derived from the distribution trait on this operator's traitSet.</p>
 *
 * <p>The full 5-dimensional cost (rows/cpu/memory/io/network) is always populated.</p>
 */
public final class PangrovaNestedLoopJoin extends Join implements PangrovaRel {

    private static final double DEFAULT_ROW_WIDTH = 128.0;

    public PangrovaNestedLoopJoin(NestedLoopJoinConfig config) {
        super(config.cluster(), config.traitSet(), config.left(), config.right(),
                config.condition(), config.variablesSet(), config.joinType());
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double leftRows = mq.getRowCount(getLeft());
        double rightRows = mq.getRowCount(getRight());

        Double leftAvgRowSize = mq.getAverageRowSize(getLeft());
        Double rightAvgRowSize = mq.getAverageRowSize(getRight());
        double leftRowWidth = leftAvgRowSize != null ? leftAvgRowSize : DEFAULT_ROW_WIDTH;
        double rightRowWidth = rightAvgRowSize != null ? rightAvgRowSize : DEFAULT_ROW_WIDTH;

        // CPU cost: O(n × m) — cartesian product comparison
        double cpu = leftRows * rightRows * 0.02;

        // Memory cost: minimal — streaming evaluation, only current rows in memory
        double memory = (leftRowWidth + rightRowWidth) * 2.0;

        // I/O cost: reading both sides (right side may be re-read per left row)
        double io = (leftRows * leftRowWidth + leftRows * rightRows * rightRowWidth) * 0.000061;

        // Network cost: derived from distribution trait on this operator's traitSet.
        double network = computeNetworkFromDistribution(
                leftRows, rightRows, leftRowWidth, rightRowWidth, planner);

        if (planner.getCostFactory() instanceof PangrovaCostFactory costFactory) {
            return costFactory.makeFiveDimCost(leftRows * rightRows, cpu, io, memory, network);
        }
        return planner.getCostFactory().makeCost(leftRows * rightRows, cpu, io);
    }

    private double computeNetworkFromDistribution(double leftRows, double rightRows,
                                                  double leftRowWidth, double rightRowWidth,
                                                  RelOptPlanner planner) {
        PangrovaDistribution dist = getTraitSet().getTrait(PangrovaDistributionTraitDef.INSTANCE);
        if (dist == null || PangrovaDistribution.ANY.equals(dist)) {
            return 0;
        }
        double networkCostPerByte = (planner.getCostFactory() instanceof PangrovaCostFactory cf)
                ? cf.config().networkCostPerByte() : 0.000300;
        return switch (dist.type()) {
            case HASH -> (leftRows * leftRowWidth + rightRows * rightRowWidth) * networkCostPerByte;
            case BROADCAST -> (Math.min(leftRows * leftRowWidth, rightRows * rightRowWidth))
                    * networkCostPerByte;
            case SINGLETON, ROUND_ROBIN -> (leftRows * leftRowWidth + rightRows * rightRowWidth)
                    * networkCostPerByte;
            case ANY -> 0;
        };
    }

    @Override
    public Join copy(
            RelTraitSet traitSet,
            RexNode conditionExpr,
            RelNode left,
            RelNode right,
            JoinRelType joinType,
            boolean semiJoinDone) {
        return new PangrovaNestedLoopJoin(new NestedLoopJoinConfig(
                getCluster(), traitSet, left, right,
                conditionExpr, variablesSet, joinType));
    }
}
