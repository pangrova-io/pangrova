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
 * Physical hash join operator for the Pangrova CBO.
 * Carries the algorithm choice metadata for the Substrait serializer.
 * Build side is the smaller input; probe side is the larger.
 *
 * <p>Memory cost: build-side rows × average row width (hash table size).
 * The build side is the smaller input.</p>
 *
 * <p>Network cost: derived from the distribution trait on this operator's traitSet.
 * When the trait requires HASH distribution, the Exchange enforcer handles the shuffle
 * cost separately, so this operator's network = 0. The full 5-dimensional cost
 * (rows/cpu/memory/io/network) is always populated.</p>
 */
public final class PangrovaHashJoin extends Join implements PangrovaRel {

    private static final double DEFAULT_ROW_WIDTH = 128.0;

    public PangrovaHashJoin(HashJoinConfig config) {
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

        // Build side is the smaller input
        double buildRows = Math.min(leftRows, rightRows);
        double buildRowWidth = (leftRows <= rightRows) ? leftRowWidth : rightRowWidth;
        double probeRows = Math.max(leftRows, rightRows);

        // Memory cost: hash table size = build-side rows × row width
        double memory = buildRows * buildRowWidth;

        // CPU cost: build cost + probe cost
        double cpu = buildRows * 0.03 + probeRows * 0.015;

        // I/O cost: reading both sides
        double io = (leftRows * leftRowWidth + rightRows * rightRowWidth) * 0.000122;

        // Network cost: derived from distribution trait on this operator's traitSet.
        // Hash join itself does not transfer data — the Exchange enforcer handles shuffle.
        double network = computeNetworkFromDistribution(
                leftRows, rightRows, leftRowWidth, rightRowWidth, planner);

        if (planner.getCostFactory() instanceof PangrovaCostFactory costFactory) {
            return costFactory.makeFiveDimCost(leftRows + rightRows, cpu, io, memory, network);
        }
        return planner.getCostFactory().makeCost(leftRows + rightRows, cpu, io);
    }

    /**
     * Network cost from the hash join's distribution trait. HASH and BROADCAST
     * distributions return zero because the PangrovaExchange enforcer is the sole
     * owner of shuffle network cost (architecture-remediation #117).
     */
    private double computeNetworkFromDistribution(double leftRows, double rightRows,
                                                  double leftRowWidth, double rightRowWidth,
                                                  RelOptPlanner planner) {
        PangrovaDistribution dist = getTraitSet().getTrait(PangrovaDistributionTraitDef.INSTANCE);
        if (dist == null || PangrovaDistribution.ANY.equals(dist)) {
            return 0;
        }
        return switch (dist.type()) {
            case HASH, BROADCAST -> 0;
            case SINGLETON, ROUND_ROBIN -> {
                double networkCostPerByte = (planner.getCostFactory() instanceof PangrovaCostFactory cf)
                        ? cf.config().networkCostPerByte() : 0.000300;
                yield (leftRows * leftRowWidth + rightRows * rightRowWidth) * networkCostPerByte;
            }
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
        return new PangrovaHashJoin(new HashJoinConfig(
                getCluster(), traitSet, left, right,
                conditionExpr, variablesSet, joinType));
    }
}
