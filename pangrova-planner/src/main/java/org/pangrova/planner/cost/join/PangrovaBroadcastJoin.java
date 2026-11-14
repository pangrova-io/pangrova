package org.pangrova.planner.cost.join;

import org.pangrova.planner.cost.PangrovaCostFactory;
import org.pangrova.planner.cost.convention.PangrovaRel;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

/**
 * Physical broadcast join operator for the Pangrova CBO.
 * The smaller (build) side is broadcast to all nodes holding the probe side.
 * Preferred when the build side is small enough to fit in memory on every node.
 *
 * <p>Memory cost: build-side rows × average row width (hash table replicated on each node).
 * Network cost: build-side data broadcast to all probe-side nodes, proportional to
 * build-side rows × row width × networkCostPerByte.</p>
 *
 * <p>The full 5-dimensional cost (rows/cpu/memory/io/network) is always populated.</p>
 */
public final class PangrovaBroadcastJoin extends Join implements PangrovaRel {

    private static final double DEFAULT_ROW_WIDTH = 128.0;

    public PangrovaBroadcastJoin(BroadcastJoinConfig config) {
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

        // Build side is the smaller input (broadcast to all nodes)
        double buildRows = Math.min(leftRows, rightRows);
        double buildRowWidth = (leftRows <= rightRows) ? leftRowWidth : rightRowWidth;
        double probeRows = Math.max(leftRows, rightRows);

        // Memory cost: hash table for build side (replicated on each node)
        double memory = buildRows * buildRowWidth;

        // CPU cost: build hash table + probe all rows
        double cpu = buildRows * 0.03 + probeRows * 0.015;

        // I/O cost: reading both sides
        double io = (leftRows * leftRowWidth + rightRows * rightRowWidth) * 0.000122;

        // Network cost: broadcast the build side to all probe-side nodes.
        // The build side data is transferred across the network.
        double networkCostPerByte = (planner.getCostFactory() instanceof PangrovaCostFactory cf)
                ? cf.config().networkCostPerByte() : 0.000300;
        double network = buildRows * buildRowWidth * networkCostPerByte;

        if (planner.getCostFactory() instanceof PangrovaCostFactory costFactory) {
            return costFactory.makeFiveDimCost(leftRows + rightRows, cpu, io, memory, network);
        }
        return planner.getCostFactory().makeCost(leftRows + rightRows, cpu, io);
    }

    @Override
    public Join copy(
            RelTraitSet traitSet,
            RexNode conditionExpr,
            RelNode left,
            RelNode right,
            JoinRelType joinType,
            boolean semiJoinDone) {
        return new PangrovaBroadcastJoin(new BroadcastJoinConfig(
                getCluster(), traitSet, left, right,
                conditionExpr, variablesSet, joinType));
    }
}
