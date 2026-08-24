package org.pangrova.planner.cost.distribution;

import java.util.List;

import org.pangrova.planner.cost.convention.PangrovaRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

/**
 * Physical Exchange operator that redistributes data to satisfy a required
 * {@link PangrovaDistribution} trait. Inserted automatically by the Volcano planner
 * when a child's distribution does not satisfy the parent's requirement.
 *
 * <p>Cost model: network cost proportional to row count × average row width,
 * plus CPU cost for hashing (HASH) or copying (BROADCAST).</p>
 */
public final class PangrovaExchange extends SingleRel implements PangrovaRel {

    private static final double NETWORK_COST_PER_BYTE = 0.01;
    private static final double HASH_CPU_FACTOR = 1.2;
    private static final double BROADCAST_CPU_FACTOR = 2.0;
    private static final double DEFAULT_ROW_WIDTH = 100.0;

    private final PangrovaDistribution targetDistribution;

    public PangrovaExchange(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            PangrovaDistribution targetDistribution) {
        super(cluster, traitSet, input);
        this.targetDistribution = targetDistribution;
    }

    public PangrovaDistribution targetDistribution() {
        return targetDistribution;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new PangrovaExchange(getCluster(), traitSet, sole(inputs), targetDistribution);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rows = mq.getRowCount(getInput());
        Double avgRowSize = mq.getAverageRowSize(getInput());
        double rowWidth = avgRowSize != null ? avgRowSize : DEFAULT_ROW_WIDTH;
        double dataBytes = rows * rowWidth;

        double costPerByte = (planner.getCostFactory() instanceof org.pangrova.planner.cost.PangrovaCostFactory cf)
                ? cf.config().networkCostPerByte() : NETWORK_COST_PER_BYTE;
        double networkCost = dataBytes * costPerByte;
        double cpuCost = switch (targetDistribution.type()) {
            case HASH -> rows * HASH_CPU_FACTOR;
            case BROADCAST -> rows * BROADCAST_CPU_FACTOR;
            case SINGLETON, ROUND_ROBIN, ANY -> rows;
        };

        if (planner.getCostFactory() instanceof org.pangrova.planner.cost.PangrovaCostFactory costFactory) {
            return costFactory.makeFiveDimCost(rows, cpuCost, 0, 0, networkCost);
        }
        return planner.getCostFactory().makeCost(rows, cpuCost, networkCost);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("distribution", targetDistribution);
    }
}
