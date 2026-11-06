package org.pangrova.planner.core.cost.aggregate;

import java.util.List;

import org.pangrova.planner.core.cost.PangrovaCostFactory;
import org.pangrova.planner.core.cost.convention.PangrovaRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;

/**
 * Physical aggregate operator for the Pangrova CBO.
 * Carries the PangrovaConvention trait so the Volcano planner can perform
 * convention-based physical layer selection.
 *
 * <p>Uses hash-based aggregation by default. CPU cost is proportional to input
 * rows (hash + aggregate function evaluation). Memory cost is proportional to
 * the number of distinct groups × row width (hash table for group keys).</p>
 */
public final class PangrovaAggregate extends Aggregate implements PangrovaRel {

    private static final double DEFAULT_ROW_WIDTH = 128.0;

    /** Estimated ratio of distinct groups to input rows when stats are unavailable. */
    private static final double DEFAULT_GROUP_RATIO = 0.1;

    public PangrovaAggregate(RelOptCluster cluster, RelTraitSet traitSet,
                           RelNode input, ImmutableBitSet groupSet,
                           List<ImmutableBitSet> groupSets,
                           List<AggregateCall> aggCalls) {
        super(cluster, traitSet, List.of(), input, groupSet, groupSets, aggCalls);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double inputRows = mq.getRowCount(getInput());
        Double avgRowSize = mq.getAverageRowSize(getInput());
        double rowWidth = avgRowSize != null ? avgRowSize : DEFAULT_ROW_WIDTH;

        // Estimate distinct groups
        Double groupCount = mq.getRowCount(this);
        double groups = groupCount != null ? groupCount : inputRows * DEFAULT_GROUP_RATIO;

        // CPU: hash each input row + evaluate aggregate functions per row
        double cpu = inputRows * 0.02 + inputRows * getAggCallList().size() * 0.005;

        // Memory: hash table for group keys
        double memory = groups * rowWidth;

        // I/O: minimal for in-memory aggregation
        double io = inputRows * rowWidth * 0.000030;

        // Network: aggregate is a local operator, no data transfer
        double network = 0;

        if (planner.getCostFactory() instanceof PangrovaCostFactory costFactory) {
            return costFactory.makeFiveDimCost(inputRows, cpu, io, memory, network);
        }
        return planner.getCostFactory().makeCost(inputRows, cpu, io);
    }

    @Override
    public Aggregate copy(RelTraitSet traitSet, RelNode input,
                          ImmutableBitSet groupSet,
                          List<ImmutableBitSet> groupSets,
                          List<AggregateCall> aggCalls) {
        return new PangrovaAggregate(getCluster(), traitSet, input,
                groupSet, groupSets, aggCalls);
    }
}
