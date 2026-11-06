package org.pangrova.planner.core.cost.sort;

import org.pangrova.planner.core.cost.PangrovaCostFactory;
import org.pangrova.planner.core.cost.convention.PangrovaRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

/**
 * Physical sort operator for the Pangrova CBO.
 * Carries the PangrovaConvention trait so the Volcano planner can perform
 * convention-based physical layer selection.
 *
 * <p>CPU cost: O(n log n) comparisons for the sort phase.
 * Memory cost: proportional to input rows × row width (in-memory sort buffer).
 * I/O cost: spill to disk when memory budget is exceeded (modeled as a fraction).</p>
 */
public final class PangrovaSort extends Sort implements PangrovaRel {

    private static final double DEFAULT_ROW_WIDTH = 128.0;

    public PangrovaSort(RelOptCluster cluster, RelTraitSet traitSet,
                      RelNode input, RelCollation collation,
                      RexNode offset, RexNode fetch) {
        super(cluster, traitSet, input, collation, offset, fetch);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rows = mq.getRowCount(getInput());
        Double avgRowSize = mq.getAverageRowSize(getInput());
        double rowWidth = avgRowSize != null ? avgRowSize : DEFAULT_ROW_WIDTH;

        // CPU: O(n log n) comparisons
        double cpu = rows * Math.log(Math.max(rows, 2)) * 0.04;

        // Memory: full sort buffer
        double memory = rows * rowWidth;

        // I/O: minimal for in-memory sort; spill modeled as fraction
        double io = rows * rowWidth * 0.000061;

        // Network: sort is a local operator, no data transfer
        double network = 0;

        if (planner.getCostFactory() instanceof PangrovaCostFactory costFactory) {
            return costFactory.makeFiveDimCost(rows, cpu, io, memory, network);
        }
        return planner.getCostFactory().makeCost(rows, cpu, io);
    }

    @Override
    public Sort copy(RelTraitSet traitSet, RelNode newInput,
                     RelCollation newCollation, RexNode offset, RexNode fetch) {
        return new PangrovaSort(getCluster(), traitSet, newInput, newCollation, offset, fetch);
    }
}
