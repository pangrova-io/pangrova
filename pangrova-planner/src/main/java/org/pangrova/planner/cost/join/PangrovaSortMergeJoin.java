package org.pangrova.planner.cost.join;

import org.pangrova.planner.cost.PangrovaCostFactory;
import org.pangrova.planner.cost.convention.PangrovaRel;
import org.pangrova.planner.cost.distribution.PangrovaDistribution;
import org.pangrova.planner.cost.distribution.PangrovaDistributionTraitDef;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Physical sort-merge join operator for the Pangrova CBO.
 * Preferred when both inputs are pre-sorted on the join key columns,
 * or when inputs are too large for hash join to fit in memory.
 *
 * <p>Memory cost: sort buffers for both sides when inputs are not pre-sorted.
 * When inputs are already sorted, memory cost is minimal (streaming merge).</p>
 *
 * <p>Network cost: derived from the distribution trait on this operator's traitSet.
 * The full 5-dimensional cost (rows/cpu/memory/io/network) is always populated.</p>
 */
public final class PangrovaSortMergeJoin extends Join implements PangrovaRel {

    private static final double DEFAULT_ROW_WIDTH = 128.0;

    /** Fraction of data held in memory for streaming merge (two rows per side). */
    private static final double STREAMING_MERGE_ROWS = 2.0;

    public PangrovaSortMergeJoin(SortMergeJoinConfig config) {
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

        boolean inputsPreSorted = areInputsPreSorted();

        double memory;
        double cpu;

        if (inputsPreSorted) {
            // Pre-sorted: streaming merge only — O(n+m) CPU, minimal memory
            cpu = (leftRows + rightRows) * 0.01;
            memory = STREAMING_MERGE_ROWS * (leftRowWidth + rightRowWidth);
        } else {
            // Unsorted: sort both sides O(n log n) + merge O(n+m), full sort buffers
            double sortCpu = leftRows * Math.log(Math.max(leftRows, 2)) * 0.04
                    + rightRows * Math.log(Math.max(rightRows, 2)) * 0.04;
            double mergeCpu = (leftRows + rightRows) * 0.01;
            cpu = sortCpu + mergeCpu;
            memory = leftRows * leftRowWidth + rightRows * rightRowWidth;
        }

        // I/O cost: reading both sides (same regardless of sort order)
        double io = (leftRows * leftRowWidth + rightRows * rightRowWidth) * 0.000122;

        // Network cost: derived from distribution trait on this operator's traitSet.
        double network = computeNetworkFromDistribution(
                leftRows, rightRows, leftRowWidth, rightRowWidth, planner);

        if (planner.getCostFactory() instanceof PangrovaCostFactory costFactory) {
            return costFactory.makeFiveDimCost(leftRows + rightRows, cpu, io, memory, network);
        }
        return planner.getCostFactory().makeCost(leftRows + rightRows, cpu, io);
    }

    private double computeNetworkFromDistribution(double leftRows, double rightRows,
                                                  double leftRowWidth, double rightRowWidth,
                                                  RelOptPlanner planner) {
        PangrovaDistribution dist = getTraitSet().getTrait(PangrovaDistributionTraitDef.INSTANCE);
        if (dist == null || dist == PangrovaDistribution.ANY) {
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

    /**
     * Checks whether both inputs are already sorted on the join key columns.
     * Uses Calcite's RelCollation trait to detect existing sort order.
     */
    private boolean areInputsPreSorted() {
        JoinInfo joinInfo = analyzeCondition();
        if (joinInfo.leftKeys.isEmpty()) {
            return false;
        }
        return hasMatchingCollation(getLeft(), joinInfo.leftKeys)
                && hasMatchingCollation(getRight(), joinInfo.rightKeys);
    }

    private static boolean hasMatchingCollation(RelNode input, List<Integer> keys) {
        if (keys.isEmpty()) {
            return false;
        }
        RelCollation collation = input.getTraitSet().getCollation();
        if (collation == null || collation.getFieldCollations().isEmpty()) {
            return false;
        }
        List<RelFieldCollation> fieldCollations = collation.getFieldCollations();
        if (fieldCollations.size() < keys.size()) {
            return false;
        }
        for (int i = 0; i < keys.size(); i++) {
            if (fieldCollations.get(i).getFieldIndex() != keys.get(i)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Join copy(
            RelTraitSet traitSet,
            RexNode conditionExpr,
            RelNode left,
            RelNode right,
            JoinRelType joinType,
            boolean semiJoinDone) {
        return new PangrovaSortMergeJoin(new SortMergeJoinConfig(
                getCluster(), traitSet, left, right,
                conditionExpr, variablesSet, joinType));
    }
}
