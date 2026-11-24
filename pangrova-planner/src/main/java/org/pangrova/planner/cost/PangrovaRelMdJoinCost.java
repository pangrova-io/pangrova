package org.pangrova.planner.cost;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;

/**
 * Prices an unselected logical join as a comparison of its input rows.
 * Its native hash/broadcast alternatives publish their own measured work and
 * network costs; treating the logical node as zero CPU makes them unreachable.
 */
public final class PangrovaRelMdJoinCost
        implements MetadataHandler<BuiltInMetadata.NonCumulativeCost> {

    private static final RelMetadataProvider PROVIDER = ReflectiveRelMetadataProvider.reflectiveSource(
            new PangrovaRelMdJoinCost(), BuiltInMethod.NON_CUMULATIVE_COST.method);

    @Override
    public org.apache.calcite.rel.metadata.MetadataDef<BuiltInMetadata.NonCumulativeCost>
            getDef() {
        return BuiltInMetadata.NonCumulativeCost.DEF;
    }

    public static RelMetadataProvider provider() {
        return PROVIDER;
    }

    /** Estimates nested-loop comparisons from the actual logical join cardinalities. */
    public RelOptCost getNonCumulativeCost(LogicalJoin join, RelMetadataQuery mq) {
        Double left = mq.getRowCount(join.getLeft());
        Double right = mq.getRowCount(join.getRight());
        Double output = mq.getRowCount(join);
        if (left == null || right == null || output == null
                || !Double.isFinite(left) || !Double.isFinite(right)
                || !Double.isFinite(output)) {
            return join.computeSelfCost(join.getCluster().getPlanner(), mq);
        }
        double comparisons = Math.min(1.0e18,
                Math.max(1.0, left) * Math.max(1.0, right));
        var planner = join.getCluster().getPlanner();
        var config = planner.getContext().unwrapOrThrow(CostModelConfig.class);
        return planner.getCostFactory().makeCost(
                output, comparisons * config.cpuOperatorCost(), 0);
    }
}
