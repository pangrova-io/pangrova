package org.pangrova.planner.cost.join;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.pangrova.planner.cost.distribution.PangrovaDistribution;
import org.pangrova.planner.cost.distribution.PangrovaExchange;

/**
 * Converts a logical join to a physical broadcast join when the build side
 * is smaller than the broadcast threshold (10 MB by default).
 * The smaller side is broadcast to all nodes holding the probe side.
 *
 * <p>Row size estimation uses statistics-based average row size from
 * {@link RelMetadataQuery#getAverageRowSize(RelNode)} when available,
 * falling back to a schema-based default of 128 bytes/row.</p>
 */
public final class BroadcastJoinRule extends RelOptRule {

    /** Default broadcast threshold: 10 MB estimated build-side size. */
    static final long BROADCAST_THRESHOLD_BYTES = 10L * 1024 * 1024;

    /** Fallback bytes per row when statistics are unavailable. */
    static final double DEFAULT_ROW_BYTES = 128.0;

    public BroadcastJoinRule() {
        super(operand(LogicalJoin.class, any()), "BroadcastJoinRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Join join = call.rel(0);
        RelMetadataQuery mq = call.getMetadataQuery();
        // The native HashJoin build input is the right side. Do not admit a
        // join because the *left* is small and then broadcast a large right.
        RelNode buildSide = join.getRight();
        double buildRows = buildSide.estimateRowCount(mq);
        double buildRowBytes = estimateRowBytes(buildSide, mq);

        double estimatedBuildBytes = buildRows * buildRowBytes;
        return estimatedBuildBytes <= BROADCAST_THRESHOLD_BYTES;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);
        RelNode buildInput = join.getRight();
        RelNode broadcastInput = new PangrovaExchange(
                join.getCluster(),
                buildInput.getTraitSet().replace(PangrovaDistribution.BROADCAST),
                buildInput,
                PangrovaDistribution.BROADCAST);
        PangrovaBroadcastJoin broadcastJoin = new PangrovaBroadcastJoin(
                new BroadcastJoinConfig(
                        join.getCluster(),
                        join.getTraitSet(),
                        join.getLeft(),
                        broadcastInput,
                        join.getCondition(),
                        join.getVariablesSet(),
                        join.getJoinType()));
        call.transformTo(broadcastJoin);
    }

    /**
     * Estimates bytes per row using statistics-based average row size when available,
     * falling back to schema-based default when stats are unavailable.
     */
    static double estimateRowBytes(RelNode relNode, RelMetadataQuery mq) {
        Double avgRowSize = mq.getAverageRowSize(relNode);
        return (avgRowSize != null && avgRowSize > 0) ? avgRowSize : DEFAULT_ROW_BYTES;
    }
}
