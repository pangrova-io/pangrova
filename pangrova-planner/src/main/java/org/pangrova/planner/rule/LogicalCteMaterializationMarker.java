package org.pangrova.planner.rule;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

/**
 * F-PLANNER-009 fix: marker {@link RelNode} that flags an input subtree as a
 * candidate for CTE materialization (compute-once, reuse-many).
 *
 * <p>Emitted by {@link CteRelOptRule#onMatch} when a subtree's digest appears
 * more than once in the plan and materialization is cost-beneficial per
 * {@link CTEMaterializationRule#shouldMaterialize}. Downstream Substrait
 * converter or executor SHOULD recognize this marker and emit a CTE
 * (TempTable / SpoolingExchange) node so the subtree is computed once and
 * reused across all references.</p>
 *
 * <p>Cost model: zero self-cost (the marker is a no-op wrapper); placing the
 * marker tells the planner "the cost of the underlying subtree should only be
 * paid once even though it appears N times in the digest".</p>
 */
public final class LogicalCteMaterializationMarker extends SingleRel {

    /** Reference count this marker represents (set at creation by the rule). */
    private final int referenceCount;

    /** Subtree key (digest hash) so downstream can dedupe parallel markers. */
    private final String subtreeKey;

    public LogicalCteMaterializationMarker(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            String subtreeKey,
            int referenceCount) {
        super(cluster, traitSet, input);
        this.subtreeKey = subtreeKey;
        this.referenceCount = referenceCount;
    }

    public String subtreeKey() {
        return subtreeKey;
    }

    public int referenceCount() {
        return referenceCount;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LogicalCteMaterializationMarker(
                getCluster(), traitSet, sole(inputs), subtreeKey, referenceCount);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // Self cost = 0; the underlying subtree's cost is already counted once
        // by the marker wrapping it. Repeated references share this single cost.
        return planner.getCostFactory().makeZeroCost();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("cte_subtree_key", subtreeKey)
                .item("references", referenceCount);
    }
}
