package org.pangrova.planner.rule;

import java.util.Collections;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

/**
 * Leaf {@link RelNode} representing a reference to a materialized CTE temp table.
 *
 * <p>When a CTE is materialized once (via {@link LogicalCteMaterializationMarker}),
 * subsequent references in the plan are replaced with this scan node instead of
 * re-computing the CTE subtree. The executor reads from the temp storage
 * identified by {@link #subtreeKey()} instead of re-computing the original
 * subtree.</p>
 *
 * <p>Cost model: minimal — only the I/O cost of reading pre-computed rows from
 * temp storage. No CPU cost for re-computation.</p>
 *
 * <p>This class is immutable and thread-safe.</p>
 */
public final class CteTempTableScan extends AbstractRelNode {

    /** Cost factor per row for reading from temp storage (sequential scan). */
    private static final double READ_COST_PER_ROW = 0.01;

    /** Name of the CTE as declared in the WITH clause. */
    private final String cteName;

    /** Digest key of the materialized subtree (used to locate the temp result). */
    private final String subtreeKey;

    /** Row type of the materialized CTE result. */
    private final RelDataType rowType;

    /** Estimated row count of the materialized CTE result. */
    private final long estimatedRows;

    /**
     * Creates a new CTE temp table scan node.
     *
     * @param cluster       Calcite cluster (provides type factory, planner context)
     * @param traitSet      trait set for this node
     * @param cteName       name of the CTE (as declared in WITH clause)
     * @param subtreeKey    digest key identifying the materialized subtree
     * @param rowType       row type of the CTE result
     * @param estimatedRows estimated row count; must be non-negative
     */
    public CteTempTableScan(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            String cteName,
            String subtreeKey,
            RelDataType rowType,
            long estimatedRows) {
        super(cluster, traitSet);
        if (cteName == null || cteName.isBlank()) {
            throw new IllegalArgumentException("cteName must not be blank");
        }
        if (subtreeKey == null || subtreeKey.isBlank()) {
            throw new IllegalArgumentException("subtreeKey must not be blank");
        }
        if (rowType == null) {
            throw new IllegalArgumentException("rowType must not be null");
        }
        if (estimatedRows < 0) {
            throw new IllegalArgumentException("estimatedRows must be non-negative");
        }
        this.cteName = cteName;
        this.subtreeKey = subtreeKey;
        this.rowType = rowType;
        this.estimatedRows = estimatedRows;
    }

    /** Returns the CTE name as declared in the WITH clause. */
    public String cteName() {
        return cteName;
    }

    /** Returns the digest key identifying the materialized subtree in temp storage. */
    public String subtreeKey() {
        return subtreeKey;
    }

    /** Returns the estimated row count of the materialized CTE result. */
    public long estimatedRows() {
        return estimatedRows;
    }

    @Override
    protected RelDataType deriveRowType() {
        return rowType;
    }

    @Override
    public List<RelNode> getInputs() {
        return Collections.emptyList();
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty() : "CteTempTableScan is a leaf node with no inputs";
        return new CteTempTableScan(
                getCluster(), traitSet, cteName, subtreeKey, rowType, estimatedRows);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // Minimal cost: reading pre-computed rows from temp storage.
        // No CPU for re-computation; only sequential I/O proportional to row count.
        double rows = estimatedRows > 0 ? estimatedRows : 1.0;
        double ioCost = rows * READ_COST_PER_ROW;
        return planner.getCostFactory().makeCost(rows, 0, ioCost);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("cte", cteName)
                .item("subtreeKey", subtreeKey)
                .item("estimatedRows", estimatedRows);
    }
}
