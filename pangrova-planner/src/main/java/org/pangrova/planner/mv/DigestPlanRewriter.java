package org.pangrova.planner.mv;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Plan rewriter that uses structural subsumption matching to determine
 * whether a query plan can be answered from a materialized view.
 *
 * <p>Structural matching supports the case where the query is a subset of
 * the MV — e.g. the MV covers a base table scan (optionally with projection),
 * and the query adds extra filters or narrows the projection on top. When
 * the MV's base tables and columns subsume the query's requirements, the
 * rewriter replaces the base table scan with a scan on the MV and preserves
 * the query's additional filter/projection on top.</p>
 */
final class DigestPlanRewriter implements MaterializedViewQueryRewriter.PlanRewriter {

    private final StructuralSubsumptionMatcher subsumptionMatcher = new StructuralSubsumptionMatcher();

    /**
     * Matches using structural subsumption: projection/filter/rollup three-dimensional
     * sub-graph matching via StructuralSubsumptionMatcher.
     */
    @Override
    public boolean matches(RelNode queryPlan, MaterializedViewDefinition mv) {
        return subsumptionMatcher.match(queryPlan, mv).isPresent();
    }

    @Override
    public RelNode rewrite(RelNode original, MaterializedViewDefinition mv) {
        RelOptCluster cluster = original.getCluster();
        RelDataType rowType = original.getRowType();
        RelOptTable mvTable = new MaterializedViewTableRef(mv.name(), rowType);
        RelNode mvScan = LogicalTableScan.create(cluster, mvTable, List.of());

        RexNode residualFilter = extractResidualFilter(original);
        if (residualFilter != null) {
            return LogicalFilter.create(mvScan, residualFilter);
        }
        return mvScan;
    }

    private static RexNode extractResidualFilter(RelNode node) {
        if (node instanceof Filter filter) {
            return filter.getCondition();
        }
        if (node instanceof Project project) {
            return extractResidualFilter(project.getInput());
        }
        return null;
    }
}
