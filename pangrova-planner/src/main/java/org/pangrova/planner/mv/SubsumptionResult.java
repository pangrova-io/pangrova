package org.pangrova.planner.mv;

import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.Set;

/**
 * Describes the compensating operations needed to answer a query from a materialized view.
 *
 * <p>When structural subsumption matching succeeds, this record captures what additional
 * operations (projection, filtering, rollup) must be applied on top of the MV scan
 * to produce the query's expected output.</p>
 *
 * @param mvName             name of the matched materialized view
 * @param projectedColumns   columns to project from the MV (empty if full projection)
 * @param residualFilters    filter predicates to apply on top of the MV scan
 * @param rollupColumns      GROUP BY columns for rollup (empty if no rollup needed)
 */
public record SubsumptionResult(
        String mvName,
        Set<String> projectedColumns,
        List<RexNode> residualFilters,
        Set<String> rollupColumns) {

    /** Freezes compensating operations before the matched view is rewritten. */
    public SubsumptionResult {
        projectedColumns = Set.copyOf(projectedColumns);
        residualFilters = List.copyOf(residualFilters);
        rollupColumns = Set.copyOf(rollupColumns);
    }

    /**
     * Returns true if the MV can be scanned directly without any compensating operations.
     */
    public boolean isExactMatch() {
        return projectedColumns.isEmpty() && residualFilters.isEmpty() && rollupColumns.isEmpty();
    }

    /**
     * Returns true if the MV can be used as a direct scan (no compensating operations needed).
     * Alias for {@link #isExactMatch()} for readability in projection-subsumption contexts.
     */
    public boolean isDirectScan() {
        return isExactMatch();
    }

    /**
     * Returns true if a rollup aggregation is needed on top of the MV scan.
     */
    public boolean needsRollup() {
        return !rollupColumns.isEmpty();
    }

    /**
     * Returns true if residual filtering is needed on top of the MV scan.
     */
    public boolean needsFilter() {
        return !residualFilters.isEmpty();
    }

    /**
     * Returns true if projection is needed (query uses subset of MV columns).
     */
    public boolean needsProjection() {
        return !projectedColumns.isEmpty();
    }
}
