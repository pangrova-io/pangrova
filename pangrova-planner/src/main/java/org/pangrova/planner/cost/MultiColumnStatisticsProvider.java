package org.pangrova.planner.cost;

import org.pangrova.planner.stats.MultiColumnStatistics;

import java.util.Optional;
import java.util.Set;

/**
 * Supplies multi-column statistics for a table's column group. Used by
 * {@link PangrovaRelMdSelectivity} to correct the naive independence assumption
 * when estimating conjunctive-predicate selectivity over correlated columns.
 *
 * <p>Implementations may query connector metadata, cached ANALYZE results, or
 * return {@link Optional#empty()} when no statistics are available (in which
 * case the estimator falls back to the independence product).</p>
 */
@FunctionalInterface
public interface MultiColumnStatisticsProvider {

    /**
     * Looks up multi-column statistics covering the given columns of a table.
     *
     * @param tableName fully qualified table name (same format as
     *                  {@link org.apache.calcite.rel.core.TableScan} qualified name)
     * @param columns   the predicate columns to look up (at least two)
     * @return statistics covering the columns, or empty if unavailable
     */
    Optional<MultiColumnStatistics> getStatistics(String tableName, Set<String> columns);

    /** Provider that always returns empty (independence baseline only). */
    static MultiColumnStatisticsProvider none() {
        return (tableName, columns) -> Optional.empty();
    }
}
