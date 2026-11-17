package org.pangrova.planner.api.statistics;

import org.pangrova.common.api.annotation.Internal;

import java.util.Optional;
import java.util.Set;

/**
 * Declared extension point; the current selectivity estimator does not wire a provider.
 * Supplies multi-column statistics for a table's column group, for estimation of
 * conjunctive-predicate selectivity over correlated columns.
 *
 * <p>Implementations may query connector metadata, cached ANALYZE results, or
 * return {@link Optional#empty()} when no statistics are available. An empty result
 * represents absent statistics, not a successful backend read after a failure.
 * Implementations must propagate read failures and support concurrent callers.
 * The caller borrows the provider and does not close its backing resources.</p>
 */
@FunctionalInterface
@Internal
public interface MultiColumnStatisticsProvider {

    /**
     * Looks up multi-column statistics covering the given columns of a table.
     *
     * @param tableName fully qualified SQL table name
     * @param columns   the predicate columns to look up (at least two)
     * @return statistics covering the columns, or empty if unavailable
     */
    Optional<MultiColumnStatistics> getStatistics(String tableName, Set<String> columns);

}
