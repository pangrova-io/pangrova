package org.pangrova.planner.cost;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Set;

/**
 * Reduces estimated scan cost when an index exists on a filter column (R11.2).
 *
 * <p>The cost factor is derived from the index's historical skip ratio:
 * if an index historically skips 70% of splits, the effective scan cost
 * is reduced to 30% of the original estimate.</p>
 *
 * <p>Thread-safe: immutable after construction. The provider callback
 * must be thread-safe.</p>
 *
 * @see StatisticsBasedCostEstimator
 */
public final class IndexCostFactor {

    private static final Logger LOG = LoggerFactory.getLogger(IndexCostFactor.class);

    /** Default skip ratio when index exists but no historical data available. */
    static final double DEFAULT_SKIP_RATIO = 0.5;

    /** Minimum cost multiplier to prevent zero-cost estimates. */
    static final double MIN_COST_MULTIPLIER = 0.05;

    private final IndexAvailabilityProvider indexProvider;

    /**
     * @param indexProvider provides index availability and skip ratio per table/column
     */
    public IndexCostFactor(IndexAvailabilityProvider indexProvider) {
        this.indexProvider = Objects.requireNonNull(indexProvider, "indexProvider");
    }

    /**
     * Computes the cost multiplier for a scan on the given table and filter columns.
     * Returns 1.0 (no reduction) if no index exists on any filter column.
     *
     * @param tableName     fully qualified table name
     * @param filterColumns columns used in WHERE predicates
     * @return cost multiplier in [MIN_COST_MULTIPLIER, 1.0]
     */
    public double computeCostMultiplier(String tableName, Set<String> filterColumns) {
        if (filterColumns == null || filterColumns.isEmpty()) {
            return 1.0;
        }
        double bestSkipRatio = 0.0;
        for (String column : filterColumns) {
            double skipRatio = indexProvider.getSkipRatio(tableName, column);
            if (skipRatio > bestSkipRatio) {
                bestSkipRatio = skipRatio;
            }
        }
        if (bestSkipRatio <= 0.0) {
            return 1.0;
        }
        double multiplier = Math.max(MIN_COST_MULTIPLIER, 1.0 - bestSkipRatio);
        LOG.debug("event=index_cost_factor table={} bestSkipRatio={} multiplier={}",
                tableName, bestSkipRatio, multiplier);
        return multiplier;
    }

    /**
     * Provides index availability and historical skip ratio per table/column.
     * Implemented by the coordinator to bridge IndexEvalMetrics data.
     */
    @FunctionalInterface
    public interface IndexAvailabilityProvider {

        /**
         * Returns the historical skip ratio for the given table and column.
         * Returns 0.0 if no index exists or no historical data is available.
         * Returns a value in (0.0, 1.0] representing the fraction of splits
         * historically skipped by the index.
         *
         * @param tableName fully qualified table name
         * @param column    filter column name
         * @return skip ratio in [0.0, 1.0]
         */
        double getSkipRatio(String tableName, String column);
    }
}
