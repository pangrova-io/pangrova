package org.pangrova.indexing.core.planner.vector;

import org.pangrova.indexing.api.vector.VectorHybridSearchStrategy;
import org.pangrova.indexing.api.vector.VectorHybridSearchStrategy.Mode;

/**
 * Selects the hybrid search strategy based on estimated filter selectivity.
 *
 * <p>When selectivity is high (most rows survive the filter), ANN search
 * runs first with an over-fetched K, then post-filters. When selectivity
 * is low (few rows survive), the filter runs first to reduce the candidate
 * set, then brute-force scans the survivors.</p>
 *
 * <p>Stateless and thread-safe.</p>
 */
public final class VectorHybridSearchPlanner {

    /** Default selectivity threshold: above this, ANN runs first. */
    static final double DEFAULT_ANN_FIRST_THRESHOLD = 0.5;

    /** Default over-fetch multiplier for ANN_THEN_FILTER mode. */
    static final double DEFAULT_OVER_FETCH_MULTIPLIER = 1.5;

    /** Selectivity range for FILTERED_ANN: lower bound (inclusive). */
    static final double FILTERED_ANN_LOW = 0.2;

    /** Selectivity range for FILTERED_ANN: upper bound (exclusive, capped by annFirstThreshold). */
    static final double FILTERED_ANN_HIGH = 0.5;

    private VectorHybridSearchPlanner() {}

    /**
     * Selects the optimal hybrid search strategy using default thresholds.
     *
     * @param estimatedSelectivity fraction of rows surviving the pre-filter (0.0 to 1.0)
     * @param topK                 user-requested number of nearest neighbors
     * @param totalRows            total row count in the table
     * @return the chosen strategy with effective K
     */
    public static VectorHybridSearchStrategy selectStrategy(
            double estimatedSelectivity, int topK, long totalRows) {
        return selectStrategy(estimatedSelectivity, topK, totalRows,
                DEFAULT_ANN_FIRST_THRESHOLD, DEFAULT_OVER_FETCH_MULTIPLIER);
    }

    /**
     * Selects the optimal hybrid search strategy with configurable thresholds.
     *
     * <p>Three-tier strategy selection based on filter selectivity:
     * <ul>
     *   <li>High selectivity ({@code >= annFirstThreshold}): ANN_THEN_FILTER with over-fetch</li>
     *   <li>Moderate selectivity ({@code [filteredAnnLow, annFirstThreshold)}): FILTERED_ANN
     *       — inline filter evaluation during graph traversal</li>
     *   <li>Low selectivity ({@code < filteredAnnLow}): FILTER_THEN_BRUTEFORCE</li>
     * </ul>
     *
     * @param estimatedSelectivity fraction of rows surviving the pre-filter (0.0 to 1.0)
     * @param topK                 user-requested number of nearest neighbors
     * @param totalRows            total row count in the table
     * @param annFirstThreshold    selectivity above which ANN runs first
     * @param overFetchMultiplier  multiplier for over-fetching candidates in ANN_THEN_FILTER
     * @return the chosen strategy with effective K
     */
    public static VectorHybridSearchStrategy selectStrategy(
            double estimatedSelectivity, int topK, long totalRows,
            double annFirstThreshold, double overFetchMultiplier) {
        return selectStrategy(estimatedSelectivity, topK, totalRows,
                annFirstThreshold, overFetchMultiplier,
                FILTERED_ANN_LOW, FILTERED_ANN_HIGH);
    }

    /**
     * Selects the optimal hybrid search strategy with fully configurable thresholds.
     *
     * @param estimatedSelectivity fraction of rows surviving the pre-filter (0.0 to 1.0)
     * @param topK                 user-requested number of nearest neighbors
     * @param totalRows            total row count in the table
     * @param annFirstThreshold    selectivity above which ANN runs first
     * @param overFetchMultiplier  multiplier for over-fetching candidates in ANN_THEN_FILTER
     * @param filteredAnnLow       lower bound for FILTERED_ANN selectivity range
     * @param filteredAnnHigh      upper bound for FILTERED_ANN selectivity range
     * @return the chosen strategy with effective K
     */
    public static VectorHybridSearchStrategy selectStrategy(
            double estimatedSelectivity, int topK, long totalRows,
            double annFirstThreshold, double overFetchMultiplier,
            double filteredAnnLow, double filteredAnnHigh) {
        if (estimatedSelectivity >= annFirstThreshold) {
            long overFetchK = Math.min(
                    (long) Math.ceil(topK / estimatedSelectivity * overFetchMultiplier),
                    totalRows);
            int effectiveK = (int) Math.max(topK, Math.min(overFetchK, Integer.MAX_VALUE));
            return new VectorHybridSearchStrategy(
                    Mode.ANN_THEN_FILTER, effectiveK);
        }
        if (estimatedSelectivity >= filteredAnnLow
                && estimatedSelectivity < filteredAnnHigh) {
            return new VectorHybridSearchStrategy(Mode.FILTERED_ANN, topK);
        }
        return new VectorHybridSearchStrategy(Mode.FILTER_THEN_BRUTEFORCE, topK);
    }
}
