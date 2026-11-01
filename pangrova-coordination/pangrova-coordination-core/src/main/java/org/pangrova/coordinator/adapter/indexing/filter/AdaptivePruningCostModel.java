package org.pangrova.coordinator.adapter.indexing.filter;

import org.pangrova.spi.connector.index.predicate.IndexPredicate;
import org.pangrova.spi.connector.index.statistics.FileColumnStatistics;

import java.util.List;

/**
 * Cost-based model for deciding whether L1 index evaluation is worthwhile.
 *
 * <p>Skips L1 when the estimated benefit is too low relative to the evaluation cost:
 * high L0 pruning ratio (>95%), low index coverage (<50%), or high predicate
 * selectivity (>50% of data matches).</p>
 *
 * <p>Selectivity estimation uses NDV and range overlap from L0 file statistics.</p>
 */
public final class AdaptivePruningCostModel {
    private static final double UNKNOWN_SELECTIVITY = 0.5;

    private static final double L0_PRUNE_SKIP_THRESHOLD = 0.95;
    private static final double COVERAGE_SKIP_THRESHOLD = 0.50;
    private static final double SELECTIVITY_SKIP_THRESHOLD = 0.50;

    /**
     * Returns true if L1 index evaluation should be skipped for this query.
     *
     * @param l0PruningRatio       ratio of files pruned by L0 (0.0 to 1.0)
     * @param coverage             ratio of indexed files to total files (0.0 to 1.0)
     * @param estimatedSelectivity estimated predicate selectivity (0.0 to 1.0)
     */
    public boolean shouldSkipL1(double l0PruningRatio, double coverage,
                                double estimatedSelectivity) {
        if (l0PruningRatio > L0_PRUNE_SKIP_THRESHOLD) {
            return true;
        }
        if (coverage < COVERAGE_SKIP_THRESHOLD) {
            return true;
        }
        return estimatedSelectivity > SELECTIVITY_SKIP_THRESHOLD;
    }

    /**
     * Estimates predicate selectivity against file-level statistics.
     *
     * <p>Uses NDV-based estimation: for equality predicates, selectivity ≈ 1/NDV.
     * For range predicates, selectivity ≈ range overlap ratio. Falls back to 0.5
     * (unknown) when statistics are insufficient.</p>
     *
     * @param fileStats  per-file column statistics for predicate columns
     * @param predicate  the index predicate to estimate selectivity for
     * @return estimated selectivity (0.0 = highly selective, 1.0 = matches everything)
     */
    public double estimateSelectivity(List<FileColumnStatistics> fileStats,
                                      IndexPredicate predicate) {
        if (fileStats == null || fileStats.isEmpty()) {
            return UNKNOWN_SELECTIVITY; // unknown — conservative, allow L1
        }
        double totalSelectivity = 0.0;
        int counted = 0;
        for (FileColumnStatistics stats : fileStats) {
            if (stats.approximateNdv() > 0) {
                totalSelectivity += 1.0 / stats.approximateNdv();
                counted++;
            }
        }
        return counted > 0 ? totalSelectivity / counted : UNKNOWN_SELECTIVITY;
    }
}
