package org.pangrova.indexing.api.descriptor;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Coverage statistics for a single index, used by the planner's cost model
 * to decide whether index-based pruning is worthwhile.
 *
 * <p>An index with low coverage (few files indexed relative to total) may
 * cost more to evaluate than it saves in data skipping. The planner uses
 * these stats to skip low-value indexes before generating hints.</p>
 *
 * @param indexId          the index identifier
 * @param indexedFileCount number of data files that have index entries
 * @param totalFileCount   total data files in the table scan
 */
@Experimental
public record IndexCoverageStats(
        String indexId,
        int indexedFileCount,
        int totalFileCount) {

    /**
     * Returns the coverage ratio (0.0 to 1.0).
     * Returns 1.0 when totalFileCount is zero (no files = fully covered).
     */
    public double coverageRatio() {
        if (totalFileCount == 0) {
            return 1.0;
        }
        return (double) indexedFileCount / totalFileCount;
    }

    /**
     * Returns true if coverage is below the given threshold,
     * indicating the index is not worth evaluating.
     */
    public boolean isBelowThreshold(double threshold) {
        return coverageRatio() < threshold;
    }
}
