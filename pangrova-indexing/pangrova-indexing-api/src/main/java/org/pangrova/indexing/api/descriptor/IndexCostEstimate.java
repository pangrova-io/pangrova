package org.pangrova.indexing.api.descriptor;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Cost estimate for a single index evaluation, used by the cost-based
 * index selection gate in {@code IndexSplitFilter}.
 *
 * <p>Fields:
 * <ul>
 *   <li>{@code estimatedSelectivity} — fraction of files expected to be pruned (0.0–1.0)</li>
 *   <li>{@code estimatedFilesToPrune} — absolute count of files expected to be pruned</li>
 *   <li>{@code totalFiles} — total files in the scan</li>
 *   <li>{@code estimatedEvalCostMicros} — estimated per-file evaluation cost in microseconds</li>
 *   <li>{@code recommended} — whether the cost model recommends using this index</li>
 * </ul>
 */
@Experimental
public record IndexCostEstimate(
        String indexId,
        double estimatedSelectivity,
        long estimatedFilesToPrune,
        long totalFiles,
        long estimatedEvalCostMicros,
        boolean recommended) {

    /**
     * Index coverage ratio: fraction of total files that have index mappings.
     * Returns 0.0 if totalFiles is zero.
     */
    public double coverageRatio() {
        return totalFiles > 0
                ? (double) estimatedFilesToPrune / totalFiles
                : 0.0;
    }

    /**
     * Creates a cost estimate indicating the index should be skipped.
     */
    public static IndexCostEstimate skip(String indexId, long totalFiles, String reason) {
        return new IndexCostEstimate(indexId, 0.0, 0, totalFiles, 0, false);
    }
}
