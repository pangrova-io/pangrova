package org.pangrova.indexing.api.descriptor;

import org.pangrova.common.api.annotation.Experimental;

import java.time.Instant;

/**
 * Tracks the coverage window of an index — how much of the table's data
 * is covered by the index.
 */
@Experimental
public record IndexCoverageWindow(
        String indexId,
        long lastIndexedSnapshotId,
        int indexedFileCount,
        int totalFileCount,
        Instant lastMaintenanceAt) {

    /**
     * Returns the ratio of indexed files to total files (0.0 to 1.0).
     */
    public double coverageRatio() {
        if (totalFileCount == 0) {
            return 1.0; // no files = fully covered
        }
        return (double) indexedFileCount / totalFileCount;
    }
}
