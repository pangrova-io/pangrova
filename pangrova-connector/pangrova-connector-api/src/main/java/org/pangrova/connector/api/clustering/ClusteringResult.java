package org.pangrova.connector.api.clustering;

import org.pangrova.common.api.annotation.Experimental;

import java.util.List;

/**
 * Result of a recluster operation.
 *
 * @param rewrittenFileCount number of files rewritten
 * @param addedFilePaths     new files created by recluster
 * @param removedFilePaths   old files replaced by recluster
 * @param durationMs         wall-clock time of recluster operation in milliseconds
 */
@Experimental
public record ClusteringResult(
        int rewrittenFileCount,
        List<String> addedFilePaths,
        List<String> removedFilePaths,
        long durationMs) {

    public ClusteringResult {
        addedFilePaths = List.copyOf(addedFilePaths);
        removedFilePaths = List.copyOf(removedFilePaths);
    }
}
