package org.pangrova.spi.connector.clustering;

import org.pangrova.common.api.annotation.Experimental;

import java.util.List;

/**
 * Request to recluster a set of data files.
 *
 * @param tableName   target table
 * @param columnNames clustering columns
 * @param mode        INTERLEAVE or LINEAR
 * @param filePaths   data files to recluster
 */
@Experimental
public record ClusteringRequest(
        ClusteringTableRef tableName,
        List<String> columnNames,
        ClusteringMode mode,
        List<String> filePaths) {

    public ClusteringRequest {
        columnNames = List.copyOf(columnNames);
        filePaths = List.copyOf(filePaths);
        if (filePaths.isEmpty()) {
            throw new IllegalArgumentException("at least 1 file required");
        }
    }
}
