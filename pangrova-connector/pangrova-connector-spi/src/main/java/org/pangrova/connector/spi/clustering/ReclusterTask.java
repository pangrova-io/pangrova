package org.pangrova.connector.spi.clustering;

import java.util.List;

/**
 * Task dispatched from Coordinator to Worker for recluster execution.
 *
 * @param taskId      unique task identifier
 * @param catalogName catalog containing the table
 * @param tableName   fully qualified table name
 * @param columnNames clustering columns
 * @param mode        INTERLEAVE or LINEAR
 * @param filePaths   data files to recluster
 */
public record ReclusterTask(
        String taskId,
        String catalogName,
        ClusteringTableRef tableName,
        List<String> columnNames,
        ClusteringMode mode,
        List<String> filePaths) {

    public ReclusterTask {
        columnNames = List.copyOf(columnNames);
        filePaths = List.copyOf(filePaths);
    }

    /**
     * Converts this task to a ClusteringRequest for connector execution.
     */
    public ClusteringRequest toRequest() {
        return new ClusteringRequest(tableName, columnNames, mode, filePaths);
    }
}
