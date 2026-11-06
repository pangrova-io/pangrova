package org.pangrova.coordinator.clustering;

import org.pangrova.connector.api.clustering.ClusteringMode;
import org.pangrova.connector.api.clustering.ClusteringState;

import java.time.Instant;
import java.util.List;

/**
 * Snapshot of clustering health for a table, returned by SHOW CLUSTERING STATUS.
 *
 * @param clusteringRatio  ratio in [0.0, 1.0]; 1.0 = perfectly clustered
 * @param totalFiles       total data files in the table
 * @param clusteredFiles   files that are well-clustered
 * @param lastReclusterTime last recluster completion time (null if never)
 * @param mode             INTERLEAVE or LINEAR
 * @param columns          clustering columns
 * @param state            current clustering state
 */
public record ClusteringStatus(
        double clusteringRatio,
        int totalFiles,
        int clusteredFiles,
        Instant lastReclusterTime,
        ClusteringMode mode,
        List<String> columns,
        ClusteringState state) {

    public ClusteringStatus {
        columns = List.copyOf(columns);
    }
}
