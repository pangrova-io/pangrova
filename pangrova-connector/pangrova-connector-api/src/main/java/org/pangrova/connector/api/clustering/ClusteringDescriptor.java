package org.pangrova.connector.api.clustering;

import org.pangrova.connector.api.enums.ClusteringMode;
import org.pangrova.connector.api.enums.ClusteringState;

import org.pangrova.common.api.annotation.Experimental;

import java.util.List;

/**
 * Describes the clustering configuration for a table.
 *
 * @param tableName   fully qualified table name
 * @param columnNames columns in clustering order
 * @param mode        INTERLEAVE or LINEAR
 * @param state       current clustering state
 */
@Experimental
public record ClusteringDescriptor(
        ClusteringTableRef tableName,
        List<String> columnNames,
        ClusteringMode mode,
        ClusteringState state) {

    public ClusteringDescriptor {
        columnNames = List.copyOf(columnNames);
        if (columnNames.isEmpty()) {
            throw new IllegalArgumentException("at least 1 column required");
        }
    }

    /**
     * Returns a new descriptor with the given state.
     */
    public ClusteringDescriptor withState(ClusteringState newState) {
        return new ClusteringDescriptor(tableName, columnNames, mode, newState);
    }
}
