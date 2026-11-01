package org.pangrova.coordinator.clustering;

import org.pangrova.spi.connector.clustering.ClusteringDescriptor;
import org.pangrova.spi.connector.clustering.ClusteringTableRef;

import java.util.Optional;

/**
 * Persistence contract for clustering descriptors.
 * Stores the clustering configuration (columns, mode, state) per table.
 */
public interface ClusteringMetadataStore {

    /**
     * Stores or updates the clustering descriptor for a table.
     */
    void store(ClusteringDescriptor descriptor);

    /**
     * Retrieves the clustering descriptor for a table.
     */
    Optional<ClusteringDescriptor> find(ClusteringTableRef tableName);

    /**
     * Removes the clustering descriptor for a table.
     */
    void remove(ClusteringTableRef tableName);
}
