package org.pangrova.coordinator.index.lifecycle;

import org.pangrova.coordinator.index.ddl.ConnectorAccessorResolver;
import org.pangrova.indexing.build.IndexBuildCoordinator;
import org.pangrova.indexing.config.IndexingConfig;
import org.pangrova.metadata.index.IndexMetadataStore;

/**
 * Aggregated dependencies for index consistency checking (≤4 params).
 *
 * @param metadataStore    index metadata persistence
 * @param buildCoordinator index build task submission
 * @param accessorResolver connector accessor resolution
 * @param config           indexing configuration
 * @param leaderElection   maintenance leader election (nullable for backward compat)
 */
public record IndexConsistencyContext(
        IndexMetadataStore metadataStore,
        IndexBuildCoordinator buildCoordinator,
        ConnectorAccessorResolver accessorResolver,
        IndexingConfig config,
        MaintenanceLeaderElection leaderElection) {

    public IndexConsistencyContext {
        if (metadataStore == null) throw new IllegalArgumentException("metadataStore must not be null");
        if (buildCoordinator == null) throw new IllegalArgumentException("buildCoordinator must not be null");
        if (accessorResolver == null) throw new IllegalArgumentException("accessorResolver must not be null");
        if (config == null) throw new IllegalArgumentException("config must not be null");
        // leaderElection is nullable for backward compatibility
    }

    /**
     * Backward-compatible constructor without leader election.
     */
    public IndexConsistencyContext(
            IndexMetadataStore metadataStore,
            IndexBuildCoordinator buildCoordinator,
            ConnectorAccessorResolver accessorResolver,
            IndexingConfig config) {
        this(metadataStore, buildCoordinator, accessorResolver, config, null);
    }
}
