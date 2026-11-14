package org.pangrova.coordinator.indexing.lifecycle;

import org.pangrova.coordinator.indexing.connector.ConnectorAccessorResolver;
import org.pangrova.coordinator.indexing.ddl.IndexBuildServices;
import org.pangrova.indexing.config.IndexingConfig;
import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;

import java.util.Objects;

/**
 * Aggregated dependencies for index consistency checking.
 *
 * @param metadataStore    index metadata persistence
 * @param buildServices    distinct data-skipping and Native build planes
 * @param accessorResolver connector accessor resolution
 * @param config           indexing configuration
 * @param leaderElection   mandatory maintenance leader election
 */
public record IndexConsistencyContext(
        IndexMetadataStore metadataStore,
        IndexBuildServices buildServices,
        ConnectorAccessorResolver accessorResolver,
        IndexingConfig config,
        MaintenanceLeaderElection leaderElection) {

    public IndexConsistencyContext {
        Objects.requireNonNull(metadataStore, "metadataStore must not be null");
        Objects.requireNonNull(buildServices, "buildServices must not be null");
        Objects.requireNonNull(accessorResolver, "accessorResolver must not be null");
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(leaderElection, "leaderElection must not be null");
    }

}
