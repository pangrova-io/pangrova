package org.pangrova.coordinator.clustering;

import org.pangrova.metadata.catalog.CatalogRegistry;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Immutable dependency bundle for ClusteringDdlExecutor (≤4 fields per governance).
 *
 * @param metadataStore   persistence for clustering descriptors
 * @param scheduler       recluster task scheduler
 * @param statusReporter  clustering status computation
 * @param catalogRegistry catalog registry for connector resolution
 */
public record ClusteringDdlContext(
        ClusteringMetadataStore metadataStore,
        ClusteringScheduler scheduler,
        ClusteringStatusReporter statusReporter,
        CatalogRegistry catalogRegistry) {

    public ClusteringDdlContext {
        checkNotNull(metadataStore, "metadataStore");
        checkNotNull(scheduler, "scheduler");
        checkNotNull(statusReporter, "statusReporter");
        checkNotNull(catalogRegistry, "catalogRegistry");
    }
}
