package org.pangrova.coordinator.index.lifecycle;

import org.pangrova.coordinator.index.ddl.ConnectorAccessorResolver;
import org.pangrova.indexing.build.IndexBuildCoordinator;
import org.pangrova.metadata.index.IndexMetadataStore;

/**
 * Aggregated dependencies for commit-driven index maintenance (≤4 params).
 *
 * @param metadataStore          index metadata persistence
 * @param buildCoordinator       index build task submission
 * @param connectorAccessorResolver connector accessor resolution
 */
public record IndexCommitContext(
        IndexMetadataStore metadataStore,
        IndexBuildCoordinator buildCoordinator,
        ConnectorAccessorResolver connectorAccessorResolver) {

    public IndexCommitContext {
        if (metadataStore == null) throw new IllegalArgumentException("metadataStore must not be null");
        if (buildCoordinator == null) throw new IllegalArgumentException("buildCoordinator must not be null");
        if (connectorAccessorResolver == null) {
            throw new IllegalArgumentException("connectorAccessorResolver must not be null");
        }
    }
}
