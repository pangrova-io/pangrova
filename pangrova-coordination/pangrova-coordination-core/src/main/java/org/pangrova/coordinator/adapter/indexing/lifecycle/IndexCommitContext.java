package org.pangrova.coordinator.adapter.indexing.lifecycle;

import org.pangrova.coordinator.adapter.indexing.ddl.ConnectorAccessorResolver;
import org.pangrova.coordinator.adapter.indexing.ddl.IndexBuildServices;
import org.pangrova.indexing.store.IndexMetadataStore;

/**
 * Aggregated dependencies for commit-driven index maintenance (≤4 params).
 *
 * @param metadataStore          index metadata persistence
 * @param buildServices          distinct data-skipping and Native build planes
 * @param connectorAccessorResolver connector accessor resolution
 */
public record IndexCommitContext(
        IndexMetadataStore metadataStore,
        IndexBuildServices buildServices,
        ConnectorAccessorResolver connectorAccessorResolver) {

    public IndexCommitContext {
        if (metadataStore == null) throw new IllegalArgumentException("metadataStore must not be null");
        if (buildServices == null) throw new IllegalArgumentException("buildServices must not be null");
        if (connectorAccessorResolver == null) {
            throw new IllegalArgumentException("connectorAccessorResolver must not be null");
        }
    }
}
