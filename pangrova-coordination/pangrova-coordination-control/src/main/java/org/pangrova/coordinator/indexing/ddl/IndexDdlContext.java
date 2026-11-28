package org.pangrova.coordinator.indexing.ddl;

import org.pangrova.coordinator.indexing.connector.ConnectorAccessorResolver;

import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;

import static org.pangrova.common.validation.Preconditions.checkNotNull;

/**
 * Immutable dependency bundle for IndexDdlExecutor (≤4 fields per governance).
 *
 * @param indexMetadataStore  persistent store for index descriptors and file mappings
 * @param buildServices       isolated data-skipping and AI Native build planes
 * @param catalogRegistry     catalog registry for table/column resolution
 * @param accessorResolver    resolves connector-specific accessors per catalog
 */
public record IndexDdlContext(
        IndexMetadataStore indexMetadataStore,
        IndexBuildServices buildServices,
        CatalogRegistry catalogRegistry,
        ConnectorAccessorResolver accessorResolver) {

    public IndexDdlContext {
        checkNotNull(indexMetadataStore, "indexMetadataStore");
        checkNotNull(buildServices, "buildServices");
        checkNotNull(catalogRegistry, "catalogRegistry");
        checkNotNull(accessorResolver, "accessorResolver");
    }
}
