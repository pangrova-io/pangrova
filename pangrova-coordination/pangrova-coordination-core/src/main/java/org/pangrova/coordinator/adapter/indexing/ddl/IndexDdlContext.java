package org.pangrova.coordinator.adapter.indexing.ddl;

import org.pangrova.metadata.core.catalog.registry.CatalogRegistry;
import org.pangrova.indexing.store.IndexMetadataStore;

import static org.pangrova.common.util.Preconditions.checkNotNull;

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
