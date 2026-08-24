package org.pangrova.coordinator.index.ddl;

import org.pangrova.indexing.build.IndexBuildCoordinator;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.index.IndexMetadataStore;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Immutable dependency bundle for IndexDdlExecutor (≤4 fields per governance).
 *
 * @param indexMetadataStore  persistent store for index descriptors and file mappings
 * @param buildCoordinator    async index build task coordinator
 * @param catalogRegistry     catalog registry for table/column resolution
 * @param accessorResolver    resolves connector-specific accessors per catalog
 */
public record IndexDdlContext(
        IndexMetadataStore indexMetadataStore,
        IndexBuildCoordinator buildCoordinator,
        CatalogRegistry catalogRegistry,
        ConnectorAccessorResolver accessorResolver) {

    public IndexDdlContext {
        checkNotNull(indexMetadataStore, "indexMetadataStore");
        checkNotNull(buildCoordinator, "buildCoordinator");
        checkNotNull(catalogRegistry, "catalogRegistry");
        checkNotNull(accessorResolver, "accessorResolver");
    }
}
