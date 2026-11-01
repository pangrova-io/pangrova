package org.pangrova.coordinator.bootstrap.module;

import org.pangrova.coordinator.bootstrap.IndexingBootstrap;
import org.pangrova.coordinator.bootstrap.IndexQueryBootstrap;
import org.pangrova.coordinator.indexing.IndexAwareSplitFetcher;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Optional indexing module (priority 200).
 * Binds the {@link IndexAwareSplitFetcher} when the indexing subsystem
 * is available. Silently skips if no {@link IndexingBootstrap} is bound.
 */
public final class IndexingModule implements CoordinatorModule {
    private static final int INDEXING_STARTUP_PRIORITY = 200;

    private static final Logger LOG = LoggerFactory.getLogger(IndexingModule.class);

    @Override
    public void configure(CoordinatorModuleContext context) {
        IndexingBootstrap indexing = context.optional(IndexingBootstrap.class);
        if (indexing != null && indexing.getSplitFilter() != null) {
            CatalogRegistry catalog = context.optional(CatalogRegistry.class);
            var fetcher = new IndexAwareSplitFetcher(
                    catalog,
                    indexing.getSplitFilter(),
                    indexing.getMetadataStore(),
                    indexing.getAccessorResolver(),
                    indexing.getArtifactBuildStore());
            context.bind(IndexAwareSplitFetcher.class, fetcher);
            LOG.info("event=module_configured module=Indexing source=full_bootstrap");
            return;
        }
        IndexQueryBootstrap queryBootstrap = context.optional(IndexQueryBootstrap.class);
        if (queryBootstrap != null && queryBootstrap.getSplitFilter() != null) {
            context.bind(org.pangrova.coordinator.indexing.filter.IndexSplitFilter.class,
                    queryBootstrap.getSplitFilter());
            LOG.info("event=module_configured module=Indexing source=query_bootstrap");
            return;
        }
        LOG.info("event=module_skipped module=Indexing reason=no_indexing_bootstrap");
    }

    @Override
    public int priority() {
        return INDEXING_STARTUP_PRIORITY;
    }
}
