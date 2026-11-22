package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.indexing.evaluation.IndexEvalMetrics;
import org.pangrova.indexing.pruning.IndexEvalConfig;
import org.pangrova.indexing.pruning.IndexSplitFilter;
import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;
import org.pangrova.observability.api.metrics.MetricsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lightweight bootstrap for index query-path components only.
 *
 * <p>Initializes: metadataStore, IndexSplitFilter,
 * IndexEvalConfig. Does not start build coordinator, commit handler,
 * garbage collector, or any maintenance-only components.</p>
 *
 * <p>Used by query-only coordinator roles to enable split filtering
 * without the overhead of the full indexing build pipeline.</p>
 */
public final class IndexQueryBootstrap implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexQueryBootstrap.class);

    private final IndexSplitFilter splitFilter;
    private final IndexMetadataStore metadataStore;

    IndexQueryBootstrap(IndexMetadataStore metadataStore,
                        MetricsProvider metricsProvider,
                        long evalTimeoutMs,
                        int evalParallelism) {
        if (metadataStore == null) {
            throw new IllegalStateException(
                    "IndexQueryBootstrap requires metadataStore but received null");
        }
        this.metadataStore = metadataStore;
        IndexEvalMetrics evalMetrics = metricsProvider != null
                ? new IndexEvalMetrics(metricsProvider) : null;
        var evalConfig = new IndexEvalConfig(evalMetrics, evalTimeoutMs, evalParallelism);
        this.splitFilter = new IndexSplitFilter(metadataStore, evalConfig);

        LOG.info("event=index_query_bootstrap_completed");
    }

    /** Returns the split filter for query-time integration. */
    public IndexSplitFilter getSplitFilter() {
        return splitFilter;
    }

    /** Creates the query-side reader while this bootstrap retains filter ownership. */
    org.pangrova.coordinator.indexing.IndexAwareSplitFetcher createSplitFetcher(
            org.pangrova.metadata.catalog.registry.CatalogRegistry catalogs) {
        return new org.pangrova.coordinator.indexing.IndexAwareSplitFetcher(
                catalogs, splitFilter, metadataStore,
                new org.pangrova.coordinator.indexing.connector.ConnectorAccessorResolver(catalogs));
    }

    @Override
    public void close() {
        try {
            splitFilter.close();
        } catch (Exception e) {
            LOG.warn("event=index_query_bootstrap_close_failed error={}", e.getMessage());
        }
        LOG.info("event=index_query_bootstrap_closed");
    }
}
