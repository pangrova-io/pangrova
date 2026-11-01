package org.pangrova.coordinator.bootstrap;

import org.pangrova.coordinator.adapter.indexing.IndexDescriptorCache;
import org.pangrova.coordinator.adapter.indexing.IndexEvalMetrics;
import org.pangrova.coordinator.adapter.indexing.filter.IndexEvalConfig;
import org.pangrova.coordinator.adapter.indexing.filter.IndexSplitFilter;
import org.pangrova.indexing.store.IndexMetadataStore;
import org.pangrova.spi.observability.MetricsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lightweight bootstrap for index query-path components only.
 *
 * <p>Initializes: metadataStore, IndexDescriptorCache, IndexSplitFilter,
 * IndexEvalConfig. Does not start build coordinator, commit handler,
 * garbage collector, or any maintenance-only components.</p>
 *
 * <p>Used by query-only coordinator roles to enable split filtering
 * without the overhead of the full indexing build pipeline.</p>
 */
public final class IndexQueryBootstrap implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexQueryBootstrap.class);

    private final IndexSplitFilter splitFilter;

    IndexQueryBootstrap(IndexMetadataStore metadataStore,
                        MetricsProvider metricsProvider,
                        long evalTimeoutMs,
                        int evalParallelism) {
        if (metadataStore == null) {
            throw new IllegalStateException(
                    "IndexQueryBootstrap requires metadataStore but received null");
        }
        IndexDescriptorCache descriptorCache = new IndexDescriptorCache(metadataStore);

        IndexEvalMetrics evalMetrics = metricsProvider != null
                ? new IndexEvalMetrics(metricsProvider) : null;
        var evalConfig = new IndexEvalConfig(evalMetrics, evalTimeoutMs, evalParallelism);
        this.splitFilter = new IndexSplitFilter(metadataStore, descriptorCache, evalConfig);

        LOG.info("event=index_query_bootstrap_completed");
    }

    /** Returns the split filter for query-time integration. */
    public IndexSplitFilter getSplitFilter() {
        return splitFilter;
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
