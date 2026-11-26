package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.indexing.evaluation.IndexEvalMetrics;
import org.pangrova.indexing.pruning.IndexEvalConfig;
import org.pangrova.indexing.pruning.IndexSplitFilter;
import org.pangrova.coordinator.indexing.connector.ConnectorAccessorResolver;
import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;
import org.pangrova.observability.api.metrics.MetricsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

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
    private final ConnectorAccessorResolver accessorResolver;
    private final Consumer<GenerationJob> snapshotValidator;

    IndexQueryBootstrap(IndexMetadataStore metadataStore,
                        MetricsProvider metricsProvider,
                        long evalTimeoutMs,
                        int evalParallelism,
                        CatalogRegistry catalogs,
                        Consumer<GenerationJob> snapshotValidator) {
        if (metadataStore == null) {
            throw new IllegalStateException(
                    "IndexQueryBootstrap requires metadataStore but received null");
        }
        this.metadataStore = metadataStore;
        this.accessorResolver = new ConnectorAccessorResolver(
                java.util.Objects.requireNonNull(catalogs, "catalogs"));
        this.snapshotValidator = java.util.Objects.requireNonNull(
                snapshotValidator, "snapshotValidator");
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

    /** Revalidates Native source pins on active-active query coordinators. */
    public void validateSnapshotPin(GenerationJob job) {
        snapshotValidator.accept(job);
    }

    /** Creates the query-side reader while this bootstrap retains filter ownership. */
    org.pangrova.coordinator.indexing.IndexAwareSplitFetcher createSplitFetcher(
            org.pangrova.metadata.catalog.registry.CatalogRegistry catalogs) {
        return new org.pangrova.coordinator.indexing.IndexAwareSplitFetcher(
                catalogs, splitFilter, metadataStore, accessorResolver);
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
