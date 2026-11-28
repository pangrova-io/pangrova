package org.pangrova.coordinator.query.execution.split;

import java.util.List;
import java.util.Map;
import org.pangrova.connector.api.metadata.SplitPlanningBudget.FileGranularity;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.planner.analysis.PlanFragment;
import org.pangrova.planner.analysis.SplitSourceDescriptor;

/** Enumerates the authenticated pinned catalog read without index pruning. Borrows its registry. */
public final class CatalogStageSplitSource implements StageSplitSource {
    private final CatalogRegistry catalogs;

    public CatalogStageSplitSource(CatalogRegistry catalogs) {
        this.catalogs = catalogs;
    }

    @Override
    public List<QuerySplit> fetchAndFilterSplits(
            PlanFragment fragment, List<SplitSourceDescriptor> sources, Map<String, Long> snapshotOverrides,
            QuerySession session, FileGranularity fileGranularity) {
        return SplitFetcher.fetchSplitsWithFilePaths(sources, catalogs,
                snapshotOverrides, fragment.queryOutputColumns(), session, fileGranularity);
    }
}
