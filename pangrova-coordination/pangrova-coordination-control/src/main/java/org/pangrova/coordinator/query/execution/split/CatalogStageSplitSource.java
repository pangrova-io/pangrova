package org.pangrova.coordinator.query.execution.split;

import org.pangrova.query.api.task.QuerySplit;

import java.util.List;
import java.util.Map;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.planner.analysis.PlanFragment;

/** Enumerates the authenticated pinned catalog read without index pruning. Borrows its registry. */
public final class CatalogStageSplitSource implements StageSplitSource {
    private final CatalogRegistry catalogs;

    public CatalogStageSplitSource(CatalogRegistry catalogs) {
        this.catalogs = catalogs;
    }

    @Override
    public List<QuerySplit> fetchAndFilterSplits(
            PlanFragment fragment, Map<String, Long> snapshotOverrides,
            String queryId, QuerySession session) {
        return SplitFetcher.fetchSplitsWithFilePaths(fragment.splitSources(), catalogs,
                snapshotOverrides, fragment.queryOutputColumns(), session);
    }
}
