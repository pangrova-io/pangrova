package org.pangrova.coordinator.query.execution.split;

import java.util.List;
import java.util.Map;

import org.pangrova.coordinator.adapter.indexing.IndexAwareSplitFetcher;
import org.pangrova.coordinator.query.execution.dispatch.dag.DAGQueryContext;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.metadata.core.catalog.registry.CatalogRegistry;
import org.pangrova.planner.SplitSourceDescriptor;
import org.pangrova.planner.StagePlanAccess;
import org.pangrova.connector.spi.metadata.Constraint;

/** Fetches stage splits with snapshot and index-aware query constraints. */
public final class StageSplitFetcher {
    private final CatalogRegistry catalogRegistry;
    private final IndexAwareSplitFetcher indexAwareSplitFetcher;

    /** Creates a split fetcher with an optional index-aware source. */
    public StageSplitFetcher(
            CatalogRegistry catalogRegistry,
            IndexAwareSplitFetcher indexAwareSplitFetcher) {
        this.catalogRegistry = catalogRegistry;
        this.indexAwareSplitFetcher = indexAwareSplitFetcher;
    }

    /** Fetches the splits for one physical stage. */
    public List<SplitFetcher.SplitWithPaths> fetch(
            StageNode plan,
            StageDAG dag,
            String queryId,
            DAGQueryContext context) {
        if (context == null) {
            throw new IllegalStateException("DAG query context is required for authenticated split lookup");
        }
        if (plan.stagePlan().mode() == StagePlanAccess.Mode.DURABLE_REPLAY) {
            throw new IllegalStateException(
                    "Durable stage allocations require the current exact persisted partition gate");
        }
        Map<String, Long> overrides = context.snapshotOverrides();
        if (isOverwrite(context)) {
            return fetchWithoutConstraints(plan, dag, overrides, context.requireLiveSession());
        }
        if (indexAwareSplitFetcher == null) {
            return fetchFromCatalog(plan, dag, overrides, context.requireLiveSession());
        }
        return indexAwareSplitFetcher.fetchAndFilterSplits(
                plan.stagePlan().requireLiveFragment(), overrides, queryId, context.requireLiveSession());
    }

    private static boolean isOverwrite(DAGQueryContext context) {
        if (context == null || context.writeConfig() == null) {
            return false;
        }
        String mode = context.writeConfig().writeMode();
        return "OVERWRITE".equals(mode) || "UPDATE".equals(mode);
    }

    private List<SplitFetcher.SplitWithPaths> fetchWithoutConstraints(
            StageNode plan,
            StageDAG dag,
            Map<String, Long> overrides, org.pangrova.coordinator.query.session.CoordinatorSessionContext session) {
        List<SplitSourceDescriptor> sources = plan.stagePlan().splitSources().stream()
                .map(source -> new SplitSourceDescriptor(
                        source.catalogName(), source.schemaName(), source.tableName(),
                        Constraint.EMPTY, source.tableColumns(), source.readBinding()))
                .toList();
        return SplitFetcher.fetchSplitsWithFilePaths(
                sources, catalogRegistry, overrides,
                plan.stagePlan().queryOutputColumns(), session);
    }

    private List<SplitFetcher.SplitWithPaths> fetchFromCatalog(
            StageNode plan,
            StageDAG dag,
            Map<String, Long> overrides, org.pangrova.coordinator.query.session.CoordinatorSessionContext session) {
        return SplitFetcher.fetchSplitsWithFilePaths(
                plan.stagePlan().splitSources(), catalogRegistry, overrides,
                plan.stagePlan().queryOutputColumns(), session);
    }
}
