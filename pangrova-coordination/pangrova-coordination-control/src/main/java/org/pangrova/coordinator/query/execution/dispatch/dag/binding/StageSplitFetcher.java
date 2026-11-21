package org.pangrova.coordinator.query.execution.dispatch.dag.binding;

import org.pangrova.planner.enums.StagePlanMode;
import org.pangrova.query.api.task.QuerySplit;

import java.util.List;
import java.util.Map;

import org.pangrova.coordinator.query.execution.split.StageSplitSource;
import org.pangrova.coordinator.query.execution.split.CatalogStageSplitSource;
import org.pangrova.coordinator.query.execution.split.SplitFetcher;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.planner.analysis.SplitSourceDescriptor;
import org.pangrova.connector.api.metadata.Constraint;

/** Fetches stage splits with snapshot and index-aware query constraints. */
public final class StageSplitFetcher {
    private final CatalogRegistry catalogRegistry;
    private final StageSplitSource splitSource;

    /** Creates a split fetcher with an optional index-aware source. */
    public StageSplitFetcher(
            CatalogRegistry catalogRegistry,
            StageSplitSource indexAwareSplitFetcher) {
        this.catalogRegistry = catalogRegistry;
        this.splitSource = indexAwareSplitFetcher == null
                ? new CatalogStageSplitSource(catalogRegistry) : indexAwareSplitFetcher;
    }

    /** Fetches a live stage using only its authenticated session, pinned selectors and write mode. */
    public List<QuerySplit> fetch(
            StageNode plan,
            QuerySession session,
            Map<String, Long> snapshotOverrides,
            String writeMode) {
        if (session == null) {
            throw new IllegalStateException("Query session is required for authenticated split lookup");
        }
        if (plan.stagePlan().mode() == StagePlanMode.DURABLE_REPLAY) {
            throw new IllegalStateException(
                    "Durable stage allocations require the current exact persisted partition gate");
        }
        Map<String, Long> overrides = snapshotOverrides == null ? Map.of() : Map.copyOf(snapshotOverrides);
        if ("OVERWRITE".equals(writeMode) || "UPDATE".equals(writeMode)) {
            return fetchWithoutConstraints(plan, overrides, session);
        }
        return splitSource.fetchAndFilterSplits(
                plan.stagePlan().requireLiveFragment(), overrides, session.queryId(), session);
    }

    private List<QuerySplit> fetchWithoutConstraints(
            StageNode plan,
            Map<String, Long> overrides, org.pangrova.query.api.session.QuerySession session) {
        List<SplitSourceDescriptor> sources = plan.stagePlan().splitSources().stream()
                .map(source -> new SplitSourceDescriptor(
                        source.catalogName(), source.schemaName(), source.tableName(),
                        Constraint.EMPTY, source.tableColumns(), source.readBinding()))
                .toList();
        return SplitFetcher.fetchSplitsWithFilePaths(
                sources, catalogRegistry, overrides,
                plan.stagePlan().queryOutputColumns(), session);
    }

}
