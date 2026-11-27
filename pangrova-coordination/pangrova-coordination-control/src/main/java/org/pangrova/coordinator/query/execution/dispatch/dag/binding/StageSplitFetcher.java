package org.pangrova.coordinator.query.execution.dispatch.dag.binding;

import org.pangrova.planner.enums.StagePlanMode;
import org.pangrova.coordinator.query.execution.split.QuerySplit;
import org.pangrova.connector.api.metadata.SplitPlanningBudget.FileGranularity;

import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;

import org.pangrova.coordinator.query.execution.split.StageSplitSource;
import org.pangrova.coordinator.query.execution.split.CatalogStageSplitSource;
import org.pangrova.coordinator.query.execution.split.SplitFetcher;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.planner.analysis.PlanSplitSourceExtractor.PreparedSources;
import org.pangrova.planner.analysis.SplitSourceDescriptor;
import org.pangrova.connector.api.metadata.Constraint;
import org.pangrova.connector.api.metadata.Domain;
import org.pangrova.connector.api.metadata.table.ColumnHandle;
import org.pangrova.coordinator.query.execution.dispatch.dag.runtime.BuildFilterGate;
import org.pangrova.planner.analysis.StageRuntimeFilter;
import org.pangrova.query.api.stage.StageCompletionCallback.BuildKeyRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Fetches stage splits with snapshot and index-aware query constraints. */
public final class StageSplitFetcher {
    private static final Logger LOG = LoggerFactory.getLogger(StageSplitFetcher.class);
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

    /** The enumerated splits and the exact compilation observations that admitted them. */
    public record FetchedSplits(List<QuerySplit> splits, PreparedSources sources) {
        public FetchedSplits {
            splits = List.copyOf(splits);
            java.util.Objects.requireNonNull(sources, "Fetched stage splits require exact source observations");
        }
    }

    /** Fetches a live stage using only its authenticated session, pinned selectors and write mode. */
    public FetchedSplits fetch(
            StageNode plan,
            QuerySession session,
            Map<String, Long> snapshotOverrides,
            String writeMode) {
        return fetch(plan, session, snapshotOverrides, writeMode, null, FileGranularity.WHOLE_OBJECT);
    }

    /** Applies proven probe ranges within the normal authorized catalog enumeration. */
    public FetchedSplits fetch(StageNode plan, QuerySession session,
            Map<String, Long> snapshotOverrides, String writeMode, BuildFilterGate filter,
            FileGranularity fileGranularity) {
        if (session == null) {
            throw new IllegalStateException("Query session is required for authenticated split lookup");
        }
        if (plan.stagePlan().mode() == StagePlanMode.DURABLE_REPLAY) {
            throw new IllegalStateException(
                    "Durable stage allocations require the current exact persisted partition gate");
        }
        Map<String, Long> overrides = snapshotOverrides == null ? Map.of() : Map.copyOf(snapshotOverrides);
        var fragment = plan.stagePlan().requireLiveFragment();
        var sourcePlan = fragment.prepareSplitSources();
        var sources = sourcePlan.descriptors();
        if ("OVERWRITE".equals(writeMode) || "UPDATE".equals(writeMode)) {
            return new FetchedSplits(fetchWithoutConstraints(
                    sources, fragment.queryOutputColumns(), overrides, session), sourcePlan);
        }
        var ranges = filter == null ? null : filter.probeRanges(plan.stageId());
        if (ranges == null || sources.size() != 1) {
            return new FetchedSplits(
                    splitSource.fetchAndFilterSplits(fragment, sources, overrides, session, fileGranularity),
                    sourcePlan);
        }
        var original = sources.get(0);
        var pruned = prune(original, filter.plan(), ranges);
        var splits = splitSource.fetchAndFilterSplits(fragment, List.of(pruned), overrides, session,
                fileGranularity);
        if (original != pruned) {
            LOG.info("event=runtime_filter_probe_splits queryId={} stageId={} splitCount={} emptyBuild={}",
                    session.queryId(), plan.stageId(), splits.size(), ranges.isEmpty());
        }
        return new FetchedSplits(splits, sourcePlan);
    }

    private static SplitSourceDescriptor prune(
            SplitSourceDescriptor source, StageRuntimeFilter filter, Map<Integer, List<BuildKeyRange>> ranges) {
        var read = source.readBinding().orElse(null);
        var selected = filter.probeRead();
        if (read == null || !read.snapshotId().equals(selected.snapshotId())
                || !read.sourceIdentity().equals(selected.sourceIdentity())
                || !read.schemaIdentity().equals(selected.schemaIdentity())) {
            return source;
        }
        for (var key : filter.keys()) {
            boolean exactField = source.tableColumns().contains(key.column())
                    && read.schemaIdentity().stream().flatMap(schema -> schema.fields().stream())
                            .anyMatch(column -> column.name().equals(key.column())
                                    && column.fieldId().orElse(-1) == key.fieldId());
            if (!exactField) return source;
        }
        Constraint constraint = ranges.isEmpty() ? Constraint.NONE
                : withBuildRanges(source.constraint(), filter, ranges);
        return new SplitSourceDescriptor(source.catalogName(), source.schemaName(), source.tableName(),
                constraint, source.tableColumns(), source.readBinding());
    }

    private static Constraint withBuildRanges(
            Constraint original, StageRuntimeFilter filter, Map<Integer, List<BuildKeyRange>> bounds) {
        if (original == Constraint.NONE) return original;
        var domains = new LinkedHashMap<>(original.getDomains());
        for (var key : filter.keys()) {
            var ranges = bounds.get(key.buildOrdinal()).stream().map(range -> new Domain.Range(
                    range.minimum(), range.maximum(), true, true)).toList();
            domains.put(new BuildKeyColumn(key.column()),
                    ranges.size() == 1 ? ranges.get(0) : new Domain.RangeUnion(ranges));
        }
        Map<ColumnHandle, Domain> immutable = java.util.Collections.unmodifiableMap(domains);
        return () -> immutable;
    }

    private record BuildKeyColumn(String columnName) implements ColumnHandle {}

    private List<QuerySplit> fetchWithoutConstraints(List<SplitSourceDescriptor> sources,
            List<String> outputColumns, Map<String, Long> overrides, QuerySession session) {
        List<SplitSourceDescriptor> unpruned = sources.stream()
                .map(source -> new SplitSourceDescriptor(
                        source.catalogName(), source.schemaName(), source.tableName(),
                        Constraint.EMPTY, source.tableColumns(), source.readBinding()))
                .toList();
        return SplitFetcher.fetchSplitsWithFilePaths(
                unpruned, catalogRegistry, overrides, outputColumns, session, FileGranularity.WHOLE_OBJECT);
    }

}
