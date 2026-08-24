package org.pangrova.coordinator.query.execution.dispatch.dag;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.pangrova.coordinator.query.execution.split.SplitFetcher;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.planner.PlanFragment;
import org.pangrova.planner.SplitSourceDescriptor;
import org.pangrova.protocol.rest.task.ExchangeSourceDescriptor;

/**
 * Builds exchange sources for downstream stages in a DAG dispatch.
 * Resolves upstream task locations and fetches splits for leaf stages.
 */
final class ExchangeSourceBuilder {

    private ExchangeSourceBuilder() {}

    /**
     * Collects exchange sources from upstream dispatched tasks for the given stage
     * and consumer partition. Each consumer task receives its own partitionId so
     * hash-exchange plans with downstream parallelism N create N consumers pulling
     * partitions 0..N-1.
     *
     * @param partitionId the consumer task's partition index (0-based)
     */
    static List<ExchangeSourceDescriptor> buildExchangeSources(
            StageNode stage, StageDAG dag,
            List<DAGDispatchEngine.StageDispatchRecord> allDispatched,
            int partitionId) {
        // PERF-T8: Convert List to Set for O(1) contains() instead of O(m).
        List<Integer> upstreamIdList = dag.edges().getOrDefault(stage.stageId(), List.of());
        if (upstreamIdList.isEmpty()) {
            return List.of();
        }
        Set<Integer> upstreamIds = Set.copyOf(upstreamIdList);
        List<ExchangeSourceDescriptor> sources = new ArrayList<>();
        for (DAGDispatchEngine.StageDispatchRecord rec : allDispatched) {
            if (upstreamIds.contains(rec.stageId())) {
                org.pangrova.shuffle.exchange.ExchangeReadMode mode =
                        dag.getEdgeReadMode(rec.stageId(), stage.stageId());
                sources.add(toExchangeSource(rec, mode, partitionId));
            }
        }
        return sources;
    }

    /**
     * Fetches splits with file path metadata for cache affinity routing.
     * Non-leaf stages return an empty list.
     */
    static List<SplitFetcher.SplitWithPaths> fetchSplitsWithFilePaths(
            StageNode stage, StageDAG dag, CatalogRegistry catalogRegistry) {
        return fetchSplitsWithFilePaths(stage, dag, catalogRegistry, java.util.Map.of());
    }

    /**
     * Fetches splits with snapshot overrides for time-travel queries.
     * Non-leaf stages return an empty list.
     */
    static List<SplitFetcher.SplitWithPaths> fetchSplitsWithFilePaths(
            StageNode stage, StageDAG dag, CatalogRegistry catalogRegistry,
            java.util.Map<String, Long> snapshotOverrides) {
        List<Integer> upstreamIds = dag.edges().getOrDefault(stage.stageId(), List.of());
        if (!upstreamIds.isEmpty()) {
            return List.of();
        }
        PlanFragment fragment = stage.planFragment();
        List<SplitSourceDescriptor> sources = fragment.splitSources();
        List<String> outputColumns = fragment.queryOutputColumns();
        return SplitFetcher.fetchSplitsWithFilePaths(
                sources, catalogRegistry, snapshotOverrides, outputColumns);
    }

    /**
     * Fetches splits with index-based filtering for leaf stages.
     * Non-leaf stages return an empty list.
     */
    static List<SplitFetcher.SplitWithPaths> fetchSplitsWithIndexFiltering(
            StageNode stage, StageDAG dag,
            org.pangrova.coordinator.index.IndexAwareSplitFetcher fetcher,
            java.util.Map<String, Long> snapshotOverrides,
            String queryId) {
        List<Integer> upstreamIds = dag.edges().getOrDefault(stage.stageId(), List.of());
        if (!upstreamIds.isEmpty()) {
            return List.of();
        }
        return fetcher.fetchAndFilterSplits(stage.planFragment(), snapshotOverrides, queryId);
    }

    private static ExchangeSourceDescriptor toExchangeSource(DAGDispatchEngine.StageDispatchRecord rec,
                                                    org.pangrova.shuffle.exchange.ExchangeReadMode mode,
                                                    int partitionId) {
        int port = rec.result().resultEndpoint().port();
        if (port <= 0) {
            port = rec.workerTransportPort();
        }
        return new ExchangeSourceDescriptor(
                rec.result().taskId(), rec.result().workerHost(),
                port, partitionId, "SHUFFLE", 0L, mode.name());
    }
}
