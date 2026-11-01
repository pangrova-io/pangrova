package org.pangrova.coordinator.adapter.indexing;

import org.pangrova.coordinator.adapter.indexing.ddl.ConnectorAccessorResolver;
import org.pangrova.coordinator.adapter.indexing.filter.IndexSplitFilter;
import org.pangrova.coordinator.generation.PublishedGenerationBindingResolver;
import org.pangrova.coordinator.query.execution.split.SplitFetcher;
import org.pangrova.persistence.api.generation.job.GenerationJobStore;
import org.pangrova.metadata.core.catalog.registry.CatalogRegistry;
import org.pangrova.indexing.store.IndexMetadataStore;
import org.pangrova.indexing.core.eval.IndexEvaluationFailures;
import org.pangrova.planner.PlanFragment;
import org.pangrova.planner.PlanSplitSourceExtractor;
import org.pangrova.planner.PlannedQuery;
import org.pangrova.planner.SplitSourceDescriptor;
import org.pangrova.indexing.core.planner.IndexHint;
import org.pangrova.indexing.core.planner.IndexPredicateAnalyzer;
import org.pangrova.spi.connector.metadata.ConnectorSplit;
import org.pangrova.spi.indexing.descriptor.IndexDescriptor;
import org.pangrova.spi.connector.index.statistics.DataFileStatistics;
import org.pangrova.spi.connector.index.statistics.FileColumnStatistics;
import org.apache.calcite.rel.RelNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Fetches splits with optional index-based filtering.
 *
 * <p>Encapsulates the fetch → filter pipeline used by the unified
 * DAG dispatch path in DAGDispatchEngine.
 * When indexing is not enabled, delegates directly to SplitFetcher.</p>
 */
public final class IndexAwareSplitFetcher {

    private static final Logger LOG = LoggerFactory.getLogger(IndexAwareSplitFetcher.class);

    private final CatalogRegistry catalogRegistry;
    private final IndexSplitFilter splitFilter;
    private final IndexMetadataStore metadataStore;
    private final ConnectorAccessorResolver accessorResolver;
    private final IndexPredicateAnalyzer predicateAnalyzer;
    private final PublishedGenerationBindingResolver artifactBindingResolver;
    private volatile org.pangrova.coordinator.generation.GeneratedValueReadResolver generatedValueReadResolver;

    /**
     * @param catalogRegistry   catalog registry for split enumeration
     * @param splitFilter       index split filter (nullable — null disables filtering)
     * @param metadataStore     index metadata store (nullable)
     * @param accessorResolver  connector accessor resolver (nullable)
     * @param artifactBuildStore immutable artifact metadata store (nullable)
     */
    public IndexAwareSplitFetcher(
            CatalogRegistry catalogRegistry,
            IndexSplitFilter splitFilter,
            IndexMetadataStore metadataStore,
            ConnectorAccessorResolver accessorResolver,
            GenerationJobStore artifactBuildStore) {
        this.catalogRegistry = catalogRegistry;
        this.splitFilter = splitFilter;
        this.metadataStore = metadataStore;
        this.accessorResolver = accessorResolver;
        this.predicateAnalyzer = new IndexPredicateAnalyzer();
        this.artifactBindingResolver = artifactBuildStore == null
                ? null : new PublishedGenerationBindingResolver(artifactBuildStore);
    }

    /** Returns the generic artifact resolver, or null when durable artifacts are disabled. */
    public PublishedGenerationBindingResolver artifactBindingResolver() {
        return artifactBindingResolver;
    }

    /** Binds the process-owned generated read service once after actual PostgreSQL initialization. */
    public synchronized void bindGeneratedValueReads(
            org.pangrova.coordinator.generation.GeneratedValueReadResolver resolver) {
        if (generatedValueReadResolver != null) {
            throw new IllegalStateException("Generated query reads are already bound");
        }
        generatedValueReadResolver = java.util.Objects.requireNonNull(resolver, "resolver");
    }

    /** Returns the generated read service; a configured artifact store may never silently omit it. */
    public org.pangrova.coordinator.generation.GeneratedValueReadResolver generatedValueReads() {
        if (artifactBindingResolver != null && generatedValueReadResolver == null) {
            throw new IllegalStateException("Durable query reads require their actual generated read service");
        }
        return generatedValueReadResolver;
    }

    /** Returns true if index filtering is active. */
    public boolean isIndexFilteringEnabled() {
        return splitFilter != null && metadataStore != null;
    }

    /**
     * Looks up index descriptors for a given table and column.
     * Used by vector search rewriting to check if a VECTOR index exists.
     *
     * @param tableName  fully qualified table name (catalog.schema.table)
     * @param columnName column name to look up indexes for
     * @return matching index descriptors, or empty list if metadata store is unavailable
     */
    public List<org.pangrova.spi.indexing.descriptor.IndexDescriptor> lookupIndexes(
            String tenantId, String tableName, String columnName) {
        if (metadataStore == null) {
            return List.of();
        }
        String[] parts = tableName.split("\\.", 3);
        if (parts.length < 3) {
            return List.of();
        }
        return metadataStore.findIndexesByTable(
                tenantId, parts[0], parts[1], parts[2]).stream()
                .filter(d -> d.columnNames().stream()
                        .anyMatch(c -> c.equalsIgnoreCase(columnName)))
                .toList();
    }

    /**
     * Fetches splits for a PlannedQuery and applies index-based filtering when enabled.
     *
     * @param plannedQuery      compiled query plan
     * @param snapshotOverrides time-travel snapshot overrides (table FQN → snapshot ID)
     * @param queryId           query identifier for logging
     * @return filtered splits with file path metadata
     */
    public List<SplitFetcher.SplitWithPaths> fetchAndFilterSplits(
            PlannedQuery plannedQuery, Map<String, Long> snapshotOverrides,
            String queryId, org.pangrova.coordinator.query.session.CoordinatorSessionContext session) {
        LOG.debug("event=index_aware_fetch_entry queryId={} filteringEnabled={}",
                queryId, isIndexFilteringEnabled());
        if (!isIndexFilteringEnabled()) {
            return SplitFetcher.fetchSplitsWithFilePaths(plannedQuery, catalogRegistry, snapshotOverrides, session);
        }
        List<ConnectorSplit> rawSplits =
                SplitFetcher.fetchRawConnectorSplits(plannedQuery, catalogRegistry, snapshotOverrides, session);
        if (rawSplits.isEmpty()) {
            return List.of();
        }
        List<ConnectorSplit> filtered = applyIndexFiltering(
                plannedQuery.plan(), rawSplits, snapshotOverrides, queryId, session);
        return SplitFetcher.toSplitWithPaths(filtered);
    }

    /**
     * Fetches splits for a PlanFragment and applies index-based filtering when enabled.
     * Coordinator dispatch code calls this overload to avoid importing Calcite types.
     *
     * @param fragment          plan fragment (encapsulates RelNode)
     * @param snapshotOverrides time-travel snapshot overrides (table FQN → snapshot ID)
     * @param queryId           query identifier for logging
     * @return filtered splits with file path metadata
     */
    public List<SplitFetcher.SplitWithPaths> fetchAndFilterSplits(
            PlanFragment fragment, Map<String, Long> snapshotOverrides,
            String queryId, org.pangrova.coordinator.query.session.CoordinatorSessionContext session) {
        return fetchAndFilterSplitsInternal(
                fragment.relNode(), snapshotOverrides, queryId, session);
    }

    private List<SplitFetcher.SplitWithPaths> fetchAndFilterSplitsInternal(
            RelNode relNode, Map<String, Long> snapshotOverrides,
            String queryId, org.pangrova.coordinator.query.session.CoordinatorSessionContext session) {
        LOG.debug("event=index_aware_fetch_entry queryId={} filteringEnabled={} relNodeType={}",
                queryId, isIndexFilteringEnabled(), relNode.getClass().getSimpleName());
        List<SplitSourceDescriptor> sources = PlanSplitSourceExtractor.extract(relNode);
        List<String> outputColumns = PlanSplitSourceExtractor.queryOutputColumns(relNode);
        if (!isIndexFilteringEnabled()) {
            return SplitFetcher.fetchSplitsWithFilePaths(
                    sources, catalogRegistry, snapshotOverrides, outputColumns, session);
        }
        List<ConnectorSplit> rawSplits =
                SplitFetcher.fetchRawConnectorSplits(
                        sources, catalogRegistry, snapshotOverrides, outputColumns, session);
        if (rawSplits.isEmpty()) {
            return List.of();
        }
        List<ConnectorSplit> filtered = applyIndexFiltering(
                relNode, rawSplits, snapshotOverrides, queryId, session);
        return SplitFetcher.toSplitWithPaths(filtered);
    }

    private List<ConnectorSplit> applyIndexFiltering(
            RelNode relNode, List<ConnectorSplit> splits,
            Map<String, Long> snapshotOverrides, String queryId,
            org.pangrova.coordinator.query.session.CoordinatorSessionContext session) {
        try {
            Map<String, List<IndexHint>> tableHints =
                    IndexHintGenerator.generate(relNode, metadataStore, predicateAnalyzer,
                            session.tenantId(), 0);
            if (tableHints.isEmpty()) {
                return splits;
            }
            return filterSplitsByTable(
                    splits, tableHints, snapshotOverrides, queryId, session);
        } catch (org.pangrova.spi.security.AccessDeniedException | SecurityException authorityFailure) {
            throw authorityFailure;
        } catch (Exception e) {
            IndexEvaluationFailures.propagateIfTerminal(e);
            LOG.warn("event=index_filtering_failed queryId={} error={} action=skip_filtering",
                    queryId, e.getMessage());
            return splits;
        }
    }

    private List<ConnectorSplit> filterSplitsByTable(
            List<ConnectorSplit> splits,
            Map<String, List<IndexHint>> tableHints,
            Map<String, Long> snapshotOverrides,
            String queryId,
            org.pangrova.coordinator.query.session.CoordinatorSessionContext session) {
        int before = splits.size();
        List<ConnectorSplit> result = new java.util.ArrayList<>(splits);
        for (var entry : tableHints.entrySet()) {
            String tableKey = entry.getKey();
            List<IndexHint> hints = entry.getValue();
            // Snapshot tokens are opaque identities, not ordered numbers.
            // Time-travel pruning may use only the exact published snapshot.
            Long querySnapshotId = snapshotOverrides != null
                    ? snapshotOverrides.get(tableKey) : null;
            List<IndexHint> eligibleHints = filterHintsBySnapshotCoverage(
                    hints, querySnapshotId, tableKey, session.tenantId());
            if (!eligibleHints.isEmpty()) {
                result = filterSplitsForTable(
                        result, tableKey, eligibleHints, snapshotOverrides, session);
            }
        }
        logFilteringResult(queryId, before, result.size());
        return result;
    }

    /**
     * Filters index hints to only those whose lastIndexedSnapshotId covers
     * the query snapshot. Indexes that have not yet indexed up to the query
     * snapshot are excluded — their files bypass L1 evaluation.
     */
    private List<IndexHint> filterHintsBySnapshotCoverage(
            List<IndexHint> hints, Long querySnapshotId,
            String tableKey, String tenantId) {
        if (querySnapshotId == null || metadataStore == null) {
            return hints;
        }
        String catalog = extractCatalogFromKey(tableKey);
        String schema = extractSchemaFromKey(tableKey);
        String table = extractTableFromKey(tableKey);
        List<IndexDescriptor> descriptors = metadataStore.findIndexesByTable(
                tenantId, catalog, schema, table);
        Map<String, IndexDescriptor> descriptorMap = descriptors.stream()
                .collect(java.util.stream.Collectors.toMap(
                        IndexDescriptor::indexId, d -> d, (a, b) -> a));
        String querySnapStr = String.valueOf(querySnapshotId);
        return hints.stream()
                .filter(hint -> {
                    IndexDescriptor desc = descriptorMap.get(hint.indexId());
                    if (desc == null || desc.lastIndexedSnapshotId() == null
                            || desc.lastIndexedSnapshotId().isEmpty()) {
                        return false;
                    }
                    return desc.lastIndexedSnapshotId().equals(querySnapStr);
                })
                .toList();
    }

    private List<ConnectorSplit> filterSplitsForTable(
            List<ConnectorSplit> splits, String tableKey,
            List<IndexHint> hints, Map<String, Long> snapshotOverrides,
            org.pangrova.coordinator.query.session.CoordinatorSessionContext session) {
        String catalogName = extractCatalogFromKey(tableKey);
        String schemaName = extractSchemaFromKey(tableKey);
        String tableName = extractTableFromKey(tableKey);
        var queryAccessors = accessorResolver == null ? null : accessorResolver.forSession(
                org.pangrova.coordinator.query.session.SessionContextConverter.toConnectorSession(
                        session, catalogName, schemaName));
        String snapshotToken = resolveSnapshotToken(tableKey, snapshotOverrides);
        Map<String, List<FileColumnStatistics>> fileStats =
                fetchFileStatistics(session.tenantId(), catalogName, schemaName, tableName,
                        snapshotToken, splits, hints, queryAccessors);
        List<ConnectorSplit> result = splitFilter.filterSplits(
                splits, hints, catalogName, queryAccessors, snapshotToken,
                fileStats, 0);
        return result;
    }

    private static void logFilteringResult(String queryId, int before, int after) {
        int skipped = before - after;
        if (skipped > 0) {
            LOG.info("event=index_split_filtering queryId={} before={} after={} skipped={}",
                    queryId, before, after, skipped);
        }
    }

    /**
     * Fetches L0 file column statistics for the given splits and predicate columns.
     *
     * @return file path → column statistics map; empty map if fetch fails or no columns
     */
    Map<String, List<FileColumnStatistics>> fetchFileStatistics(
            String tenantId, String catalog, String schema, String table, String snapshotToken,
            List<ConnectorSplit> splits, List<IndexHint> hints, ConnectorAccessorResolver queryAccessors) {
        // Unpinned queries cannot use statistics whose source snapshot is unknown.
        if (snapshotToken == null || snapshotToken.isBlank()) {
            return Map.of();
        }
        List<String> predicateColumns = extractPredicateColumns(hints);
        if (predicateColumns.isEmpty()) {
            return Map.of();
        }
        List<String> dataFilePaths = splits.stream()
                .flatMap(s -> s.dataFilePaths().stream())
                .distinct()
                .toList();
        if (dataFilePaths.isEmpty()) {
            return Map.of();
        }
        try {
            Map<String, List<FileColumnStatistics>> stats =
                    metadataStore.getFileStatistics(
                            tenantId, catalog, schema, table, snapshotToken, dataFilePaths, predicateColumns);
            if (stats != null && !stats.isEmpty()) {
                return stats;
            }
        } catch (org.pangrova.spi.security.AccessDeniedException | SecurityException authorityFailure) {
            throw authorityFailure;
        } catch (Exception e) {
            LOG.warn("event=file_statistics_fetch_failed catalog={} schema={} table={} error={}",
                    catalog, schema, table, e.getMessage());
        }
        return fetchFileStatisticsFromConnector(
                catalog, schema, table, snapshotToken, dataFilePaths, predicateColumns, queryAccessors);
    }

    private Map<String, List<FileColumnStatistics>> fetchFileStatisticsFromConnector(
            String catalog, String schema, String table, String snapshotToken,
            List<String> dataFilePaths, List<String> predicateColumns, ConnectorAccessorResolver queryAccessors) {
        if (queryAccessors == null) {
            return Map.of();
        }
        try {
            List<DataFileStatistics> connectorStats =
                    queryAccessors.resolveFileStatistics(
                            catalog, schema, table, dataFilePaths);
            if (connectorStats.isEmpty()) {
                return Map.of();
            }
            java.util.Set<String> columnFilter = new java.util.HashSet<>(predicateColumns);
            java.util.Set<String> requestedFiles = java.util.Set.copyOf(dataFilePaths);
            Map<String, List<FileColumnStatistics>> result = new java.util.HashMap<>();
            for (DataFileStatistics dfs : connectorStats) {
                if (!snapshotToken.equals(dfs.snapshotId()) || !requestedFiles.contains(dfs.dataFilePath())) {
                    continue;
                }
                List<FileColumnStatistics> filtered = dfs.columnStatistics().stream()
                        .filter(cs -> columnFilter.contains(cs.columnName()))
                        .toList();
                if (!filtered.isEmpty()) {
                    result.put(dfs.dataFilePath(), filtered);
                }
            }
            return result;
        } catch (org.pangrova.spi.security.AccessDeniedException | SecurityException authorityFailure) {
            throw authorityFailure;
        } catch (Exception e) {
            LOG.warn("event=connector_file_stats_failed catalog={} schema={} table={} error={}",
                    catalog, schema, table, e.getMessage());
            return Map.of();
        }
    }

    /**
     * Extracts distinct predicate column names from index hints.
     */
    private static List<String> extractPredicateColumns(List<IndexHint> hints) {
        return hints.stream()
                .map(h -> h.predicate().column())
                .distinct()
                .toList();
    }

    private static String extractCatalogFromKey(String tableKey) {
        int dot = tableKey.indexOf('.');
        return dot > 0 ? tableKey.substring(0, dot) : tableKey;
    }

    private static String extractSchemaFromKey(String tableKey) {
        int firstDot = tableKey.indexOf('.');
        if (firstDot < 0) {
            return "default";
        }
        int secondDot = tableKey.indexOf('.', firstDot + 1);
        return secondDot > 0
                ? tableKey.substring(firstDot + 1, secondDot)
                : tableKey.substring(firstDot + 1);
    }

    private static String extractTableFromKey(String tableKey) {
        int lastDot = tableKey.lastIndexOf('.');
        return lastDot > 0 ? tableKey.substring(lastDot + 1) : tableKey;
    }

    /**
     * Resolves snapshot token from snapshotOverrides for a given table key.
     * Table key format: "{catalog}.{schema}.{table}".
     * snapshotOverrides key format: "{catalog}.{schema}.{table}" → snapshot ID (Long).
     *
     * @return snapshot token as string, or null if no override exists
     */
    private static String resolveSnapshotToken(
            String tableKey, Map<String, Long> snapshotOverrides) {
        if (snapshotOverrides == null || snapshotOverrides.isEmpty()) {
            return null;
        }
        Long snapshotId = snapshotOverrides.get(tableKey);
        return snapshotId != null ? String.valueOf(snapshotId) : null;
    }
}
