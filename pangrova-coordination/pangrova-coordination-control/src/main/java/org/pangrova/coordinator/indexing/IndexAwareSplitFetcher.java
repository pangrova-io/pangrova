package org.pangrova.coordinator.indexing;

import org.pangrova.query.api.session.QuerySession;
import org.pangrova.connector.api.index.predicate.IndexPredicate;
import java.util.function.Supplier;
import java.util.Optional;
import org.pangrova.query.api.task.QuerySplit;

import org.pangrova.coordinator.indexing.connector.ConnectorAccessorResolver;
import org.pangrova.indexing.pruning.IndexSplitFilter;
import org.pangrova.coordinator.query.execution.split.SplitFetcher;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;
import org.pangrova.indexing.evaluation.IndexEvaluationFailures;
import org.pangrova.planner.analysis.PlanFragment;
import org.pangrova.planner.analysis.PlannedQuery;
import org.pangrova.planner.analysis.SplitSourceDescriptor;
import org.pangrova.indexing.api.pruning.IndexHint;
import org.pangrova.indexing.pruning.IndexPredicateAnalyzer;
import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.connector.api.index.statistics.DataFileStatistics;
import org.pangrova.connector.api.index.statistics.FileColumnStatistics;
import org.pangrova.planner.analysis.ScanPredicates;
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
public final class IndexAwareSplitFetcher implements org.pangrova.coordinator.query.execution.split.StageSplitSource {

    private static final Logger LOG = LoggerFactory.getLogger(IndexAwareSplitFetcher.class);

    private final CatalogRegistry catalogRegistry;
    private final IndexSplitFilter splitFilter;
    private final IndexMetadataStore metadataStore;
    private final ConnectorAccessorResolver accessorResolver;
    private final IndexPredicateAnalyzer predicateAnalyzer;

    /**
     * @param catalogRegistry   catalog registry for split enumeration
     * @param splitFilter       index split filter (nullable — null disables filtering)
     * @param metadataStore     index metadata store (nullable)
     * @param accessorResolver  connector accessor resolver (nullable)
     */
    public IndexAwareSplitFetcher(
            CatalogRegistry catalogRegistry,
            IndexSplitFilter splitFilter,
            IndexMetadataStore metadataStore,
            ConnectorAccessorResolver accessorResolver) {
        this.catalogRegistry = catalogRegistry;
        this.splitFilter = splitFilter;
        this.metadataStore = metadataStore;
        this.accessorResolver = accessorResolver;
        this.predicateAnalyzer = new IndexPredicateAnalyzer();
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
    public List<org.pangrova.indexing.api.descriptor.IndexDescriptor> lookupIndexes(
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
    public List<QuerySplit> fetchAndFilterSplits(
            PlannedQuery plannedQuery, Map<String, Long> snapshotOverrides,
            String queryId, QuerySession session) {
        return fetchAndFilterSplits(plannedQuery.splitSources(), plannedQuery.queryOutputColumns(),
                plannedQuery::singleScanPredicates, snapshotOverrides, queryId, session);
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
    public List<QuerySplit> fetchAndFilterSplits(
            PlanFragment fragment, Map<String, Long> snapshotOverrides,
            String queryId, QuerySession session) {
        return fetchAndFilterSplits(fragment.splitSources(), fragment.queryOutputColumns(),
                fragment::singleScanPredicates, snapshotOverrides, queryId, session);
    }

    private List<QuerySplit> fetchAndFilterSplits(
            List<SplitSourceDescriptor> sources, List<String> outputColumns,
            Supplier<Optional<ScanPredicates>> selection,
            Map<String, Long> snapshotOverrides, String queryId,
            QuerySession session) {
        LOG.debug("event=index_aware_fetch_entry queryId={} filteringEnabled={}", queryId, isIndexFilteringEnabled());
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
                selection, rawSplits, snapshotOverrides, queryId, session);
        return SplitFetcher.toSplitWithPaths(filtered);
    }

    private List<ConnectorSplit> applyIndexFiltering(
            Supplier<Optional<ScanPredicates>> selection, List<ConnectorSplit> splits,
            Map<String, Long> snapshotOverrides, String queryId,
            QuerySession session) {
        try {
            var scope = selection.get();
            if (scope.isEmpty()) return splits;
            var scan = scope.orElseThrow();
            var table = scan.table();
            var predicates = scan.predicates();
            var columns = predicates.stream().map(IndexPredicate::column)
                    .distinct().toList();
            var indexes = metadataStore.findReadyIndexesForColumns(
                    session.tenantId(), table.catalog(), table.schema(), table.table(), columns, false);
            if (indexes.isEmpty()) return splits;
            var hints = predicateAnalyzer.analyze(predicates, indexes);
            if (hints.isEmpty()) return splits;
            Map<String, List<IndexHint>> tableHints = Map.of(table.toString(), hints);
            return filterSplitsByTable(
                    splits, tableHints, snapshotOverrides, queryId, session);
        } catch (org.pangrova.security.api.exception.AccessDeniedException | SecurityException authorityFailure) {
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
            QuerySession session) {
        int before = splits.size();
        List<ConnectorSplit> result = new java.util.ArrayList<>(splits);
        for (var entry : tableHints.entrySet()) {
            String tableKey = entry.getKey();
            List<IndexHint> hints = entry.getValue();
            if (!hints.isEmpty()) {
                result = filterSplitsForTable(result, tableKey, hints, snapshotOverrides, session);
            }
        }
        logFilteringResult(queryId, before, result.size());
        return result;
    }

    private List<ConnectorSplit> filterSplitsForTable(
            List<ConnectorSplit> splits, String tableKey,
            List<IndexHint> hints, Map<String, Long> snapshotOverrides,
            QuerySession session) {
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
        } catch (org.pangrova.security.api.exception.AccessDeniedException | SecurityException authorityFailure) {
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
        } catch (org.pangrova.security.api.exception.AccessDeniedException | SecurityException authorityFailure) {
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
