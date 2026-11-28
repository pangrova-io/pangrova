package org.pangrova.coordinator.indexing;

import org.pangrova.query.api.session.QuerySession;
import org.pangrova.connector.api.index.predicate.IndexPredicate;
import java.util.Optional;
import org.pangrova.coordinator.query.execution.split.QuerySplit;

import org.pangrova.coordinator.indexing.connector.ConnectorAccessorResolver;
import org.pangrova.indexing.pruning.IndexSplitFilter;
import org.pangrova.coordinator.query.execution.split.SplitFetcher;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;
import org.pangrova.indexing.evaluation.IndexEvaluationFailures;
import org.pangrova.planner.analysis.PlanFragment;
import org.pangrova.planner.analysis.SplitSourceDescriptor;
import org.pangrova.indexing.api.pruning.IndexHint;
import org.pangrova.indexing.pruning.IndexPredicateAnalyzer;
import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.connector.api.metadata.SplitPlanningBudget.FileGranularity;
import org.pangrova.indexing.api.evaluation.IndexEvaluatedSplit;
import org.pangrova.protocol.source.CanonicalConnectorSplit;
import org.pangrova.connector.api.index.statistics.DataFileStatistics;
import org.pangrova.connector.api.index.statistics.FileColumnStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.function.UnaryOperator;
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
     * Looks up index descriptors for metadata inspection.
     * Query dispatch must use {@link #lookupIndexesForQuery(String, String, String, List)}.
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
                .filter(d -> d.columnNames().contains(columnName))
                .toList();
    }

    /** Returns READY indexes only when their exact indexed-column schema matches the planned scan. */
    public List<org.pangrova.indexing.api.descriptor.IndexDescriptor> lookupIndexesForQuery(
            String tenantId, String tableName, String columnName, List<SplitSourceDescriptor> sources) {
        if (metadataStore == null) {
            return List.of();
        }
        String[] parts = tableName.split("\\.", 3);
        if (parts.length < 3) {
            return List.of();
        }
        var table = new org.pangrova.connector.api.metadata.table.QualifiedTableName(
                parts[0], parts[1], parts[2]);
        var source = sourceForTable(sources, table);
        if (source.isEmpty()) {
            return List.of();
        }
        String resolvedColumn;
        try {
            var fields = source.orElseThrow().readBinding()
                    .flatMap(org.pangrova.connector.api.metadata.table.ConnectorReadBinding::schemaIdentity)
                    .orElseThrow().fields();
            resolvedColumn = org.pangrova.indexing.api.descriptor.IndexSchemaFingerprint
                    .resolveColumn(columnName, fields).name();
        } catch (IllegalArgumentException unresolved) {
            return List.of();
        }
        return metadataStore.findIndexesByTable(tenantId, parts[0], parts[1], parts[2]).stream()
                .filter(index -> index.state() == org.pangrova.indexing.api.enums.IndexState.READY)
                .filter(index -> index.columnNames().contains(resolvedColumn))
                .filter(index -> matchesSourceSchema(index, source.orElseThrow()))
                .toList();
    }

    /**
     * Fetches splits for a PlanFragment and applies index-based filtering when enabled.
     * Coordinator dispatch code calls this overload to avoid importing Calcite types.
     *
     * @param fragment          plan fragment (encapsulates RelNode)
     * @param sources           bound scan descriptors and their exact planning constraints
     * @param snapshotOverrides time-travel snapshot overrides (table FQN → snapshot ID)
     * @param fileGranularity   physical file units accepted by the stage
     * @return filtered splits with file path metadata
     */
    @Override
    public List<QuerySplit> fetchAndFilterSplits(
            PlanFragment fragment, List<SplitSourceDescriptor> sources, Map<String, Long> snapshotOverrides,
            QuerySession session, FileGranularity fileGranularity) {
        String queryId = session.queryId();
        boolean filtering = isIndexFilteringEnabled();
        LOG.debug("event=index_aware_fetch_entry queryId={} filteringEnabled={}", queryId, filtering);
        List<String> outputColumns = fragment.queryOutputColumns();
        if (!filtering) {
            return SplitFetcher.fetchSplitsWithFilePaths(
                    sources, catalogRegistry, snapshotOverrides, outputColumns, session, fileGranularity);
        }
        List<ConnectorSplit> rawSplits =
                SplitFetcher.fetchRawConnectorSplits(
                        sources, catalogRegistry, snapshotOverrides, outputColumns, session, fileGranularity);
        if (rawSplits.isEmpty()) {
            return List.of();
        }
        List<ConnectorSplit> filtered = applyIndexFiltering(
                fragment, sources, rawSplits, snapshotOverrides, session, fileGranularity);
        return SplitFetcher.toSplitWithPaths(filtered);
    }

    private List<ConnectorSplit> applyIndexFiltering(
            PlanFragment fragment, List<SplitSourceDescriptor> sources,
            List<ConnectorSplit> splits, Map<String, Long> snapshotOverrides,
            QuerySession session, FileGranularity fileGranularity) {
        String queryId = session.queryId();
        try {
            var scope = fragment.singleScanPredicates();
            if (scope.isEmpty()) return splits;
            var scan = scope.orElseThrow();
            var table = scan.table();
            var predicates = scan.predicates();
            var columns = predicates.stream().map(IndexPredicate::column)
                    .distinct().toList();
            var indexes = metadataStore.findReadyIndexesForColumns(
                    session.tenantId(), table.catalog(), table.schema(), table.table(), columns, false);
            if (indexes.isEmpty()) return splits;
            var source = sourceForTable(sources, table);
            if (source.isEmpty()) return splits;
            var schemaMatched = indexes.stream()
                    .filter(index -> matchesSourceSchema(index, source.orElseThrow()))
                    .toList();
            if (schemaMatched.isEmpty()) return splits;
            var hints = predicateAnalyzer.analyze(predicates, schemaMatched);
            if (hints.isEmpty()) return splits;
            var filtered = filterSplitsForTable(splits, table.toString(), hints, snapshotOverrides,
                    session, fileGranularity);
            logFilteringResult(queryId, splits.size(), filtered.size());
            return filtered;
        } catch (org.pangrova.security.api.exception.AccessDeniedException | SecurityException authorityFailure) {
            throw authorityFailure;
        } catch (Exception e) {
            IndexEvaluationFailures.propagateIfTerminal(e);
            LOG.warn("event=index_filtering_failed queryId={} error={} action=skip_filtering",
                    queryId, e.getMessage());
            return splits;
        }
    }

    private List<ConnectorSplit> filterSplitsForTable(
            List<ConnectorSplit> splits, String tableKey,
            List<IndexHint> hints, Map<String, Long> snapshotOverrides,
            QuerySession session, FileGranularity fileGranularity) {
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
        if (fileGranularity == FileGranularity.BYTE_RANGES) {
            return filterByDataObject(splits, tableKey, files -> splitFilter.filterSplits(
                    files, hints, catalogName, queryAccessors, snapshotToken, fileStats, 0));
        }
        return splitFilter.filterSplits(splits, hints, catalogName, queryAccessors, snapshotToken, fileStats, 0);
    }

    /** Evaluates each immutable file once and shares its decision with every physical byte range. */
    static List<ConnectorSplit> filterByDataObject(List<ConnectorSplit> splits, String tableKey,
            UnaryOperator<List<ConnectorSplit>> filter) {
        boolean hasByteRanges = splits.stream().anyMatch(split ->
                split instanceof CanonicalConnectorSplit canonical && canonical.toDataSplit().getStartOffset() > 0);
        if (!hasByteRanges) return filter.apply(splits);

        Map<IndexFile, ConnectorSplit> firstByFile = new HashMap<>();
        Map<ConnectorSplit, ConnectorSplit> siblings = new IdentityHashMap<>();
        List<ConnectorSplit> representatives = new ArrayList<>(splits.size());
        for (ConnectorSplit split : splits) {
            var file = immutableIndexFile(split, tableKey);
            if (file != null) {
                var first = firstByFile.putIfAbsent(file, split);
                if (first != null) {
                    siblings.put(split, first);
                    continue;
                }
            }
            representatives.add(split);
        }
        if (siblings.isEmpty()) return filter.apply(splits);

        Map<ConnectorSplit, ConnectorSplit> surviving = new IdentityHashMap<>();
        for (var kept : filter.apply(representatives)) {
            ConnectorSplit original = kept instanceof IndexEvaluatedSplit evaluated ? evaluated.unwrap() : kept;
            surviving.put(original, kept);
        }
        List<ConnectorSplit> result = new ArrayList<>(splits.size());
        for (var split : splits) {
            var first = siblings.get(split);
            var kept = surviving.get(first == null ? split : first);
            if (kept != null) {
                result.add(first == null ? kept : replicateEvaluation(split, kept));
            }
        }
        return result;
    }

    private static ConnectorSplit replicateEvaluation(ConnectorSplit sibling, ConnectorSplit representative) {
        return representative instanceof IndexEvaluatedSplit evaluated
                ? new IndexEvaluatedSplit(sibling, evaluated.evalResult()) : sibling;
    }

    private static IndexFile immutableIndexFile(ConnectorSplit split, String tableKey) {
        if (!(split instanceof CanonicalConnectorSplit canonical)) return null;
        var data = canonical.toDataSplit();
        if (!data.hasFileDetail() || !data.getFileDetail().hasIceberg() || !data.getSourceId().equals(tableKey)
                || data.getSourceSnapshot().isBlank() || data.getDataObjectId().isBlank()) {
            return null;
        }
        return new IndexFile(data.getSourceSnapshot(), data.getDataObjectId());
    }

    private record IndexFile(String snapshot, String object) {}

    private static Optional<SplitSourceDescriptor> sourceForTable(
            List<SplitSourceDescriptor> sources,
            org.pangrova.connector.api.metadata.table.QualifiedTableName table) {
        var matching = sources.stream()
                .filter(source -> source.catalogName().equals(table.catalog())
                        && source.schemaName().equals(table.schema())
                        && source.tableName().equals(table.table()))
                .toList();
        if (matching.isEmpty()) {
            return Optional.empty();
        }
        var schema = matching.get(0).readBinding()
                .flatMap(org.pangrova.connector.api.metadata.table.ConnectorReadBinding::schemaIdentity);
        if (schema.isEmpty()) {
            return Optional.empty();
        }
        var fields = schema.orElseThrow().fields();
        if (matching.stream().anyMatch(source -> source.readBinding()
                .flatMap(org.pangrova.connector.api.metadata.table.ConnectorReadBinding::schemaIdentity)
                .map(identity -> !identity.fields().equals(fields)).orElse(true))) {
            return Optional.empty();
        }
        return Optional.of(matching.get(0));
    }

    private boolean matchesSourceSchema(
            org.pangrova.indexing.api.descriptor.IndexDescriptor index,
            SplitSourceDescriptor source) {
        if (!org.pangrova.indexing.api.descriptor.IndexSchemaFingerprint.isCurrent(index.sourceSchemaFingerprint())) {
            return false;
        }
        var schema = source.readBinding()
                .flatMap(org.pangrova.connector.api.metadata.table.ConnectorReadBinding::schemaIdentity);
        if (schema.isEmpty()) {
            return false;
        }
        String fingerprint;
        try {
            fingerprint = org.pangrova.indexing.api.descriptor.IndexSchemaFingerprint.of(
                    index.columnNames(), schema.orElseThrow().fields());
        } catch (IllegalArgumentException missing) {
            logSelectedSchemaMismatch(index, source, "indexed_column_missing");
            return false;
        }
        if (fingerprint.equals(index.sourceSchemaFingerprint())) {
            return true;
        }
        logSelectedSchemaMismatch(index, source, "source_schema_mismatch");
        return false;
    }

    private static void logSelectedSchemaMismatch(
            org.pangrova.indexing.api.descriptor.IndexDescriptor index,
            SplitSourceDescriptor source, String reason) {
        // The selected read may be historical, or its captured current declaration
        // may have been superseded since planning. Only live reconciliation can
        // establish a durable descriptor mismatch. This read must simply skip.
        LOG.debug("event=index_selected_schema_mismatch indexId={} table={} reason={} action=skip_index",
                index.indexId(), source.qualifiedName(), reason);
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
