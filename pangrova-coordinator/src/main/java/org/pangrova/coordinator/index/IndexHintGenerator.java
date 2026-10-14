package org.pangrova.coordinator.index;

import org.pangrova.metadata.index.IndexMetadataStore;
import org.pangrova.planner.index.IndexHint;
import org.pangrova.planner.index.IndexPredicateAnalyzer;
import org.pangrova.spi.index.IndexCoverageStats;
import org.pangrova.spi.index.IndexDescriptor;
import org.pangrova.spi.index.predicate.IndexPredicate;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.plan.RelOptTable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Generates index hints by extracting predicates from a Calcite RelNode,
 * looking up READY indexes from the metadata store, and running the
 * predicate-index compatibility analysis.
 *
 * <p>Returns a map of table key ("{catalog}.{schema}.{table}") to index hints.
 * Empty map means no applicable indexes were found. Until predicate extraction
 * tracks scan provenance, multi-scan plans receive no scalar hints.</p>
 */
public final class IndexHintGenerator {

    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(IndexHintGenerator.class);

    private IndexHintGenerator() {}

    /**
     * Generates index hints for all table scans in the RelNode tree.
     *
     * @param relNode           optimized query plan
     * @param metadataStore     index metadata for READY index lookup
     * @param analyzer          predicate-index compatibility analyzer
     * @return table key → index hints (empty if no applicable indexes)
     */
    public static Map<String, List<IndexHint>> generate(
            RelNode relNode,
            IndexMetadataStore metadataStore,
            IndexPredicateAnalyzer analyzer,
            String tenantId) {
        return generate(relNode, metadataStore, analyzer, tenantId, 0);
    }

    /**
     * Generates index hints with coverage-aware filtering.
     *
     * @param relNode           optimized query plan
     * @param metadataStore     index metadata for READY index lookup
     * @param analyzer          predicate-index compatibility analyzer
     * @param totalDataFiles    total data files in the scan (0 = skip coverage filtering)
     * @return table key → index hints (empty if no applicable indexes)
     */
    public static Map<String, List<IndexHint>> generate(
            RelNode relNode,
            IndexMetadataStore metadataStore,
            IndexPredicateAnalyzer analyzer,
            String tenantId,
            int totalDataFiles) {
        return generate(relNode, metadataStore, analyzer, tenantId, totalDataFiles, false);
    }

    /**
     * Generates index hints with coverage-aware filtering and stale index control.
     *
     * @param relNode           optimized query plan
     * @param metadataStore     index metadata for READY index lookup
     * @param analyzer          predicate-index compatibility analyzer
     * @param totalDataFiles    total data files in the scan (0 = skip coverage filtering)
     * @param includeStale      whether to include STALE indexes in evaluation
     * @return table key → index hints (empty if no applicable indexes)
     */
    public static Map<String, List<IndexHint>> generate(
            RelNode relNode,
            IndexMetadataStore metadataStore,
            IndexPredicateAnalyzer analyzer,
            String tenantId,
            int totalDataFiles,
            boolean includeStale) {
        List<IndexPredicate> predicates = FilterPredicateExtractor.extract(relNode);
        if (predicates.isEmpty()) {
            return Map.of();
        }
        List<TableScanInfo> tableScans = extractTableScanInfo(relNode);
        LOG.debug("event=hint_gen_table_scans count={}", tableScans.size());
        if (tableScans.isEmpty()) {
            return Map.of();
        }
        // FilterPredicateExtractor currently returns an unscoped predicate list.
        // A same-named column in a second scan cannot safely inherit that
        // predicate: a negative Bloom result would drop valid join rows.
        if (tableScans.size() != 1) {
            LOG.debug("event=index_hint_multi_scan_unscoped scanCount={}", tableScans.size());
            return Map.of();
        }
        Map<String, List<IndexHint>> result = new LinkedHashMap<>();
        for (TableScanInfo info : tableScans) {
            generateForTable(
                    info, predicates, metadataStore, analyzer,
                    tenantId, totalDataFiles, includeStale, result);
        }
        return Map.copyOf(result);
    }

    private static void generateForTable(TableScanInfo info, List<IndexPredicate> predicates,
                                         IndexMetadataStore metadataStore,
                                         IndexPredicateAnalyzer analyzer,
                                         String tenantId,
                                         int totalDataFiles,
                                         boolean includeStale,
                                         Map<String, List<IndexHint>> result) {
        List<IndexPredicate> tablePredicates = predicates.stream()
                .filter(p -> info.columnNames.contains(p.column()))
                .toList();
        LOG.debug("event=hint_gen_table_predicates table={} columns={} predicateCount={}",
                info.tableKey(), info.columnNames, tablePredicates.size());
        if (tablePredicates.isEmpty()) {
            return;
        }
        List<String> predColumns = tablePredicates.stream().map(IndexPredicate::column)
                .distinct().toList();
        List<IndexDescriptor> indexes = metadataStore.findReadyIndexesForColumns(
                tenantId, info.catalog, info.schema, info.table, predColumns, includeStale);
        LOG.debug("event=hint_gen_indexes table={} predColumns={} readyIndexCount={}",
                info.tableKey(), predColumns, indexes.size());
        if (indexes.isEmpty()) {
            return;
        }
        List<IndexHint> hints = analyzeHints(
                tablePredicates, indexes, metadataStore, analyzer, totalDataFiles);
        if (!hints.isEmpty()) {
            result.put(info.tableKey(), hints);
        }
    }

    private static List<IndexHint> analyzeHints(List<IndexPredicate> predicates,
                                                List<IndexDescriptor> indexes,
                                                IndexMetadataStore metadataStore,
                                                IndexPredicateAnalyzer analyzer,
                                                int totalDataFiles) {
        if (totalDataFiles <= 0) {
            return analyzer.analyze(predicates, indexes);
        }
        Map<String, IndexCoverageStats> coverageStats = buildCoverageStats(
                indexes, metadataStore, totalDataFiles);
        return analyzer.analyze(predicates, indexes, coverageStats);
    }

    private static Map<String, IndexCoverageStats> buildCoverageStats(
            List<IndexDescriptor> indexes, IndexMetadataStore metadataStore, int totalDataFiles) {
        List<String> indexIds = indexes.stream()
                .map(IndexDescriptor::indexId).toList();
        Map<String, Integer> fileCounts = metadataStore.getIndexedFileCounts(indexIds);
        Map<String, IndexCoverageStats> coverageStats = new java.util.HashMap<>();
        for (IndexDescriptor idx : indexes) {
            int indexed = fileCounts.getOrDefault(idx.indexId(), 0);
            coverageStats.put(idx.indexId(),
                    new IndexCoverageStats(idx.indexId(), indexed, totalDataFiles));
        }
        return coverageStats;
    }

    private static List<TableScanInfo> extractTableScanInfo(RelNode node) {
        List<TableScanInfo> infos = new ArrayList<>();
        collectTableScanInfo(node, infos);
        return infos;
    }

    private static void collectTableScanInfo(RelNode node, List<TableScanInfo> out) {
        if (node instanceof TableScan scan) {
            RelOptTable table = scan.getTable();
            if (table == null) {
                throw new IllegalStateException("TableScan has no table");
            }
            List<String> qn = table.getQualifiedName();
            Set<String> cols = scan.getRowType().getFieldNames().stream()
                    .collect(Collectors.toSet());
            out.add(TableScanInfo.fromQualifiedName(qn, cols));
        }
        for (RelNode input : node.getInputs()) {
            collectTableScanInfo(input, out);
        }
    }

    private record TableScanInfo(
            String catalog, String schema, String table, Set<String> columnNames) {

        String tableKey() {
            return catalog + "." + schema + "." + table;
        }

        static TableScanInfo fromQualifiedName(List<String> qn, Set<String> cols) {
            return switch (qn.size()) {
                case 3 -> new TableScanInfo(qn.get(0), qn.get(1), qn.get(2), cols);
                case 2 -> new TableScanInfo("default", qn.get(0), qn.get(1), cols);
                default -> new TableScanInfo(
                        "default", "default", qn.get(qn.size() - 1), cols);
            };
        }
    }
}
