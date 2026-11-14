package org.pangrova.indexing.pruning;

import org.pangrova.indexing.api.descriptor.IndexCoverageStats;
import org.pangrova.indexing.api.pruning.IndexHint;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.connector.api.index.predicate.ContainsElement;
import org.pangrova.connector.api.index.predicate.EqualTo;
import org.pangrova.connector.api.index.predicate.InList;
import org.pangrova.connector.api.index.predicate.IndexPredicate;
import org.pangrova.connector.api.index.predicate.IsNotNull;
import org.pangrova.connector.api.index.predicate.IsNull;
import org.pangrova.connector.api.index.predicate.LikePattern;
import org.pangrova.connector.api.index.predicate.OrPredicate;
import org.pangrova.connector.api.index.predicate.RangeQuery;
import org.pangrova.connector.api.index.predicate.SpatialQuery;
import org.pangrova.connector.api.index.predicate.TextMatch;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Matches query predicates to compatible READY indexes, producing IndexHints
 * sorted by evaluation cost ascending.
 *
 * <p>The compatibility matrix is derived from the actual evaluate() implementations
 * in each IndexStrategyProvider. A predicate matches an index iff the index covers
 * the predicate's column and the (indexType, predicateType) pair is in the matrix.</p>
 */
public final class IndexPredicateAnalyzer {

    /**
     * Evaluation cost ordering — cheaper index types first.
     * Based on computational complexity: hash/bloom O(1), btree O(log n), etc.
     */
    private static final Map<IndexType, Integer> EVAL_COST = Map.ofEntries(
            Map.entry(IndexType.BLOOM_FILTER, 0),
            Map.entry(IndexType.HASH, 1),
            Map.entry(IndexType.BITMAP, 2),
            Map.entry(IndexType.BTREE, 3),
            Map.entry(IndexType.SPARSE, 4),
            Map.entry(IndexType.PREFIX, 5),
            Map.entry(IndexType.NGRAM_BLOOM_FILTER, 6),
            Map.entry(IndexType.INVERTED, 7),
            Map.entry(IndexType.R_TREE, 8),
            Map.entry(IndexType.GIN, 9));

    private static final Comparator<IndexHint> BY_COST = Comparator.comparingInt(
            h -> EVAL_COST.getOrDefault(h.indexType(), Integer.MAX_VALUE));

    /** Default coverage threshold below which an index is skipped. */
    private static final double DEFAULT_COVERAGE_THRESHOLD = 0.3;

    /**
     * Analyzes predicates against available indexes and produces sorted hints.
     *
     * @param predicates available predicates from the query plan
     * @param indexes    READY (and optionally STALE) indexes for the target table
     * @return index hints sorted by evaluation cost ascending, no duplicates
     */
    public List<IndexHint> analyze(
            List<IndexPredicate> predicates,
            List<IndexDescriptor> indexes) {
        if (predicates.isEmpty() || indexes.isEmpty()) {
            return List.of();
        }

        List<IndexHint> hints = new ArrayList<>();
        Set<IndexHint> seen = new HashSet<>();

        for (IndexDescriptor index : indexes) {
            if (isMultiColumnBTree(index)) {
                addMultiColumnBTreeHints(index, predicates, seen, hints);
            } else {
                addSingleColumnHints(index, predicates, seen, hints);
            }
        }

        hints.sort(BY_COST);
        return List.copyOf(hints);
    }

    /**
     * Coverage-aware analysis: filters out indexes whose file coverage is below
     * the default threshold before generating hints.
     *
     * <p>An index covering only 10% of data files costs more to evaluate than
     * it saves in data skipping. This overload lets the coordinator pass
     * coverage stats so the planner can make cost-effective decisions.</p>
     *
     * @param predicates    available predicates from the query plan
     * @param indexes       READY indexes for the target table
     * @param coverageStats index coverage stats keyed by indexId (nullable — null skips filtering)
     * @return index hints sorted by evaluation cost ascending
     */
    public List<IndexHint> analyze(
            List<IndexPredicate> predicates,
            List<IndexDescriptor> indexes,
            Map<String, IndexCoverageStats> coverageStats) {
        if (coverageStats == null || coverageStats.isEmpty()) {
            return analyze(predicates, indexes);
        }
        List<IndexDescriptor> filtered = indexes.stream()
                .filter(idx -> {
                    IndexCoverageStats stats = coverageStats.get(idx.indexId());
                    return stats == null || !stats.isBelowThreshold(DEFAULT_COVERAGE_THRESHOLD);
                })
                .toList();
        return analyze(predicates, filtered);
    }

    private static boolean coversColumn(IndexDescriptor index, String column) {
        return index.columnNames().contains(column);
    }

    private static boolean isMultiColumnBTree(IndexDescriptor index) {
        return index.indexType() == IndexType.BTREE && index.columnNames().size() > 1;
    }

    /**
     * For multi-column BTree: only emit hints when predicate columns form a
     * leftmost prefix of the index's columnNames order.
     */
    private static void addMultiColumnBTreeHints(
            IndexDescriptor index,
            List<IndexPredicate> predicates,
            Set<IndexHint> seen,
            List<IndexHint> hints) {
        Set<String> prefixCols = collectBTreeLeftmostPrefix(index, predicates);
        if (prefixCols.isEmpty()) {
            return;
        }
        emitHintsForColumns(index, predicates, prefixCols, seen, hints);
    }

    /**
     * Computes the leftmost prefix columns of a multi-column BTree index
     * that have compatible predicates.
     */
    private static Set<String> collectBTreeLeftmostPrefix(
            IndexDescriptor index,
            List<IndexPredicate> predicates) {
        Set<String> predicateCols = new HashSet<>();
        for (IndexPredicate p : predicates) {
            if (isCompatible(IndexType.BTREE, p)) {
                predicateCols.add(p.column());
            }
        }
        int prefixLen = 0;
        for (String col : index.columnNames()) {
            if (!predicateCols.contains(col)) {
                break;
            }
            prefixLen++;
        }
        if (prefixLen == 0) {
            return Set.of();
        }
        return new HashSet<>(index.columnNames().subList(0, prefixLen));
    }

    /**
     * Emits index hints for predicates whose columns are in the given set
     * and are compatible with the index type.
     */
    private static void emitHintsForColumns(
            IndexDescriptor index,
            List<IndexPredicate> predicates,
            Set<String> columns,
            Set<IndexHint> seen,
            List<IndexHint> hints) {
        for (IndexPredicate predicate : predicates) {
            if (!columns.contains(predicate.column()) || !isCompatible(index.indexType(), predicate)) {
                continue;
            }
            IndexHint hint = new IndexHint(index.indexId(), index.indexType(), index.generation(), predicate);
            if (seen.add(hint)) {
                hints.add(hint);
            }
        }
    }

    /**
     * Standard single-column or non-BTree multi-column matching.
     */
    private static void addSingleColumnHints(
            IndexDescriptor index,
            List<IndexPredicate> predicates,
            Set<IndexHint> seen,
            List<IndexHint> hints) {
        for (IndexPredicate predicate : predicates) {
            if (!coversColumn(index, predicate.column()) || !isPredicateCompatible(index.indexType(), predicate)) {
                continue;
            }
            IndexHint hint = new IndexHint(index.indexId(), index.indexType(), index.generation(), predicate);
            if (seen.add(hint)) {
                hints.add(hint);
            }
        }
    }

    /**
     * Checks compatibility for any predicate type, including OR predicates.
     */
    private static boolean isPredicateCompatible(IndexType indexType, IndexPredicate predicate) {
        if (predicate instanceof OrPredicate or) {
            return isOrCompatible(indexType, or);
        }
        return isCompatible(indexType, predicate);
    }

    /**
     * Predicate-index compatibility matrix derived from actual engine evaluate() implementations.
     *
     * <pre>
     * IndexType          | Supported Predicates
     * -------------------|---------------------------------------------
     * BTREE              | EqualTo, RangeQuery, InList, IsNull, IsNotNull
     * HASH               | EqualTo, InList
     * BLOOM_FILTER       | EqualTo, InList
     * BITMAP             | EqualTo, InList
     * SPARSE             | RangeQuery
     * PREFIX             | LikePattern (prefix only: 'abc%')
     * NGRAM_BLOOM_FILTER | LikePattern (substring: '%abc%')
     * INVERTED           | TextMatch
     * R_TREE             | SpatialQuery
     * GIN                | ContainsElement
     * </pre>
     */
    static boolean isCompatible(IndexType indexType, IndexPredicate predicate) {
        return switch (indexType) {
            case BTREE -> predicate instanceof EqualTo
                    || predicate instanceof RangeQuery
                    || predicate instanceof InList
                    || predicate instanceof IsNull
                    || predicate instanceof IsNotNull;
            case HASH, BLOOM_FILTER, BITMAP -> predicate instanceof EqualTo
                    || predicate instanceof InList;
            case SPARSE -> predicate instanceof RangeQuery;
            case PREFIX, NGRAM_BLOOM_FILTER -> predicate instanceof LikePattern;
            case INVERTED -> predicate instanceof TextMatch;
            case R_TREE -> predicate instanceof SpatialQuery;
            case GIN -> predicate instanceof ContainsElement;
            case VECTOR -> false; // Native ANN artifacts are not file-pruning predicates.
        };
    }

    /**
     * Checks if an OR predicate is compatible with an index type.
     * All branches must be individually compatible for the OR to be usable.
     */
    private static boolean isOrCompatible(IndexType indexType, OrPredicate or) {
        return or.branches().stream().allMatch(branch -> isCompatible(indexType, branch));
    }
}
