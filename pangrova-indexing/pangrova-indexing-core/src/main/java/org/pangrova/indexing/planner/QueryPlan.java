package org.pangrova.indexing.planner;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable query plan wrapper carrying index hints keyed by table scan reference.
 *
 * <p>The key is a composite string "{catalog}.{schema}.{table}" identifying
 * the table scan node. Hints are produced by {@link IndexPredicateAnalyzer}
 * and consumed by IndexSplitFilter in the coordinator.</p>
 *
 * <p>No ThreadLocal propagation — hints are passed explicitly through the
 * query execution pipeline.</p>
 */
public final class QueryPlan {

    private final Map<String, List<IndexHint>> indexHintMap;

    /**
     * @param indexHintMap table scan key → index hints; null treated as empty
     */
    public QueryPlan(Map<String, List<IndexHint>> indexHintMap) {
        this.indexHintMap = indexHintMap != null ? Map.copyOf(indexHintMap) : Map.of();
    }

    /**
     * Returns index hints for the given table scan, or empty list if none.
     *
     * @param tableScanKey composite key "{catalog}.{schema}.{table}"
     * @return immutable list of index hints, never null
     */
    public List<IndexHint> hintsFor(String tableScanKey) {
        return indexHintMap.getOrDefault(
                Objects.requireNonNull(tableScanKey), List.of());
    }

    /** Returns true if no index hints exist for any table scan. */
    public boolean isEmpty() {
        return indexHintMap.isEmpty();
    }

    /** Returns the number of table scans with index hints. */
    public int size() {
        return indexHintMap.size();
    }
}
