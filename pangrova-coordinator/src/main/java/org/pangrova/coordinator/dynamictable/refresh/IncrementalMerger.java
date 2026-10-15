package org.pangrova.coordinator.dynamictable.refresh;

import org.pangrova.coordinator.dynamictable.DynamicTableRefreshDispatchFunction;
import org.pangrova.coordinator.dynamictable.eval.IncrementalResult;
import org.pangrova.planner.incremental.ColumnMergeStrategy;
import org.pangrova.planner.incremental.MergeOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Merges old (materialized) aggregation results with delta (incremental) results
 * using operator-specific strategies.
 *
 * <p>When a native dispatch function is provided via
 * {@link #mergeNative(DynamicTableRefreshDispatchFunction, org.pangrova.metadata.dynamictable.DynamicTableState,
 * String)},
 * the merge is delegated to a native worker via Substrait MergeJoinRel or
 * UnionAggregateRel plan. Otherwise, coordinator-side merge is performed.</p>
 *
 * <p>For each group key, combines the old and delta values according to the
 * {@link MergeOperator} defined for each aggregation column:</p>
 * <ul>
 *   <li>{@code SUM} → {@code old + delta}</li>
 *   <li>{@code MIN} → {@code min(old, delta)}</li>
 *   <li>{@code MAX} → {@code max(old, delta)}</li>
 *   <li>{@code AVG_DECOMPOSE} → {@code (old_sum + delta_sum) / (old_count + delta_count)}</li>
 * </ul>
 *
 * <p>Group keys are identified by columns NOT listed in the merge strategies.
 * New groups appearing only in the delta are added directly.</p>
 */
public final class IncrementalMerger {

    private static final Logger LOG = LoggerFactory.getLogger(IncrementalMerger.class);

    private IncrementalMerger() {
    }

    /**
     * Delegates the incremental merge to a native worker via Substrait plan.
     * The worker executes a MergeJoinRel or UnionAggregateRel in Arrow columnar
     * memory and writes results directly to the target table.
     *
     * @param nativeDispatch native dispatch function
     * @param state          current Dynamic Table state
     * @param checkpoint     source checkpoint for the merge
     * @return empty result (worker wrote directly to target)
     */
    public static IncrementalResult mergeNative(
            DynamicTableRefreshDispatchFunction nativeDispatch,
            org.pangrova.metadata.dynamictable.DynamicTableState state,
            String checkpoint) {
        Objects.requireNonNull(nativeDispatch, "nativeDispatch must not be null");
        Objects.requireNonNull(state, "state must not be null");
        Objects.requireNonNull(nativeDispatch.apply(state, checkpoint),
                "native merge must return a terminal commit outcome")
                .requireCommittedCheckpoint(checkpoint);
        LOG.info("event=incremental_merge_native_dispatch dtName={} checkpoint={}",
                state.dtName(), checkpoint);
        return new IncrementalResult(List.of(), 0, 0);
    }

    /**
     * Merge old aggregates with delta aggregates using the given strategies.
     *
     * @param oldResult   materialized aggregation result (may be empty for first refresh)
     * @param deltaResult incremental aggregation result from delta scan
     * @param strategies  per-column merge strategies from {@code IncrementalAnalyzer}
     * @param groupKeys   column names that form the GROUP BY key
     * @return merged result combining old and delta
     */
    public static IncrementalResult merge(
            IncrementalResult oldResult,
            IncrementalResult deltaResult,
            List<ColumnMergeStrategy> strategies,
            List<String> groupKeys) {

        Objects.requireNonNull(oldResult, "oldResult must not be null");
        Objects.requireNonNull(deltaResult, "deltaResult must not be null");
        Objects.requireNonNull(strategies, "strategies must not be null");
        Objects.requireNonNull(groupKeys, "groupKeys must not be null");

        // Index old records by group key for O(1) lookup
        Map<String, Map<String, Object>> oldByKey = indexByGroupKey(oldResult.records(), groupKeys);

        List<Map<String, Object>> merged = new ArrayList<>();
        int mergeCount = 0;

        // Process delta records — merge with old or add new groups
        Map<String, Map<String, Object>> deltaByKey = indexByGroupKey(deltaResult.records(), groupKeys);
        for (Map.Entry<String, Map<String, Object>> entry : deltaByKey.entrySet()) {
            String key = entry.getKey();
            Map<String, Object> deltaRow = entry.getValue();
            Map<String, Object> oldRow = oldByKey.remove(key);

            if (oldRow != null) {
                merged.add(mergeRow(oldRow, deltaRow, strategies, groupKeys));
                mergeCount++;
            } else {
                // New group — add delta directly
                merged.add(new LinkedHashMap<>(deltaRow));
            }
        }

        // Add remaining old groups not present in delta (unchanged)
        for (Map<String, Object> oldRow : oldByKey.values()) {
            merged.add(new LinkedHashMap<>(oldRow));
        }

        long totalProcessed = oldResult.processedRows() + deltaResult.processedRows();
        LOG.info("event=incremental_merge old_groups={} delta_groups={} merged_groups={} "
                        + "new_groups={} total_output={}",
                oldResult.records().size(), deltaResult.records().size(),
                mergeCount, deltaByKey.size() - mergeCount, merged.size());

        return new IncrementalResult(merged, totalProcessed, mergeCount);
    }

    private static Map<String, Object> mergeRow(
            Map<String, Object> oldRow,
            Map<String, Object> deltaRow,
            List<ColumnMergeStrategy> strategies,
            List<String> groupKeys) {

        Map<String, Object> result = new LinkedHashMap<>(oldRow);

        for (int i = 0; i < strategies.size(); i++) {
            ColumnMergeStrategy strategy = strategies.get(i);
            String aggColumn = findAggregationColumn(oldRow, strategy, i, groupKeys);
            if (aggColumn == null) {
                continue;
            }

            Object oldVal = oldRow.get(aggColumn);
            Object deltaVal = deltaRow.get(aggColumn);

            if (strategy.mergeOperator() == MergeOperator.AVG_DECOMPOSE) {
                result.put(aggColumn, mergeAvg(oldRow, deltaRow, oldVal, deltaVal));
            } else {
                result.put(aggColumn, mergeValues(oldVal, deltaVal, strategy.mergeOperator()));
            }
        }

        return result;
    }

    private static Object mergeValues(Object oldVal, Object deltaVal, MergeOperator operator) {
        if (oldVal == null) {
            return deltaVal;
        }
        if (deltaVal == null) {
            return oldVal;
        }

        return switch (operator) {
            case SUM -> addNumbers(oldVal, deltaVal);
            case MIN -> minNumbers(oldVal, deltaVal);
            case MAX -> maxNumbers(oldVal, deltaVal);
            case AVG_DECOMPOSE -> throw new IllegalStateException(
                    "AVG_DECOMPOSE should be handled by mergeAvg, not mergeValues");
        };
    }

    /**
     * Merges AVG by computing (old_sum + delta_sum) / (old_count + delta_count).
     * The AVG column stores the sum component; the corresponding count column
     * (matched by "count" in its name) tracks the denominator.
     */
    private static Object mergeAvg(
            Map<String, Object> oldRow,
            Map<String, Object> deltaRow,
            Object oldSum,
            Object deltaSum) {

        Object totalSum = addNumbers(orZero(oldSum), orZero(deltaSum));

        String countColumn = findCountColumn(oldRow);
        if (countColumn == null) {
            throw new IllegalStateException(
                    "AVG_DECOMPOSE requires a count column but none found in row keys: "
                            + oldRow.keySet());
        }

        Object totalCount = addNumbers(
                orZero(oldRow.get(countColumn)),
                orZero(deltaRow.get(countColumn)));
        if (isZero(totalCount)) {
            return null;
        }
        return divide(totalSum, totalCount);
    }

    private static Object orZero(Object value) {
        return value != null ? value : 0L;
    }

    private static boolean isZero(Object count) {
        if (count instanceof Long lc) {
            return lc == 0L;
        }
        return count instanceof Number nc && nc.doubleValue() == 0.0;
    }

    private static Object divide(Object totalSum, Object totalCount) {
        if (totalSum instanceof Long ls && totalCount instanceof Long lc) {
            return ls / lc;
        }
        return ((Number) totalSum).doubleValue() / ((Number) totalCount).doubleValue();
    }

    /**
     * Finds a column whose name contains "count" (case-insensitive),
     * used as the denominator for AVG decomposition.
     */
    private static String findCountColumn(Map<String, Object> row) {
        for (String col : row.keySet()) {
            if (col.toLowerCase().contains("count")) {
                return col;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static Comparable<Object> toComparable(Object val) {
        if (val instanceof Comparable<?>) {
            return (Comparable<Object>) val;
        }
        throw new IllegalArgumentException("Value is not comparable: " + val.getClass());
    }

    private static Object addNumbers(Object a, Object b) {
        if (a instanceof Long la && b instanceof Long lb) {
            return la + lb;
        }
        if (a instanceof Number na && b instanceof Number nb) {
            return na.doubleValue() + nb.doubleValue();
        }
        throw new IllegalArgumentException(
                "Cannot add non-numeric values: " + a.getClass() + " + " + b.getClass());
    }

    private static Object minNumbers(Object a, Object b) {
        Comparable<Object> ca = toComparable(a);
        return ca.compareTo(b) <= 0 ? a : b;
    }

    private static Object maxNumbers(Object a, Object b) {
        Comparable<Object> ca = toComparable(a);
        return ca.compareTo(b) >= 0 ? a : b;
    }

    /**
     * Find the aggregation column name in the row that corresponds to the given strategy.
     * First tries name-based matching (column contains operator name). If that fails,
     * falls back to ordinal position among non-group-key columns — safe because the
     * strategy list and the query's SELECT list share the same aggregation order.
     */
    private static String findAggregationColumn(Map<String, Object> row,
                                                ColumnMergeStrategy strategy,
                                                int strategyIndex,
                                                List<String> groupKeys) {
        String opLower = strategy.sourceOperator().toLowerCase();
        for (String col : row.keySet()) {
            if (groupKeys.contains(col)) {
                continue;
            }
            if (col.toLowerCase().contains(opLower)) {
                return col;
            }
        }
        List<String> aggCols = new ArrayList<>();
        for (String col : row.keySet()) {
            if (!groupKeys.contains(col)) {
                aggCols.add(col);
            }
        }
        if (strategyIndex < aggCols.size()) {
            return aggCols.get(strategyIndex);
        }
        return null;
    }

    private static Map<String, Map<String, Object>> indexByGroupKey(
            List<Map<String, Object>> records, List<String> groupKeys) {
        Map<String, Map<String, Object>> index = new LinkedHashMap<>();
        for (Map<String, Object> record : records) {
            String key = buildGroupKey(record, groupKeys);
            index.put(key, record);
        }
        return index;
    }

    private static String buildGroupKey(Map<String, Object> record, List<String> groupKeys) {
        if (groupKeys.isEmpty()) {
            return "__global__";
        }
        StringBuilder sb = new StringBuilder();
        for (String key : groupKeys) {
            if (sb.length() > 0) {
                sb.append('|');
            }
            Object val = record.get(key);
            sb.append(val == null ? "null" : val.toString());
        }
        return sb.toString();
    }
}
