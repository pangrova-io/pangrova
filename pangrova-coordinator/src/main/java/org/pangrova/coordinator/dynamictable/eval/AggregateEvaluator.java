package org.pangrova.coordinator.dynamictable.eval;

import org.pangrova.coordinator.dynamictable.detect.ChangeResult;
import org.pangrova.coordinator.dynamictable.quota.RefreshMemoryQuota;
import org.pangrova.coordinator.dynamictable.DynamicTableRefreshDispatchFunction;
import org.pangrova.metadata.dynamictable.DynamicTableState;
import org.pangrova.spi.streaming.CdcEnvelope;
import org.pangrova.spi.streaming.CdcOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Evaluator for GROUP BY aggregation queries.
 *
 * <p>When a native dispatch function is provided, delegates GROUP BY
 * aggregation to a native worker via Substrait HashAggregateRel plan.
 * Otherwise performs coordinator-side aggregation with running state.</p>
 *
 * <p>Supports incremental merge (INSERT), retract (DELETE), and
 * UPDATE = retract old + merge new.</p>
 *
 * <p>Aggregation functions supported: SUM, COUNT, AVG.</p>
 */
public final class AggregateEvaluator implements IncrementalEvaluator {

    private static final Logger LOG = LoggerFactory.getLogger(AggregateEvaluator.class);

    /**
     * Estimated bytes per group entry: group key (~64B) + aggregation values (~64B).
     * Used for memory quota accounting.
     */
    public static final long ESTIMATED_ENTRY_BYTES = 128L;

    private final List<String> groupByKeys;
    private final List<AggregateFunction> aggregations;
    private final RefreshMemoryQuota quota;
    private final DynamicTableRefreshDispatchFunction nativeDispatch;
    private final Map<String, Map<String, Number>> state;

    /**
     * @param groupByKeys  columns used for grouping (must not be null or empty)
     * @param aggregations aggregation function definitions (must not be null or empty)
     * @param quota        memory quota for bounding aggregation state growth
     * @param nativeDispatch native plan dispatch function (nullable; when provided,
     *                       delegates to worker via Substrait HashAggregateRel)
     */
    public AggregateEvaluator(
            List<String> groupByKeys,
            List<AggregateFunction> aggregations,
            RefreshMemoryQuota quota,
            DynamicTableRefreshDispatchFunction nativeDispatch) {
        Objects.requireNonNull(groupByKeys, "groupByKeys must not be null");
        Objects.requireNonNull(aggregations, "aggregations must not be null");
        Objects.requireNonNull(quota, "quota must not be null");
        this.groupByKeys = List.copyOf(groupByKeys);
        this.aggregations = List.copyOf(aggregations);
        this.quota = quota;
        this.nativeDispatch = nativeDispatch;
        this.state = new HashMap<>();
    }

    /**
     * Constructor without native dispatch (coordinator-side aggregation).
     */
    public AggregateEvaluator(
            List<String> groupByKeys,
            List<AggregateFunction> aggregations,
            RefreshMemoryQuota quota) {
        this(groupByKeys, aggregations, quota, null);
    }

    @Override
    public IncrementalResult evaluate(DynamicTableState dtState, ChangeResult changes) {
        Objects.requireNonNull(dtState, "dtState must not be null");
        Objects.requireNonNull(changes, "changes must not be null");

        if (nativeDispatch != null) {
            return delegateToNativeWorker(dtState, changes);
        }

        if (!(changes instanceof ChangeResult.CdcBatch batch)) {
            return IncrementalResult.empty();
        }

        int merges = 0;
        long processed = 0;

        for (CdcEnvelope envelope : batch.events()) {
            processed++;
            merges += applyEvent(envelope);
        }

        List<Map<String, Object>> results = buildResults();
        return new IncrementalResult(results, processed, merges);
    }

    /**
     * Delegates GROUP BY aggregation to a native worker. The worker executes
     * Substrait HashAggregateRel in Arrow columnar memory and writes results
     * directly to the target table.
     */
    private IncrementalResult delegateToNativeWorker(DynamicTableState dtState, ChangeResult changes) {
        String checkpoint = changes.extractCheckpoint();
        nativeDispatch.apply(dtState, checkpoint);
        long rowCount = changes instanceof ChangeResult.CdcBatch batch
                ? batch.events().size() : 0;
        LOG.info("event=aggregate_native_dispatch dtName={} inputRows={}",
                dtState.dtName(), rowCount);
        return new IncrementalResult(List.of(), rowCount, 0);
    }

    private int applyEvent(CdcEnvelope envelope) {
        CdcOperation op = envelope.operation();
        return switch (op) {
            case INSERT -> mergeRow(envelope.after().toMap());
            case DELETE -> retractRow(envelope.before().toMap());
            case UPDATE -> {
                int count = retractRow(envelope.before().toMap());
                count += mergeRow(envelope.after().toMap());
                yield count;
            }
        };
    }

    private int mergeRow(Map<String, Object> row) {
        String groupKey = extractGroupKey(row);
        boolean[] isNew = {false};
        Map<String, Number> groupState = state.computeIfAbsent(
                groupKey, k -> {
                    isNew[0] = true;
                    return initGroupState();
                });

        if (isNew[0]) {
            quota.allocate(ESTIMATED_ENTRY_BYTES);
        }

        for (AggregateFunction agg : aggregations) {
            Number value = toNumber(row.get(agg.sourceColumn()));
            applyMerge(groupState, agg, value);
        }
        return 1;
    }

    private void applyMerge(Map<String, Number> groupState, AggregateFunction agg, Number value) {
        if (agg.type() == AggregateFunction.Type.AVG) {
            String sumKey = agg.outputName() + "_sum";
            String countKey = agg.outputName() + "_count";
            groupState.put(sumKey, groupState.get(sumKey).longValue() + value.longValue());
            groupState.put(countKey, groupState.get(countKey).longValue() + 1);
        } else {
            Number current = groupState.get(agg.outputName());
            groupState.put(agg.outputName(), agg.merge(current, value));
        }
    }

    private int retractRow(Map<String, Object> row) {
        String groupKey = extractGroupKey(row);
        Map<String, Number> groupState = state.get(groupKey);
        if (groupState == null) {
            return 0;
        }

        for (AggregateFunction agg : aggregations) {
            Number value = toNumber(row.get(agg.sourceColumn()));
            applyRetract(groupState, agg, value);
        }
        return 1;
    }

    private void applyRetract(Map<String, Number> groupState, AggregateFunction agg, Number value) {
        if (agg.type() == AggregateFunction.Type.AVG) {
            String sumKey = agg.outputName() + "_sum";
            String countKey = agg.outputName() + "_count";
            groupState.put(sumKey, groupState.get(sumKey).longValue() - value.longValue());
            groupState.put(countKey, groupState.get(countKey).longValue() - 1);
        } else {
            Number current = groupState.get(agg.outputName());
            groupState.put(agg.outputName(), agg.retract(current, value));
        }
    }

    private String extractGroupKey(Map<String, Object> row) {
        StringBuilder sb = new StringBuilder();
        for (String key : groupByKeys) {
            if (!sb.isEmpty()) {
                sb.append('\0');
            }
            sb.append(row.get(key));
        }
        return sb.toString();
    }

    private Map<String, Number> initGroupState() {
        Map<String, Number> init = new LinkedHashMap<>();
        for (AggregateFunction agg : aggregations) {
            if (agg.type() == AggregateFunction.Type.AVG) {
                init.put(agg.outputName() + "_sum", 0L);
                init.put(agg.outputName() + "_count", 0L);
            } else {
                init.put(agg.outputName(), agg.identity());
            }
        }
        return init;
    }

    private List<Map<String, Object>> buildResults() {
        List<Map<String, Object>> results = new ArrayList<>();
        for (Map.Entry<String, Map<String, Number>> entry : state.entrySet()) {
            Map<String, Object> row = new LinkedHashMap<>();
            String[] keyValues = entry.getKey().split("\0", -1);
            for (int i = 0; i < groupByKeys.size(); i++) {
                row.put(groupByKeys.get(i), keyValues[i]);
            }
            Map<String, Number> groupState = entry.getValue();
            for (AggregateFunction agg : aggregations) {
                if (agg.type() == AggregateFunction.Type.AVG) {
                    long sum = groupState.get(agg.outputName() + "_sum").longValue();
                    long count = groupState.get(agg.outputName() + "_count").longValue();
                    row.put(agg.outputName(), count == 0 ? 0L : sum / count);
                } else {
                    row.put(agg.outputName(), groupState.get(agg.outputName()));
                }
            }
            results.add(row);
        }
        return results;
    }

    /** Returns a snapshot of the current aggregation state (for testing). */
    public Map<String, Map<String, Number>> currentState() {
        return Map.copyOf(state);
    }

    /** Resets all aggregation state. */
    void reset() {
        quota.release(state.size() * ESTIMATED_ENTRY_BYTES);
        state.clear();
    }

    private static Number toNumber(Object value) {
        if (value instanceof Number n) {
            return n;
        }
        if (value == null) {
            return 0L;
        }
        return Long.parseLong(value.toString());
    }

    /**
     * Defines a single aggregation function (SUM, COUNT, or AVG).
     *
     * @param type         aggregation type
     * @param sourceColumn source column to aggregate
     * @param outputName   output column name for the aggregated value
     */
    public record AggregateFunction(
            Type type,
            String sourceColumn,
            String outputName
    ) {

        public AggregateFunction {
            Objects.requireNonNull(type, "type must not be null");
            Objects.requireNonNull(sourceColumn, "sourceColumn must not be null");
            Objects.requireNonNull(outputName, "outputName must not be null");
        }

        public enum Type { SUM, COUNT, AVG }

        Number identity() {
            return 0L;
        }

        Number merge(Number current, Number value) {
            return switch (type) {
                case SUM -> current.longValue() + value.longValue();
                case COUNT -> current.longValue() + 1;
                case AVG -> throw new UnsupportedOperationException(
                        "AVG merge handled via dual state in AggregateEvaluator");
            };
        }

        Number retract(Number current, Number value) {
            return switch (type) {
                case SUM -> current.longValue() - value.longValue();
                case COUNT -> current.longValue() - 1;
                case AVG -> throw new UnsupportedOperationException(
                        "AVG retract handled via dual state in AggregateEvaluator");
            };
        }
    }
}
