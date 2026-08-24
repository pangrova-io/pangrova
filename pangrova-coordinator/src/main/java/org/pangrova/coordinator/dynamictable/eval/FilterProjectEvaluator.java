package org.pangrova.coordinator.dynamictable.eval;

import org.pangrova.coordinator.dynamictable.detect.ChangeResult;
import org.pangrova.coordinator.dynamictable.DynamicTableRefreshDispatchFunction;
import org.pangrova.metadata.dynamictable.DynamicTableState;
import org.pangrova.spi.streaming.CdcEnvelope;
import org.pangrova.spi.streaming.CdcOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Evaluator for simple SELECT ... WHERE ... queries without aggregation.
 *
 * <p>When a native dispatch function is provided, delegates filter and project
 * operations to a native worker via Substrait FilterRel + ProjectRel plan.
 * Otherwise falls back to coordinator-side row-level processing for small
 * batches or when native dispatch is unavailable.</p>
 *
 * <p>For CDC batches, only INSERT and UPDATE_AFTER rows produce output;
 * DELETE rows are ignored (no aggregation state to retract).</p>
 */
public final class FilterProjectEvaluator implements IncrementalEvaluator {

    private static final Logger LOG = LoggerFactory.getLogger(FilterProjectEvaluator.class);

    private final Predicate<Map<String, Object>> filter;
    private final List<String> projectionColumns;
    private final DynamicTableRefreshDispatchFunction nativeDispatch;

    /**
     * @param filter             row-level filter predicate (must not be null)
     * @param projectionColumns  columns to retain in output (must not be null or empty)
     * @param nativeDispatch     native plan dispatch function (nullable; when provided,
     *                           delegates to worker via Substrait FilterRel + ProjectRel)
     */
    public FilterProjectEvaluator(
            Predicate<Map<String, Object>> filter,
            List<String> projectionColumns,
            DynamicTableRefreshDispatchFunction nativeDispatch) {
        this.filter = Objects.requireNonNull(filter, "filter must not be null");
        Objects.requireNonNull(projectionColumns, "projectionColumns must not be null");
        this.projectionColumns = List.copyOf(projectionColumns);
        this.nativeDispatch = nativeDispatch;
    }

    /**
     * Constructor without native dispatch (coordinator-side processing).
     */
    public FilterProjectEvaluator(
            Predicate<Map<String, Object>> filter,
            List<String> projectionColumns) {
        this(filter, projectionColumns, null);
    }

    @Override
    public IncrementalResult evaluate(DynamicTableState state, ChangeResult changes) {
        Objects.requireNonNull(state, "state must not be null");
        Objects.requireNonNull(changes, "changes must not be null");

        if (nativeDispatch != null) {
            return delegateToNativeWorker(state, changes);
        }

        if (changes instanceof ChangeResult.CdcBatch batch) {
            return evaluateCdcBatch(batch);
        }
        if (changes instanceof ChangeResult.Incremental inc) {
            return evaluateIncremental(inc);
        }
        return IncrementalResult.empty();
    }

    /**
     * Delegates filter and project to a native worker. The worker executes
     * Substrait FilterRel + ProjectRel in Arrow columnar memory and writes
     * results directly to the target table.
     */
    private IncrementalResult delegateToNativeWorker(DynamicTableState state, ChangeResult changes) {
        String checkpoint = changes.extractCheckpoint();
        nativeDispatch.apply(state, checkpoint);
        long rowCount = countInputRows(changes);
        LOG.info("event=filter_project_native_dispatch dtName={} inputRows={}",
                state.dtName(), rowCount);
        return new IncrementalResult(List.of(), rowCount, 0);
    }

    private static long countInputRows(ChangeResult changes) {
        if (changes instanceof ChangeResult.CdcBatch batch) {
            return batch.events().size();
        }
        return 0;
    }

    private IncrementalResult evaluateCdcBatch(ChangeResult.CdcBatch batch) {
        List<Map<String, Object>> results = new ArrayList<>();
        long processed = 0;

        for (CdcEnvelope envelope : batch.events()) {
            processed++;
            Map<String, Object> row = extractRow(envelope);
            if (row != null && filter.test(row)) {
                results.add(project(row));
            }
        }
        return new IncrementalResult(results, processed, 0);
    }

    private IncrementalResult evaluateIncremental(ChangeResult.Incremental inc) {
        throw new UnsupportedOperationException(
                "FilterProjectEvaluator does not support ChangeResult.Incremental; "
                        + "snapshot mode non-aggregation queries must be rejected at feasibility check");
    }

    private Map<String, Object> extractRow(CdcEnvelope envelope) {
        if (envelope.operation() == CdcOperation.DELETE) {
            return null;
        }
        return envelope.after() != null ? envelope.after().toMap() : null;
    }

    private Map<String, Object> project(Map<String, Object> row) {
        Map<String, Object> projected = new LinkedHashMap<>();
        for (String col : projectionColumns) {
            projected.put(col, row.get(col));
        }
        return projected;
    }
}
