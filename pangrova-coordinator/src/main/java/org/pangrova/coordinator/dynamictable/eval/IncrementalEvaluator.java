package org.pangrova.coordinator.dynamictable.eval;

import org.pangrova.coordinator.dynamictable.detect.ChangeResult;
import org.pangrova.metadata.dynamictable.DynamicTableState;

/**
 * Applies the defining query's logic to a batch of incremental changes,
 * producing output rows for the target table.
 *
 * <p>Implementations handle different query shapes:</p>
 * <ul>
 *   <li>{@link FilterProjectEvaluator} — simple SELECT ... WHERE ... (no aggregation)</li>
 *   <li>{@link AggregateEvaluator} — GROUP BY with SUM/COUNT/AVG etc.</li>
 * </ul>
 */
public interface IncrementalEvaluator {

    /**
     * Evaluate incremental changes against the defining query logic.
     *
     * @param state   current Dynamic Table state (checkpoint, schema info)
     * @param changes detected changes from the source table
     * @return evaluation result containing output rows and processing stats
     */
    IncrementalResult evaluate(DynamicTableState state, ChangeResult changes);
}
