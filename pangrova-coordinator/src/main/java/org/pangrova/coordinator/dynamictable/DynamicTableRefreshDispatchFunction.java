package org.pangrova.coordinator.dynamictable;

import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.metadata.dynamictable.DynamicTableState;

/**
 * Compiles a Dynamic Table refresh query into a Substrait plan and dispatches
 * it to a native worker via the query pipeline.
 *
 * <p>The compiled plan follows the pattern:
 * ReadRel(CDC source, incremental splits) → FilterRel(DT WHERE) →
 * ProjectRel(DT SELECT) → HashAggregateRel(GROUP BY) → WriteRel(DT target, OVERWRITE).
 *
 * <p>The coordinator only orchestrates scheduling and commit — data processing
 * happens entirely on the native worker in Arrow columnar memory.</p>
 */
@FunctionalInterface
public interface DynamicTableRefreshDispatchFunction {

    /**
     * Compiles the DT refresh query to a Substrait plan and dispatches to a worker.
     *
     * @param state      current Dynamic Table state (defines the query, source, target)
     * @param checkpoint source checkpoint for incremental CDC splits
     * @return dispatch result with worker task information
     */
    DispatchResult apply(DynamicTableState state, String checkpoint);
}
