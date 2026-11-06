package org.pangrova.coordinator.query.execution.phase;

import org.pangrova.query.core.task.DispatchResult;

/**
 * Result of a query execution phase.
 * Terminal short-circuits the pipeline; Continue passes updated context to the next phase.
 */
public sealed interface PhaseResult {

    /** Pipeline short-circuit: return this result immediately. */
    record Terminal(DispatchResult result) implements PhaseResult {}

    /** Continue to next phase with updated context. */
    record Continue(QueryExecutionContext updated) implements PhaseResult {}
}
