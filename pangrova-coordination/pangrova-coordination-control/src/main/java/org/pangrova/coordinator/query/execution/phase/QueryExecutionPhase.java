package org.pangrova.coordinator.query.execution.phase;

/**
 * A single phase in the query execution pipeline.
 * Each phase transforms or short-circuits the query context.
 */
public interface QueryExecutionPhase {

    /** Executes this phase, returning Terminal to short-circuit or Continue to proceed. */
    PhaseResult execute(QueryExecutionContext context);

    /** Human-readable phase name for logging and metrics. */
    String name();
}
