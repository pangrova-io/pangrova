package org.pangrova.coordinator.bootstrap.module;

import org.pangrova.coordinator.query.execution.dispatch.StatementCompletionListener;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.StageLifecycleHandler;
import org.pangrova.coordinator.query.execution.phase.QueryExecutionPhase;

/**
 * Context provided to {@link CoordinatorModule#configure} for declaring
 * bindings and registering pipeline components. Modules use this API to
 * contribute query phases, stage handlers, and completion listeners without
 * knowing the assembly details.
 */
public interface CoordinatorModuleContext {

    /**
     * Binds a service instance to the given type. Later modules and the
     * assembler can retrieve it via {@link #require} or {@link #optional}.
     */
    <T> void bind(Class<T> type, T instance);

    /**
     * Returns a previously bound service, throwing if absent.
     *
     * @throws IllegalStateException if no binding exists for the type
     */
    <T> T require(Class<T> type);

    /**
     * Returns a previously bound service, or null if absent.
     */
    <T> T optional(Class<T> type);

    /** Adds a query execution phase to the pipeline. */
    void addQueryPhase(QueryExecutionPhase phase);

    /** Adds a stage lifecycle handler for DAG dispatch events. */
    void addStageHandler(StageLifecycleHandler handler);

    /** Adds a statement completion listener for post-DDL/DML side effects. */
    void addCompletionListener(StatementCompletionListener listener);
}
