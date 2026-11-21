package org.pangrova.coordinator.query.execution.dispatch.dag.handler;

/**
 * Handler for stage lifecycle events within DAG dispatch.
 * Each implementation encapsulates a single cross-cutting concern
 * (shuffle, adaptive query, retry, commit notification, speculation).
 *
 * <p>Handlers are invoked by the DAG dispatch engine on each stage
 * state transition. Exceptions thrown by a handler are caught and
 * logged — they never propagate to other handlers.</p>
 */
public interface StageLifecycleHandler {

    /**
     * Called when a stage transitions to SCHEDULING.
     * Use for resource registration (e.g., shuffle setup).
     */
    void onScheduling(StageLifecycleEvent event);

    /**
     * Called when a stage transitions to RUNNING.
     * Default no-op: most handlers do not need this event.
     */
    default void onRunning(StageLifecycleEvent event) {
        // intentional no-op — override only if the handler needs RUNNING events
    }

    /**
     * Called when a stage transitions to FINISHED.
     * Default no-op: most handlers do not need this event.
     */
    default void onFinished(StageLifecycleEvent event) {
        // intentional no-op — override only if the handler needs FINISHED events
    }

    /**
     * Called when a stage transitions to FAILED.
     * Default no-op: most handlers do not need this event.
     */
    default void onFailed(StageLifecycleEvent event) {
        // intentional no-op — override only if the handler needs FAILED events
    }

    /**
     * Called when the entire query DAG completes successfully (all stages finished).
     * Use for query-level resource cleanup (e.g., shuffle resource release).
     * Default no-op: most handlers do not need this event.
     *
     * @param queryId the completed query identifier
     */
    default void onQueryCompleted(String queryId) {
        // intentional no-op — override only if the handler needs query completion events
    }

    /**
     * Called when the query DAG fails or is cancelled.
     * Use for query-level resource cleanup on failure paths.
     * Default no-op: most handlers do not need this event.
     *
     * @param queryId the failed query identifier
     */
    default void onQueryFailed(String queryId) {
        // intentional no-op — override only if the handler needs query failure events
    }
}
