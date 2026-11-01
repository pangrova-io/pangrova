package org.pangrova.coordinator.query.execution.dispatch.dag;

import java.util.function.Consumer;
import org.pangrova.client.error.ErrorDetail;

/**
 * Callback for routing terminal task states to the DAG dispatch engine.
 * Wired from {@code TaskStatusReceiver} to {@code DAGDispatchEngine.onTaskTerminal}
 * so that task completion events drive stage state transitions (event-driven,
 * like Spark's TaskSetManager → DAGScheduler.taskEnded bridge).
 *
 * <p>Nullable in {@code TaskStatusReceiverContext} — disabled when null
 * (e.g. tests that don't use DAG dispatch).</p>
 */
public interface StageCompletionCallback {

    /**
     * Called when a task reaches a terminal state (FINISHED, FAILED, or CANCELLED).
     *
     * @param queryId the owning query
     * @param taskId  the terminal task
     * @param state   terminal state string (FINISHED, FAILED, CANCELLED)
     * @param error   error detail, nullable (null for FINISHED/CANCELLED)
     */
    void onTaskTerminal(String queryId, String taskId, String state, ErrorDetail error);

    /**
     * Returns true if the DAG engine considers the query fully complete
     * (all stages finished). Used by TaskStatusReceiver to avoid premature
     * query finalization when downstream stages are still being dispatched.
     *
     * <p>Default returns true for backward compatibility with non-DAG callers.</p>
     */
    default boolean isQueryComplete(String queryId) {
        return true;
    }

    /**
     * Notifies a deferred finalizer exactly once when the query DAG terminates.
     * An observer registered after termination but before query cleanup receives
     * the outcome immediately.
     * The value is true only for successful completion; failure and cancellation report false.
     */
    void onQueryTerminal(String queryId, Consumer<Boolean> listener);
}
