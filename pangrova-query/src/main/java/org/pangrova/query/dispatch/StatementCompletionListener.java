package org.pangrova.query.dispatch;

/**
 * Listener invoked after a statement completes successfully.
 * Used for side effects: schema refresh, cache invalidation, compiler update.
 * Listeners are ordered by priority (lower value = earlier execution).
 */
public interface StatementCompletionListener {

    /** Called when a statement execution completes. */
    void onCompleted(StatementCompletionEvent event);

    /**
     * Execution priority. Lower values run first.
     * Default is 100; schema refresh uses 10, cache invalidation uses 20.
     */
    default int priority() {
        return 100;
    }
}
