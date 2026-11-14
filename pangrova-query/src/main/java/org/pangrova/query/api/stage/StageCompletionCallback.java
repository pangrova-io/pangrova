package org.pangrova.query.api.stage;

import java.util.function.Consumer;
import org.pangrova.common.api.annotation.Internal;
import org.pangrova.query.api.error.QueryFailure;

/** Terminal observations exchanged between task reporting and the query DAG owner. */
@Internal
public interface StageCompletionCallback {

    /**
     * Reports the terminal state for the logical task slot. A successful speculative
     * copy settles its original slot. Error diagnostics may be absent.
     */
    void onTaskTerminal(String queryId, String taskId, String state, QueryFailure error);

    /** Returns true when every stage required for successful query completion has finished. */
    boolean isQueryComplete(String queryId);

    /**
     * Delivers one terminal outcome to a deferred finalizer. An observer registered
     * after termination and before query cleanup receives the outcome immediately.
     * Only successful completion delivers true.
     */
    void onQueryTerminal(String queryId, Consumer<Boolean> listener);
}
