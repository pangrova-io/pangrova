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

    /** Carries the elected physical attempt's build summary with the logical terminal event. */
    default void onTaskTerminal(String queryId, String taskId, String state, QueryFailure error,
            BuildKeyRange buildKeyRange) {
        onTaskTerminal(queryId, taskId, state, error);
    }

    /** Inclusive signed bounds, or a complete summary with no non-null keys. */
    record BuildKeyRange(long minimum, long maximum, boolean empty) {
        public BuildKeyRange {
            if (minimum > maximum || (empty && (minimum != 0 || maximum != 0))) {
                throw new IllegalArgumentException("Invalid build key range");
            }
        }

        public BuildKeyRange merge(BuildKeyRange other) {
            if (empty) return other;
            if (other.empty) return this;
            return new BuildKeyRange(Math.min(minimum, other.minimum), Math.max(maximum, other.maximum), false);
        }
    }

    /** Includes a retired DAG whose terminal outcome is still retained for late task reports. */
    boolean ownsQueryCompletion(String queryId);

    /** Returns true when every stage required for successful query completion has finished. */
    boolean isQueryComplete(String queryId);

    /**
     * Delivers one terminal outcome to a deferred finalizer. An observer registered
     * after termination and before query cleanup receives the outcome immediately.
     * Only successful completion delivers true.
     */
    void onQueryTerminal(String queryId, Consumer<Boolean> listener);
}
