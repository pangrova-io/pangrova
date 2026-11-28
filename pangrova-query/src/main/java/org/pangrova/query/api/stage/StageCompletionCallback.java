package org.pangrova.query.api.stage;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.pangrova.common.api.annotation.Internal;
import org.pangrova.query.api.error.QueryFailure;

/** Execution observations exchanged between task reporting and the query DAG owner. */
@Internal
public interface StageCompletionCallback {

    /**
     * Reports the terminal state for the logical task slot. A successful speculative
     * copy settles its original slot. Error diagnostics may be absent.
     */
    void onTaskTerminal(String queryId, String taskId, String state, QueryFailure error);

    /** Reports that the issued physical attempt entered Native execution. */
    default void onTaskRunning(String queryId, String taskId, int stageId) {}

    /** Identifies the winning physical attempt and its optional completed build summary. */
    default void onSuccessfulTaskTerminal(String queryId, String logicalTaskId,
            SuccessfulAttempt attempt, Map<Integer, List<BuildKeyRange>> buildKeyRanges) {
        onTaskTerminal(queryId, logicalTaskId, "FINISHED", null);
    }

    record SuccessfulAttempt(String taskId, int partitionId, long planEpoch, int attemptId) {}

    /** Inclusive signed bounds of one interval in the completed build summary. */
    record BuildKeyRange(long minimum, long maximum) {
        public static final int MAX_RANGES = 16;

        public BuildKeyRange {
            if (minimum > maximum) throw new IllegalArgumentException("Invalid build key range");
        }

        public BuildKeyRange merge(BuildKeyRange other) {
            return new BuildKeyRange(Math.min(minimum, other.minimum), Math.max(maximum, other.maximum));
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
