package org.pangrova.coordinator.query.task;

import java.util.function.Consumer;

import org.pangrova.query.api.stage.StageCompletionCallback;
import org.pangrova.query.api.error.QueryFailure;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Checks query finalization against the explicit DAG owner's task outcomes. */
class EventDrivenQueryCompletionTest {

    @Test
    void completionRequiresAnExplicitDecisionByTheDagOwner() throws Exception {
        assertThat(StageCompletionCallback.class.getMethod("isQueryComplete", String.class).isDefault()).isFalse();
    }

    @Test
    void customCallback_tracksTaskTerminalStates() {
        var callback = new TrackingCallback();

        callback.onTaskTerminal("q-1", "t-1", "FINISHED", null);
        callback.onTaskTerminal("q-1", "t-2", "FAILED",
                new QueryFailure("INTERNAL", "ERR", "boom"));

        assertThat(callback.finishedCount).isEqualTo(1);
        assertThat(callback.failedCount).isEqualTo(1);
    }

    @Test
    void customCallback_reportsQueryCompleteWhenAllTasksDone() {
        var callback = new TrackingCallback();
        callback.expectedTaskCount = 2;

        callback.onTaskTerminal("q-1", "t-1", "FINISHED", null);
        assertThat(callback.isQueryComplete("q-1")).isFalse();

        callback.onTaskTerminal("q-1", "t-2", "FINISHED", null);
        assertThat(callback.isQueryComplete("q-1")).isTrue();
    }

    @Test
    void customCallback_cancelledTasksCountAsTerminal() {
        var callback = new TrackingCallback();
        callback.expectedTaskCount = 1;

        callback.onTaskTerminal("q-2", "t-1", "CANCELLED", null);
        assertThat(callback.isQueryComplete("q-2")).isTrue();
        assertThat(callback.cancelledCount).isEqualTo(1);
    }

    // --- Test doubles ---

    /**
     * Test callback that tracks terminal task counts.
     */
    private static final class TrackingCallback implements StageCompletionCallback {
        int finishedCount;
        int failedCount;
        int cancelledCount;
        int expectedTaskCount = Integer.MAX_VALUE;
        Consumer<Boolean> terminalListener;

        @Override
        public void onTaskTerminal(String queryId, String taskId,
                                   String state, QueryFailure error) {
            switch (state) {
                case "FINISHED" -> finishedCount++;
                case "FAILED" -> failedCount++;
                case "CANCELLED" -> cancelledCount++;
            }
            if (terminalListener != null && isQueryComplete(queryId)) {
                terminalListener.accept(failedCount == 0 && cancelledCount == 0);
                terminalListener = null;
            }
        }

        @Override
        public boolean isQueryComplete(String queryId) {
            return (finishedCount + failedCount + cancelledCount) >= expectedTaskCount;
        }

        @Override
        public void onQueryTerminal(String queryId, Consumer<Boolean> listener) {
            if (isQueryComplete(queryId)) {
                listener.accept(failedCount == 0 && cancelledCount == 0);
            } else {
                terminalListener = listener;
            }
        }
    }
}
