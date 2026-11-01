package org.pangrova.coordinator.query.task;

import java.util.function.Consumer;

import org.pangrova.coordinator.query.execution.dispatch.dag.StageCompletionCallback;
import org.pangrova.client.error.ErrorDetail;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * G-SCHED-08: Tests the StageCompletionCallback interface defaults and
 * custom implementations for event-driven task-to-stage bridging.
 *
 * <p>Validates:</p>
 * <ul>
 *   <li>Default isQueryComplete returns true (backward compat)</li>
 *   <li>Custom callback tracks task terminal states</li>
 *   <li>onTaskTerminal bridges FINISHED/FAILED/CANCELLED to custom logic</li>
 * </ul>
 */
class EventDrivenQueryCompletionTest {

    @Test
    void defaultIsQueryComplete_returnsTrue() {
        StageCompletionCallback callback = new MinimalCallback();
        assertThat(callback.isQueryComplete("any")).isTrue();
    }

    @Test
    void customCallback_tracksTaskTerminalStates() {
        var callback = new TrackingCallback();

        callback.onTaskTerminal("q-1", "t-1", "FINISHED", null);
        callback.onTaskTerminal("q-1", "t-2", "FAILED",
                new ErrorDetail("INTERNAL", "ERR", "boom"));

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

    /** Minimal callback using only default methods. */
    private static final class MinimalCallback implements StageCompletionCallback {
        @Override
        public void onTaskTerminal(String queryId, String taskId,
                                   String state, ErrorDetail error) {
            // no-op for testing defaults
        }

        @Override
        public void onQueryTerminal(String queryId, Consumer<Boolean> listener) {
            listener.accept(true);
        }
    }

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
                                   String state, ErrorDetail error) {
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
