package org.pangrova.coordinator.task;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link TaskFailureCallback} integration in {@link CoordinatorTaskTracker}.
 * Verifies BUG-X01 fix: task failure propagation via callback.
 */
class TaskFailureCallbackTest {

    private static final String QUERY_ID = "q-cb";

    private CoordinatorTaskTracker tracker;

    @BeforeEach
    void setUp() {
        tracker = new CoordinatorTaskTracker();
    }

    @Test
    void failedTaskTriggersCallback() {
        List<String> captured = new ArrayList<>();
        tracker.setTaskFailureCallback((queryId, taskId) ->
                captured.add(queryId + ":" + taskId));

        tracker.registerTask(QUERY_ID, "t-1");
        tracker.markScheduled(QUERY_ID, "t-1");
        tracker.updateTaskState(QUERY_ID, "t-1", "RUNNING");
        tracker.updateTaskState(QUERY_ID, "t-1", "FAILED");

        assertThat(captured).containsExactly(QUERY_ID + ":t-1");
    }

    @Test
    void finishedTaskDoesNotTriggerCallback() {
        List<String> captured = new ArrayList<>();
        tracker.setTaskFailureCallback((queryId, taskId) ->
                captured.add(queryId + ":" + taskId));

        tracker.registerTask(QUERY_ID, "t-1");
        tracker.markScheduled(QUERY_ID, "t-1");
        tracker.updateTaskState(QUERY_ID, "t-1", "RUNNING");
        tracker.updateTaskState(QUERY_ID, "t-1", "FINISHED");

        assertThat(captured).isEmpty();
    }

    @Test
    void runningTaskDoesNotTriggerCallback() {
        List<String> captured = new ArrayList<>();
        tracker.setTaskFailureCallback((queryId, taskId) ->
                captured.add(queryId + ":" + taskId));

        tracker.registerTask(QUERY_ID, "t-1");
        tracker.markScheduled(QUERY_ID, "t-1");
        tracker.updateTaskState(QUERY_ID, "t-1", "RUNNING");

        assertThat(captured).isEmpty();
    }

    @Test
    void noCallbackRegisteredDoesNotThrow() {
        // No callback set — must be null-safe
        tracker.registerTask(QUERY_ID, "t-1");
        tracker.markScheduled(QUERY_ID, "t-1");
        tracker.updateTaskState(QUERY_ID, "t-1", "RUNNING");
        tracker.updateTaskState(QUERY_ID, "t-1", "FAILED");

        // Reaching here without exception proves null-safety
        assertThat(tracker.getTaskState(QUERY_ID, "t-1"))
                .isEqualTo(TaskStateMachine.State.FAILED);
    }

    @Test
    void callbackNotTriggeredOnDuplicateFailedTransition() {
        List<String> captured = new ArrayList<>();
        tracker.setTaskFailureCallback((queryId, taskId) ->
                captured.add(queryId + ":" + taskId));

        tracker.registerTask(QUERY_ID, "t-1");
        tracker.markScheduled(QUERY_ID, "t-1");
        tracker.updateTaskState(QUERY_ID, "t-1", "RUNNING");
        tracker.updateTaskState(QUERY_ID, "t-1", "FAILED");

        // Second FAILED transition should be a no-op (already FAILED, no valid transition)
        tracker.updateTaskState(QUERY_ID, "t-1", "FAILED");

        assertThat(captured).hasSize(1);
    }

    @Test
    void multipleTaskFailuresEachTriggerCallback() {
        List<String> captured = new ArrayList<>();
        tracker.setTaskFailureCallback((queryId, taskId) ->
                captured.add(queryId + ":" + taskId));

        tracker.registerTask(QUERY_ID, "t-1");
        tracker.registerTask(QUERY_ID, "t-2");
        tracker.markScheduled(QUERY_ID, "t-1");
        tracker.markScheduled(QUERY_ID, "t-2");
        tracker.updateTaskState(QUERY_ID, "t-1", "RUNNING");
        tracker.updateTaskState(QUERY_ID, "t-2", "RUNNING");

        tracker.updateTaskState(QUERY_ID, "t-1", "FAILED");
        tracker.updateTaskState(QUERY_ID, "t-2", "FAILED");

        assertThat(captured).containsExactly(
                QUERY_ID + ":t-1",
                QUERY_ID + ":t-2");
    }

    @Test
    void cancelledWorkerStateTriggersFailed() {
        List<String> captured = new ArrayList<>();
        tracker.setTaskFailureCallback((queryId, taskId) ->
                captured.add(queryId + ":" + taskId));

        tracker.registerTask(QUERY_ID, "t-1");
        tracker.markScheduled(QUERY_ID, "t-1");
        tracker.updateTaskState(QUERY_ID, "t-1", "RUNNING");
        // CANCELLED maps to FAILED in mapWorkerState
        tracker.updateTaskState(QUERY_ID, "t-1", "CANCELLED");

        assertThat(captured).containsExactly(QUERY_ID + ":t-1");
    }

    @Test
    void concurrentFailuresAllTriggerCallback() throws Exception {
        int taskCount = 20;
        CopyOnWriteArrayList<String> captured = new CopyOnWriteArrayList<>();
        tracker.setTaskFailureCallback((queryId, taskId) ->
                captured.add(queryId + ":" + taskId));

        for (int i = 0; i < taskCount; i++) {
            tracker.registerTask(QUERY_ID, "t-" + i);
            tracker.markScheduled(QUERY_ID, "t-" + i);
            tracker.updateTaskState(QUERY_ID, "t-" + i, "RUNNING");
        }

        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch gate = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < taskCount; i++) {
            String taskId = "t-" + i;
            futures.add(pool.submit(() -> {
                awaitLatch(gate);
                tracker.updateTaskState(QUERY_ID, taskId, "FAILED");
            }));
        }

        gate.countDown();
        for (Future<?> f : futures) {
            f.get();
        }
        pool.shutdown();

        assertThat(captured).hasSize(taskCount);
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
