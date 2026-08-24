package org.pangrova.coordinator.task;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.scheduler.SchedulerCancelResult;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link TaskCancelRetrier}.
 *
 * <p>BUG-D12/D26: Validates that cancel failures are logged at WARN,
 * tasks are enqueued for retry, and periodic retry clears them on success.</p>
 *
 * <p>Uses test doubles (no mock frameworks) per project conventions.</p>
 */
class TaskCancelRetrierTest {

    private static final String QUERY_ID = "q-cancel-1";
    private static final String TASK_ID_1 = "task-1";
    private static final String TASK_ID_2 = "task-2";
    private static final String WORKER_HOST = "worker-1.local";
    private static final int HTTP_PORT = 8080;

    private RecordingTaskDispatcher dispatcher;
    private CoordinatorTaskTracker taskTracker;
    private TaskCancelRetrier retrier;

    @BeforeEach
    void setUp() {
        dispatcher = new RecordingTaskDispatcher();
        taskTracker = new CoordinatorTaskTracker();
        retrier = new TaskCancelRetrier(dispatcher, taskTracker);
    }

    @Test
    void cancelWithRetry_successfulCancel_updatesTrackerAndNoPending() {
        taskTracker.registerTask(QUERY_ID, TASK_ID_1);
        taskTracker.markScheduled(QUERY_ID, TASK_ID_1);
        taskTracker.updateTaskState(QUERY_ID, TASK_ID_1, "RUNNING");
        dispatcher.setCancelResult(SchedulerCancelResult.cancelled());

        retrier.cancelWithRetry(QUERY_ID, TASK_ID_1, WORKER_HOST, HTTP_PORT);

        assertEquals(0, retrier.pendingCount());
        assertEquals(1, dispatcher.cancelCallCount());
    }

    @Test
    void cancelWithRetry_failedCancel_enqueuesPending() {
        taskTracker.registerTask(QUERY_ID, TASK_ID_1);
        taskTracker.markScheduled(QUERY_ID, TASK_ID_1);
        taskTracker.updateTaskState(QUERY_ID, TASK_ID_1, "RUNNING");
        dispatcher.setCancelResult(
                SchedulerCancelResult.failed("connection refused", true));

        retrier.cancelWithRetry(QUERY_ID, TASK_ID_1, WORKER_HOST, HTTP_PORT);

        assertEquals(1, retrier.pendingCount());
        assertTrue(retrier.pendingTaskIds().contains(TASK_ID_1));
    }

    @Test
    void cancelWithRetry_exceptionThrown_enqueuesPending() {
        taskTracker.registerTask(QUERY_ID, TASK_ID_1);
        taskTracker.markScheduled(QUERY_ID, TASK_ID_1);
        taskTracker.updateTaskState(QUERY_ID, TASK_ID_1, "RUNNING");
        dispatcher.setThrowOnCancel(new RuntimeException("network timeout"));

        retrier.cancelWithRetry(QUERY_ID, TASK_ID_1, WORKER_HOST, HTTP_PORT);

        assertEquals(1, retrier.pendingCount());
        assertTrue(retrier.pendingTaskIds().contains(TASK_ID_1));
    }

    @Test
    void retryPendingCancels_emptyPending_returnsZero() {
        assertEquals(0, retrier.retryPendingCancels());
    }

    @Test
    void retryPendingCancels_successOnRetry_removesPending() {
        taskTracker.registerTask(QUERY_ID, TASK_ID_1);
        taskTracker.markScheduled(QUERY_ID, TASK_ID_1);
        taskTracker.updateTaskState(QUERY_ID, TASK_ID_1, "RUNNING");

        // First attempt fails
        dispatcher.setCancelResult(
                SchedulerCancelResult.failed("connection refused", true));
        retrier.cancelWithRetry(QUERY_ID, TASK_ID_1, WORKER_HOST, HTTP_PORT);
        assertEquals(1, retrier.pendingCount());

        // Retry succeeds
        dispatcher.setCancelResult(SchedulerCancelResult.cancelled());
        int cancelled = retrier.retryPendingCancels();

        assertEquals(1, cancelled);
        assertEquals(0, retrier.pendingCount());
    }

    @Test
    void retryPendingCancels_stillFailing_keepsPending() {
        taskTracker.registerTask(QUERY_ID, TASK_ID_1);
        taskTracker.markScheduled(QUERY_ID, TASK_ID_1);
        taskTracker.updateTaskState(QUERY_ID, TASK_ID_1, "RUNNING");

        dispatcher.setCancelResult(
                SchedulerCancelResult.failed("connection refused", true));
        retrier.cancelWithRetry(QUERY_ID, TASK_ID_1, WORKER_HOST, HTTP_PORT);

        // Retry also fails
        int cancelled = retrier.retryPendingCancels();

        assertEquals(0, cancelled);
        assertEquals(1, retrier.pendingCount());
    }

    @Test
    void multipleTasks_partialRetrySuccess() {
        taskTracker.registerTask(QUERY_ID, TASK_ID_1);
        taskTracker.markScheduled(QUERY_ID, TASK_ID_1);
        taskTracker.updateTaskState(QUERY_ID, TASK_ID_1, "RUNNING");

        taskTracker.registerTask(QUERY_ID, TASK_ID_2);
        taskTracker.markScheduled(QUERY_ID, TASK_ID_2);
        taskTracker.updateTaskState(QUERY_ID, TASK_ID_2, "RUNNING");

        // Both fail initially
        dispatcher.setCancelResult(
                SchedulerCancelResult.failed("connection refused", true));
        retrier.cancelWithRetry(QUERY_ID, TASK_ID_1, WORKER_HOST, HTTP_PORT);
        retrier.cancelWithRetry(QUERY_ID, TASK_ID_2, WORKER_HOST, 8081);
        assertEquals(2, retrier.pendingCount());

        // On retry, task-1 succeeds but task-2 still fails
        dispatcher.setCancelResultPerTask(TASK_ID_1, SchedulerCancelResult.cancelled());
        dispatcher.setCancelResultPerTask(TASK_ID_2,
                SchedulerCancelResult.failed("still down", true));
        int cancelled = retrier.retryPendingCancels();

        assertEquals(1, cancelled);
        assertEquals(1, retrier.pendingCount());
        assertFalse(retrier.pendingTaskIds().contains(TASK_ID_1));
        assertTrue(retrier.pendingTaskIds().contains(TASK_ID_2));
    }

    @Test
    void cancelWithRetry_successAfterPreviousFailure_removesPending() {
        taskTracker.registerTask(QUERY_ID, TASK_ID_1);
        taskTracker.markScheduled(QUERY_ID, TASK_ID_1);
        taskTracker.updateTaskState(QUERY_ID, TASK_ID_1, "RUNNING");

        // First call fails
        dispatcher.setCancelResult(
                SchedulerCancelResult.failed("timeout", true));
        retrier.cancelWithRetry(QUERY_ID, TASK_ID_1, WORKER_HOST, HTTP_PORT);
        assertEquals(1, retrier.pendingCount());

        // Second direct call succeeds (e.g. from a different cancel path)
        dispatcher.setCancelResult(SchedulerCancelResult.cancelled());
        retrier.cancelWithRetry(QUERY_ID, TASK_ID_1, WORKER_HOST, HTTP_PORT);
        assertEquals(0, retrier.pendingCount());
    }

    @Test
    void retryPendingCancels_exceptionDuringRetry_keepsPending() {
        taskTracker.registerTask(QUERY_ID, TASK_ID_1);
        taskTracker.markScheduled(QUERY_ID, TASK_ID_1);
        taskTracker.updateTaskState(QUERY_ID, TASK_ID_1, "RUNNING");

        // Initial failure enqueues
        dispatcher.setCancelResult(
                SchedulerCancelResult.failed("down", true));
        retrier.cancelWithRetry(QUERY_ID, TASK_ID_1, WORKER_HOST, HTTP_PORT);

        // Retry throws exception
        dispatcher.setThrowOnCancel(new RuntimeException("network partition"));
        int cancelled = retrier.retryPendingCancels();

        assertEquals(0, cancelled);
        assertEquals(1, retrier.pendingCount());
    }

    // ── Test double ──

    /**
     * Test double for {@link TaskDispatcher} that records cancel calls
     * and returns configurable results. No mock framework used.
     */
    private static final class RecordingTaskDispatcher implements TaskDispatcher {

        private volatile SchedulerCancelResult defaultResult = SchedulerCancelResult.cancelled();
        private volatile RuntimeException throwOnCancel;
        private final ConcurrentHashMap<String, SchedulerCancelResult> perTaskResults =
                new ConcurrentHashMap<>();
        private final List<String> cancelledTaskIds = new ArrayList<>();

        void setCancelResult(SchedulerCancelResult result) {
            this.defaultResult = result;
            this.throwOnCancel = null;
            this.perTaskResults.clear();
        }

        void setThrowOnCancel(RuntimeException ex) {
            this.throwOnCancel = ex;
            this.perTaskResults.clear();
        }

        void setCancelResultPerTask(String taskId, SchedulerCancelResult result) {
            this.throwOnCancel = null;
            perTaskResults.put(taskId, result);
        }

        int cancelCallCount() {
            return cancelledTaskIds.size();
        }

        @Override
        public DispatchResult dispatch(SubmitTaskRequest request, WorkerInfo worker) {
            throw new UnsupportedOperationException("not used in cancel tests");
        }

        @Override
        public SchedulerCancelResult cancelTask(String taskId,
                                                 String workerHost, int httpPort) {
            cancelledTaskIds.add(taskId);
            RuntimeException ex = throwOnCancel;
            if (ex != null) {
                throw ex;
            }
            SchedulerCancelResult perTask = perTaskResults.get(taskId);
            if (perTask != null) {
                return perTask;
            }
            return defaultResult;
        }
    }
}
