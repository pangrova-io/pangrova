package org.pangrova.worker.task;

import org.pangrova.worker.enums.TaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Cancels the tracked attempt and retires its executor through the same lifecycle as completion. */
final class WorkerTaskCancellation {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerTaskCancellation.class);
    private final WorkerTaskRegistry tasks;
    private final TaskLifecycleManager lifecycle;
    private final TaskLifecycleManager.LifecycleCallback callback;

    WorkerTaskCancellation(WorkerTaskRegistry tasks, TaskLifecycleManager lifecycle,
                           TaskLifecycleManager.LifecycleCallback callback) {
        this.tasks = tasks;
        this.lifecycle = lifecycle;
        this.callback = callback;
    }

    TaskCancelResult cancelTask(String taskId) {
        TaskInfo info = tasks.get(taskId);
        if (info == null) {
            return TaskCancelResult.notFound(taskId);
        }
        TaskState current = info.getState();
        if (lifecycle == null) {
            if (!callback.transition(taskId, TaskState.CANCELLING)) {
                return TaskCancelResult.notCancellable(taskId, current.name());
            }
            callback.transition(taskId, TaskState.CANCELLED);
            callback.clearCoordinatorIndex(info);
            return TaskCancelResult.cancelled();
        }
        if (current != TaskState.CANCELLING
                && !callback.transition(taskId, TaskState.CANCELLING)) {
            return TaskCancelResult.notCancellable(taskId, current.name());
        }
        lifecycle.cancelNativeExecutor(info);
        // Terminal resource release does not reopen the exact admission identity.
        LOG.info("event=task_cancel_requested taskId={} queryId={}", taskId, info.queryId());
        return TaskCancelResult.cancelled();
    }

    void releaseTaskExecutor(String taskId) {
        TaskInfo info = tasks.get(taskId);
        if (info != null && lifecycle != null) {
            // Explicit retirement abandons an unclaimed result; completion already committed keeps
            // its state.
            if (info.getState() == TaskState.RUNNING
                    || info.getState() == TaskState.INITIALIZING) {
                cancelTask(taskId);
            }
            lifecycle.releaseRetiredTaskExecutor(info, callback);
        }
    }
}
