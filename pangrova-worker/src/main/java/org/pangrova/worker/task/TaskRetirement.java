package org.pangrova.worker.task;

import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskState;

/**
 * Publishes retirement only after the actual owner has joined and supplied its terminal receipt.
 */
final class TaskRetirement {
    private TaskRetirement() {}

    static void release(
            TaskInfo info,
            TaskLifecycleManager.LifecycleCallback callback,
            TaskStatusCollector collector,
            Runnable releaseResources) {
        release(
                info,
                callback,
                collector,
                releaseResources,
                new java.util.concurrent.atomic.AtomicBoolean());
    }

    private static void release(
            TaskInfo info,
            TaskLifecycleManager.LifecycleCallback callback,
            TaskStatusCollector collector,
            Runnable releaseResources,
            java.util.concurrent.atomic.AtomicBoolean completionPending) {
        try {
            releaseResources.run();
            TaskReport actual =
                    collector
                            .executionDrainReceipt(info)
                            .orElseThrow(
                                    () ->
                                            new IllegalStateException(
                                                    "Task retirement lacks actual execution drain evidence"));
            boolean transitioned = reconcile(info, callback, actual);
            if (transitioned && info.getState() == TaskManager.TaskState.FINISHED) {
                completionPending.set(true);
            }
            collector.reportReleasedTerminal(info);
            callback.clearCoordinatorIndex(info);
            if (completionPending.get()) {
                callback.notifyCompletion(info.queryId());
                completionPending.set(false);
            }
        } catch (RuntimeException | Error failure) {
            info.retainCleanupRetry(
                    () -> release(info, callback, collector, releaseResources, completionPending));
            throw failure;
        }
    }

    private static boolean reconcile(
            TaskInfo info, TaskLifecycleManager.LifecycleCallback callback, TaskReport actual) {
        boolean transitioned = false;
        while (true) {
            var current = info.getState();
            if (current == TaskManager.TaskState.FINISHED) {
                if (actual.getState() != TaskState.TASK_STATE_FINISHED) {
                    throw new IllegalStateException(
                            "Committed success conflicts with actual retired execution");
                }
                return transitioned;
            }
            if (current == TaskManager.TaskState.FAILED
                    || current == TaskManager.TaskState.CANCELLED) {
                return transitioned;
            }
            var target = next(current, actual.getState());
            if (callback.transition(info.taskId(), target)) {
                transitioned = true;
            } else if (info.getState() == current) {
                throw new IllegalStateException(
                        "Retired task cannot reach its actual terminal state: " + current);
            }
        }
    }

    private static TaskManager.TaskState next(TaskManager.TaskState current, TaskState actual) {
        if (current == TaskManager.TaskState.RECEIVED) {
            return TaskManager.TaskState.INITIALIZING;
        }
        if (current == TaskManager.TaskState.CANCELLING) {
            return TaskManager.TaskState.CANCELLED;
        }
        return switch (actual) {
            case TASK_STATE_FINISHED ->
                    current == TaskManager.TaskState.INITIALIZING
                            ? TaskManager.TaskState.RUNNING
                            : current == TaskManager.TaskState.RUNNING
                                    ? TaskManager.TaskState.FINISHING
                                    : TaskManager.TaskState.FINISHED;
            case TASK_STATE_FAILED -> TaskManager.TaskState.FAILED;
            case TASK_STATE_CANCELLED -> {
                if (current == TaskManager.TaskState.FINISHING) {
                    throw new IllegalStateException(
                            "Actual cancellation conflicts with committed completion");
                }
                yield TaskManager.TaskState.CANCELLING;
            }
            default ->
                    throw new IllegalStateException(
                            "Retirement receipt is not terminal: " + actual);
        };
    }
}
