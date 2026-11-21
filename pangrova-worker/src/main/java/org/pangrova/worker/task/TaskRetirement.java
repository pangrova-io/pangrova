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
            if (transitioned && info.getState() == org.pangrova.worker.enums.TaskState.FINISHED) {
                completionPending.set(true);
            }
            callback.clearCoordinatorIndex(info);
            collector.reportReleasedTerminal(info);
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
            if (current == org.pangrova.worker.enums.TaskState.FINISHED) {
                if (actual.getState() != TaskState.TASK_STATE_FINISHED) {
                    throw new IllegalStateException(
                            "Committed success conflicts with actual retired execution");
                }
                return transitioned;
            }
            if (current == org.pangrova.worker.enums.TaskState.FAILED
                    || current == org.pangrova.worker.enums.TaskState.CANCELLED) {
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

    private static org.pangrova.worker.enums.TaskState next(
            org.pangrova.worker.enums.TaskState current, TaskState actual) {
        if (current == org.pangrova.worker.enums.TaskState.RECEIVED) {
            return org.pangrova.worker.enums.TaskState.INITIALIZING;
        }
        if (current == org.pangrova.worker.enums.TaskState.CANCELLING) {
            return org.pangrova.worker.enums.TaskState.CANCELLED;
        }
        return switch (actual) {
            case TASK_STATE_FINISHED ->
                    current == org.pangrova.worker.enums.TaskState.INITIALIZING
                            ? org.pangrova.worker.enums.TaskState.RUNNING
                            : current == org.pangrova.worker.enums.TaskState.RUNNING
                                    ? org.pangrova.worker.enums.TaskState.FINISHING
                                    : org.pangrova.worker.enums.TaskState.FINISHED;
            case TASK_STATE_FAILED -> org.pangrova.worker.enums.TaskState.FAILED;
            case TASK_STATE_CANCELLED -> {
                if (current == org.pangrova.worker.enums.TaskState.FINISHING) {
                    throw new IllegalStateException(
                            "Actual cancellation conflicts with committed completion");
                }
                yield org.pangrova.worker.enums.TaskState.CANCELLING;
            }
            default ->
                    throw new IllegalStateException(
                            "Retirement receipt is not terminal: " + actual);
        };
    }
}
