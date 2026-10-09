package org.pangrova.worker.task;

import org.pangrova.spi.execution.TaskExecutor;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/** Quiesces actual task owners without treating failed cleanup as successful shutdown. */
final class TaskManagerShutdown {
    private TaskManagerShutdown() {}

    static void close(TaskManagerContext deps, TaskExecutor executor,
                      TaskLifecycleManager lifecycle, TaskLifecycleManager.LifecycleCallback callback,
                      Collection<TaskInfo> tasks) {
        if (deps == null) return;
        RuntimeException failure = attempt(() -> deps.completionMonitorExecutor().shutdownNow(),
                null, "stop completion monitors");
        for (var info : tasks) {
            failure = attempt(() -> cancelNative(executor, info, callback), failure, "cancel task=" + info.taskId());
        }
        failure = attempt(() -> closeOwner(deps.resultTransport()), failure, "result readers");
        if (deps.exchangeTransport() instanceof AutoCloseable exchange) {
            failure = attempt(() -> closeOwner(exchange), failure, "exchange readers");
        }
        for (var info : tasks) {
            failure = attempt(() -> lifecycle.releaseTaskForShutdown(info, callback), failure,
                    "task=" + info.taskId() + " query=" + info.queryId());
        }
        failure = attempt(() -> awaitMonitorExit(deps), failure, "completion monitors");
        if (failure != null) throw failure;
        // Reader shutdown and task release still need this executor's global
        // and task command handles on retry. Never retire their control plane
        // while a dependent owner or monitor has failed to drain.
        if (executor instanceof AutoCloseable nativeOwner) {
            failure = attempt(() -> closeOwner(nativeOwner), null, "native executor");
        }
        if (failure != null) throw failure;
    }

    private static void cancelNative(TaskExecutor executor, TaskInfo info,
                                     TaskLifecycleManager.LifecycleCallback callback) {
        if (info.getState() == TaskManager.TaskState.INITIALIZING
                || info.getState() == TaskManager.TaskState.RUNNING) {
            callback.transition(info.taskId(), TaskManager.TaskState.CANCELLING);
        }
        // A completed execution or the FINISHING commit point must never become cancellation.
        if (info.getState() != TaskManager.TaskState.CANCELLING) return;
        long handle = info.executorHandle();
        if (handle == 0) return;
        try {
            executor.cancelTask(handle);
        } catch (RuntimeException failure) {
            // A joined release can race the shutdown snapshot. Only the
            // authoritative successful handle retirement makes this harmless.
            if (info.executorHandle() != 0) throw failure;
        }
    }

    private static void closeOwner(AutoCloseable owner) {
        if (owner == null) return;
        try {
            owner.close();
        } catch (Exception failure) {
            throw new IllegalStateException("Actual shutdown owner did not close", failure);
        }
    }

    private static void awaitMonitorExit(TaskManagerContext deps) {
        try {
            if (!deps.completionMonitorExecutor().awaitTermination(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Task completion monitors still retain execution resources");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while joining task completion monitors", failure);
        }
    }

    private static RuntimeException attempt(Runnable operation, RuntimeException aggregate, String owner) {
        try {
            operation.run();
        } catch (RuntimeException | Error failure) {
            var observed = new IllegalStateException("Task-manager shutdown failed for " + owner, failure);
            if (aggregate == null) return observed;
            aggregate.addSuppressed(observed);
        }
        return aggregate;
    }
}
