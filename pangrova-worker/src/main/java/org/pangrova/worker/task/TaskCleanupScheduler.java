package org.pangrova.worker.task;

import org.pangrova.worker.enums.TaskState;

import org.pangrova.common.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Schedules and executes periodic cleanup of terminal tasks from a task registry.
 *
 * <p>Removes tasks in terminal state (FINISHED, FAILED, CANCELLED) that have been
 * in that state longer than a configurable retention duration, preventing unbounded
 * memory growth in long-running worker processes.</p>
 *
 * <p>Concurrency: thread-safe. Cleanup iterates over the weakly consistent registry view
 * and removes entries using compare-and-remove only after resource release.</p>
 */
@ThreadSafe
final class TaskCleanupScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(TaskCleanupScheduler.class);

    private TaskCleanupScheduler() {}

    /**
     * Removes tasks in terminal state (FINISHED, FAILED, CANCELLED) that have been
     * in that state longer than the specified retention duration.
     *
     * @param tasks     the task registry to clean
     * @param retention minimum age of terminal tasks before removal
     * @param resourceRelease callback that joins resources before removing their index
     * @return number of tasks removed
     */
    public static int cleanupTerminalTasks(
            WorkerTaskRegistry tasks,
            Duration retention,
            java.util.function.Consumer<TaskInfo> resourceRelease) {
        return cleanupTerminalTasks(tasks, retention, resourceRelease, false);
    }

    private static int cleanupTerminalTasks(
            WorkerTaskRegistry tasks,
            Duration retention,
            java.util.function.Consumer<TaskInfo> resourceRelease,
            boolean periodic) {
        Instant cutoff = Instant.now().minus(retention);
        int removed = 0;
        RuntimeException failures = null;
        for (Map.Entry<String, TaskInfo> entry : tasks.entries()) {
            TaskInfo info = entry.getValue();
            if (tasks.get(entry.getKey()) != info) continue;
            try {
                // This action exists only after a real release failed. It
                // carries the original FSM continuation, never inferred success.
                if (cleanupTrackedTask(entry, tasks, cutoff, resourceRelease, periodic)) removed++;
            } catch (RuntimeException failure) {
                if (failures == null) failures = new IllegalStateException("Task resource cleanup sweep failed");
                failures.addSuppressed(new IllegalStateException("Task cleanup failed: " + info.taskId(), failure));
            }
        }
        if (removed > 0) LOG.info("event=terminal_tasks_cleaned count={}", removed);
        if (failures != null) throw failures;
        return removed;
    }

    private static boolean cleanupTrackedTask(Map.Entry<String, TaskInfo> entry,
                                              WorkerTaskRegistry tasks, Instant cutoff,
                                              java.util.function.Consumer<TaskInfo> resourceRelease,
                                              boolean periodic) {
        TaskInfo info = entry.getValue();
        if (tasks.get(entry.getKey()) != info) return false;
        if (periodic && info.scheduleRegisteredResultCleanup(cutoff)) {
            return false;
        }
        info.retryCleanup();
        return cleanupTask(info, tasks, cutoff, resourceRelease);
    }

    private static boolean cleanupTask(TaskInfo info,
                                       WorkerTaskRegistry tasks, Instant cutoff,
                                       java.util.function.Consumer<TaskInfo> resourceRelease) {
        TaskState state = info.getState();
        if ((state == TaskState.FINISHED
                || state == TaskState.FAILED
                || state == TaskState.CANCELLED)) {
            if (info.resourcesReleased() || terminalRetentionElapsed(info, cutoff) || resultDeadlineExpired(info)) {
                // Retain the exact task and its grant if the actual release fails.
                resourceRelease.accept(info);
                return tasks.remove(info);
            }
        }
        return false;
    }

    static boolean terminalRetentionElapsed(TaskInfo info, Instant cutoff) {
        // The retention clock starts at terminal transition, even for long-running tasks.
        Instant referenceTime = info.terminalTime();
        return (referenceTime == null ? info.startTime() : referenceTime).isBefore(cutoff);
    }

    private static boolean resultDeadlineExpired(TaskInfo info) {
        var submitted = info.request();
        return submitted != null
                && submitted.taskRequest().getExecutionOptions().getDirectResultOutput()
                && submitted.taskRequest().getDeadlineEpochMs() <= System.currentTimeMillis();
    }

    /**
     * Starts periodic cleanup of terminal tasks using the provided executor.
     *
     * @param executor                scheduled executor for periodic cleanup
     * @param tasks                   the task registry to clean
     * @param retention               minimum age before terminal task removal
     * @param interval                interval between cleanup runs
     * @param resourceRelease callback that joins resources before removing their index
     */
    public static void startPeriodicCleanup(
            ScheduledExecutorService executor,
            WorkerTaskRegistry tasks,
            Duration retention,
            Duration interval,
            java.util.function.Consumer<TaskInfo> resourceRelease) {
        executor.scheduleAtFixedRate(
                () -> {
                    try {
                        cleanupTerminalTasks(tasks, retention, resourceRelease, true);
                    } catch (Exception e) {
                        LOG.warn("event=terminal_task_cleanup_error", e);
                    }
                },
                interval.toSeconds(), interval.toSeconds(), TimeUnit.SECONDS);
        LOG.info("event=terminal_task_cleanup_started retentionSeconds={} intervalSeconds={}",
                retention.toSeconds(), interval.toSeconds());
    }
}
