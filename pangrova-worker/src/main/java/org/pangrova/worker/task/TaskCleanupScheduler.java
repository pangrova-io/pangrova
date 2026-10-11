package org.pangrova.worker.task;

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
 * <p>Removes tasks in terminal state (FINISHED, FAILED, CANCELLED) that have been in that state
 * longer than a configurable retention duration, preventing unbounded memory growth in long-running
 * worker processes.
 *
 * <p>Concurrency: thread-safe. Cleanup iterates over a ConcurrentHashMap snapshot and removes
 * entries using compare-and-remove only after resource release.
 */
@ThreadSafe
public final class TaskCleanupScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(TaskCleanupScheduler.class);

    private TaskCleanupScheduler() {}

    /**
     * Removes tasks in terminal state (FINISHED, FAILED, CANCELLED) that have been in that state
     * longer than the specified retention duration.
     *
     * @param tasks the task registry to clean
     * @param retention minimum age of terminal tasks before removal
     * @param resourceRelease callback that joins resources before removing their index
     * @param consumedResultProbe actual Native result-owner drain evidence
     * @return number of tasks removed
     */
    public static int cleanupTerminalTasks(
            Map<String, TaskInfo> tasks,
            Duration retention,
            java.util.function.Consumer<TaskInfo> resourceRelease,
            java.util.function.Predicate<TaskInfo> consumedResultProbe) {
        Instant cutoff = Instant.now().minus(retention);
        int removed = 0;
        RuntimeException failures = null;
        for (Map.Entry<String, TaskInfo> entry : tasks.entrySet()) {
            TaskInfo info = entry.getValue();
            if (tasks.get(entry.getKey()) != info) {
                continue;
            }
            try {
                // This action exists only after a real release failed. It
                // carries the original FSM continuation, never inferred success.
                info.retryCleanup();
                if (cleanupTask(entry, tasks, cutoff, resourceRelease, consumedResultProbe)) {
                    removed++;
                }
            } catch (RuntimeException failure) {
                if (failures == null) {
                    failures = new IllegalStateException("Task resource cleanup sweep failed");
                }
                failures.addSuppressed(
                        new IllegalStateException(
                                "Task cleanup failed: " + info.taskId(), failure));
            }
        }
        if (removed > 0) {
            LOG.info("event=terminal_tasks_cleaned count={}", removed);
        }
        if (failures != null) {
            throw failures;
        }
        return removed;
    }

    private static boolean cleanupTask(
            Map.Entry<String, TaskInfo> entry,
            Map<String, TaskInfo> tasks,
            Instant cutoff,
            java.util.function.Consumer<TaskInfo> resourceRelease,
            java.util.function.Predicate<TaskInfo> consumedResultProbe) {
        TaskInfo info = entry.getValue();
        TaskManager.TaskState state = info.getState();
        if ((state == TaskManager.TaskState.FINISHED
                || state == TaskManager.TaskState.FAILED
                || state == TaskManager.TaskState.CANCELLED)) {
            // Prefer terminalTime() over startTime() so that long-running
            // tasks aren't evicted immediately after they terminate,
            // leaving result fetchers with no buffered data.
            Instant referenceTime = info.terminalTime();
            if (referenceTime == null) {
                referenceTime = info.startTime();
            }
            if (info.resourcesReleased()
                    || referenceTime.isBefore(cutoff)
                    || resultDeadlineExpired(info)
                    || consumedResultProbe.test(info)) {
                // Retain the exact task and its grant if the actual release fails.
                resourceRelease.accept(info);
                return tasks.remove(entry.getKey(), info);
            }
        }
        return false;
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
     * @param executor scheduled executor for periodic cleanup
     * @param tasks the task registry to clean
     * @param retention minimum age before terminal task removal
     * @param interval interval between cleanup runs
     * @param resourceRelease callback that joins resources before removing their index
     * @param consumedResultProbe actual Native result-owner drain evidence
     */
    public static void startPeriodicCleanup(
            ScheduledExecutorService executor,
            Map<String, TaskInfo> tasks,
            Duration retention,
            Duration interval,
            java.util.function.Consumer<TaskInfo> resourceRelease,
            java.util.function.Predicate<TaskInfo> consumedResultProbe) {
        executor.scheduleAtFixedRate(
                () -> {
                    try {
                        cleanupTerminalTasks(
                                tasks, retention, resourceRelease, consumedResultProbe);
                    } catch (Exception e) {
                        LOG.warn("event=terminal_task_cleanup_error", e);
                    }
                },
                interval.toSeconds(),
                interval.toSeconds(),
                TimeUnit.SECONDS);
        LOG.info(
                "event=terminal_task_cleanup_started retentionSeconds={} intervalSeconds={}",
                retention.toSeconds(),
                interval.toSeconds());
    }
}
