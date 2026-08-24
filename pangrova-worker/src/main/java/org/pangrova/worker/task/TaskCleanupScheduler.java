package org.pangrova.worker.task;

import org.pangrova.common.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
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
 * <p>Concurrency: thread-safe. Cleanup iterates over a ConcurrentHashMap snapshot
 * and removes entries atomically via iterator.</p>
 */
@ThreadSafe
public final class TaskCleanupScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(TaskCleanupScheduler.class);

    private TaskCleanupScheduler() {}

    /**
     * Removes tasks in terminal state (FINISHED, FAILED, CANCELLED) that have been
     * in that state longer than the specified retention duration.
     *
     * @param tasks     the task registry to clean
     * @param retention minimum age of terminal tasks before removal
     * @param coordinatorIndexRemover callback to remove coordinator index entries
     * @return number of tasks removed
     */
    public static int cleanupTerminalTasks(
            Map<String, TaskInfo> tasks,
            Duration retention,
            java.util.function.Consumer<TaskInfo> coordinatorIndexRemover) {
        Instant cutoff = Instant.now().minus(retention);
        int removed = 0;
        Iterator<Map.Entry<String, TaskInfo>> iterator = tasks.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, TaskInfo> entry = iterator.next();
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
                if (referenceTime.isBefore(cutoff)) {
                    iterator.remove();
                    coordinatorIndexRemover.accept(info);
                    removed++;
                }
            }
        }
        if (removed > 0) {
            LOG.info("event=terminal_tasks_cleaned count={}", removed);
        }
        return removed;
    }

    /**
     * Starts periodic cleanup of terminal tasks using the provided executor.
     *
     * @param executor                scheduled executor for periodic cleanup
     * @param tasks                   the task registry to clean
     * @param retention               minimum age before terminal task removal
     * @param interval                interval between cleanup runs
     * @param coordinatorIndexRemover callback to remove coordinator index entries
     */
    public static void startPeriodicCleanup(
            ScheduledExecutorService executor,
            Map<String, TaskInfo> tasks,
            Duration retention,
            Duration interval,
            java.util.function.Consumer<TaskInfo> coordinatorIndexRemover) {
        executor.scheduleAtFixedRate(
                () -> {
                    try {
                        cleanupTerminalTasks(tasks, retention, coordinatorIndexRemover);
                    } catch (Exception e) {
                        LOG.warn("event=terminal_task_cleanup_error error={}", e.getMessage());
                    }
                },
                interval.toSeconds(), interval.toSeconds(), TimeUnit.SECONDS);
        LOG.info("event=terminal_task_cleanup_started retentionSeconds={} intervalSeconds={}",
                retention.toSeconds(), interval.toSeconds());
    }
}
