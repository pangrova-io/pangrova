/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.worker.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Handles drain, cleanup, and bulk cancel operations for the task registry.
 * Extracted from TaskManager to keep each class under the 500-line limit.
 */
final class TaskDrainExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(TaskDrainExecutor.class);
    private static final int DEFAULT_DRAIN_TIMEOUT_SECONDS = 30;

    private final ConcurrentHashMap<String, TaskInfo> tasks;
    private final TaskLifecycleManager lifecycleManager;
    private final Consumer<String> cancelTask;

    private volatile CountDownLatch drainLatch;
    private volatile boolean draining;

    TaskDrainExecutor(ConcurrentHashMap<String, TaskInfo> tasks,
                      TaskLifecycleManager lifecycleManager,
                      Consumer<String> cancelTask) {
        this.tasks = tasks;
        this.lifecycleManager = lifecycleManager;
        this.cancelTask = cancelTask;
    }

    boolean isDraining() {
        return draining;
    }

    void beginDraining() {
        draining = true;
    }

    void signalTerminalTransition() {
        var latch = drainLatch;
        if (latch != null) {
            latch.countDown();
        }
    }

    void drainAndWait() {
        drainAndWait(DEFAULT_DRAIN_TIMEOUT_SECONDS);
    }

    void drainAndWait(int timeoutSeconds) {
        beginDraining();
        List<String> nonTerminalTaskIds = collectNonTerminalTaskIds();
        int running = nonTerminalTaskIds.size();
        LOG.info("event=task_manager_draining count={} timeoutSeconds={}", running, timeoutSeconds);
        if (running == 0) {
            return;
        }
        var latch = new CountDownLatch(running);
        this.drainLatch = latch;
        releaseAlreadyTerminal(latch, nonTerminalTaskIds);
        awaitDrainLatch(latch, timeoutSeconds);
        int forceCancelled = forceCancelNonTerminal();
        if (forceCancelled > 0) {
            LOG.warn("event=drain_force_cancelled count={}", forceCancelled);
        }
        LOG.info("event=task_manager_drained");
    }

    private static boolean isTerminal(TaskManager.TaskState state) {
        return state == TaskManager.TaskState.FINISHED || state == TaskManager.TaskState.FAILED
                || state == TaskManager.TaskState.CANCELLED;
    }

    private static boolean isActive(TaskManager.TaskState state) {
        return state == TaskManager.TaskState.RECEIVED || state == TaskManager.TaskState.INITIALIZING
                || state == TaskManager.TaskState.RUNNING || state == TaskManager.TaskState.FINISHING
                || state == TaskManager.TaskState.CANCELLING;
    }

    private List<String> collectNonTerminalTaskIds() {
        List<String> ids = new ArrayList<>();
        for (TaskInfo info : tasks.values()) {
            if (!isTerminal(info.getState())) {
                ids.add(info.taskId());
            }
        }
        return ids;
    }

    private void releaseAlreadyTerminal(CountDownLatch latch, List<String> taskIds) {
        for (String taskId : taskIds) {
            TaskInfo info = tasks.get(taskId);
            if (info != null && isTerminal(info.getState())) {
                latch.countDown();
            }
        }
    }

    private void awaitDrainLatch(CountDownLatch latch, int timeoutSeconds) {
        try {
            if (!latch.await(timeoutSeconds, TimeUnit.SECONDS)) {
                LOG.warn("event=drain_timeout remaining={}", latch.getCount());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("event=drain_interrupted");
        } finally {
            this.drainLatch = null;
        }
    }

    private int forceCancelNonTerminal() {
        int forceCancelled = 0;
        for (TaskInfo info : tasks.values()) {
            if (!isTerminal(info.getState())) {
                cancelTask.accept(info.taskId());
                forceCancelled++;
            }
        }
        return forceCancelled;
    }

    int cancelAllRunningTasks() {
        int cancelled = 0;
        for (var entry : tasks.entrySet()) {
            if (isActive(entry.getValue().getState())) {
                cancelTask.accept(entry.getKey());
                cancelled++;
            }
        }
        int released = releaseTerminalExecutorsCount();
        cleanupTerminalTasks(Duration.ZERO);
        LOG.info("event=cancel_all_running_tasks cancelled={} released={}", cancelled, released);
        return cancelled + released;
    }

    private int releaseTerminalExecutorsCount() {
        int released = 0;
        if (lifecycleManager != null) {
            for (var entry : tasks.entrySet()) {
                if (isTerminal(entry.getValue().getState())) {
                    lifecycleManager.releaseFinishedTaskExecutor(entry.getValue());
                    released++;
                }
            }
        }
        return released;
    }

    int releaseTerminalTaskExecutors() {
        int released = releaseTerminalExecutorsCount();
        if (released > 0) {
            cleanupTerminalTasks(Duration.ZERO);
            LOG.debug("event=release_terminal_executors released={}", released);
        }
        return released;
    }

    int cleanupTerminalTasks(Duration retention) {
        return TaskCleanupScheduler.cleanupTerminalTasks(tasks, retention,
                this::releaseTerminalTask, this::resultResourcesDrained);
    }

    boolean resultResourcesDrained(TaskInfo info) {
        return lifecycleManager != null && lifecycleManager.resultResourcesDrained(info);
    }

    void releaseTerminalTask(TaskInfo info) {
        if (lifecycleManager != null) {
            lifecycleManager.releaseFinishedTaskExecutor(info);
        } else if (info.executorHandle() != 0 || info.allocatedMemory() != 0
                || info.allocatedBufferMemory() != 0 || info.taskMemoryPool() != null) {
            throw new IllegalStateException("Terminal task owns resources without a release authority");
        }
    }

    void startPeriodicCleanup(ScheduledExecutorService executor,
                              Duration retention, Duration interval,
                              Consumer<TaskInfo> removeIndex) {
        TaskCleanupScheduler.startPeriodicCleanup(executor, tasks, retention, interval, info -> {
            releaseTerminalTask(info);
            removeIndex.accept(info);
        }, this::resultResourcesDrained);
    }
}
