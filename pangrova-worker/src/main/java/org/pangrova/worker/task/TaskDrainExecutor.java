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

import org.pangrova.worker.enums.TaskState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Stops new registrations and waits for the exact tracked tasks to finish before forcing cancellation.
 */
final class TaskDrainExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(TaskDrainExecutor.class);
    private static final int DEFAULT_DRAIN_TIMEOUT_SECONDS = 30;

    private final WorkerTaskRegistry tasks;
    private final TaskLifecycleManager lifecycleManager;
    private final Consumer<String> cancelTask;

    private final Object terminalChanges = new Object();
    private volatile boolean draining;
    private volatile ScheduledExecutorService cleanupExecutor;

    TaskDrainExecutor(
            WorkerTaskRegistry tasks,
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
        tasks.closeAdmissions();
        draining = true;
    }

    void signalTerminalTransition() {
        synchronized (terminalChanges) {
            terminalChanges.notifyAll();
        }
    }

    void drainAndWait() {
        drainAndWait(DEFAULT_DRAIN_TIMEOUT_SECONDS);
    }

    void drainAndWait(int timeoutSeconds) {
        beginDraining();
        Set<String> pending = new HashSet<>();
        for (TaskInfo info : tasks.values()) {
            if (!isTerminal(info.getState())) pending.add(info.taskId());
        }
        LOG.info("event=task_manager_draining count={} timeoutSeconds={}", pending.size(), timeoutSeconds);
        if (pending.isEmpty()) return;
        awaitTerminalTasks(pending, timeoutSeconds);
        int forceCancelled = forceCancelNonTerminal();
        if (forceCancelled > 0) {
            LOG.warn("event=drain_force_cancelled count={}", forceCancelled);
        }
        LOG.info("event=task_manager_drained");
    }

    private static boolean isTerminal(TaskState state) {
        return state == TaskState.FINISHED
                || state == TaskState.FAILED
                || state == TaskState.CANCELLED;
    }

    private static boolean isActive(TaskState state) {
        return state == TaskState.RECEIVED
                || state == TaskState.INITIALIZING
                || state == TaskState.RUNNING
                || state == TaskState.FINISHING
                || state == TaskState.CANCELLING;
    }

    private void awaitTerminalTasks(Set<String> pending, int timeoutSeconds) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        synchronized (terminalChanges) {
            while (true) {
                pending.removeIf(id -> {
                    TaskInfo info = tasks.get(id);
                    return info == null || isTerminal(info.getState());
                });
                if (pending.isEmpty()) return;
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    LOG.warn("event=drain_timeout remaining={}", pending.size());
                    return;
                }
                try {
                    TimeUnit.NANOSECONDS.timedWait(terminalChanges, remaining);
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    LOG.warn("event=drain_interrupted");
                    return;
                }
            }
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
        for (var entry : tasks.entries()) {
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
        Throwable failure = null;
        if (lifecycleManager != null) {
            for (var entry : tasks.entries()) {
                if (isTerminal(entry.getValue().getState())) {
                    try {
                        lifecycleManager.releaseFinishedTaskExecutor(entry.getValue());
                        released++;
                    } catch (RuntimeException | Error rejected) {
                        if (failure == null) {
                            failure = rejected;
                        } else if (failure != rejected) {
                            failure.addSuppressed(rejected);
                        }
                    }
                }
            }
        }
        if (failure instanceof Error fatal) {
            throw fatal;
        }
        if (failure != null) {
            throw (RuntimeException) failure;
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
        return TaskCleanupScheduler.cleanupTerminalTasks(
                tasks, retention, this::releaseTerminalTask);
    }

    void watchFinishedResult(TaskInfo info, org.pangrova.worker.execution.TaskExecutor taskExecutor,
                             Consumer<TaskInfo> removeIndex) {
        ScheduledExecutorService scheduler = cleanupExecutor;
        var submitted = info.request();
        long handle = info.executorHandle();
        if (scheduler == null || draining || submitted == null || handle == 0
                || !submitted.taskRequest().getExecutionOptions().getDirectResultOutput()) return;
        var nativeOwner = org.pangrova.worker.execution.NativeTaskExecutor.ownerOf(taskExecutor).orElseThrow(() ->
                new IllegalStateException("Direct result task has no Native result owner"));
        var registration = new TaskResultRelease(info, tasks, scheduler, task ->
                lifecycleManager.releaseResultTaskExecutor(task, removeIndex),
                () -> lifecycleManager.retryInitialResultStatus(info));
        if (!info.installResultRelease(registration)) return;
        if (tasks.get(info.taskId()) != info) {
            registration.cancel();
            return;
        }
        try {
            nativeOwner.watchResultDrain(handle, registration::drained);
        } finally {
            registration.expireAt(submitted.taskRequest().getDeadlineEpochMs());
        }
    }

    void releaseTerminalTask(TaskInfo info) {
        if (lifecycleManager != null) {
            lifecycleManager.releaseFinishedTaskExecutor(info);
        } else if (info.executorHandle() != 0
                || info.allocatedMemory() != 0
                || info.allocatedBufferMemory() != 0
                || info.taskMemoryPool() != null) {
            throw new IllegalStateException(
                    "Terminal task owns resources without a release authority");
        }
    }

    void startPeriodicCleanup(
            ScheduledExecutorService executor,
            Duration retention,
            Duration interval,
            Consumer<TaskInfo> removeIndex) {
        cleanupExecutor = executor;
        TaskCleanupScheduler.startPeriodicCleanup(
                executor,
                tasks,
                retention,
                interval,
                info -> {
                    releaseTerminalTask(info);
                    removeIndex.accept(info);
                });
    }
}
