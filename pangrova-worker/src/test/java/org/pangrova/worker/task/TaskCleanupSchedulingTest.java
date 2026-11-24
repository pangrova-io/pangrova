package org.pangrova.worker.task;

import org.pangrova.worker.enums.TaskState;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Scheduling/ownership components only; predicates do not represent Native drain evidence. */
class TaskCleanupSchedulingTest {
    @Test
    void resultDrainReleasesExactOwnerAndCancelsItsDeadline() throws Exception {
        var tasks = new WorkerTaskRegistry(2);
        var task = finishedTask("consumed");
        tasks.register(task);
        var timer = new java.util.concurrent.ScheduledThreadPoolExecutor(1);
        timer.setRemoveOnCancelPolicy(true);
        var called = new CountDownLatch(1);
        try {
            var registration = new TaskResultRelease(task, tasks, timer, actual -> {
                assertThat(actual).isSameAs(task);
                called.countDown();
                return java.util.concurrent.CompletableFuture.completedFuture(null);
            }, () -> { });
            assertThat(task.installResultRelease(registration)).isTrue();
            registration.expireAt(System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1));
            assertThat(tasks.get(task.taskId())).isSameAs(task);
            registration.drained();
            registration.drained();
            assertThat(called.await(2, TimeUnit.SECONDS)).isTrue();
            timer.submit(() -> { }).get(2, TimeUnit.SECONDS);
            assertThat(tasks.values()).isEmpty();
            assertThat(timer.getQueue()).isEmpty();
        } finally {
            timer.shutdownNow();
            assertThat(timer.awaitTermination(2, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void anUnclaimedResultStillHasAnExactDeadlineWithoutScanning() throws Exception {
        var tasks = new WorkerTaskRegistry(2);
        var task = finishedTask("expires");
        tasks.register(task);
        var timer = new java.util.concurrent.ScheduledThreadPoolExecutor(1);
        timer.setRemoveOnCancelPolicy(true);
        var called = new CountDownLatch(1);
        try {
            var registration = new TaskResultRelease(task, tasks, timer, actual -> {
                assertThat(actual).isSameAs(task);
                called.countDown();
                return java.util.concurrent.CompletableFuture.completedFuture(null);
            }, () -> { });
            assertThat(task.installResultRelease(registration)).isTrue();
            registration.expireAt(System.currentTimeMillis());
            assertThat(called.await(2, TimeUnit.SECONDS)).isTrue();
            timer.submit(() -> { }).get(2, TimeUnit.SECONDS);
            assertThat(tasks.values()).isEmpty();
        } finally {
            timer.shutdownNow();
            assertThat(timer.awaitTermination(2, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void retentionEvictsOnlyTerminalOwnerAndIsIdempotent() {
        var tasks = new WorkerTaskRegistry(256);
        var terminal = finishedTask("terminal");
        var running = new TaskInfo("running", "other-query", null, Instant.now().minusSeconds(3600));
        running.compareAndSetState(TaskState.RECEIVED, TaskState.RUNNING);
        tasks.register(terminal);
        tasks.register(running);
        var released = new java.util.ArrayList<TaskInfo>();
        assertThat(TaskCleanupScheduler.cleanupTerminalTasks(tasks, Duration.ZERO, released::add))
                .isEqualTo(1);
        assertThat(released).containsExactly(terminal);
        assertThat(tasks.values()).containsExactly(running);
        assertThat(TaskCleanupScheduler.cleanupTerminalTasks(tasks, Duration.ZERO, released::add))
                .isZero();
        assertThat(released).containsExactly(terminal);
    }

    @Test
    void failedReleaseRemainsOwnedWhileOtherTasksCanBeCleaned() {
        var tasks = new WorkerTaskRegistry(256);
        var failed = finishedTask("failed-release");
        var other = finishedTask("other-release");
        tasks.register(failed);
        tasks.register(other);
        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                TaskCleanupScheduler.cleanupTerminalTasks(tasks, Duration.ZERO, task -> {
                    if (task == failed) throw new IllegalStateException("owner has not drained");
                })).isInstanceOf(IllegalStateException.class)
                .hasMessage("Task resource cleanup sweep failed");
        assertThat(tasks.values()).containsExactly(failed);
        assertThat(TaskCleanupScheduler.cleanupTerminalTasks(tasks, Duration.ZERO, task -> {}))
                .isEqualTo(1);
        assertThat(tasks.values()).isEmpty();
    }

    @Test
    void concurrentReplacementIsNeverRemovedWithRetiredAttempt() {
        var tasks = new WorkerTaskRegistry(256);
        var old = finishedTask("attempt-key");
        var replacement = new TaskInfo("attempt-key", "other-query", null, Instant.now());
        tasks.register(old);
        assertThat(TaskCleanupScheduler.cleanupTerminalTasks(tasks, Duration.ZERO,
                task -> {
                    assertThat(tasks.remove(task)).isTrue();
                    tasks.register(replacement);
                })).isZero();
        assertThat(tasks.get(old.taskId())).isSameAs(replacement);
    }

    @Test
    void simultaneousAdmissionsRespectCapacityAndFailedRollbacksRemainReachable() throws Exception {
        var tasks = new WorkerTaskRegistry(1);
        var ready = new CountDownLatch(2);
        var start = new CountDownLatch(1);
        var pool = Executors.newFixedThreadPool(2);
        try {
            java.util.concurrent.Callable<Boolean> submit = () -> {
                var info = finishedTask(tasks.nextId("query"));
                tasks.requireCapacity();
                ready.countDown();
                assertThat(start.await(5, TimeUnit.SECONDS)).isTrue();
                try {
                    tasks.register(info);
                    return true;
                } catch (org.pangrova.worker.exception.TaskAdmissionException rejected) {
                    assertThat(rejected.reason()).isEqualTo(
                            org.pangrova.worker.enums.TaskAdmissionReason.CAPACITY);
                    return false;
                }
            };
            var first = pool.submit(submit);
            var second = pool.submit(submit);
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            assertThat(java.util.List.of(first.get(5, TimeUnit.SECONDS), second.get(5, TimeUnit.SECONDS)))
                    .containsExactlyInAnyOrder(true, false);
            assertThat(tasks.values()).hasSize(1);

            var failed = finishedTask(tasks.nextId("query"));
            tasks.retainForCleanup(failed);
            tasks.retainForCleanup(failed);
            assertThat(tasks.get(failed.taskId())).isSameAs(failed);
            assertThat(tasks.values()).hasSize(2);
            assertThat(tasks.remove(failed)).isTrue();
        } finally {
            start.countDown();
            pool.shutdownNow();
            assertThat(pool.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void drainRechecksTheExactTasksAndRejectsPublicationAfterItsSnapshot() throws Exception {
        var tasks = new WorkerTaskRegistry(2);
        var first = new TaskInfo("first", "query", null, Instant.now());
        var second = new TaskInfo("second", "query", null, Instant.now());
        tasks.register(first);
        tasks.register(second);
        var forced = new java.util.concurrent.ConcurrentLinkedQueue<String>();
        var drain = new TaskDrainExecutor(tasks, null, forced::add);
        var pool = Executors.newSingleThreadExecutor();
        try {
            var joined = pool.submit(() -> drain.drainAndWait(5));
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2)).until(drain::isDraining);
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> tasks.register(finishedTask("late")))
                    .isInstanceOf(org.pangrova.worker.exception.TaskAdmissionException.class);
            assertThat(first.compareAndSetState(TaskState.RECEIVED, TaskState.CANCELLED)).isTrue();
            drain.signalTerminalTransition();
            drain.signalTerminalTransition();
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> joined.get(100, TimeUnit.MILLISECONDS))
                    .isInstanceOf(java.util.concurrent.TimeoutException.class);
            assertThat(second.compareAndSetState(TaskState.RECEIVED, TaskState.CANCELLED)).isTrue();
            drain.signalTerminalTransition();
            joined.get(2, TimeUnit.SECONDS);
            assertThat(forced).isEmpty();
        } finally {
            pool.shutdownNow();
            assertThat(pool.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static TaskInfo finishedTask(String id) {
        var info = new TaskInfo(id, "query", null, Instant.now());
        assertThat(info.compareAndSetState(TaskState.RECEIVED, TaskState.RUNNING)).isTrue();
        assertThat(info.compareAndSetState(TaskState.RUNNING, TaskState.FINISHED)).isTrue();
        return info;
    }
}
