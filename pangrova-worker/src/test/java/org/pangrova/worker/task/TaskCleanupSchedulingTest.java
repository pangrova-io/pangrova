package org.pangrova.worker.task;

import org.pangrova.worker.enums.TaskState;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Scheduling/ownership components only; predicates do not represent Native drain evidence. */
class TaskCleanupSchedulingTest {
    @Test
    void incrementalProbeIsBoundedAndRetainsFailedReleaseUntilRetry() {
        var tasks = new WorkerTaskRegistry(256);
        int owners = TaskCleanupScheduler.MAX_CONSUMED_RESULT_PROBES + 1;
        for (int i = 0; i < owners; i++) tasks.register(finishedTask("owner-" + i));
        var attempted = new java.util.concurrent.atomic.AtomicInteger();
        var fail = new AtomicBoolean(true);
        var sweep = new TaskCleanupScheduler.ConsumedResultSweep(tasks, actual -> {
            attempted.incrementAndGet();
            if (fail.get()) throw new IllegalStateException("actual release rejected");
        }, actual -> true);

        sweep.run();
        assertThat(attempted).hasValue(TaskCleanupScheduler.MAX_CONSUMED_RESULT_PROBES);
        assertThat(tasks.values()).hasSize(owners);
        fail.set(false);
        sweep.run(); // Finish this walk rather than starving the last owner.
        assertThat(attempted).hasValue(owners);
        assertThat(tasks.values()).hasSize(owners - 1);
        sweep.run(); // Begin the next walk; retained failures are still owned and retryable.
        assertThat(tasks.values()).isEmpty();
        assertThat(attempted).hasValue(owners + owners - 1);
    }

    @Test
    void consumedResultIsReleasedWithoutWaitingForRetentionSweep() throws Exception {
        var tasks = new WorkerTaskRegistry(256);
        var info = finishedTask("consumed");
        tasks.register(info);
        var release = new CountDownLatch(1);
        var consumed = new AtomicBoolean();
        var probed = new CountDownLatch(1);
        var timer = Executors.newSingleThreadScheduledExecutor();
        try {
            TaskCleanupScheduler.startPeriodicCleanup(timer, tasks, Duration.ofHours(1), Duration.ofHours(1),
                    actual -> {
                        assertThat(actual).isSameAs(info);
                        assertThat(consumed).isTrue();
                        release.countDown();
                    }, actual -> {
                        assertThat(actual).isSameAs(info);
                        probed.countDown();
                        return consumed.get();
                    });
            assertThat(probed.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(tasks.get(info.taskId())).isSameAs(info);
            assertThat(release.getCount()).isEqualTo(1);
            consumed.set(true);
            assertThat(release.await(2, TimeUnit.SECONDS)).isTrue();
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
        var running = new TaskInfo("running", "other-query", null, null, Instant.now().minusSeconds(3600));
        running.compareAndSetState(TaskState.RECEIVED, TaskState.RUNNING);
        tasks.register(terminal);
        tasks.register(running);
        var released = new java.util.ArrayList<TaskInfo>();
        assertThat(TaskCleanupScheduler.cleanupTerminalTasks(tasks, Duration.ZERO, released::add, task -> false))
                .isEqualTo(1);
        assertThat(released).containsExactly(terminal);
        assertThat(tasks.values()).containsExactly(running);
        assertThat(TaskCleanupScheduler.cleanupTerminalTasks(tasks, Duration.ZERO, released::add, task -> false))
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
                }, task -> false)).isInstanceOf(IllegalStateException.class)
                .hasMessage("Task resource cleanup sweep failed");
        assertThat(tasks.values()).containsExactly(failed);
        assertThat(TaskCleanupScheduler.cleanupTerminalTasks(tasks, Duration.ZERO, task -> {}, task -> false))
                .isEqualTo(1);
        assertThat(tasks.values()).isEmpty();
    }

    @Test
    void concurrentReplacementIsNeverRemovedWithRetiredAttempt() {
        var tasks = new WorkerTaskRegistry(256);
        var old = finishedTask("attempt-key");
        var replacement = new TaskInfo("attempt-key", "other-query", null, null, Instant.now());
        tasks.register(old);
        assertThat(TaskCleanupScheduler.cleanupTerminalTasks(tasks, Duration.ZERO,
                task -> {
                    assertThat(tasks.remove(task)).isTrue();
                    tasks.register(replacement);
                }, task -> false)).isZero();
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
                            org.pangrova.worker.exception.TaskAdmissionException.Reason.CAPACITY);
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
        var first = new TaskInfo("first", "query", null, null, Instant.now());
        var second = new TaskInfo("second", "query", null, null, Instant.now());
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
        var info = new TaskInfo(id, "query", null, null, Instant.now());
        assertThat(info.compareAndSetState(TaskState.RECEIVED, TaskState.RUNNING)).isTrue();
        assertThat(info.compareAndSetState(TaskState.RUNNING, TaskState.FINISHED)).isTrue();
        return info;
    }
}
