package org.pangrova.worker.task;

import org.pangrova.worker.task.state.TaskState;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
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
        var tasks = new ConcurrentHashMap<String, TaskInfo>();
        int owners = TaskCleanupScheduler.MAX_CONSUMED_RESULT_PROBES + 1;
        for (int i = 0; i < owners; i++) tasks.put("owner-" + i, finishedTask("owner-" + i));
        var attempted = new java.util.concurrent.atomic.AtomicInteger();
        var fail = new AtomicBoolean(true);
        var sweep = new TaskCleanupScheduler.ConsumedResultSweep(tasks, actual -> {
            attempted.incrementAndGet();
            if (fail.get()) throw new IllegalStateException("actual release rejected");
        }, actual -> true);

        sweep.run();
        assertThat(attempted).hasValue(TaskCleanupScheduler.MAX_CONSUMED_RESULT_PROBES);
        assertThat(tasks).hasSize(owners);
        fail.set(false);
        sweep.run(); // Finish this walk rather than starving the last owner.
        assertThat(attempted).hasValue(owners);
        assertThat(tasks).hasSize(owners - 1);
        sweep.run(); // Begin the next walk; retained failures are still owned and retryable.
        assertThat(tasks).isEmpty();
        assertThat(attempted).hasValue(owners + owners - 1);
    }

    @Test
    void consumedResultIsReleasedWithoutWaitingForRetentionSweep() throws Exception {
        var tasks = new ConcurrentHashMap<String, TaskInfo>();
        var info = finishedTask("consumed");
        tasks.put(info.taskId(), info);
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
            assertThat(tasks).containsEntry(info.taskId(), info);
            assertThat(release.getCount()).isEqualTo(1);
            consumed.set(true);
            assertThat(release.await(2, TimeUnit.SECONDS)).isTrue();
            timer.submit(() -> { }).get(2, TimeUnit.SECONDS);
            assertThat(tasks).isEmpty();
        } finally {
            timer.shutdownNow();
            assertThat(timer.awaitTermination(2, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static TaskInfo finishedTask(String id) {
        var info = new TaskInfo(id, "query", null, null, Instant.now());
        assertThat(info.compareAndSetState(TaskState.RECEIVED, TaskState.RUNNING)).isTrue();
        assertThat(info.compareAndSetState(TaskState.RUNNING, TaskState.FINISHED)).isTrue();
        return info;
    }
}
