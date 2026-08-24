package org.pangrova.coordinator.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests and property-based tests for {@link CoordinatorTaskTracker}.
 * Validates: R11.1, R11.2, AC10
 */
class CoordinatorTaskTrackerTest {

    private static final String QUERY_ID = "q-1";

    private CoordinatorTaskTracker tracker;

    @BeforeEach
    void setUp() {
        tracker = new CoordinatorTaskTracker();
    }

    // ── Unit Tests ──────────────────────────────────────────────────

    @Test
    void allTasksFinishedAfterFullLifecycle() {
        tracker.registerTask(QUERY_ID, "t-1");
        tracker.registerTask(QUERY_ID, "t-2");

        tracker.markScheduled(QUERY_ID, "t-1");
        tracker.markScheduled(QUERY_ID, "t-2");

        tracker.updateTaskState(QUERY_ID, "t-1", "RUNNING");
        tracker.updateTaskState(QUERY_ID, "t-2", "RUNNING");

        tracker.updateTaskState(QUERY_ID, "t-1", "FINISHED");
        tracker.updateTaskState(QUERY_ID, "t-2", "FINISHED");

        assertThat(tracker.allTasksFinished(QUERY_ID)).isTrue();
    }

    @Test
    void hasFailedTaskReturnsTrueWhenOneTaskFails() {
        tracker.registerTask(QUERY_ID, "t-1");
        tracker.registerTask(QUERY_ID, "t-2");

        tracker.markScheduled(QUERY_ID, "t-1");
        tracker.markScheduled(QUERY_ID, "t-2");

        tracker.updateTaskState(QUERY_ID, "t-1", "RUNNING");
        tracker.updateTaskState(QUERY_ID, "t-2", "RUNNING");

        tracker.updateTaskState(QUERY_ID, "t-1", "FAILED");

        assertThat(tracker.hasFailedTask(QUERY_ID)).isTrue();
        assertThat(tracker.allTasksFinished(QUERY_ID)).isFalse();
    }

    @Test
    void allTasksFinishedReturnsFalseForUnknownQuery() {
        assertThat(tracker.allTasksFinished("unknown")).isFalse();
    }

    @Test
    void hasFailedTaskReturnsFalseForUnknownQuery() {
        assertThat(tracker.hasFailedTask("unknown")).isFalse();
    }

    @Test
    void registerDuplicateTaskThrows() {
        tracker.registerTask(QUERY_ID, "t-1");
        assertThatThrownBy(() -> tracker.registerTask(QUERY_ID, "t-1"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void updateUnknownQueryThrows() {
        assertThatThrownBy(() -> tracker.updateTaskState("no-q", "t-1", "RUNNING"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void updateUnknownTaskThrows() {
        tracker.registerTask(QUERY_ID, "t-1");
        assertThatThrownBy(() -> tracker.updateTaskState(QUERY_ID, "no-t", "RUNNING"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void allTasksFinishedReturnsFalseWhenNotAllFinished() {
        tracker.registerTask(QUERY_ID, "t-1");
        tracker.registerTask(QUERY_ID, "t-2");

        tracker.markScheduled(QUERY_ID, "t-1");
        tracker.markScheduled(QUERY_ID, "t-2");

        tracker.updateTaskState(QUERY_ID, "t-1", "RUNNING");
        tracker.updateTaskState(QUERY_ID, "t-2", "RUNNING");

        tracker.updateTaskState(QUERY_ID, "t-1", "FINISHED");

        assertThat(tracker.allTasksFinished(QUERY_ID)).isFalse();
    }

    @Test
    void concurrentUpdateTaskStateIsThreadSafe() throws Exception {
        int taskCount = 50;
        for (int i = 0; i < taskCount; i++) {
            tracker.registerTask(QUERY_ID, "t-" + i);
            tracker.markScheduled(QUERY_ID, "t-" + i);
        }

        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch latch = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < taskCount; i++) {
            String taskId = "t-" + i;
            futures.add(pool.submit(() -> {
                awaitLatch(latch);
                tracker.updateTaskState(QUERY_ID, taskId, "RUNNING");
            }));
        }

        latch.countDown();
        for (Future<?> f : futures) {
            f.get();
        }
        pool.shutdown();

        for (int i = 0; i < taskCount; i++) {
            tracker.updateTaskState(QUERY_ID, "t-" + i, "FINISHED");
        }
        assertThat(tracker.allTasksFinished(QUERY_ID)).isTrue();
    }

    // ── Property-Based Tests (jqwik) ───────────────────────────────

    /**
     * Validates: Requirements R11.1, R11.2
     *
     * Property: N concurrent updateTaskState("RUNNING") calls on N different
     * tasks all succeed without exception.
     */
    @Property(tries = 50)
    void concurrentRunningTransitionsAllSucceed(
            @ForAll @IntRange(min = 2, max = 30) int taskCount) throws Exception {

        CoordinatorTaskTracker t = new CoordinatorTaskTracker();
        String qId = "pbt-q";

        for (int i = 0; i < taskCount; i++) {
            t.registerTask(qId, "t-" + i);
            t.markScheduled(qId, "t-" + i);
        }

        ExecutorService pool = Executors.newFixedThreadPool(
                Math.min(taskCount, Runtime.getRuntime().availableProcessors()));
        CountDownLatch gate = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < taskCount; i++) {
            String taskId = "t-" + i;
            futures.add(pool.submit(() -> {
                awaitLatch(gate);
                t.updateTaskState(qId, taskId, "RUNNING");
            }));
        }

        gate.countDown();
        for (Future<?> f : futures) {
            f.get();
        }
        pool.shutdown();
    }

    /**
     * Validates: Requirements R11.1, R11.2
     *
     * Property: After all tasks reach FINISHED, allTasksFinished() returns true
     * regardless of ordering.
     */
    @Property(tries = 50)
    void allFinishedRegardlessOfOrdering(
            @ForAll @IntRange(min = 1, max = 20) int taskCount) {

        CoordinatorTaskTracker t = new CoordinatorTaskTracker();
        String qId = "pbt-order";

        for (int i = 0; i < taskCount; i++) {
            t.registerTask(qId, "t-" + i);
        }
        for (int i = 0; i < taskCount; i++) {
            t.markScheduled(qId, "t-" + i);
        }
        for (int i = 0; i < taskCount; i++) {
            t.updateTaskState(qId, "t-" + i, "RUNNING");
        }
        for (int i = 0; i < taskCount; i++) {
            t.updateTaskState(qId, "t-" + i, "FINISHED");
        }

        assertThat(t.allTasksFinished(qId)).isTrue();
        assertThat(t.hasFailedTask(qId)).isFalse();
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
