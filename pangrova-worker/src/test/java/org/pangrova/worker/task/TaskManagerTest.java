package org.pangrova.worker.task;

import org.pangrova.worker.enums.TaskState;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

import org.pangrova.protocol.execution.PlanFragment;
import org.pangrova.protocol.execution.PlanNodeId;
import org.pangrova.execution.api.identity.QueryId;
import org.pangrova.protocol.execution.TaskDescriptor;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for TaskManager concurrent task submission.
 * Validates: R3.8, AC3
 */
class TaskManagerTest {

    /**
     * Property 5: N concurrent submitTask() calls produce N unique taskIds.
     *
     * Strategy: spawn N threads, each calling submitTask() concurrently via
     * a CountDownLatch barrier. Collect all returned taskIds into a concurrent
     * set and assert size == N (no duplicates).
     *
     * Validates: Requirements R3.8
     */
    @Property(tries = 50)
    void concurrentSubmitProducesUniqueTaskIds(
            @ForAll @IntRange(min = 2, max = 50) int n) throws InterruptedException {

        TaskManager manager = TaskManager.forHttpWiringOnly();
        Set<String> taskIds = ConcurrentHashMap.newKeySet();
        CountDownLatch startBarrier = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(n);
        ExecutorService pool = Executors.newFixedThreadPool(n);

        for (int i = 0; i < n; i++) {
            pool.submit(() -> {
                try {
                    startBarrier.await();
                    String id = manager.submitTask(buildDescriptor());
                    taskIds.add(id);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        startBarrier.countDown();
        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        pool.shutdown();

        assertThat(taskIds).hasSize(n);
    }

    /**
     * Property 5 (cont.): all submitted tasks reach at least INITIALIZING state.
     *
     * Strategy: after N concurrent submissions, verify every returned taskId
     * has a state present and is at least INITIALIZING (not RECEIVED).
     *
     * Validates: Requirements R3.8, AC3
     */
    @Property(tries = 50)
    void allSubmittedTasksReachInitializingState(
            @ForAll @IntRange(min = 2, max = 50) int n) throws InterruptedException {

        TaskManager manager = TaskManager.forHttpWiringOnly();
        Set<String> taskIds = ConcurrentHashMap.newKeySet();
        CountDownLatch startBarrier = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(n);
        ExecutorService pool = Executors.newFixedThreadPool(n);

        for (int i = 0; i < n; i++) {
            pool.submit(() -> {
                try {
                    startBarrier.await();
                    String id = manager.submitTask(buildDescriptor());
                    taskIds.add(id);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        startBarrier.countDown();
        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        pool.shutdown();

        for (String taskId : taskIds) {
            assertThat(manager.getTaskState(taskId))
                    .isPresent()
                    .hasValue(TaskState.INITIALIZING);
        }
    }

    private static TaskDescriptor buildDescriptor() {
        return new TaskDescriptor(
                new QueryId("test-query"),
                new TestPlanFragment(),
                List.of(),
                Map.of(), 0, 0, "COMPUTE", org.pangrova.protocol.execution.TaskResourceLimit.NONE);
    }

    /**
     * Minimal PlanFragment implementation for testing — no real plan logic needed.
     */
    private static final class TestPlanFragment implements PlanFragment {
        @Override
        public PlanNodeId getId() {
            return new PlanNodeId("test-node");
        }

        @Override
        public List<PlanNodeId> getChildFragmentIds() {
            return Collections.emptyList();
        }
    }
}
