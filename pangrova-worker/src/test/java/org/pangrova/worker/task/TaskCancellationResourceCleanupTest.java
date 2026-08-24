package org.pangrova.worker.task;

import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.resource.ResourceManager;
import org.pangrova.resource.memory.MemoryArbitrator;
import org.pangrova.resource.spill.SpillManager;
import org.pangrova.resource.thread.ThreadBudgetTracker;
import org.pangrova.spi.execution.PlanFragment;
import org.pangrova.spi.execution.PlanNodeId;
import org.pangrova.spi.execution.QueryId;
import org.pangrova.spi.execution.TaskDescriptor;
import org.pangrova.spi.execution.TaskExecutor;
import org.pangrova.worker.task.memory.MemoryRefs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates F-WORKER-EXEC-02 fix: cancelled tasks release all resources
 * (executor handle, shuffle server, memory) instead of leaking them.
 *
 * <p>The bug: when a task was cancelled, the completion monitor's
 * {@code onTaskCompleted} tried to transition from CANCELLED → FINISHING,
 * which failed, causing early return before {@code releaseResources.run()}.
 * The fix ensures resources are always released regardless of state transition
 * outcome.</p>
 */
class TaskCancellationResourceCleanupTest {

    private static final String WORKER_ID = "test-worker-cancel";

    private TaskManager taskManager;
    private ScheduledExecutorService scheduler;

    @AfterEach
    void tearDown() {
        if (taskManager != null) {
            taskManager.close();
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    /**
     * Core test: a running task that gets cancelled must still have its
     * native executor released when the native task finishes.
     */
    @Test
    void cancelledTaskReleasesExecutorHandle() throws Exception {
        CancellationAwareExecutor executor = new CancellationAwareExecutor();
        scheduler = ManagedExecutorFactory.newScheduledThreadPool("test-cancel-monitor", 2);
        MemoryArbitrator arbitrator = new MemoryArbitrator(1L * 1024 * 1024 * 1024, 0);
        ThreadBudgetTracker tpm = new ThreadBudgetTracker(8, 4, 2);
        ResourceManager rm = new ResourceManager(arbitrator, tpm, new SpillManager());
        TaskManagerContext deps = new TaskManagerContext(
                new RecordingTaskStatusReporter(), executor,
                new MemoryRefs(rm), TransportRefs.empty(), scheduler);
        taskManager = new TaskManager(deps, WORKER_ID, 0.0);

        TaskDescriptor descriptor = new TaskDescriptor(
                new QueryId("q-cancel"), new TestPlanFragment(), List.of(), Map.of());
        SubmitTaskRequest request = SubmitTaskRequest.builder()
                .taskId("coord-cancel-1")
                .queryId("q-cancel")
                .stageId(0)
                .partitionId(0)
                .taskType("COMPUTE")
                .planFragment("cGxhbg==")
                .splits(List.of())
                .exchangeSources(List.of())
                .resourceLimit(null)
                .sessionProperties(Map.of())
                .outputConfig(null)
                .writeConfig(null)
                .build();

        // Submit — executor starts but does not finish until signalled
        String taskId = taskManager.submitTask(descriptor, request);

        // Wait for task to reach RUNNING
        awaitState(taskId, TaskManager.TaskState.RUNNING, 3000);

        // Cancel the task
        TaskCancelResult result = taskManager.cancelTask(taskId);
        assertThat(result.isSuccess()).isTrue();
        assertThat(taskManager.getTaskState(taskId))
                .isPresent()
                .hasValue(TaskManager.TaskState.CANCELLED);

        // The native cancel signal was sent — the executor sees this and finishes.
        // The completion monitor polls isFinished() which returns true after cancel.
        // With the F-WORKER-EXEC-02 fix, releaseTask is called even though the
        // task is already CANCELLED.

        // Wait for executor release
        boolean released = executor.awaitRelease(5, TimeUnit.SECONDS);
        assertThat(released)
                .as("Executor handle must be released after cancellation")
                .isTrue();
    }

    /**
     * Verifies that the completion monitor's onTaskCompleted callback releases
     * resources even when the task is already in CANCELLED state (transition
     * to FINISHING fails).
     */
    @Test
    void completionMonitorReleasesResourcesForCancelledTask() throws Exception {
        scheduler = ManagedExecutorFactory.newScheduledThreadPool("test-cancel-monitor", 1);
        CancellationAwareExecutor executor = new CancellationAwareExecutor();
        TaskManagerContext deps = buildContext(executor, scheduler);
        TaskStatusCollector statusCollector = new TaskStatusCollector(
                new RecordingTaskStatusReporter(), WORKER_ID, null);
        TaskCompletionMonitor monitor = new TaskCompletionMonitor(deps, statusCollector);

        TaskInfo info = buildTaskInfo("task-cancelled");
        // Simulate task already being in CANCELLED state
        info.compareAndSetState(TaskManager.TaskState.RECEIVED, TaskManager.TaskState.INITIALIZING);
        info.compareAndSetState(TaskManager.TaskState.INITIALIZING, TaskManager.TaskState.RUNNING);
        info.compareAndSetState(TaskManager.TaskState.RUNNING, TaskManager.TaskState.CANCELLED);

        AtomicBoolean resourcesReleased = new AtomicBoolean(false);
        CountDownLatch releaseLatch = new CountDownLatch(1);

        // Build lifecycle callback — transition to FINISHING will fail (already CANCELLED)
        TaskLifecycleManager.LifecycleCallback callback = new TaskLifecycleManager.LifecycleCallback() {
            @Override
            public boolean transition(String taskId, TaskManager.TaskState target) {
                // Simulates TaskManager.transition — CANCELLED has no valid transitions
                return false;
            }
            @Override
            public void clearCoordinatorIndex(TaskInfo i) {}
            @Override
            public void notifyCompletion(String queryId) {}
        };

        Runnable releaseResources = () -> {
            resourcesReleased.set(true);
            releaseLatch.countDown();
        };

        // Invoke onTaskCompleted — before the fix, this would return early without
        // calling releaseResources because transition to FINISHING fails
        monitor.onTaskCompleted("task-cancelled", info, callback, releaseResources);

        assertThat(releaseLatch.await(1, TimeUnit.SECONDS)).isTrue();
        assertThat(resourcesReleased.get())
                .as("Resources must be released even when task is already CANCELLED")
                .isTrue();
    }

    /**
     * Full integration test: submit, cancel, verify all cleanup happens.
     */
    @Test
    void fullLifecycleCancelReleasesAllResources() throws Exception {
        TrackingTaskExecutor executor = new TrackingTaskExecutor();
        scheduler = ManagedExecutorFactory.newScheduledThreadPool("test-cancel-full", 2);
        MemoryArbitrator arbitrator = new MemoryArbitrator(1L * 1024 * 1024 * 1024, 0);
        ThreadBudgetTracker tpm = new ThreadBudgetTracker(8, 4, 2);
        ResourceManager rm = new ResourceManager(arbitrator, tpm, new SpillManager());
        TaskManagerContext deps = new TaskManagerContext(
                new RecordingTaskStatusReporter(), executor,
                new MemoryRefs(rm), TransportRefs.empty(), scheduler);
        taskManager = new TaskManager(deps, WORKER_ID, 0.0);

        TaskDescriptor descriptor = new TaskDescriptor(
                new QueryId("q-full-cancel"), new TestPlanFragment(), List.of(), Map.of());
        SubmitTaskRequest request = SubmitTaskRequest.builder()
                .taskId("coord-full-1")
                .queryId("q-full-cancel")
                .stageId(0)
                .partitionId(0)
                .taskType("COMPUTE")
                .planFragment("cGxhbg==")
                .splits(List.of())
                .exchangeSources(List.of())
                .resourceLimit(null)
                .sessionProperties(Map.of())
                .outputConfig(null)
                .writeConfig(null)
                .build();

        String taskId = taskManager.submitTask(descriptor, request);
        awaitState(taskId, TaskManager.TaskState.RUNNING, 3000);

        // Cancel
        taskManager.cancelTask(taskId);

        // Wait for release
        boolean released = executor.awaitRelease(5, TimeUnit.SECONDS);
        assertThat(released)
                .as("Native executor must be released after cancel")
                .isTrue();
        assertThat(executor.cancelCount.get())
                .as("cancelTask must have been called on native executor")
                .isGreaterThanOrEqualTo(1);
        assertThat(executor.releaseCount.get())
                .as("releaseTask must have been called exactly once")
                .isEqualTo(1);
    }

    // --- Helpers ---

    private void awaitState(String taskId, TaskManager.TaskState expected, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            var state = taskManager.getTaskState(taskId);
            if (state.isPresent() && state.get() == expected) {
                return;
            }
            // Also break if already past expected (e.g. already CANCELLED)
            if (state.isPresent() && state.get().ordinal() > expected.ordinal()) {
                return;
            }
            Thread.sleep(20);
        }
    }

    private TaskManagerContext buildContext(TaskExecutor executor, ScheduledExecutorService sched) {
        MemoryArbitrator arbitrator = new MemoryArbitrator(1L * 1024 * 1024 * 1024, 0);
        ThreadBudgetTracker tpm = new ThreadBudgetTracker(8, 4, 2);
        ResourceManager rm = new ResourceManager(arbitrator, tpm, new SpillManager());
        return new TaskManagerContext(
                new RecordingTaskStatusReporter(), executor,
                new MemoryRefs(rm), TransportRefs.empty(), sched);
    }

    private TaskInfo buildTaskInfo(String taskId) {
        TaskDescriptor descriptor = new TaskDescriptor(
                new QueryId("test-query"), new TestPlanFragment(), List.of(), Map.of());
        SubmitTaskRequest request = SubmitTaskRequest.builder()
                .taskId(taskId)
                .queryId("test-query")
                .stageId(0)
                .partitionId(0)
                .taskType("COMPUTE")
                .planFragment("cGxhbg==")
                .splits(List.of())
                .exchangeSources(List.of())
                .resourceLimit(null)
                .sessionProperties(Map.of())
                .outputConfig(null)
                .writeConfig(null)
                .build();
        return new TaskInfo(taskId, "test-query", request, descriptor, java.time.Instant.now());
    }

    /**
     * TaskExecutor that blocks isFinished until cancelled, then reports finished.
     * Tracks release calls to verify cleanup.
     */
    static final class CancellationAwareExecutor implements TaskExecutor {
        private final java.util.concurrent.ConcurrentHashMap<Long, Boolean> cancelled =
                new java.util.concurrent.ConcurrentHashMap<>();
        private final CountDownLatch releaseLatch = new CountDownLatch(1);
        private long nextHandle = 100;

        @Override
        public long createTask(byte[] taskRequestBytes) {
            return nextHandle++;
        }

        @Override
        public void startTask(long h) {}

        @Override
        public boolean isFinished(long h) {
            // Only report finished AFTER cancel signal received
            return cancelled.containsKey(h);
        }

        @Override
        public void releaseTask(long h) {
            releaseLatch.countDown();
        }

        @Override
        public void cancelTask(long h) {
            cancelled.put(h, Boolean.TRUE);
        }

        boolean awaitRelease(long timeout, TimeUnit unit) throws InterruptedException {
            return releaseLatch.await(timeout, unit);
        }
    }

    /**
     * TaskExecutor that tracks cancel and release calls for assertions.
     */
    static final class TrackingTaskExecutor implements TaskExecutor {
        final AtomicInteger cancelCount = new AtomicInteger(0);
        final AtomicInteger releaseCount = new AtomicInteger(0);
        private final java.util.concurrent.ConcurrentHashMap<Long, Boolean> cancelled =
                new java.util.concurrent.ConcurrentHashMap<>();
        private final CountDownLatch releaseLatch = new CountDownLatch(1);
        private long nextHandle = 200;

        @Override
        public long createTask(byte[] taskRequestBytes) {
            return nextHandle++;
        }

        @Override
        public void startTask(long h) {}

        @Override
        public boolean isFinished(long h) {
            return cancelled.containsKey(h);
        }

        @Override
        public void releaseTask(long h) {
            releaseCount.incrementAndGet();
            releaseLatch.countDown();
        }

        @Override
        public void cancelTask(long h) {
            cancelCount.incrementAndGet();
            cancelled.put(h, Boolean.TRUE);
        }

        boolean awaitRelease(long timeout, TimeUnit unit) throws InterruptedException {
            return releaseLatch.await(timeout, unit);
        }
    }

    private static final class TestPlanFragment implements PlanFragment {
        @Override
        public PlanNodeId getId() {
            return new PlanNodeId("test-node");
        }

        @Override
        public List<PlanNodeId> getChildFragmentIds() {
            return List.of();
        }
    }
}
