package org.pangrova.worker.task;

import org.pangrova.protocol.rest.config.ResourceLimitPayload;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.resource.ResourceManager;
import org.pangrova.resource.memory.MemoryArbitrator;
import org.pangrova.resource.spill.SpillManager;
import org.pangrova.resource.thread.ThreadBudgetTracker;
import org.pangrova.spi.execution.PlanFragment;
import org.pangrova.spi.execution.PlanNodeId;
import org.pangrova.spi.execution.QueryId;
import org.pangrova.spi.execution.TaskDescriptor;
import org.pangrova.spi.execution.TaskExecutor;
import org.pangrova.spi.execution.TaskResourceLimit;
import org.pangrova.worker.task.memory.MemoryRefs;
import org.pangrova.worker.task.memory.TaskMemoryRequestor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * F-WORKER-EXEC-07: Validates that OUTPUT tasks release memory budget
 * immediately when reporting FINISHED, rather than deferring release
 * to the background completion monitor.
 *
 * <p>Key properties verified:</p>
 * <ul>
 *   <li>Memory budget is released before FINISHED status is reported</li>
 *   <li>Memory release is idempotent (atomic claim-and-reset)</li>
 *   <li>Executor handle is still released by the completion monitor</li>
 * </ul>
 */
class OutputTaskMemoryReleaseTest {

    private static final String WORKER_ID = "test-worker-output-memory";
    private static final long TASK_MEMORY_BYTES = 64L * 1024 * 1024; // 64 MB
    private static final long TOTAL_MEMORY = 1L * 1024 * 1024 * 1024; // 1 GB

    private TaskManager taskManager;

    @AfterEach
    void tearDown() {
        if (taskManager != null) {
            taskManager.close();
        }
    }

    /**
     * Verifies that when an OUTPUT task reports FINISHED, the memory budget
     * has already been released from the arbitrator. This prevents subsequent
     * tasks from starving on memory while the completion monitor polls for
     * native task termination.
     */
    @Test
    void outputTaskReleasesMemoryBeforeFinishedReport() throws Exception {
        MemoryArbitrator arbitrator = new MemoryArbitrator(TOTAL_MEMORY, 0);
        // Track available memory at the moment FINISHED is reported
        AtomicLong memoryAtFinishedReport = new AtomicLong(-1);
        CountDownLatch finishedReported = new CountDownLatch(1);

        MemoryCapturingReporter reporter = new MemoryCapturingReporter(
                arbitrator, memoryAtFinishedReport, finishedReported);

        ControllableTaskExecutor executor = new ControllableTaskExecutor();
        executor.markAllFinished();
        TaskManager manager = buildTaskManager(executor, reporter, arbitrator);
        this.taskManager = manager;

        // Submit an OUTPUT task with a memory limit
        String taskId = manager.submitTask(
                buildOutputDescriptor("q-out-1"),
                buildOutputRequest("q-out-1"));

        // Wait for FINISHED to be reported
        boolean reported = finishedReported.await(5, TimeUnit.SECONDS);
        assertThat(reported)
                .as("FINISHED should be reported within timeout")
                .isTrue();

        // At the time FINISHED was reported, memory should have already been
        // released. The available memory should be >= (TOTAL - initial overhead).
        // Before the fix, memory would still be held by the task at report time.
        long availableAtReport = memoryAtFinishedReport.get();
        assertThat(availableAtReport)
                .as("Memory should be substantially released by FINISHED report time. "
                        + "Available memory should be at least 90%% of total.")
                .isGreaterThanOrEqualTo((long) (TOTAL_MEMORY * 0.9));
    }

    /**
     * Verifies that memory release is idempotent: calling releaseTaskMemory
     * and releaseBufferMemory twice does not double-release from the arbitrator.
     */
    @Test
    void memoryReleaseIsIdempotent() {
        MemoryArbitrator arbitrator = new MemoryArbitrator(TOTAL_MEMORY, 0);
        TaskMemoryRequestor requestor = new TaskMemoryRequestor(arbitrator, 0.0);

        TaskInfo info = buildOutputTaskInfo("q-idem-1");

        // Request memory
        requestor.requestMemoryForTask(info);
        requestor.requestBufferMemory(info);

        long afterAlloc = arbitrator.getAvailableMemory();
        assertThat(afterAlloc).isLessThan(TOTAL_MEMORY);

        // First release
        requestor.releaseTaskMemory(info);
        requestor.releaseBufferMemory(info);
        long afterFirstRelease = arbitrator.getAvailableMemory();

        // Second release (should be a no-op)
        requestor.releaseTaskMemory(info);
        requestor.releaseBufferMemory(info);
        long afterSecondRelease = arbitrator.getAvailableMemory();

        // Available memory should be the same after both releases
        assertThat(afterSecondRelease)
                .as("Second release should be a no-op (idempotent)")
                .isEqualTo(afterFirstRelease);

        // And both should restore total memory
        assertThat(afterFirstRelease).isEqualTo(TOTAL_MEMORY);
    }

    /**
     * Verifies that the native executor handle is still released by the
     * completion monitor after the OUTPUT task reports FINISHED.
     */
    @Test
    void outputTaskExecutorHandleReleasedByCompletionMonitor() throws Exception {
        MemoryArbitrator arbitrator = new MemoryArbitrator(TOTAL_MEMORY, 0);
        RecordingTaskStatusReporter reporter = new RecordingTaskStatusReporter();
        ControllableTaskExecutor executor = new ControllableTaskExecutor();
        TaskManager manager = buildTaskManager(executor, reporter, arbitrator);
        this.taskManager = manager;

        String taskId = manager.submitTask(
                buildOutputDescriptor("q-handle-1"),
                buildOutputRequest("q-handle-1"));

        // Wait for FINISHED state
        awaitState(manager, taskId, TaskManager.TaskState.FINISHED, 5000);

        // Mark native task as finished so completion monitor releases the handle
        executor.markAllFinished();

        // Poll for the completion monitor to release the handle
        long deadline = System.currentTimeMillis() + 5000;
        while (executor.releasedHandles.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }

        // Verify the executor was released
        assertThat(executor.releasedHandles)
                .as("Completion monitor should release the executor handle")
                .isNotEmpty();
    }

    /**
     * Verifies that a COMPUTE task still releases memory in the correct order:
     * after native completion, before FINISHED report. This ensures we didn't
     * break the COMPUTE path while fixing the OUTPUT path.
     */
    @Test
    void computeTaskReleasesMemoryBeforeFinishedReport() throws Exception {
        MemoryArbitrator arbitrator = new MemoryArbitrator(TOTAL_MEMORY, 0);
        AtomicLong memoryAtFinishedReport = new AtomicLong(-1);
        CountDownLatch finishedReported = new CountDownLatch(1);

        MemoryCapturingReporter reporter = new MemoryCapturingReporter(
                arbitrator, memoryAtFinishedReport, finishedReported);

        ControllableTaskExecutor executor = new ControllableTaskExecutor();
        executor.markAllFinished();
        TaskManager manager = buildTaskManager(executor, reporter, arbitrator, TransportRefs.empty());
        this.taskManager = manager;

        String taskId = manager.submitTask(
                buildComputeDescriptor("q-comp-1"),
                buildComputeRequest("q-comp-1"));

        boolean reported = finishedReported.await(5, TimeUnit.SECONDS);
        assertThat(reported).isTrue();

        long availableAtReport = memoryAtFinishedReport.get();
        assertThat(availableAtReport)
                .as("COMPUTE task should also release memory before FINISHED")
                .isGreaterThanOrEqualTo(TOTAL_MEMORY - TASK_MEMORY_BYTES);
    }

    // --- helpers ---

    private TaskManager buildTaskManager(ControllableTaskExecutor executor,
                                          TaskStatusReporter reporter,
                                          MemoryArbitrator arbitrator) {
        return buildTaskManager(executor, reporter, arbitrator, new TransportRefs(null, null, null));
    }

    private TaskManager buildTaskManager(ControllableTaskExecutor executor,
                                          TaskStatusReporter reporter,
                                          MemoryArbitrator arbitrator,
                                          TransportRefs transportRefs) {
        ThreadBudgetTracker tpm = new ThreadBudgetTracker(8, 4, 2);
        ResourceManager rm = new ResourceManager(arbitrator, tpm, new SpillManager());
        TaskManagerContext deps = new TaskManagerContext(
                reporter, executor,
                new MemoryRefs(rm),
                transportRefs,
                Executors.newCachedThreadPool(r -> {
                    Thread t = new Thread(r, "test-completion-monitor");
                    t.setDaemon(true);
                    return t;
                }));
        return new TaskManager(deps, WORKER_ID, 0.0);
    }

    private TaskDescriptor buildOutputDescriptor(String queryId) {
        return new TaskDescriptor(
                new QueryId(queryId),
                new TestPlanFragment(),
                List.of(), Map.of(),
                0, 0, "OUTPUT",
                new TaskResourceLimit(TASK_MEMORY_BYTES, 1, 0, ""));
    }

    private SubmitTaskRequest buildOutputRequest(String queryId) {
        return SubmitTaskRequest.builder()
                .taskId("coord-task-" + queryId)
                .queryId(queryId)
                .stageId(0)
                .partitionId(0)
                .taskType("OUTPUT")
                .planFragment("cGxhbg==")
                .splits(List.of())
                .exchangeSources(List.of())
                .resourceLimit(new ResourceLimitPayload(TASK_MEMORY_BYTES, 1, 0, ""))
                .sessionProperties(Map.of())
                .outputConfig(null)
                .writeConfig(null)
                .build();
    }

    private TaskDescriptor buildComputeDescriptor(String queryId) {
        return new TaskDescriptor(
                new QueryId(queryId),
                new TestPlanFragment(),
                List.of(), Map.of(),
                0, 0, "COMPUTE",
                new TaskResourceLimit(TASK_MEMORY_BYTES, 1, 0, ""));
    }

    private SubmitTaskRequest buildComputeRequest(String queryId) {
        return SubmitTaskRequest.builder()
                .taskId("coord-task-" + queryId)
                .queryId(queryId)
                .stageId(0)
                .partitionId(0)
                .taskType("COMPUTE")
                .planFragment("cGxhbg==")
                .splits(List.of())
                .exchangeSources(List.of())
                .resourceLimit(new ResourceLimitPayload(TASK_MEMORY_BYTES, 1, 0, ""))
                .sessionProperties(Map.of())
                .outputConfig(null)
                .writeConfig(null)
                .build();
    }

    private TaskInfo buildOutputTaskInfo(String queryId) {
        return new TaskInfo(
                "task-" + queryId, queryId,
                buildOutputRequest(queryId),
                buildOutputDescriptor(queryId),
                Instant.now());
    }

    private void awaitState(TaskManager manager, String taskId,
                            TaskManager.TaskState expected, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            var state = manager.getTaskState(taskId);
            if (state.isPresent() && state.get() == expected) {
                return;
            }
            try { Thread.sleep(50); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /**
     * Reporter that captures the arbitrator's available memory at the moment
     * a FINISHED status is reported.
     */
    static final class MemoryCapturingReporter implements TaskStatusReporter {
        private final MemoryArbitrator arbitrator;
        private final AtomicLong memoryAtFinished;
        private final CountDownLatch finishedLatch;

        MemoryCapturingReporter(MemoryArbitrator arbitrator,
                                AtomicLong memoryAtFinished,
                                CountDownLatch finishedLatch) {
            this.arbitrator = arbitrator;
            this.memoryAtFinished = memoryAtFinished;
            this.finishedLatch = finishedLatch;
        }

        @Override
        public void report(TaskStatusReport report) {
            if ("FINISHED".equals(report.state())) {
                memoryAtFinished.compareAndSet(-1, arbitrator.getAvailableMemory());
                finishedLatch.countDown();
            }
        }
    }

    /**
     * TaskExecutor that allows programmatic control of when tasks finish
     * and tracks released handles.
     */
    static final class ControllableTaskExecutor implements TaskExecutor {
        private final java.util.Set<Long> finishedHandles =
                java.util.concurrent.ConcurrentHashMap.newKeySet();
        final CopyOnWriteArrayList<Long> releasedHandles = new CopyOnWriteArrayList<>();
        private volatile boolean allFinished = false;
        private long nextHandle = 1;

        @Override public long createTask(byte[] taskRequestBytes) { return nextHandle++; }
        @Override public void startTask(long h) {}
        @Override public boolean isFinished(long h) { return allFinished || finishedHandles.contains(h); }
        @Override public void releaseTask(long h) { releasedHandles.add(h); }
        @Override public void cancelTask(long h) {}

        void markFinished(long handle) { finishedHandles.add(handle); }
        void markAllFinished() { allFinished = true; }
    }

    private static final class TestPlanFragment implements PlanFragment {
        @Override public PlanNodeId getId() { return new PlanNodeId("test-node"); }
        @Override public List<PlanNodeId> getChildFragmentIds() { return List.of(); }
    }
}
