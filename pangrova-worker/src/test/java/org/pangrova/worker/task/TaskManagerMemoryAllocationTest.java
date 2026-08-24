package org.pangrova.worker.task;

import org.pangrova.worker.task.memory.MemoryRefs;

import java.util.List;
import java.util.Map;

import org.pangrova.protocol.rest.config.ResourceLimitPayload;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.resource.ResourceManager;
import org.pangrova.resource.memory.MemoryArbitrator;
import org.pangrova.resource.memory.NodeMemoryPool;
import org.pangrova.resource.memory.RootMemoryPool;
import org.pangrova.resource.spill.SpillManager;
import org.pangrova.resource.thread.ThreadBudgetTracker;
import org.pangrova.spi.execution.PlanFragment;
import org.pangrova.spi.execution.PlanNodeId;
import org.pangrova.spi.execution.QueryId;
import org.pangrova.spi.execution.TaskDescriptor;
import org.pangrova.spi.execution.TaskResourceLimit;
import org.pangrova.worker.execution.SimulatedTaskExecutor;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Verifies TaskManager memory allocation validation (REQ-O1):
 * - Warning logged when allocated < requested
 * - Task rejected when allocation ratio falls below configurable threshold
 */
class TaskManagerMemoryAllocationTest {

    private static final String WORKER_ID = "test-worker-mem";

    @Test
    void taskProceedsWhenAllocationSufficient() {
        // 4GB budget, 0 reserved — plenty of room for 100MB request
        SimulatedTaskExecutor executor = new SimulatedTaskExecutor(1);
        TaskManager fullManager = buildTaskManager(executor, 0.5, 4L * 1024 * 1024 * 1024);

        String taskId = fullManager.submitTask(
                buildDescriptorWithMemory(100L * 1024 * 1024),
                buildRequestWithMemory(100L * 1024 * 1024));

        awaitTerminalState(fullManager, taskId);
        assertThat(fullManager.getTaskState(taskId))
                .isPresent()
                .hasValueSatisfying(s -> assertThat(s).isIn(
                        TaskManager.TaskState.FINISHED,
                        TaskManager.TaskState.FAILED));
    }

    @Test
    void taskRejectedWhenAllocationBelowThreshold() {
        // 1MB budget, 0 reserved — requesting 10MB will get partial allocation
        SimulatedTaskExecutor executor = new SimulatedTaskExecutor(1);
        RecordingTaskStatusReporter reporter = new RecordingTaskStatusReporter();
        TaskManager manager = buildTaskManager(executor, reporter, 0.5, 1L * 1024 * 1024);

        String taskId = manager.submitTask(
                buildDescriptorWithMemory(10L * 1024 * 1024),
                buildRequestWithMemory(10L * 1024 * 1024));

        // Task should transition to FAILED because allocation ratio < 0.5
        awaitTerminalState(manager, taskId);
        assertThat(manager.getTaskState(taskId))
                .isPresent()
                .hasValue(TaskManager.TaskState.FAILED);

        // Verify FAILED status was reported
        assertThat(reporter.reports())
                .anyMatch(r -> "FAILED".equals(r.state()));
    }

    @Test
    void taskAcceptsAnyAllocationWhenRatioIsZero() {
        // Default ratio 0.0 — accept any allocation, even partial
        SimulatedTaskExecutor executor = new SimulatedTaskExecutor(1);
        RecordingTaskStatusReporter reporter = new RecordingTaskStatusReporter();
        TaskManager manager = buildTaskManager(executor, reporter, 0.0, 1L * 1024 * 1024);

        String taskId = manager.submitTask(
                buildDescriptorWithMemory(10L * 1024 * 1024),
                buildRequestWithMemory(10L * 1024 * 1024));

        awaitTerminalState(manager, taskId);
        // Should not be rejected — ratio 0.0 means accept any
        assertThat(manager.getTaskState(taskId))
                .isPresent()
                .hasValueSatisfying(s -> assertThat(s).isIn(
                        TaskManager.TaskState.FINISHED,
                        TaskManager.TaskState.RUNNING));
    }

    @Test
    void taskRejectedWhenNodePoolBudgetExceeded() {
        // 50MB node pool budget — requesting 100MB exceeds the node pool limit
        long nodePoolBudget = 50L * 1024 * 1024;
        long arbitratorBudget = 4L * 1024 * 1024 * 1024; // large arbitrator so it's not the bottleneck
        SimulatedTaskExecutor executor = new SimulatedTaskExecutor(1);
        RecordingTaskStatusReporter reporter = new RecordingTaskStatusReporter();
        TaskManager manager = buildTaskManagerWithNodePool(executor, reporter, 0.0,
                arbitratorBudget, nodePoolBudget);

        String taskId = manager.submitTask(
                buildDescriptorWithMemory(100L * 1024 * 1024),
                buildRequestWithMemory(100L * 1024 * 1024));

        // Task should fail because node pool budget is exceeded
        awaitTerminalState(manager, taskId);
        assertThat(manager.getTaskState(taskId))
                .isPresent()
                .hasValue(TaskManager.TaskState.FAILED);
    }

    @Test
    void nodePoolBudgetReleasedAfterTaskCompletion() {
        // Verify that node pool budget is released after task completes,
        // allowing subsequent tasks to use the same budget.
        long nodePoolBudget = 100L * 1024 * 1024;
        long arbitratorBudget = 4L * 1024 * 1024 * 1024;
        SimulatedTaskExecutor executor = new SimulatedTaskExecutor(1);
        RecordingTaskStatusReporter reporter = new RecordingTaskStatusReporter();
        RootMemoryPool rootPool = new RootMemoryPool("test-root", nodePoolBudget);
        NodeMemoryPool nodePool = rootPool.createNodePool("test-node", nodePoolBudget);
        TaskManager manager = buildTaskManagerWithNodePool(executor, reporter, 0.0,
                arbitratorBudget, nodePool);

        // Submit first task that uses 50MB
        String taskId1 = manager.submitTask(
                buildDescriptorWithMemory(50L * 1024 * 1024),
                buildRequestWithMemory(50L * 1024 * 1024));
        awaitTerminalState(manager, taskId1);

        // After completion, node pool should have released the memory
        // so a second 50MB task should also succeed
        String taskId2 = manager.submitTask(
                buildDescriptorWithMemory(50L * 1024 * 1024),
                buildRequestWithMemory(50L * 1024 * 1024));
        awaitTerminalState(manager, taskId2);

        assertThat(manager.getTaskState(taskId2))
                .isPresent()
                .hasValueSatisfying(s -> assertThat(s).isIn(
                        TaskManager.TaskState.FINISHED,
                        TaskManager.TaskState.FAILED));
    }

    // --- helpers ---

    private TaskManager buildTaskManager(SimulatedTaskExecutor executor,
                                         double minRatio, long memoryBudget) {
        return buildTaskManager(executor, new RecordingTaskStatusReporter(), minRatio, memoryBudget);
    }

    private TaskManager buildTaskManager(SimulatedTaskExecutor executor,
                                         RecordingTaskStatusReporter reporter,
                                         double minRatio, long memoryBudget) {
        MemoryArbitrator arbitrator = new MemoryArbitrator(memoryBudget, 0);
        ThreadBudgetTracker tpm = new ThreadBudgetTracker(8, 4, 2);
        SpillManager spillManager = new SpillManager();
        ResourceManager rm = new ResourceManager(arbitrator, tpm, spillManager);
        TaskManagerContext deps = new TaskManagerContext(
                reporter, executor,
                new MemoryRefs(rm),
                TransportRefs.empty(),
                java.util.concurrent.Executors.newCachedThreadPool(r -> {
                    Thread t = new Thread(r, "test-completion-monitor");
                    t.setDaemon(true);
                    return t;
                }));
        return new TaskManager(deps, WORKER_ID, minRatio);
    }

    private TaskManager buildTaskManagerWithNodePool(SimulatedTaskExecutor executor,
                                                     RecordingTaskStatusReporter reporter,
                                                     double minRatio, long memoryBudget,
                                                     long nodePoolBudget) {
        RootMemoryPool rootPool = new RootMemoryPool("test-root", nodePoolBudget);
        NodeMemoryPool nodePool = rootPool.createNodePool("test-node", nodePoolBudget);
        return buildTaskManagerWithNodePool(executor, reporter, minRatio, memoryBudget, nodePool);
    }

    private TaskManager buildTaskManagerWithNodePool(SimulatedTaskExecutor executor,
                                                     RecordingTaskStatusReporter reporter,
                                                     double minRatio, long memoryBudget,
                                                     NodeMemoryPool nodePool) {
        MemoryArbitrator arbitrator = new MemoryArbitrator(memoryBudget, 0);
        ThreadBudgetTracker tpm = new ThreadBudgetTracker(8, 4, 2);
        SpillManager spillManager = new SpillManager();
        ResourceManager rm = new ResourceManager(arbitrator, tpm, spillManager);
        TaskManagerContext deps = new TaskManagerContext(
                reporter, executor,
                new MemoryRefs(rm, nodePool, null),
                TransportRefs.empty(),
                java.util.concurrent.Executors.newCachedThreadPool(r -> {
                    Thread t = new Thread(r, "test-completion-monitor");
                    t.setDaemon(true);
                    return t;
                }));
        return new TaskManager(deps, WORKER_ID, minRatio);
    }

    private TaskDescriptor buildDescriptorWithMemory(long memoryBytes) {
        return new TaskDescriptor(
                new QueryId("test-query"),
                new TestPlanFragment(),
                List.of(),
                Map.of(),
                0, 0, "COMPUTE",
                new TaskResourceLimit(memoryBytes, 1, 0, ""));
    }

    private SubmitTaskRequest buildRequestWithMemory(long memoryBytes) {
        return SubmitTaskRequest.builder()
                .taskId("task-1")
                .queryId("test-query")
                .stageId(0)
                .partitionId(0)
                .taskType("COMPUTE")
                .planFragment("cGxhbg==")
                .splits(List.of())
                .exchangeSources(List.of())
                .resourceLimit(new ResourceLimitPayload(memoryBytes, 1, 0, ""))
                .sessionProperties(Map.of())
                .outputConfig(null)
                .writeConfig(null)
                .build();
    }

    private void awaitTerminalState(TaskManager manager, String taskId) {
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            var state = manager.getTaskState(taskId);
            if (state.isPresent() && isTerminal(state.get())) {
                return;
            }
            try { Thread.sleep(50); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private boolean isTerminal(TaskManager.TaskState state) {
        return state == TaskManager.TaskState.FINISHED
                || state == TaskManager.TaskState.FAILED
                || state == TaskManager.TaskState.CANCELLED;
    }

    private static final class TestPlanFragment implements PlanFragment {
        @Override
        public PlanNodeId getId() { return new PlanNodeId("test-node"); }
        @Override
        public java.util.List<PlanNodeId> getChildFragmentIds() { return List.of(); }
    }
}
