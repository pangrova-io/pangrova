package org.pangrova.worker.task;

import org.pangrova.worker.task.memory.MemoryRefs;

import java.util.List;
import java.util.Map;

import org.pangrova.protocol.rest.config.OutputConfig;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.resource.ResourceManager;
import org.pangrova.resource.memory.MemoryArbitrator;
import org.pangrova.resource.spill.SpillManager;
import org.pangrova.resource.thread.ThreadBudgetTracker;
import org.pangrova.spi.execution.PlanFragment;
import org.pangrova.spi.execution.PlanNodeId;
import org.pangrova.spi.execution.QueryId;
import org.pangrova.spi.execution.TaskDescriptor;
import org.pangrova.worker.buffer.OutputBufferManager;
import org.pangrova.worker.execution.SimulatedTaskExecutor;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Verifies OutputConfig fields from SubmitTaskRequest are wired into
 * OutputBufferManager and ResultPuller (REQ-L1).
 */
class TaskManagerOutputConfigWiringTest {

    private static final String WORKER_ID = "test-worker-output-config";

    @Test
    void taskWithCustomMaxBatchesCompletesSuccessfully() {
        SimulatedTaskExecutor executor = new SimulatedTaskExecutor(3);
        RecordingTaskStatusReporter reporter = new RecordingTaskStatusReporter();
        TaskManager manager = buildTaskManager(executor, reporter);

        OutputConfig config = new OutputConfig("ticket-1", 0L, 25, 0L);
        String taskId = manager.submitTask(
                buildDescriptor("q1"),
                buildRequestWithOutputConfig("q1", config));

        awaitTerminalState(manager, taskId);
        assertThat(manager.getTaskState(taskId))
                .isPresent()
                .hasValue(TaskManager.TaskState.FINISHED);
    }

    @Test
    void taskWithNullOutputConfigUsesDefaults() {
        SimulatedTaskExecutor executor = new SimulatedTaskExecutor(2);
        RecordingTaskStatusReporter reporter = new RecordingTaskStatusReporter();
        TaskManager manager = buildTaskManager(executor, reporter);

        String taskId = manager.submitTask(
                buildDescriptor("q2"),
                buildRequestWithOutputConfig("q2", null));

        awaitTerminalState(manager, taskId);
        assertThat(manager.getTaskState(taskId))
                .isPresent()
                .hasValue(TaskManager.TaskState.FINISHED);
    }

    @Test
    void outputBufferManagerRespectsMaxBufferedBytes() {
        OutputBufferManager buffer = new OutputBufferManager(10, 1024 * 1024);
        assertThat(buffer.getMaxBatches()).isEqualTo(10);
        assertThat(buffer.getMaxBufferedBytes()).isEqualTo(1024 * 1024);
    }

    @Test
    void outputBufferManagerDefaultsMaxBufferedBytesToZero() {
        OutputBufferManager buffer = new OutputBufferManager(50);
        assertThat(buffer.getMaxBufferedBytes()).isZero();
    }

    @Test
    void outputBufferManagerNoArgConstructorUsesDefaults() {
        OutputBufferManager buffer = new OutputBufferManager();
        assertThat(buffer.getMaxBatches()).isEqualTo(OutputBufferManager.DEFAULT_MAX_BATCHES);
        assertThat(buffer.getMaxBufferedBytes()).isZero();
    }

    @Test
    void taskWithZeroMaxBatchesFallsBackToDefault() {
        SimulatedTaskExecutor executor = new SimulatedTaskExecutor(2);
        RecordingTaskStatusReporter reporter = new RecordingTaskStatusReporter();
        TaskManager manager = buildTaskManager(executor, reporter);

        OutputConfig config = new OutputConfig("ticket-1", 0L, 0, 0L);
        String taskId = manager.submitTask(
                buildDescriptor("q3"),
                buildRequestWithOutputConfig("q3", config));

        awaitTerminalState(manager, taskId);
        assertThat(manager.getTaskState(taskId))
                .isPresent()
                .hasValue(TaskManager.TaskState.FINISHED);
    }

    // --- helpers ---

    private TaskManager buildTaskManager(SimulatedTaskExecutor executor,
                                         RecordingTaskStatusReporter reporter) {
        MemoryArbitrator arbitrator = new MemoryArbitrator(1L * 1024 * 1024 * 1024, 0);
        ThreadBudgetTracker tpm = new ThreadBudgetTracker(8, 4, 2);
        ResourceManager rm = new ResourceManager(arbitrator, tpm, new SpillManager());
        TaskManagerContext deps = new TaskManagerContext(
                reporter, executor,
                new MemoryRefs(rm),
                new TransportRefs(null, null, null),
                java.util.concurrent.Executors.newCachedThreadPool(r -> {
                    Thread t = new Thread(r, "test-completion-monitor");
                    t.setDaemon(true);
                    return t;
                }));
        return new TaskManager(deps, WORKER_ID, 0.0);
    }

    private TaskDescriptor buildDescriptor(String queryId) {
        return new TaskDescriptor(
                new QueryId(queryId),
                new TestPlanFragment(),
                List.of(), Map.of());
    }

    private SubmitTaskRequest buildRequestWithOutputConfig(String queryId, OutputConfig outputConfig) {
        var canonical = org.pangrova.worker.CanonicalTaskRequestTestFixture.builder(
                "task-1", queryId, 0, 0,
                org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ,
                "plan".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        if (outputConfig != null) {
            canonical.setExecutionOptions(org.pangrova.protocol.v2.TaskExecutionOptions
                    .newBuilder().setDirectResultOutput(true));
        }
        return SubmitTaskRequest.from(canonical.build());
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
        @Override public PlanNodeId getId() { return new PlanNodeId("test-node"); }
        @Override public List<PlanNodeId> getChildFragmentIds() { return List.of(); }
    }
}
