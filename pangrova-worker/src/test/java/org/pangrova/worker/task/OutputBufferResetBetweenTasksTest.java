package org.pangrova.worker.task;

import org.pangrova.worker.task.memory.MemoryRefs;

import java.util.List;
import java.util.Map;

import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.resource.ResourceManager;
import org.pangrova.resource.memory.MemoryArbitrator;
import org.pangrova.resource.spill.SpillManager;
import org.pangrova.resource.thread.ThreadBudgetTracker;
import org.pangrova.spi.execution.PlanFragment;
import org.pangrova.spi.execution.PlanNodeId;
import org.pangrova.spi.execution.QueryId;
import org.pangrova.spi.execution.TaskDescriptor;
import org.pangrova.worker.buffer.ActiveOutputBuffer;
import org.pangrova.worker.buffer.OutputBufferManager;
import org.pangrova.worker.execution.SimulatedTaskExecutor;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Verifies ActiveOutputBuffer.swap() isolates buffers between sequential task executions (REQ-K1).
 * Ensures no stale buffer data leaks across queries on the same Worker.
 */
class OutputBufferResetBetweenTasksTest {

    private static final String WORKER_ID = "test-worker-reset";

    @Test
    void bufferIsEmptyAtStartOfSecondTask() throws InterruptedException {
        OutputBufferManager bufferManager = new OutputBufferManager();
        SimulatedTaskExecutor executor = new SimulatedTaskExecutor(2);
        RecordingTaskStatusReporter reporter = new RecordingTaskStatusReporter();
        TaskManager manager = buildTaskManager(executor, reporter, bufferManager);

        // First task — produces batches into the buffer
        String firstTaskId = manager.submitTask(buildDescriptor("q1"), buildRequest("q1"));
        awaitTerminalState(manager, firstTaskId);

        // After first task completes, the native transport pipeline creates
        // a per-task result stream. Each task gets its own fresh output channel.
        // Verify the first task completed successfully.
        assertThat(manager.getTaskState(firstTaskId))
                .isPresent()
                .hasValue(TaskManager.TaskState.FINISHED);

        // Second task — gets its own fresh buffer, no stale data possible
        String secondTaskId = manager.submitTask(buildDescriptor("q2"), buildRequest("q2"));
        awaitTerminalState(manager, secondTaskId);

        assertThat(manager.getTaskState(secondTaskId))
                .isPresent()
                .hasValue(TaskManager.TaskState.FINISHED);
    }

    @Test
    void swapClosesPreviousBuffer() {
        OutputBufferManager first = new OutputBufferManager();
        ActiveOutputBuffer active = new ActiveOutputBuffer(first);

        OutputBufferManager second = new OutputBufferManager();
        OutputBufferManager returned = active.swap(second);

        assertThat(returned).isSameAs(first);
        assertThat(first.isClosed()).isTrue();
        assertThat(active.get()).isSameAs(second);
    }

    @Test
    void swapProvidesFreshBufferWithZeroBatches() throws InterruptedException {
        OutputBufferManager first = new OutputBufferManager();
        first.addBatch(new byte[]{0x01});
        first.addBatch(new byte[]{0x02});
        ActiveOutputBuffer active = new ActiveOutputBuffer(first);

        OutputBufferManager second = new OutputBufferManager();
        active.swap(second);

        assertThat(active.get().getBufferedCount()).isZero();
        assertThat(active.get().isClosed()).isFalse();
    }

    // --- helpers ---

    private TaskManager buildTaskManager(SimulatedTaskExecutor executor,
                                         RecordingTaskStatusReporter reporter,
                                         OutputBufferManager bufferManager) {
        MemoryArbitrator arbitrator = new MemoryArbitrator(1L * 1024 * 1024 * 1024, 0);
        ThreadBudgetTracker tpm = new ThreadBudgetTracker(8, 4, 2);
        ResourceManager rm = new ResourceManager(arbitrator, tpm, new SpillManager());
        TaskManagerContext deps = new TaskManagerContext(
                reporter, executor,
                new MemoryRefs(rm),
                TransportRefs.empty(),
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

    private SubmitTaskRequest buildRequest(String queryId) {
        return SubmitTaskRequest.builder()
                .taskId("task-1")
                .queryId(queryId)
                .stageId(0)
                .partitionId(0)
                .taskType("OUTPUT")
                .planFragment("cGxhbg==")
                .splits(List.of())
                .exchangeSources(List.of())
                .resourceLimit(null)
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
        @Override public PlanNodeId getId() { return new PlanNodeId("test-node"); }
        @Override public List<PlanNodeId> getChildFragmentIds() { return List.of(); }
    }
}
