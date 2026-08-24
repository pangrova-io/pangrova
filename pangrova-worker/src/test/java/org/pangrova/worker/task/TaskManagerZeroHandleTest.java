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
import org.pangrova.spi.execution.TaskExecutor;


import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Verifies TaskManager fails fast on zero executor handle (REQ-O2, P0).
 * A zero handle from createTask() indicates native allocation failure.
 */
class TaskManagerZeroHandleTest {

    private static final String WORKER_ID = "test-worker-handle";

    @Test
    void taskFailsOnZeroHandle() {
        ZeroHandleTaskExecutor executor = new ZeroHandleTaskExecutor();
        RecordingTaskStatusReporter reporter = new RecordingTaskStatusReporter();
        TaskManager manager = buildTaskManager(executor, reporter);

        String taskId = manager.submitTask(buildDescriptor(), buildRequest());

        awaitTerminalState(manager, taskId);
        assertThat(manager.getTaskState(taskId))
                .isPresent()
                .hasValue(TaskManager.TaskState.FAILED);

        assertThat(reporter.reports())
                .anyMatch(r -> "FAILED".equals(r.state()));
    }

    @Test
    void taskSucceedsOnNonZeroHandle() {
        NonZeroHandleTaskExecutor executor = new NonZeroHandleTaskExecutor();
        RecordingTaskStatusReporter reporter = new RecordingTaskStatusReporter();
        TaskManager manager = buildTaskManager(executor, reporter);

        String taskId = manager.submitTask(buildDescriptor(), buildRequest());

        awaitTerminalState(manager, taskId);
        assertThat(manager.getTaskState(taskId))
                .isPresent()
                .hasValue(TaskManager.TaskState.FINISHED);
    }

    // --- helpers ---

    private TaskManager buildTaskManager(TaskExecutor executor,
                                         RecordingTaskStatusReporter reporter) {
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

    private TaskDescriptor buildDescriptor() {
        return new TaskDescriptor(
                new QueryId("test-query"),
                new TestPlanFragment(),
                List.of(), Map.of());
    }

    private SubmitTaskRequest buildRequest() {
        return SubmitTaskRequest.builder()
                .taskId("task-1")
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

    /**
     * TaskExecutor that always returns 0 from createTask — simulates native allocation failure.
     */
    static final class ZeroHandleTaskExecutor implements TaskExecutor {
        @Override public long createTask(byte[] taskRequestBytes) { return 0; }
        @Override public void startTask(long h) {}
        @Override public void releaseTask(long h) {}
        @Override public void cancelTask(long h) {}
    }

    /**
     * TaskExecutor that returns a valid handle and produces 1 batch then EOF.
     */
    static final class NonZeroHandleTaskExecutor implements TaskExecutor {
        private long nextHandle = 1;
        @Override public long createTask(byte[] taskRequestBytes) { return nextHandle++; }
        @Override public void startTask(long h) {}
        @Override public boolean isFinished(long h) { return true; }
        @Override public void releaseTask(long h) {}
        @Override public void cancelTask(long h) {}
    }

    private static final class TestPlanFragment implements PlanFragment {
        @Override public PlanNodeId getId() { return new PlanNodeId("test-node"); }
        @Override public List<PlanNodeId> getChildFragmentIds() { return List.of(); }
    }
}
