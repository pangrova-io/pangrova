package org.pangrova.worker.task;

import org.pangrova.worker.task.memory.MemoryRefs;

import java.util.List;
import java.util.Map;

import org.pangrova.protocol.rest.config.ShuffleConfig;
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
 * Verifies that shuffle drain failures propagate to the task lifecycle (BUG-D33/D44).
 *
 * <p>Before the fix, {@code TaskLifecycleManager.executeTask} used the 2-arg overload
 * {@code startShufflePipelineIfNeeded(info, handle)} which passed {@code null} for
 * {@code onError}. Shuffle drain errors were logged but not propagated — the task
 * could report success while shuffle data was incomplete (silent data loss).</p>
 *
 * <p>After the fix, the 3-arg overload is used with the {@code onError} callback,
 * ensuring shuffle drain failures transition the task to FAILED state.</p>
 */
class ShuffleErrorCallbackPropagationTest {

    private static final String WORKER_ID = "test-worker-shuffle-error";

    @Test
    void shuffleDrainFailurePropagatesTaskToFailed() {
        ShuffleDrainFailingExecutor executor = new ShuffleDrainFailingExecutor();
        RecordingTaskStatusReporter reporter = new RecordingTaskStatusReporter();
        TaskManager manager = buildTaskManager(executor, reporter);

        ShuffleConfig shuffleConfig = new ShuffleConfig(
                "PRESTO_PAGES", "lz4", 1000, "MEMORY_ONLY", 4, List.of(0));
        String taskId = manager.submitTask(
                buildDescriptor("q-shuffle-err"),
                buildComputeRequestWithShuffle("q-shuffle-err", shuffleConfig));

        awaitTerminalState(manager, taskId);

        assertThat(manager.getTaskState(taskId))
                .as("Task must transition to FAILED when shuffle drain throws")
                .isPresent()
                .hasValue(TaskManager.TaskState.FAILED);

        assertThat(reporter.reports())
                .as("Status reporter must receive FAILED status for shuffle drain error")
                .anyMatch(r -> "FAILED".equals(r.state()));
    }

    @Test
    void computeTaskWithoutShuffleConfigIgnoresShufflePipeline() {
        ShuffleDrainFailingExecutor executor = new ShuffleDrainFailingExecutor();
        RecordingTaskStatusReporter reporter = new RecordingTaskStatusReporter();
        TaskManager manager = buildTaskManager(executor, reporter);

        // No shuffle config — shuffle pipeline should not start
        String taskId = manager.submitTask(
                buildDescriptor("q-no-shuffle"),
                buildComputeRequestWithShuffle("q-no-shuffle", null));

        awaitTerminalState(manager, taskId);

        assertThat(manager.getTaskState(taskId))
                .as("Task without shuffle config should complete normally")
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
                new TransportRefs(
                        new org.pangrova.worker.shuffle.NativeShuffleTransport(executor, "localhost"),
                        TransportRefs.empty().resultTransport(), null),
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

    private SubmitTaskRequest buildComputeRequestWithShuffle(String queryId,
                                                              ShuffleConfig shuffleConfig) {
        return SubmitTaskRequest.builder()
                .taskId("task-1")
                .queryId(queryId)
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
                .dynamicFilters(List.of())
                .shuffleConfig(shuffleConfig)
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
     * TaskExecutor that throws on executeCommand — simulates shuffle server start failure.
     * In the native Flight architecture, shuffle errors surface through
     * executeCommand (JNI command channel), not getShuffleBatch.
     */
    static final class ShuffleDrainFailingExecutor implements TaskExecutor {
        @Override public long createTask(byte[] taskRequestBytes) { return 1L; }
        @Override public void startTask(long h) {}
        @Override public boolean isFinished(long h) { return true; }
        @Override public void releaseTask(long h) {}
        @Override public void cancelTask(long h) {}
        @Override public byte[] executeCommand(long h, byte[] commandProto) {
            throw new RuntimeException("Simulated shuffle server start failure");
        }
    }

    private static final class TestPlanFragment implements PlanFragment {
        @Override public PlanNodeId getId() { return new PlanNodeId("test-node"); }
        @Override public List<PlanNodeId> getChildFragmentIds() { return List.of(); }
    }
}
