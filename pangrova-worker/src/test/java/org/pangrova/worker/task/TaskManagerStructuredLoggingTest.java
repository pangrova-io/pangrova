package org.pangrova.worker.task;

import org.pangrova.resource.ResourceManager;
import org.pangrova.worker.task.memory.MemoryRefs;
import org.pangrova.resource.memory.MemoryArbitrator;
import org.pangrova.resource.spill.SpillManager;
import org.pangrova.resource.thread.ThreadBudgetTracker;
import org.pangrova.spi.execution.PlanFragment;
import org.pangrova.spi.execution.PlanNodeId;
import org.pangrova.spi.execution.QueryId;
import org.pangrova.spi.execution.TaskDescriptor;
import org.pangrova.spi.execution.TaskExecutor;

import org.pangrova.worker.execution.SimulatedTaskExecutor;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.pangrova.protocol.rest.task.SubmitTaskRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * Tests TaskManager lifecycle logging behavior.
 * Verifies that submit, complete, and error paths execute without error.
 *
 * <p>Structured logging format (event=... key=value) is verified by
 * StructuredLoggingPropertyTest at the project level.</p>
 */
class TaskManagerStructuredLoggingTest {

    private static final String WORKER_ID = "test-worker-log";
    private static final String PLAN_BASE64 =
            Base64.getEncoder().encodeToString(new byte[]{1, 2, 3});

    @Test
    void taskSubmitDoesNotThrow() throws InterruptedException {
        TaskManager tm = buildTaskManager(new SimulatedTaskExecutor(1));

        assertThatNoException().isThrownBy(() -> submitTestTask(tm));
        awaitTerminal(tm);
    }

    @Test
    void taskCompletesSuccessfully() throws InterruptedException {
        TaskManager tm = buildTaskManager(new SimulatedTaskExecutor(1));

        String taskId = submitTestTask(tm);
        awaitTerminal(tm);

        assertThat(tm.getTaskState(taskId))
                .isPresent()
                .hasValue(TaskManager.TaskState.FINISHED);
    }

    @Test
    void taskFailureTransitionsToFailed() throws InterruptedException {
        // Executor that throws from isFinished() — simulates a NATIVE_CRASHED
        // failure (SIGSEGV); per NativeTaskFailureClassifier this should be
        // categorized as fail-fast (no transient retry), so the task transitions
        // to FAILED within the 5s deadline.
        TaskExecutor failingExecutor = new TaskExecutor() {
            @Override public long createTask(byte[] b) { return 1L; }
            @Override public void startTask(long h) {}
            @Override public boolean isFinished(long h) {
                throw new RuntimeException("SIGSEGV in native task");
            }
            @Override public void releaseTask(long h) {}
            @Override public void cancelTask(long h) {}
        };
        TaskManager tm = buildTaskManager(failingExecutor);

        String taskId = submitTestTask(tm);
        awaitTerminal(tm);

        assertThat(tm.getTaskState(taskId))
                .isPresent()
                .hasValue(TaskManager.TaskState.FAILED);
    }

    @Test
    void submitWithoutExecutionWiringDoesNotThrow() {
        TaskManager tm = TaskManager.forHttpWiringOnly();
        TaskDescriptor descriptor = new TaskDescriptor(
                new QueryId("query-1"), new StubPlanFragment(),
                List.of(), Map.of());

        assertThatNoException().isThrownBy(() -> tm.submitTask(descriptor));
    }

    // --- Helpers ---

    private TaskManager buildTaskManager(TaskExecutor executor) {
        ResourceManager rm = createTestResourceManager();
        TaskManagerContext deps = new TaskManagerContext(
                new RecordingTaskStatusReporter(), executor,
                new MemoryRefs(rm),
                TransportRefs.empty(),
                java.util.concurrent.Executors.newCachedThreadPool(r -> {
                    Thread t = new Thread(r, "test-completion-monitor");
                    t.setDaemon(true);
                    return t;
                }));
        return new TaskManager(deps, WORKER_ID, 0.0);
    }

    private ResourceManager createTestResourceManager() {
        MemoryArbitrator arbitrator = new MemoryArbitrator(
                512L * 1024 * 1024, 32L * 1024 * 1024);
        return new ResourceManager(
                arbitrator, new ThreadBudgetTracker(8, 4, 2), new SpillManager());
    }

    private String submitTestTask(TaskManager tm) {
        SubmitTaskRequest request = SubmitTaskRequest.builder()
                .taskId("task-1")
                .queryId("query-1")
                .stageId(0)
                .partitionId(0)
                .taskType("COMPUTE")
                .planFragment(PLAN_BASE64)
                .splits(List.of())
                .exchangeSources(List.of())
                .resourceLimit(null)
                .sessionProperties(Map.of())
                .outputConfig(null)
                .writeConfig(null)
                .build();
        TaskDescriptor descriptor = new TaskDescriptor(
                new QueryId("query-1"), new StubPlanFragment(),
                List.of(), Map.of());
        return tm.submitTask(descriptor, request);
    }

    private void awaitTerminal(TaskManager tm) throws InterruptedException {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
        while (System.currentTimeMillis() < deadline) {
            if (tm.getRunningTaskCount() == 0) {
                Thread.sleep(50);
                return;
            }
            Thread.sleep(10);
        }
    }

    /** Minimal PlanFragment for test descriptor construction. */
    private static final class StubPlanFragment implements PlanFragment {
        @Override
        public PlanNodeId getId() {
            return new PlanNodeId("root");
        }

        @Override
        public List<PlanNodeId> getChildFragmentIds() {
            return List.of();
        }
    }
}
